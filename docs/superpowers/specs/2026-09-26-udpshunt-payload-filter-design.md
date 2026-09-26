# udpshunt 入站负载特征门（payload filter）设计（v0.1.12 候选）

日期：2026-09-26
状态：设计已与作者确认方向（内容门 / any-of allowlist / 多规则），待审
前置：`2026-09-23-udpshunt-blacklist-design.md`（COW 原子表惯例、被拦包静默丢弃
与记账语义、可观测风格全部沿用）

## 1. 背景与目标

生产环境某面向公网的 listener（下文统称"生产 listener"），其全部合法流量为
单一私有二进制 UDP 协议：帧以固定魔数字节开头，帧格式细节属内部协议规范，
不在本仓库记录。黑名单按源封禁是被动响应——每轮攻击都要先打上门、再逐个
拉黑，名单条目持续追加。内容门按负载特征只放行已知合法协议，未知来源的
垃圾报文（扫描、随机 payload）在进入会话/转发路径之前即被丢弃，不占用
会话表、fd 与后端配额。

- G1 默认关闭：listener 无 `payload_filter` 节（或 `rules` 为空）时，行为与
  现状完全一致（热路径仅多一次 nil 判断）。
- G2 allowlist 语义：数据包按规则顺序匹配，**任一规则命中即放行**，全部不中
  判为非法——"拦非法"的本质是放行已知合法签名。
- G3 静默丢弃：非法包在黑名单检查之后、常规记账之前丢弃，不建会话、不转发、
  不进 normal 客户端统计（与被拉黑包同款语义，spec §9 沿用）。
- G4 热加载：规则随 `/reload` 生效（COW 整体换表，与黑名单/超时同款）。
- G5 可观测：`illegal_packets` 计数（metrics + /status + TUI 头部）；非法包
  可选用 request_log 记录（默认关闭）。
- G6 通用性：门是"hex 前缀 + 偏移 + 最小长度"，不绑定任何具体协议，未来
  任何带稳定签名的协议可复用。

## 2. 非目标

- 全量协议校验：不校验魔数之外的任何帧字段。垃圾流量的特征是"没有魔数"，
  魔数 + 最小长度已足够；深度校验增加每包成本且协议固件变更时有误杀风险。
- 每 IP 非法流量统计表（blocked 表那样的 per-IP 视图）：v1 只做总量计数；
  需要按 IP 溯源时开 `log_illegal` 配合 request_log。
- 运行时 API 增删规则：规则是低频变更的配置项，config + `/reload` 足够
  （与黑名单不同——名单是运维高频操作，故有 API）。
- 响应方向（后端 → 客户端）不过门：门只管设备入口。
- 内核态方案：iptables u32 / socket BPF / XDP 均不在本项目内（XDP 在部分
  生产主机的老旧内核（如 3.10）上不可用；该量级 pps 下用户态比较成本不可
  测）。

## 3. 配置（internal/config）

listener 级可选节：

```yaml
listeners:
  - name: edge-ingest          # 示例名
    bind: ":3333"
    backends: [...]
    payload_filter:          # 省略整个节 = 功能关闭
      rules:
        - magic_hex: "00112233"  # 示例占位；真实值取自内部协议规范，不入库
          offset: 0              # 魔数在 payload 中的起始偏移，默认 0
          min_length: 8          # 帧最短长度，缺省 = offset + len(magic)
      log_illegal: false         # 默认 false：非法包是否写 request_log
```

多协议 listener 配多条规则，任一命中放行。上例数值均为占位：生产的真实
魔数与最小帧长（按帧定长头部长度推导）来自内部协议规范，本仓库只固化
机制、不记录取值。

校验（沿用 `listeners[%d] (%s)` 报错风格，config.Validate 内逐条）：

- `magic_hex`：必填，`hex.DecodeString` 可解析且非空（与 health_check.payload
  同款校验，config.go:259 先例）。
- `offset`：>= 0，且 `offset + len(magic) <= 65536`（MaxPacketSize 边界）。
- `min_length`：>= 0；0 = 缺省，由 `Compile` 补 `offset + len(magic)`（补齐
  单点在编译器，applyDefaults 不碰此节）。
- 空报文（len 0）天然非法（< 任何 min_length）。

## 4. 匹配器（新包 internal/payloadfilter）

```go
// RuleConfig carries the yaml-tagged raw rule (magic as hex string).
type RuleConfig struct {
    MagicHex  string `yaml:"magic_hex"`
    Offset    int    `yaml:"offset"`
    MinLength int    `yaml:"min_length"` // 0 -> offset+len(Magic) at compile time
}

// Rules is an immutable compiled snapshot.
type Rules struct { /* []compiledRule{magic []byte; offset, minLen int} */ }

func Compile(cfg []RuleConfig) (*Rules, error) // 校验 + hex 解码 + 缺省补齐
func (r *Rules) Allowed(pkt []byte) bool       // 任一命中 true；nil 接收方 true
func (r *Rules) Len() int
```

- `Allowed` 热路径形态：`len(pkt) >= minLen && bytes.Equal(pkt[off:off+n], magic)`
  逐条短路与；编译期已解码 magic，运行期零分配、零 hex 开销。
- 依赖方向与 blocklist 相同：config 导入 payloadfilter（Validate 里 Compile
  试编译整组规则），listener 导入 payloadfilter 使用编译产物；无环。
- 监听器以 `atomic.Pointer[payloadfilter.Rules]` 持有；nil = 门关闭。

## 5. 数据路径（internal/listener）

`Run` 批收循环内，黑名单块之后、`met.PacketsIn` 记账之前：

```text
if pf := l.rules.Load(); pf != nil && !pf.Allowed(bufs[i][:sizes[i]]) {
    l.met.Illegal(1)                       // Prometheus 计数 + OnIllegal 回调
    if l.logIllegal.Load() && l.reqLog != nil {  // 原子布尔，随 reload 更新
        l.reqLog.Record(... Outcome: requestlog.OutcomeIllegal)
    }
    continue
}
```

- 丢弃语义与被拉黑包逐项一致：静默（无 debug 以上日志，防日志自炸）、
  不进 `PacketsIn/BytesIn`、不进 normal clientstats 表、不建会话。
- 顺序取"黑名单在前、内容门在后"：已拉黑源继续只算 `blocked_packets`，
  计数语义不漂移；两道检查都是原子载入 + 短比较，先后无成本差异。
- `requestlog` 新增 `OutcomeIllegal = "illegal"` 常量。

## 6. reload 语义（cmd/udpshunt + internal/listener）

- reload 循环（现 `UpdateTimeout`/`UpdateLogBlocked` 所在处，app.go:662 一带）
  对每个 listener：新配置有规则 → `Compile` 成功后 `UpdateRules(*Rules)` 原子
  换表；无规则 → 换入 nil 关门。编译失败则整个 reload 照既有约定失败回滚。
- `UpdateLogIllegal(bool)` 同 `UpdateLogBlocked` 款式（原子布尔）。
- 换表成功记事件 `payload_filter_reloaded rules=N`（N=0 表示关门）。
- **不关存量会话**（与黑名单拉黑即断不同）：规则收紧后，既有会话只是不再被
  转发新包，自然空闲超时回收——此前没有任何"需要撤销的放权"。

## 7. 可观测性

- metrics：`udpshunt_illegal_packets_total`（counter，listener 标签），以及
  `OnIllegal func(int64)` 回调——与 `OnBlacklisted`（metrics.go:37）同款。
- app 持 `illegalPackets atomic.Int64`（app.go:63 的 `blBlocked` 同位），喂给
  `/status` 顶层新字段 `illegal_packets: N`（与 `blocked_packets` 并列）。
- TUI 头部：`illegal N > 0` 时在 `blocked N` 旁追加 `illegal N`（tui.go:198-204
  同款式）。
- `/clients` 不加 scope（v1 无 per-IP 非法表，见非目标）。

## 8. 并发与不变量

- `Rules` 编译后不可变；写侧（启动 / reload）整体 `UpdateRules` 原子换表，
  绝不原地修改——与 blocklist.Container、balancer 池同一 COW 惯例。
- 热路径每包新增成本：一次原子载入 + N 次定长 `bytes.Equal`（N = 规则数，
  实际 1~3），无锁、无分配；300pps 下不可测，10 万 pps 亦非瓶颈。
- 事件与编译都在写侧路径完成，数据路径零写操作。

## 9. 测试计划

- **payloadfilter**：Compile 合法/非法表驱动（坏 hex、负 offset、越界 offset、
  min_length 缺省补齐）；Allowed 命中/不命中/短包/空包/多规则任一命中/
  offset > 0；nil 接收方恒放行。
- **listener**：非法包不转发、不建会话、不进 PacketsIn 与 normal 表，illegal
  计数 +1 且 OnIllegal 回调触发；`log_illegal` 开/关两态的 request_log 行为；
  合法包逐项不受影响；黑名单 IP 优先于内容门（只算 blocked）。
- **config**：payload_filter 校验表驱动（合法/坏 hex/负 offset/越界）；缺省
  min_length 补齐；无节 = 零值直通。
- **app/admin**：reload 换规则生效（收紧 → 原合法报文变非法；置空 → 关门）、
  编译失败回滚；`/status.illegal_packets`；`payload_filter_reloaded` 事件。
- **integration**（echo 后端）：非法报文后端零收到、客户端无回包；合法帧
  正常 round-trip。
- **tui**：illegal N > 0 时头部出现 `illegal` 字段。

## 10. 兼容性

- 配置：`payload_filter` 节可选，缺省行为与现状逐字节一致（热路径多一次
  原子载入的 nil 判断）。
- `/status`：纯新增字段。request_log：新增 outcome 字符串值，消费方按字符串
  处理，向后兼容。metrics：新增序列，无删改。
- README：blacklist 章节后新增 Payload filter 小节（配置示例、any-of 语义、
  `log_illegal` 的量级警示、上线前置的抓包验证）；packaging/config/udpshunt.yaml
  加注释示例（默认注释状态）。
- 单元/服务文件无改动（不涉及新持久化目录或权限）。

## 11. 上线前置（运维，非代码）

生产主机启用前先抓包验证两个前提（tcpdump 在网卡层，黑名单丢弃发生在
socket 层，被拉黑源的流量仍可观测）：

```bash
tcpdump -i eth0 -s0 -X -c 30 'udp port <生产端口>' > /tmp/cap.txt
```

1. 合法帧确以配置的魔数字节开头（实证线上字节序与配置一致）；
2. 攻击/扫描流量确实不带魔数（决定本门对当前攻击的实际拦截率——若攻击
   伪造合法帧，则归黑名单职责，两层正交：黑名单按源，内容门按负载）。
魔数是配置项：即使线上字节序与预期不符，改 `magic_hex` 即可，无需改码。
