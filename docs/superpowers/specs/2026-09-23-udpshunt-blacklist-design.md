# udpshunt 客户端 IP 黑名单设计（v0.1.9）

日期：2026-09-23
状态：设计已与作者确认方向（CIDR / 运行时 API / 可选被拦日志），待审
前置：`2026-09-22-udpshunt-health-design.md`（COW 原子表惯例、事件/指标风格沿用）

## 1. 背景与目标

生产部署（bdhg，room-temp 监听器 `0.0.0.0:3333`）对公网开放：任何客户端 IP 都能
创建会话、占用 fd 与后端配额。需要按客户端 IP 拒绝服务：

- G1 拦截：黑名单内 IP 的数据包在转发前**静默丢弃**（UDP 无回应），不建会话。
- G2 拉黑即断：IP 进入名单时，其**存量会话立即关闭**。
- G3 静态配置：顶层 `blacklist` 配置段，精确 IP 与 CIDR 前缀，随 `/reload` 热加载。
- G4 运行时管理：admin API 增删查名单，运维从 clients 视图发现滥用即可拉黑。
- G5 可观测：拦截计数（metrics + /status + TUI 头部）、增删事件；被拦包可选用
   request_log 记录（默认关闭）。

## 2. 非目标

- 自动拉黑 / fail2ban 式速率封禁（client_stats 已具备数据基础，留 v2）。
- 名单持久化：运行时增删不落盘，重启后只剩配置文件条目（要长久的条目写进 YAML）。
- 每 listener 独立名单（v1 全局一份；现有部署三个 listener 指向同一批后端，全局
  语义即"封 IP"本意）。
- 管理端口鉴权（既有立场不变：9155 绑 127.0.0.1，README 已警示）。
- 前缀 trie：条目量级按百以内设计，线性扫前缀即可（YAGNI）。

## 3. 配置（internal/config）

```yaml
blacklist:
  entries:            # 省略或空 = 名单为空，功能闲置
    - 203.0.113.7     # 精确 IP（内部规范化为 /32 或 /128）
    - 198.51.100.0/24 # CIDR
  log_blocked: false  # 默认 false：被拦包是否写 request_log（见 §8）
```

校验：每个条目用 `net/netip` 的 `ParsePrefix`（裸 IP 补 `/<w>`）或 `ParseAddr` 解析，
非法即配置错误；`Is4In6` 统一 `Unmap()` 规范化；重复条目静默去重；IPv4/IPv6 均可，
但必须与监听地址族匹配才可能命中（文档注明，不做转换）。

`Blacklist` 配置结构体：`Entries []string`、`LogBlocked bool`（零值 false）。
配置段整体缺省 = 空名单 + log_blocked=false，行为与无此功能一致。

## 4. 匹配器（新包 internal/blocklist）

```go
type List struct { /* 不可变快照 */ }
func New(entries []string) (*List, error)   // 解析、规范化、去重、构建
func (l *List) Blocked(a netip.Addr) bool   // O(1) 精确 map 命中 + 短前缀线性扫
func (l *List) Entries() []string           // 规范化形式（GET /blacklist 返回）
func (l *List) Len() int

type Container struct { /* atomic.Pointer[List] */ }
func (c *Container) Swap(l *List)            // COW 整体替换（reload / API 增删后重建）
func (c *Container) Load() *List
func (c *Container) Blocked(a netip.Addr) bool // nil-safe：空容器返回 false
```

内部结构：`exact map[netip.Addr]struct{}`（主机前缀）+ `prefixes []netip.Prefix`
（短于主机长度的前缀）。`Blocked` 先查 map，再线性扫 prefixes。列表不可变，读侧
零锁 —— 与 balancer 池、onDown 策略表同一 COW 惯例。

## 5. 数据路径拦截（internal/listener）

- `listener.New` 增参 `bl *blocklist.Container`（nil = 无此功能，跳过检查）。
- `Run` 的批收循环内、`handle` 之前逐包检查：
  命中 → `met.Blacklisted(1)`（新计数器，带 listener 标签，与其他指标同风格）→
  按 `log_blocked` 决定是否写 request_log（§8）→ `continue`（不进 client_stats、
  不建会话、不进 forwarded 日志）。
- 客户端 IP 提取：`*net.UDPAddr` → `netip.Addr`（`Unmap()` 规范化后查表）。
- `log_blocked` 开关随配置可变：Listener 以原子布尔持有（更新路径与
  `UpdateTimeout` 同款式），reload 生效。
- 被拦包**不产生** debug 以上日志（攻击进行时每秒可达数百包，日志自炸；只有计数
  与事件）。

## 6. 存量会话清理（internal/session）

```go
// CloseClients closes every session whose client address satisfies pred and
// returns how many were closed.
func (m *Manager) CloseClients(pred func(client *net.UDPAddr) bool) int
```

内部复用现成的 `closeWhere` 分片遍历。调用时机：
- reload 后：对**新进名单**的条目（旧有效名单 → 新名单的差集中每个前缀/地址）
  关闭匹配会话，事件 `blacklist_enforced closed=N`；
- API 拉黑单个条目：同样立即关闭并计数。

## 7. 运行时 API 与 reload 语义（internal/admin + cmd/udpshunt）

路由（挂在既有 mux）：

- `GET /blacklist` → `{"entries": [...], "log_blocked": bool, "blocked_packets": N}`
- `POST /blacklist`，body `{"entry": "203.0.113.7 或 198.51.100.0/24"}` →
  校验（非法 400），加入有效名单，关闭匹配会话，事件 `blacklist_added`
- `DELETE /blacklist?entry=<URL 编码条目>` → 从有效名单移除，事件
  `blacklist_removed`；条目源自配置文件时同样生效（下次 reload 若配置仍含该条目
  则重新生效，事件可见）

**有效名单 = 配置条目 ∪ 运行时新增 − 运行时删除**（union 模型）。reload 重建
配置来源部分，保留运行时增删 —— 明确的设计取向：**reload 不会静默解封运维手工
拉黑的攻击者**；代价是"配置删了条目但运行时删过它"这类组合的最终状态以运行时
为准（事件流水可解释一切）。运行时增删不持久化，重启后回到纯配置名单。

App 持有：`blocklist *blocklist.Container`（全局一份，各 listener 共享）、
`blAdds/blDels map[string]struct{}`（运行时增删集合，重建有效名单的依据：
有效 = 配置 ∪ blAdds − blDels）、`blBlocked atomic.Int64`（累计拦截数，
/status 与 TUI 用）。

## 8. 被拦日志（默认关）

`blacklist.log_blocked: true` 时，每个被拦包写一条 request_log：

```json
{"time":"...","listener":"room-temp","client":"203.0.113.7:51820","backend":"","bytes":74,"outcome":"blacklisted"}
```

`requestlog` 增加 `OutcomeBlacklisted` 常量。**默认必须为 false**：攻击流量下该
开关会把日志量放大到与攻击 pps 同量级（每包一条），README 用显著措辞警示；
正常用途是低频滥用源的审计溯源。

## 9. 可观测性

- metrics：`udpshunt_blacklisted_packets_total`（counter，listener 标签，与其他
  per-listener 指标同风格）。
- `/status`：顶层新增 `blacklist: {entries: N, blocked_packets: M}`。
- 事件：`blacklist_added` / `blacklist_removed`（含条目与来源）/ 
  `blacklist_enforced`（关存量会话数）。
- TUI：头部在 `blocked_packets > 0` 时追加 `blocked N`；clients 视图不加标记
  （被拦 IP 不再产生统计，视图保持合法流量语义）。

## 10. 并发与不变量

- 名单读侧零锁：`Container` 为原子指针，`Blocked` 单次原子载入后查不可变结构。
- 写侧（reload / API）重建新 List 后整体 `Swap`，绝不原地修改。
- listener 批收循环内每包一次原子载入 + map 查询，无锁、无分配。
- 事件/会话清理在写侧路径完成（不在数据路径做任何写操作）。

## 11. 测试计划

- **blocklist**：解析/规范化（裸 IP↔前缀、Is4In6、去重、非法拒绝）；Blocked
  精确/前缀/跨地址族不命中；Entries 规范化往返；空名单。
- **listener**：被拦 IP 的包不转发、不建会话、计数 +1；`log_blocked` 开关两种
  情况的 request_log 行为；合法 IP 不受影响。
- **session**：CloseClients 按谓词关闭、计数正确、不误伤。
- **admin/app**：GET/POST/DELETE 全流程（含 400）；拉黑立即关会话；reload 的
  union 语义（配置新增生效、运行时增删跨 reload 保留、配置删除但运行时也删过 →
  维持删除）；/status 字段。
- **config**：条目校验表驱动（合法/非法/IP 带前缀长度/重复）。

## 12. 兼容性

- 配置：`blacklist` 段可选，缺省行为与现状逐字节一致（热路径多一次 nil 检查）。
- `/status`：纯新增字段。
- request_log：新增 outcome 值，消费方按字符串处理，向后兼容。
- README：新章节（配置示例、API 用法、log_blocked 的量级警示、reload 的 union
  语义）；packaging/config/udpshunt.yaml 加注释示例。
