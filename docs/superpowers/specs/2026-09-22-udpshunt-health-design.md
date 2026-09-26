# udpshunt 可信健康检查（v0.1.6）设计

日期：2026-09-22
状态：已与作者确认设计，待实施
前置：`2026-09-15-udpshunt-design.md`（§3 会话、§5 健康检查、§7 并发模型）

## 1. 背景与诊断修正

某生产 listener 的真实事故（千余会话规模）：三个后端在 7 秒内连环
标记 down，全部会话被强制关闭。最初归因"探测超时误判"**不成立**：

- 事故时三个后端刚完成 `rise: 2` 恢复（连续三个 backend_up 事件），说明探测正在成功。
- `fall: 3 × interval: 5s` 的主动判死最快需 10s+，而首个 down 距最后一个 up 仅 7s——
  数学上不可能由探测路径触发。
- 实际触发者：**数据路径的被动 `ReportError`**。会话 socket 是 connected 的
  （`listener.go:226`），后端 ICMP 错误经 `ReportError`（relay 读错 / 上游写错 / dial 失败）
  裸计数，1 秒内累计 3 个即判死并 `CloseBackend` 强杀全部会话。后端近瘫时错误风暴
  → 连环误杀 → 流量压垮幸存者。

暴露的三个缺陷：

1. **被动错误裸判死**：无时间维度、无确认环节，突发 ICMP 即死刑。
2. **判死无归因**：backend_down 事件不含来源，只能靠时间差反推。
3. **down ⇒ 强杀会话**：误判代价被放大（数千会话瞬间蒸发）。

另有一处归因错误：`listener.go:283-292` 下行转发给**客户端**失败（客户端侧 NAT
失联，常态）也被计入后端错误 —— 客户端侧问题不该由后端背锅。

## 2. 目标

判定语义改为：**活 = 任意一种活性证据（探测成功 ∪ 真实回包）；死 = 错误满额后
再经独立确认（探测失败或时间窗口），且期间无任何活性证据。**

- G1 两阶段判死：错误计满 `fall` 只进**嫌疑（suspect）**态；嫌疑态照常接流；
  由探测失败（主动模式）或时间窗口（被动模式）确认判死；任何 `ReportSuccess`
  撤销嫌疑。
- G2 判死归因：错误带来源（probe / upstream_write / dial / relay_read），
  事件、日志、`/status` 均可回答"谁杀的、为什么"。
- G3 `on_down: close | drain`：down 时可选不强杀存量会话（每 listener 可配，默认 close）。
- G4 修复客户端写失败误记后端错误。

## 3. 非目标

- 自适应探测频率 / 按流量跳过探测（探测本身 4 字节，成本可忽略）。
- TUI / WebUI 的 SUSPECT 徽标展示（`/status` 字段先出，UI 后续跟进）。
- 新增健康检查配置键：`rise/fall/interval/timeout` 语义全部不变，零新键。
- `backend_evicted`（重载移除后端）语义变化 —— 始终立即关闭会话。

## 4. 状态机（internal/balancer）

### 4.1 状态与存储

每后端在现有原子字段上增加：

```go
type backend struct {
    // 既有：addr, healthy, errCount, successCount, downSince
    suspectSince   atomic.Int64            // 进入嫌疑态的 unix 纳秒；0 = 非嫌疑
    lastErrSource  atomic.Pointer[string]  // 最近一次 ReportError 的来源
    downConfirmBy  atomic.Pointer[string]  // 判死确认者："probe" | "window"
}
```

嫌疑态表示为 `healthy=true && suspectSince!=0`（**可路由**，Pick/Snapshot 视同健康，
`BackendState.Healthy` 保持 true，既有消费方不受影响）。`suspectSince` 兼作嫌疑标记与
时间戳，避免第二个布尔。

### 4.2 迁移

```
Healthy ──ReportError 使 errCount>=Fall──▶ Suspect
Suspect ──ReportSuccess(任意来源)─────────▶ Healthy   （清零 errCount）
Suspect ──主动模式：ReportError(source=probe)──▶ Down  （确认者=probe）
Suspect ──被动模式：suspect 龄 >= Cooldown 且无成功──▶ Down（确认者=window）
Down    ──既有恢复路径不变（rise×success / 冷却）──▶ Healthy
```

- `Suspect→Down`（主动）：**只有探测来源的错误**能确认判死。数据路径错误在嫌疑期
  继续计数（供观测），不触发状态变化。
- `Suspect→Down`（被动，`ActiveChecks=false`）：在 `usable()` 的 Pick/Snapshot 遍历中
  检查嫌疑龄，`>= Options.Cooldown`（默认 10s，与被动恢复同一时钟基础）即确认 ——
  沿用"收集、池读完成后统一触发回调"的既有纪律，回调绝不在锁内触发。
- 嫌疑期内 `ReportSuccess` 单次即撤销（清 errCount、清 suspectSince）；`rise` 仅约束
  `Down→Healthy`，语义不变。
- 确认判死时置 `downSince` 与 `downConfirmBy`，沿用现有"元数据先于翻转、CAS 赢家
  触发回调恰好一次"的纪律。
- 重载 `SetHealth` 改动 fall 只影响后续 `Healthy→Suspect` 的进入门槛，不影响已在
  嫌疑态的后端（其出路只有成功/确认两条）。

### 4.3 API 变更

```go
func (b *Balancer) ReportError(addr, source string)   // 原无 source
func (b *Balancer) ReportSuccess(addr, source string)  // source: "probe" | "reply"
type State uint8 // Healthy / Suspect / Down
func (b *Balancer) SetOnStateChange(fn func(addr string, from, to State))
type BackendState struct {
    Addr           string
    Healthy        bool      // 嫌疑期保持 true
    Suspect        bool      // 新增
    SuspectSince   int64     // unix 纳秒，非嫌疑为 0
    ErrCount       int64     // 新增（观测）
    LastErrSource  string    // 新增（观测）
    DownConfirmBy  string    // 新增（观测，判死后有效）
}
```

`from,to` 三态化后，回调仍满足：每次迁移恰好触发一次、绝不持锁触发。

## 5. 错误来源归因（internal/listener / internal/health）

| 调用点 | source |
|---|---|
| `health.go` probeLoop 探测失败 | `probe` |
| `listener.go` 会话上游写失败（forward） | `upstream_write` |
| `listener.go` DialUDP 失败（createSession） | `dial` |
| `listener.go` relay 读到后端 ICMP 错误（downstream） | `relay_read` |
| ~~listener.go 客户端方向 WriteToUDP 失败~~ | **删除 ReportError 与 BackendError 指标**（G4；会话仍 Remove+return） |

`ReportSuccess` 来源：探测成功 = `probe`，后端真实回包 = `reply`。用于
`backend_suspect_cleared` 事件的 `by=` 字段。

## 6. on_down 会话策略（cmd/udpshunt/app.go + internal/config）

```yaml
listeners:
  - name: demo
    on_down: drain   # close（默认）| drain
```

- `close`：现行为 —— down 即 `CloseBackend`。
- `drain`：down 不杀存量会话，按 `session_timeout` 自然过期；新会话本就不会
  Pick 到 down 后端。事件/日志带 `policy=drain draining=N`。
- 重载移除后端（`backend_evicted`）**始终 close**，与 on_down 无关。
- 实现约束：回调闭包不能捕获创建时的 `lc.OnDown`（bind 不变的重载走
  `updateListenerLocked`，闭包不重建）。策略表存 `atomic.Pointer` 指向不可变
  `map[listenerName]policy`（copy-on-write，与代码库既有惯例一致）；**回调内绝不取
  `a.mu`** —— `updateListenerLocked` 持 `a.mu` 调 `bal.Snapshot()` 会触发回调，
  取锁即死锁。
- 配置校验：`on_down ∈ {"", "close", "drain"}`，空 = close。
- 文档须注明：drain 会话过期前仍占 `sessions.max` 预算。

## 7. 可观测性

- 事件（`/status` recent events 与 TUI events 区）：
  - `backend_suspect  <listener> <addr> errors=<n> last_error=<source>`
  - `backend_suspect_cleared  <listener> <addr> by=<probe|reply>`
  - `backend_down  <listener> <addr> closed=<n> confirmed_by=<probe|window> errors=<n> last_error=<source> [policy=drain draining=<n>]`
  - `backend_up` 不变。
- `/status` backend 视图透出 `BackendState` 新字段（向后兼容，纯新增）。
- 指标：`SetBackendHealthy` 不变（嫌疑 = healthy=1）；不新增指标。

## 8. 并发不变量（承接 §7 原设计）

- 热路径（ReportError/ReportSuccess/Pick/Snapshot）仍全程无锁：新增字段全为原子。
- 回调触发纪律不变：迁移后触发、CAS 赢家唯一触发、池读完成后统一触发（被动确认
  与被动恢复共用该路径）、绝不在 `a.mu` 内触发。
- 元数据先于状态翻转落盘的观察者保证，扩展到 `suspectSince` / `downConfirmBy`。

## 9. 测试计划

- **balancer**（表驱动 + 短 Cooldown 实时钟，沿既有测试风格）：
  - 迁移全矩阵：错误满额→嫌疑；嫌疑+探测错误→down（主动）；嫌疑+Cooldown 到期→down
    （被动，经 Pick 与 Snapshot 两个入口）；嫌疑+任意成功→恢复；down 后 rise 恢复不变。
  - 嫌疑期数据路径错误不判死（主动模式）；嫌疑期后端仍可被 Pick。
  - 回调恰好一次、from/to 正确（既有 CAS 测试模式扩展）。
  - `BackendState` 新字段取值正确（含非嫌疑时 SuspectSince=0）。
- **health**：真实 UDP socket —— 忽略探测但回真实流量的后端保持健康；探测也失败
  的后端经嫌疑确认判死。
- **listener**：四个 source 归因正确；客户端写失败不再 ReportError / 不再计
  BackendError（G4）。
- **app**：on_down=drain 时 down 不关会话（会话存活至 timeout）、close 时照关；
  事件文本含归因字段；on_down 热加载生效（bind 不变重载后策略切换）；
  `updateListenerLocked` 期间 Snapshot 触发回调无死锁（回归）。
- **config**：on_down 校验（合法/非法值）。

## 10. 兼容性

- 配置：零新增健康检查键；`on_down` 可选、默认 close，旧配置行为不变
  （除 G4 的归因修正与两阶段判死本身）。
- `/status`：纯新增字段，既有消费方（TUI/WebUI/脚本）不受影响。
- 行为变化（有意为之）：判死延迟增加 —— 主动模式最多多一个 `interval+timeout`，
  被动模式最多多一个 `Cooldown`；换取突发免疫。
- README：健康检查章节重写（两阶段语义 + 来源表 + on_down），
  `packaging/config/udpshunt.yaml` 示例同步。
