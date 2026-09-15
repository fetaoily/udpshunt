# udpshunt 设计文档

- 日期：2026-09-15
- 状态：待用户评审
- 仓库：`github.com/fetaoily/udpshunt`

## 0. 概述

udpshunt 是一个用 Go 编写的高性能 UDP 四层负载均衡器 / 端口转发器：

- 监听本地 UDP 端口，将流量按会话分发到一组后端
- 完整代理模式（后端看到的是 udpshunt 的源 IP，回包原路返回客户端）
- 目标平台：Linux 生产部署；Windows/macOS 走可移植路径（开发调试用）
- 核心质量目标：高性能、易用、易维护、稳定运行（7×24）

### 已确认的关键决策

| 决策点 | 结论 |
|---|---|
| 产品形态 | 通用 UDP 负载均衡器（适配多种流量形态，配置驱动） |
| 代理模式 | 完整代理（会话表 + 每会话上游 connected socket） |
| 语言 | Go |
| 平台策略 | Linux 优先优化（构建标签），其他平台可移植回退 |
| 健康检查 | 被动 + 主动结合 |
| 监控 | Prometheus + TUI 子命令 + 内嵌 Web 单页 |

### 标注的假设（用户未逐条确认，可推翻）

- **假设 A**：Web 监控采用"轻量内嵌"方案（见 §9.3），完整管理台明确排除。
- **假设 B**：全部后端不健康时 fail-open（继续轮询发送 + 指标告警），不做可配开关。
- **假设 C**：admin 端口默认绑定 `127.0.0.1`，不内置登录认证。
- **假设 D**：会话终结语义统一为"后端离开（健康摘除或配置移除）→ 其上会话立即终止"。

## 1. 架构分层

单二进制 Go 程序。依赖方向自上而下，禁止反向依赖：

```
cmd/udpshunt/        main：CLI 参数、配置加载、信号处理、进程生命周期
internal/config      YAML 解析、校验、默认值；配置为不可变快照，热重载=原子替换
internal/listener    前端 UDP 监听器，每监听器一个收包循环 goroutine
internal/session     分片会话表（256 片）、会话生命周期、空闲老化
internal/balancer    后端池 + 三种均衡算法
internal/health      被动健康（错误计数）+ 主动探测（raw/dns 模式）+ 状态机
internal/pktio       平台抽象包 I/O：
                     - Linux 构建标签 → x/net ReadBatch/WriteBatch（recvmmsg/sendmmsg）
                     - 其余平台    → 逐包 ReadFromUDP/WriteToUDP
internal/state       状态聚合层：热路径原子计数器 + 快照聚合 + 内存环形缓冲（5min@1s）
internal/metrics     Prometheus 指标注册与暴露
internal/admin       单一 HTTP 端口（默认 127.0.0.1:9155），服务全部管理端点：
                     GET /status（JSON 快照）、GET /metrics（Prometheus）、
                     POST /reload、GET /ui/*（内嵌 Web SPA）
internal/tui         Bubble Tea TUI（udpshunt tui 子命令）
web/                 Vue3 + Vite + ECharts 前端源码；构建产物 embed 进二进制
```

外部依赖（保持最小）：`golang.org/x/net`（批量 I/O）、`prometheus/client_golang`、
`gopkg.in/yaml.v3`、`charmbracelet/bubbletea` + `lipgloss`（TUI）、
`go.uber.org/goleak`（测试）。

## 2. 核心数据流

### 2.1 上行（客户端 → 后端）

1. 监听 socket 批量收包（Linux 一次 `recvmmsg` 收 32~64 包；缓冲来自 `sync.Pool`）
2. 逐包查会话表（键 = 监听器 + 客户端 ip:port）：
   - 未命中 → 创建会话：均衡算法选一个健康后端 → `DialUDP` connect 到后端
     （connected socket：内核过滤杂包 + ICMP 错误上浮为读写错误）→ 写入会话表
   - 命中 → 刷新空闲时间戳
3. 聚合本批包，经各会话的上游 socket 批量发出（Linux `sendmmsg`）

### 2.2 下行（后端 → 客户端）

1. 每会话一个 goroutine 阻塞读自己的上游 socket（Go netpoller/epoll 多路复用；
   十万级 goroutine 为 Go 常态）
2. 收到包 → 通过监听 socket `WriteToUDP(clientAddr)` 发回（客户端看到的源地址
   即其发包目的地址）
3. 刷新会话时间戳

## 3. 会话生命周期

- **创建**：首次收到该客户端的包 → 选后端 → 分配上游 socket → 入表
- **老化**：会话表按客户端地址哈希分 256 片，每片独立 mutex；后台定时器错峰扫描
  （每秒扫 1/8 片），超过空闲超时的会话关闭上游 socket 并移除
- **上限**：`sessions.max` 可配置，达到后新会话被拒并计数（指标可见）；默认 0（不限）
- **终结语义（统一规则，假设 D）**：后端离开池（健康摘除或热重载移除）→ 其上所有
  会话立即终止；客户端下次发包建立新会话、落到新后端。热重载与健康摘除共享同一
  条代码路径
- **端口耗尽**：同一本地 IP 到同一后端的并发会话上限约 6.4 万（UDP 源端口 16 位），
  多后端按 (本地IP, 后端地址) 元组扩展；文档标注。不做多源 IP（YAGNI，需要时再议）

## 4. 负载均衡器

每监听器独立后端池。算法 per-listener 可配：

| 算法 | 实现 | 适用 |
|---|---|---|
| `round_robin`（默认） | 原子计数器轮转 | 通用 |
| `least_sessions` | 各后端活跃会话数（分片表聚合的原子计数） | 后端性能不均 |
| `source_hash` | 客户端 IP 一致性哈希（后端增减最小扰动） | 需要粘性 |

选择时过滤不健康后端；全部不健康时 **fail-open**（假设 B）：继续按轮询发送，
健康指标同步飙升用于告警——避免一次误判打黑整个入口。

## 5. 健康检查

- **被动**（始终开启）：上游 socket 读写错误（含 ICMP port unreachable 上浮的
  `ECONNREFUSED`）计数，超阈值 → 标记 down
- **主动**（per-listener 可配）：
  - `raw` 模式：发送配置的十六进制/文本载荷，超时窗口内收到任意回包即健康
  - `dns` 模式：发送一条 A 记录查询，收到响应即健康（内置便捷模式）
  - 参数：`interval` / `timeout` / `rise` / `fall`
- **状态机**：`UP --fall 连败--> DOWN --冷却期--> 半开 --rise 连成--> UP`；
  被动触发与主动探测共享此状态机

## 6. 配置模型

```yaml
listeners:
  - name: dns-in
    bind: 0.0.0.0:53
    backends: [10.0.0.1:53, 10.0.0.2:53]
    balance: round_robin          # round_robin | least_sessions | source_hash
    session_timeout: 30s          # 覆盖全局默认
    health_check:
      mode: dns                   # none | raw | dns（raw 模式需另配 payload 字段）
      interval: 5s
      timeout: 1s
      rise: 2
      fall: 3

sessions:
  timeout: 60s
  max: 0                          # 0 = 不限

admin:
  bind: 127.0.0.1:9155            # 单端口服务 /metrics /status /reload /ui
                                   #（假设 C：默认仅本机可达）

logging:
  level: info
  format: json                    # json | text
```

- CLI 覆盖配置：`udpshunt -c /etc/udpshunt.yaml --admin.bind=:9155`
- **热重载**：`SIGHUP` 或 `POST /reload` → 重新解析校验（失败则保留旧配置并报错）
  → 原子替换快照 → 新增监听器启动；移除的监听器按 §3 语义终结会话后关闭；
  后端池增量更新（仅对发生变化的监听器生效）

## 7. 性能工程

- 收包批量化（`recvmmsg`）+ 发包聚合（`sendmmsg`）；逐包路径仅作非 Linux 回退
- 缓冲全部走 `sync.Pool`，热路径零分配目标
- `SO_RCVBUF` 可配置（默认调大）
- 指标全原子计数器，热路径无锁收集；分片锁仅在会话建立/销毁时短暂持有
- **性能目标（以随仓库交付的 benchmark 实测为准，非承诺值）**：
  单监听器 ≥ 20 万 pps 转发（4 核虚机）、吞吐 ≥ 3 Gbps、并发会话 ≥ 50 万
- P1 可选优化：`SO_REUSEPORT` 多 socket 收包分片（先测单 socket，有瓶颈再上）
- 内存治理：文档指导设置 `GOMEMLIMIT`

## 8. 可观测性（数据侧）

- **Prometheus 指标**：per-listener 收发包/字节、会话 active/created/expired/rejected、
  per-backend 包数与错误、健康状态变迁计数、热重载次数、运行时长
- **slog 结构化日志**：会话创建/销毁（debug）、后端状态切换与重载（info）、
  错误（warn/error）
- **GET /status JSON 快照**：总览 + 每监听器 + 每后端视图模型 + 最近事件 +
  近 5 分钟速率历史（来自内存环形缓冲）——TUI / Web / curl 共用

## 9. 监控前端

### 9.1 共同底座

守护进程保持无头。TUI 与 Web 均为 admin HTTP API 的客户端；`internal/state`
是唯一状态源，同时服务 Prometheus、/status、环形缓冲。

### 9.2 TUI（`udpshunt tui`）

- Bubble Tea + Lip Gloss；默认连本机 admin 端口，`--addr` 连远程
- 总览：总 pps / 带宽、活跃会话数、后端健康比例
- 每监听器/每后端：速率迷你图、会话数、健康状态（UP/DOWN/半开 颜色区分）
- 底部：最近事件流（后端状态切换、热重载记录）
- 刷新：1s 轮询 `/status`

### 9.3 内嵌 Web 单页（假设 A）

- Vue3 + Vite + ECharts；构建产物经 Go `embed.FS` 塞进二进制，admin 端口服务 `/ui`
- 内容与 TUI 对齐：总览面板 + 监听器/后端列表 + 实时速率迷你图 + 事件流；
  2s 轮询 `/status`
- 不做：登录认证、历史数据库、告警配置（历史与告警 = Prometheus + Grafana，
  M5 交付现成 dashboard 模板 JSON）
- 前端源码在 `web/`，构建产物入库策略：仓库含预构建 dist（保证 `go build` 即得
  完整二进制），Makefile 提供重新构建前端的目标

## 10. 稳定性设计

- **fd 泄漏防线**：socket 关闭收敛到会话生命周期的单一关闭点；soak 测试
  （长时间高 churn 压测 + fd/内存曲线断言）
- **goroutine 泄漏**：CI 用 goleak 检测
- **优雅退出**：SIGTERM/SIGINT → 停收新包 → 下行路径 2s flush 窗口 → 全部关闭退出
- **崩溃面最小化**：数据面完全不解析包内容（纯转发），无解析即无畸形包崩溃面；
  包缓冲按 UDP 上限 64KB
- 崩溃自动重启交给 systemd `Restart=always` / Docker（进程内不自做监护）
- **CI（GitHub Actions）**：linux amd64/arm64 构建+测试、windows 可移植路径测试、
  golangci-lint、soak/benchmark 定期跑；发布用 goreleaser

## 11. 里程碑

| 阶段 | 内容 | 验收 |
|---|---|---|
| M1 MVP | 完整代理核心 + 分片会话表 + 轮询 + 被动健康 + YAML + slog | 集成测试：真实 UDP 收发、超时老化、后端故障切换 |
| M2 生产化 | 三种算法 + 主动健康 + SIGHUP 热重载 + Prometheus + 状态快照 API | 热重载生效、指标完整 |
| M3 性能 | Linux 批量 I/O + benchmark 套件 +（视结果）SO_REUSEPORT | 达到 §7 目标并出报告 |
| M4 监控前端 | `udpshunt tui` + 内嵌 Web 单页 | TUI/Web 实时展示状态与速率 |
| M5 交付 | systemd / Docker 多架构 / goreleaser / 文档 + Grafana 模板 | 一条命令安装运行 |

实施按里程碑顺序推进，每个里程碑一个实施计划周期（M1 优先）。

## 12. 明确不做（YAGNI 清单）

- 完整 Web 管理台（登录、历史库、告警配置）——用 Grafana 替代
- 透明/DNAT 转发模式、eBPF/XDP 数据面——除非未来实测有不可达瓶颈
- 多源 IP 上游绑定、进程内崩溃自监护、内置用户认证
- Web 端 WebSocket/SSE 实时推送（轮询够用）
