# udpshunt

A high-performance UDP layer-4 load balancer and port forwarder written in Go.

Status: M4 (monitoring frontends: `udpshunt tui`, the embedded web UI at `/ui`, per-listener rate history). Design: `docs/superpowers/specs/2026-09-15-udpshunt-design.md`

## Build

    go build -o udpshunt ./cmd/udpshunt

## Run

    udpshunt -c /etc/udpshunt.yaml

See `examples/basic.yaml` for the config format: listeners bind a UDP port and
forward traffic to a pool of backends. Sessions idle out after
`session_timeout` (default 60s, capped by `sessions.max` when set). SIGTERM/SIGINT
triggers a graceful drain.

## Balancing modes

`balance` selects how new sessions pick a backend:

- `round_robin` — even rotation across healthy backends (default).
- `least_sessions` — backend with the fewest live sessions; ties keep the earliest backend in stable pool order.
- `source_hash` — stable rendezvous hash of the client IP, so a client keeps its backend as long as it stays healthy; when no backend is healthy it fails open via the same rendezvous hash over all backends.

## Listener socket buffer

`read_buffer` sets the kernel receive buffer (`SO_RCVBUF`, in bytes) on the
listener socket. `0` (the default) requests 4 MiB, comfortably above the OS
baseline; negative values are rejected at config load:

```yaml
listeners:
  - name: dns-in
    bind: 0.0.0.0:53
    backends: [10.0.0.1:53]
    read_buffer: 8388608  # 8 MiB
```

The request is best-effort: if the kernel refuses (for example a low
`net.core.rmem_max`), udpshunt logs a warning and keeps the socket.

## Health checks

Optional `health_check` block per listener enables active probing:

```yaml
health_check:
  mode: raw            # none (default) | raw | dns
  payload: "70696e67"  # raw mode: hex-encoded probe bytes ("ping")
  interval: 5s
  timeout: 1s          # must be shorter than interval
  rise: 2              # consecutive successes to mark a backend up
  fall: 3              # consecutive failures to mark a backend down
```

`raw` sends the payload and counts any reply within `timeout` as success;
`dns` sends a built-in DNS query and counts any response (even NXDOMAIN).
A backend marked down stops receiving new sessions and its live sessions are
closed; with active checks on, recovery requires `rise` consecutive successes,
otherwise a passive cooldown applies. A failed config reload never changes
what is running.

## Admin API

`admin.bind` serves four routes:

- `GET /metrics` — Prometheus text format (private registry, `udpshunt_` prefix).
- `GET /status` — JSON snapshot: uptime, per-listener backends (health, session counts), session totals, recent events, and per-listener rate history (last 5 min at 1s samples).
- `POST /reload` — reload the config file and apply it.
- `GET /ui` — the embedded web dashboard (below).

## Terminal dashboard

`udpshunt tui` renders a live dashboard (uptime, totals, per-listener
sparklines, backend health, recent events) from a running udpshunt's admin
API — the daemon itself must already be running:

    udpshunt tui --addr http://127.0.0.1:9155 --interval 1s

- `--addr` — admin API base URL (default `http://127.0.0.1:9155`).
- `--interval` — poll interval (default `1s`).

Press `q` or Ctrl-C to quit.

## Web UI

The admin API also serves a single-page dashboard at `/ui` on the same port
(uptime and totals, per-listener rate charts, backend health, recent events;
polls `/status` every 2s). The built asset is committed in
`internal/webui/dist` and embedded into the binary via `go:embed` — no extra
files are needed at runtime.

There is no built-in authentication on the admin port (design assumption C):
`/ui`, `/status` and `/metrics` are open to anyone who can reach it. Keep
`admin.bind` on loopback (the default `127.0.0.1:9155`) or put the port
behind an authenticating proxy before exposing it.

To rebuild it after changing `web/src`, run from the repo root (requires
node >= 20 and npm):

    sh web/build.sh

That runs `npm ci && npm run build` in `web/` and copies `dist/` into
`internal/webui/dist`. For iteration with live reload, `npm run dev` in `web/`
starts vite (dev-only proxy to `http://127.0.0.1:9155` for `/status` and
`/metrics`).

## Hot reload

Both `kill -HUP <pid>` (not delivered on Windows) and
`curl -X POST http://127.0.0.1:9155/reload` re-read the `-c` file. Listeners
are diffed against the running set: removed listeners stop, a `bind` change
restarts the socket, everything else (backends, balance mode, session
timeout, health checks) updates live. On any error the previous config keeps
serving.

Metric families: `udpshunt_packets_in_total`, `udpshunt_bytes_in_total`,
`udpshunt_packets_out_total`, `udpshunt_bytes_out_total` (label `listener`);
`udpshunt_backend_packets_in_total`, `udpshunt_backend_packets_out_total`,
`udpshunt_backend_errors_total`, `udpshunt_backend_healthy`,
`udpshunt_backend_state_changes_total` (labels `listener`, `backend`);
`udpshunt_sessions_created_total` (label `listener`),
`udpshunt_sessions_expired_total`, `udpshunt_sessions_rejected_total`,
`udpshunt_sessions_active`; `udpshunt_reloads_total`,
`udpshunt_reload_failures_total`, `udpshunt_uptime_seconds`.

## Memory tuning

When running many listeners or sessions, set `GOMEMLIMIT` to roughly 80% of
the container or host memory budget so the Go GC targets that limit instead of
growing until the kernel OOM-kills the process. `GOMEMLIMIT` governs the Go
heap only: kernel socket buffers and the receive arena are outside it.

Each listener holds ~4 MiB of kernel receive buffers by default (see
`read_buffer` above; Linux doubles `SO_RCVBUF` requests, so 4 MiB requested
is about an 8 MiB kernel ceiling), plus a receive arena and pooled relay
buffers (relay memory scales with in-flight packets, not live sessions). The
64×64 KiB (4 MiB) arena is Linux-only; non-Linux platforms size a
single-packet arena. Size `GOMEMLIMIT` with that baseline in mind.

## Performance

Frontend receives are batched: one `recvmmsg` (up to 64 packets per wakeup)
on Linux, single-packet reads as the portable fallback.

    go test ./internal/listener/ -bench . -benchtime 2s -run '^$' -count=3
    # Linux only (batch path):
    go test ./internal/pktio/ -bench . -benchtime 2s -run '^$'

Measured numbers, the spec-target gap analysis and the SO_REUSEPORT
deferral live in `docs/benchmarks.md`. Pair benchmark runs with the
`GOMEMLIMIT` guidance above.

## Development

    go test ./...
