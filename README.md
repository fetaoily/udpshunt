# udpshunt

udpshunt is a high-performance UDP layer-4 load balancer and port forwarder
written in Go. It binds UDP listeners and relays datagrams to a pool of
backends as a full proxy: clients are terminated at udpshunt, which
re-originates traffic from its own sockets and tracks per-client sessions with
idle timeouts. It ships round-robin, least-sessions and source-hash balancing,
optional active health checks, hot config reload, and Prometheus metrics —
plus an embedded web dashboard served from the binary (`/ui` on the admin
port) and a terminal dashboard (`udpshunt tui`).

Status: M5 complete (M1 core, M2 production, M3 performance, M4 monitoring, M5 delivery). Spec §11 closed; the first v* tag runs the release pipeline.

## Install

The config file lives at `/etc/udpshunt/udpshunt.yaml` for every install
method — the same path is the `-c` flag default, the systemd unit's
`ExecStart`, and the Docker image's `CMD`.

### Debian, Ubuntu (deb) and RHEL-family (rpm) and Alpine (apk)

Releases (v0.1.1 and later) ship native packages built by goreleaser/nfpm.
Download the package for your platform from the release page, then:

    # Debian / Ubuntu
    sudo apt install ./udpshunt_<version>_amd64.deb
    # RHEL / CentOS / Fedora
    sudo dnf install ./udpshunt_<version>_amd64.rpm
    # Alpine
    apk add --allow-untrusted ./udpshunt_<version>_amd64.apk

    sudo systemctl enable --now udpshunt

Each package installs the binary at `/usr/bin/udpshunt`, the systemd unit at
`/usr/lib/systemd/system/udpshunt.service`, and a starter config at
`/etc/udpshunt/udpshunt.yaml`. The config is a conffile (`%config(noreplace)`
on rpm): package upgrades never overwrite a config you have edited. Alpine
ships the systemd unit too, but Alpine itself runs OpenRC — adapt as needed.

### macOS (Homebrew)

Releases also publish a Homebrew formula to the `fetaoily/homebrew-tap` tap:

    brew install fetaoily/tap/udpshunt

### From release archives

Every release publishes `udpshunt_<version>_<os>_<arch>.tar.gz` archives
(`.zip` for windows) plus a `checksums.txt`. Unpack and run the binary with
`-c` pointing at your config file; the flag defaults to
`/etc/udpshunt/udpshunt.yaml`.

### From source

    go build -o udpshunt ./cmd/udpshunt

### Docker

    docker build -t udpshunt .
    docker run --restart=unless-stopped -v $PWD/udpshunt.yaml:/etc/udpshunt/udpshunt.yaml -p 53:53/udp -p 9155:9155/tcp udpshunt

Mounting under the `/etc/udpshunt/` directory — the single file as shown, or
the whole directory with `-v $PWD/conf:/etc/udpshunt:ro` — keeps host-side
config replacement visible to the container, whereas bind-mounting a single
file at a fixed path goes stale when an editor replaces the file (new inode).
The admin port has no built-in authentication (see
[Admin API](#admin-api)), so publish `9155/tcp` only where that is acceptable.

### systemd

    sudo cp udpshunt /usr/local/bin/
    sudo cp packaging/systemd/udpshunt.service /etc/systemd/system/
    sudo systemctl enable --now udpshunt

The unit expects the binary at `/usr/local/bin/udpshunt` and the config at
`/etc/udpshunt/udpshunt.yaml` (its `ExecStart` is
`/usr/local/bin/udpshunt -c /etc/udpshunt/udpshunt.yaml`). It runs as `nobody` with
`Restart=always` and `CAP_NET_BIND_SERVICE`, so listeners on low ports (e.g.
`:53`) work without root.

## Quick start

    go build -o udpshunt ./cmd/udpshunt
    ./udpshunt -c examples/basic.yaml

Then open <http://127.0.0.1:9155/ui/> — the embedded dashboard, no extra
files needed. The example config binds `127.0.0.1:19000` and round-robins to
`127.0.0.1:19001` and `127.0.0.1:19002`.

The example also runs an active health check (`raw` mode: send `"ping"`,
expect any reply within 1s), so the backends only show UP once something
answers. Ship the bundled echo backend for that — one instance per backend:

    go run ./examples/echo -addr 127.0.0.1:19001
    go run ./examples/echo -addr 127.0.0.1:19002

If your real backends do not reply to unknown payloads, set
`health_check.mode: none` in the config (passive health only) or change
`health_check.payload` to a valid request for your protocol.

### Send a test request

Bash's built-in `/dev/udp` needs nothing installed — each redirection uses a
fresh source port, like a new client:

    echo "hello-$(date +%s)" > /dev/udp/127.0.0.1/19000

    for i in $(seq 1 20); do echo "req-$i" > /dev/udp/127.0.0.1/19000; sleep 0.2; done

Watch the requests land in the per-request log (the dashboard and
`/status` update live too):

    tail -f /var/log/udpshunt/udpshunt-requests-$(date +%F).log

Lines appear even when no backend answers — those carry
`"outcome":"upstream_error"`. To see `"outcome":"forwarded"`, give the
listener a backend that replies: on a source checkout, run the bundled echo
server (`go run ./examples/echo -addr 127.0.0.1:19001`); on a package
install, a python3 echo works with nothing extra installed:

    python3 - <<'EOF'
    import socket
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(("127.0.0.1", 19001))
    print("echo backend on 127.0.0.1:19001")
    while True:
        d, a = s.recvfrom(2048)
        s.sendto(b"echo:" + d, a)
    EOF

## Configuration

`udpshunt -c /etc/udpshunt/udpshunt.yaml` loads a YAML config (see
`examples/basic.yaml`): listeners bind a UDP port and forward traffic to a
pool of backends. Sessions idle out after `session_timeout` (default 60s,
inherited from `sessions.timeout`); the live-session count is capped by
`sessions.max` when set (`0`, the default, means unlimited). SIGTERM/SIGINT
triggers a graceful drain. Top-level keys: `listeners`, `sessions` (`timeout`,
`max`), `logging` (`level`: debug|info|warn|error, default info; `format`:
json|text, default json), `admin` (`bind`, default `127.0.0.1:9155`),
`request_log` and `client_stats` (below).

### Request log

`request_log` writes one JSON line per client packet to a file per day.
It is **enabled by default**; set `enabled: false` to turn it off:

```yaml
request_log:
  enabled: true
  dir: /var/log/udpshunt   # default: /var/log/udpshunt on Unix,
                           # %ProgramData%\udpshunt\logs on Windows
  retention_days: 30       # files older than this are deleted
```

Each line records the packet with its outcome:

```json
{"time":"2026-09-17T14:30:05.123+08:00","listener":"dns-in","client":"203.0.113.7:51820","backend":"10.0.0.1:53","bytes":74,"outcome":"forwarded"}
```

`outcome` is one of `forwarded`, `no_backend` (empty backend pool),
`rejected` (session cap reached) or `upstream_error` (dial or write to the
backend failed). At day rollover the previous day's file is gzip-compressed
in the background (`*.log.gz`); files older than `retention_days` are
deleted at startup and once per day. The log never slows the proxy: entries
go through a bounded in-memory queue, and when the queue fills (disk slower
than traffic) entries are dropped and counted — the drop total is exposed
as `request_log.dropped` in `/status`. If the directory cannot be created
or written, udpshunt logs one warning and keeps running without the log.

Notes per install method: the deb/rpm packages ship the systemd unit with
`LogsDirectory=udpshunt`, so `/var/log/udpshunt` is created and writable at
service start (needs systemd ≥ 240; older systemd silently skips it and
udpshunt falls back to disabled-with-warning). In Docker, mount a volume and
point `request_log.dir` at it — the default path is not writable by the
non-root image user.

### Client stats

`client_stats` tracks per-client-IP statistics for the TUI and web UI:
requests, responses, upstream/downstream bytes and their rates, aggregated
per IP across listeners. It is **enabled by default**; set `enabled: false`
to turn it off:

```yaml
client_stats:
  enabled: true
  dir: /var/log/udpshunt   # default: same default dir as request_log
  retention_days: 30       # snapshot files older than this are deleted
  max_ips: 65536           # in-memory cap; least-recently-active IPs are
                           # evicted beyond it (counted as evicted)
  snapshot_interval: 60s   # how often today's totals are written to disk
```

Counters are per-day. A snapshot of today's table is rewritten atomically
every `snapshot_interval` (and once more on graceful shutdown), so a restart
continues the current day's totals instead of starting from zero. At
midnight the day's file is finalized, gzip-compressed
(`udpshunt-clients-YYYY-MM-DD.jsonl.gz`) and the in-memory table resets —
one JSON line per IP:

```json
{"ip":"203.0.113.7","requests":412,"responses":398,"bytes_in":30114,"bytes_out":128960,"last_seen":1789712345123456789}
```

The hot path pays one sharded-map lookup per packet (the same structure as
the session table); rates are computed by a 1s background tick, never on
the data path. If the directory cannot be created, udpshunt logs one
warning and keeps running with client stats disabled. The table is served
by `GET /clients` (below) and rendered in the TUI (`c` key) and in the web
UI's clients panel.

### Balancing modes

`balance` selects how new sessions pick a backend:

- `round_robin` — even rotation across healthy backends (default).
- `least_sessions` — backend with the fewest live sessions; ties keep the earliest backend in stable pool order.
- `source_hash` — stable rendezvous hash of the client IP, so a client keeps its backend as long as it stays healthy; when no backend is healthy it fails open via the same rendezvous hash over all backends.

### Listener socket buffer

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

### Health checks

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

### Hot reload

Both `kill -HUP <pid>` (not delivered on Windows) and
`curl -X POST http://127.0.0.1:9155/reload` re-read the `-c` file. Listeners
are diffed against the running set: removed listeners stop, a `bind` change
restarts the socket, everything else (backends, balance mode, session
timeout, health checks) updates live. On any error the previous config keeps
serving.

## Admin API

`admin.bind` serves five routes:

- `GET /metrics` — Prometheus text format (private registry, `udpshunt_` prefix).
- `GET /status` — JSON snapshot: uptime, per-listener backends (health, session counts), session totals, request-log state (`dropped` counter), recent events, and per-listener rate history (last 5 min at 1s samples).
- `GET /clients` — per-client-IP table (`?sort=<column>&order=asc|desc&limit=<n>`, default `requests`/`desc`/200; columns: `ip`, `requests`, `responses`, `bytes_in`, `bytes_out`, `pps_in`, `bps_in`, `bps_out`, `last_seen`). Returns `{enabled, tracked, evicted, rows}`.
- `POST /reload` — reload the config file and apply it.
- `GET /ui` — the embedded web dashboard (below).

## Monitoring

### Metric families

Scrape `GET /metrics` on the admin port. Families:
`udpshunt_packets_in_total`, `udpshunt_bytes_in_total`,
`udpshunt_packets_out_total`, `udpshunt_bytes_out_total` (label `listener`);
`udpshunt_backend_packets_in_total`, `udpshunt_backend_packets_out_total`,
`udpshunt_backend_errors_total`, `udpshunt_backend_healthy`,
`udpshunt_backend_state_changes_total` (labels `listener`, `backend`);
`udpshunt_sessions_created_total` (label `listener`),
`udpshunt_sessions_expired_total`, `udpshunt_sessions_rejected_total`,
`udpshunt_sessions_active`; `udpshunt_reloads_total`,
`udpshunt_reload_failures_total`, `udpshunt_uptime_seconds`.

### Grafana

Import `deploy/grafana-dashboard.json` (Dashboards -> New -> Import) and pick
your Prometheus datasource when prompted — the dashboard defines a
`DS_PROMETHEUS` datasource variable for that choice. Eight panels cover packet
and byte throughput, active/rejected sessions, backend health and backend
errors. The data source is the admin port's `/metrics` endpoint, e.g. a scrape
job with `targets: ["127.0.0.1:9155"]` (`metrics_path` defaults to
`/metrics`).

### Terminal dashboard

`udpshunt tui` renders a live dashboard (uptime, totals, per-listener
sparklines, backend health, recent events) from a running udpshunt's admin
API — the daemon itself must already be running:

    udpshunt tui --addr http://127.0.0.1:9155 --interval 1s

- `--addr` — admin API base URL (default `http://127.0.0.1:9155`).
- `--interval` — poll interval (default `1s`).

Press `c` to open the per-client-IP table (top clients by today's traffic,
htop-style): `s` cycles the sort column, `r` reverses the direction, `c`
returns to the dashboard. Press `q` or Ctrl-C to quit.

### Web UI

The admin API also serves a single-page dashboard at `/ui` on the same port
(uptime and totals, per-listener rate charts, backend health, recent events;
polls `/status` every 2s). It also shows a clients panel — the per-client-IP
table with click-to-sort column headers, polled from `/clients` every 2s.
The built asset is committed in
`internal/webui/dist` and embedded into the binary via `go:embed` — no extra
files are needed at runtime.

There is no built-in authentication on the admin port (design assumption C):
`/ui`, `/status` and `/metrics` are open to anyone who can reach it. Keep
`admin.bind` on loopback (the default `127.0.0.1:9155`) or put the port
behind an authenticating proxy before exposing it.

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
deferral live in `docs/benchmarks.md`. The `recvmmsg` rows there read
"pending CI bench run" until the first manual `bench` job of the `ci`
workflow fills them — the dev box cannot exercise the Linux batch path. Pair
benchmark runs with the `GOMEMLIMIT` guidance above.

## Development

    go build -o udpshunt ./cmd/udpshunt
    go test ./...

The Grafana dashboard in `deploy/` is import-checked by `go test ./deploy/`
(valid JSON, all metric families referenced, datasource variable defined).
To rebuild the web UI after changing `web/src`, run from the repo root
(requires node >= 20 and npm):

    sh web/build.sh

That runs `npm ci && npm run build` in `web/` and copies `dist/` into
`internal/webui/dist`. For iteration with live reload, `npm run dev` in `web/`
starts vite (dev-only proxy to `http://127.0.0.1:9155` for `/status` and
`/metrics`).

Design doc: `docs/superpowers/specs/2026-09-15-udpshunt-design.md`.
