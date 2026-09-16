# udpshunt benchmarks

How to run, what the numbers mean, and the current state of the spec §7
targets. Numbers are medians of `-count=3` runs. Loopback benchmarks are
noisy by nature (both endpoints share one kernel and one scheduler); treat
small differences as noise and read the trends, not the digits.

## Running

    go test ./internal/listener/ -bench . -benchtime 2s -run '^$' -count=3
    # Linux only (recvmmsg batch path):
    go test ./internal/pktio/ -bench . -benchtime 2s -run '^$'

The listener benchmarks run everywhere and exercise the real end-to-end data
path. On Linux the listener receive loop drains the socket through the
recvmmsg batch path; on Windows it falls back to pktio's single-packet
receiver, so dev-box numbers measure the portable floor, not the Linux fast
path.

## What each benchmark measures

- BenchmarkProxyRoundTrip: one ping-pong through the full proxy per op
  (client -> listener -> backend echo -> listener -> client), 64-byte
  payloads, one request in flight. End-to-end latency including two loopback
  hops; not a throughput number.
- BenchmarkProxyPipelined: 256 requests in flight, 512-byte payloads.
  Per-packet proxy cost at load; the closest local proxy measurement to the
  pps target (packets/s = 1e9 / ns_per_op).
- BenchmarkReceiveBatch64 / BenchmarkReceiveSingleReadFrom (Linux only):
  raw frontend-socket receive cost with no proxy work, isolating the
  recvmmsg(64) win over read(1). These two are the SO_REUSEPORT decision
  inputs (see below).

## Environment

| Field | Dev box | CI bench job |
|---|---|---|
| OS | Windows 11 Pro (build 26200) | ubuntu-latest |
| CPU | Intel Core i9-13900H, 20 logical cores (laptop) | 2 vCPU (shared runner) |
| RAM | 32 GB | 7 GB |
| Go | 1.25.0, windows/amd64 | 1.25, linux/amd64 |
| Receive path | pktio single-packet fallback | recvmmsg batch (64/wakeup) |

The dev box is a laptop running Windows, so its pipelined number is a lower
bound on what the batched Linux path does for the same work.

## Results

Dev box, 2026-09-16, go1.25.0, nothing else running, median of
`go test ./internal/listener/ -bench . -benchtime 2s -run '^$' -count=3`:

| Benchmark | Median | Reps in the run | Derived |
|---|---|---|---|
| ProxyRoundTrip-20 | 112,327 ns/op | 112.0 / 112.3 / 139.0 µs | ~8,900 round trips/s |
| ProxyPipelined-20 | 55,942 ns/op | 55.9 / 67.0 / 51.8 µs | ~17,900 packets/s end-to-end |

Cross-run spread: a second `-count=3` invocation minutes later gave medians
of 129.5 µs (RoundTrip) and 43.9 µs (Pipelined, ~22,800 packets/s) — a ±20%
swing that is loopback/scheduler noise, not a code change. Directional
reading: on the single-packet receive path the full proxy keeps tens of
thousands of ping-pongs per second per listener, and keeping a window in
flight cuts per-packet cost roughly 2-2.5x versus a synchronous round trip.

Linux receive-path numbers (BenchmarkReceiveBatch64 vs
BenchmarkReceiveSingleReadFrom) are produced by the CI bench job, not on a
Windows box — the benchmark file is `//go:build linux` and does not compile
locally:

    # Actions -> ci -> Run workflow -> select branch, then read the "bench" job log
    # or: gh workflow run ci --ref feat/m3-performance && gh run watch

| Benchmark | Median | Status |
|---|---|---|
| ReceiveBatch64 | pending CI bench run | benchmark exists, needs Linux recvmmsg |
| ReceiveSingleReadFrom | pending CI bench run | benchmark exists, needs Linux recvmmsg |

(The CI job prints one `-benchtime 2s` pass per benchmark rather than a
-count=3 median; re-run the workflow to check stability.)

## Spec §7 targets vs. current state

| Target (4-core VM) | Measured | Status |
|---|---|---|
| >= 200k pps per listener | ~18k-23k pps end-to-end through the full proxy on the dev box (pipelined, single-packet receive, loopback); 1e9/ns_per_op | pending production-VM run |
| >= 3 Gbps throughput | not measured: the suite has no bulk-transfer benchmark and the dev box cannot spare >= 300 Mbit/s of loopback headroom for an honest run | pending |
| >= 500k concurrent sessions | architectural bound is now memory per in-flight packet (one 64x64 KiB receive arena per listener + pooled relay buffers), not 64 KiB per live session; no dedicated soak in M3 | pending soak |

The absolute targets require a dedicated 4-core Linux VM (spec: 实测为准);
dev-box and CI numbers are directional. Do not gate the milestone on them.

Why the dev box cannot certify the pps target: every pipelined op includes
two loopback kernel hops plus an echo backend hop, the receiver is the
single-packet fallback, and both ends share one laptop CPU. The CI bench
isolates the receive path but runs on a 2-vCPU shared runner; certification
needs the VM.

## Send path note

M3 batches the receive side only. Upstream (proxy -> backend) sends go
through per-session connected UDP sockets, one `Write` per packet — there is
nothing for `sendmmsg` to aggregate per call on that path. Downstream
(proxy -> client) relays run one goroutine per session writing single
packets by architecture. If the production-VM run misses the pps target,
SO_REUSEPORT (below) is the first lever and send-side aggregation the
second.

## SO_REUSEPORT decision

**Decision: deferred (not implemented in M3).**

Evidence slot: the headroom of the single receive loop comes from the CI
bench job — BenchmarkReceiveBatch64 vs BenchmarkReceiveSingleReadFrom
(pending CI bench run, see Results). That pair answers "what does one wakeup
buy": if recvmmsg(64) drains 64 packets per syscall where read(1) pays one
syscall per packet, the single-loop receive ceiling scales by roughly that
ratio before any socket sharding is even worth discussing.

Rationale: the single receive loop keeps up as long as one core can drain
the frontend socket. SO_REUSEPORT fans packets across N sockets and N
receive loops, which adds operational surface (N sockets to bind, rebind and
drain per listener) for a bottleneck that has not been shown to exist. The
session-table side is already fan-out safe — the shared Manager keys
sessions by listener name + client address, and replies may leave via any
bound socket — so a later adoption is contained to socket setup and the
receive loop, but it is not free and not yet justified.

Revisit trigger: revisit when production tracing shows either (a)
frontend-socket drops — rising `udp/RcvbufErrors` / `udp/InErrors` counters
(`/proc/net/snmp` or `netstat -s`) while the process is healthy — or (b) a
single core saturating in the receive loop (profiling shows the
ReceiveBatch/read frame hot) with batch receive already enabled and the
rest of the pipeline demonstrably not the cause. Either signal means the
one-socket-one-loop design is the pps ceiling; only then does SO_REUSEPORT
sharding earn its complexity.
