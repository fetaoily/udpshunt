# udpshunt

A high-performance UDP layer-4 load balancer and port forwarder written in Go.

Status: M1 (core proxy). Design: `docs/superpowers/specs/2026-09-15-udpshunt-design.md`

## Build

    go build -o udpshunt ./cmd/udpshunt

## Run

    udpshunt -c /etc/udpshunt.yaml

See `examples/basic.yaml` for the config format: listeners bind a UDP port and
forward traffic to a pool of backends (round-robin). Sessions idle out after
`session_timeout` (default 60s). SIGTERM/SIGINT triggers a graceful drain.

## Development

    go test ./...
