# Payload Filter Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Per-listener optional ingress payload gate: only datagrams matching at least one configured magic-byte rule are forwarded; everything else is silently dropped with a dedicated counter.

**Architecture:** New `internal/payloadfilter` package compiles YAML rules into an immutable snapshot; the listener holds it behind `atomic.Pointer` (COW, same convention as the blocklist container) and checks it in the receive loop after the blacklist check, before general accounting. App wires the metrics hook into a lock-free `/status` total and swaps rules on reload.

**Tech Stack:** Go stdlib only (encoding/hex, bytes, sync/atomic); existing prometheus client, yaml.v3.

**Spec:** `docs/superpowers/specs/2026-09-26-udpshunt-payload-filter-design.md`

## Global Constraints

- Generated code, comments, README and packaged config examples: English only.
- No Co-Authored-By lines in commits; commit body = markdown bullet list; conventional subject (`feat(...)`, `test(...)`, `docs(...)`).
- Hot path: no locks, no allocations. Rules snapshots are immutable; the only mutation is `atomic.Pointer` swap.
- Config is additive: no `payload_filter` section (or empty `rules`) = behavior identical to today except one nil-check on an atomic load.
- Docs and config examples must NOT contain internal-company information (hostnames, the real production magic value, business names, internal IPs). Use `00112233` as the placeholder magic everywhere in docs.
- Rollback semantics for bad rules ride the existing convention: `config.Validate` compiles the rules, so an invalid rule fails `config.Load` → `reload_failed` → previous config keeps running.
- Every task ends green: `go test -race ./...` and `go vet ./...` pass before commit.
- Target release: v0.1.12.

---

### Task 1: `internal/payloadfilter` package

**Files:**
- Create: `internal/payloadfilter/payloadfilter.go`
- Test: `internal/payloadfilter/payloadfilter_test.go`

**Interfaces:**
- Consumes: nothing (stdlib only).
- Produces (Tasks 2, 4, 5 depend on these exact signatures):
  - `type RuleConfig struct { MagicHex, Offset, MinLength string/int/int with yaml tags }`
  - `func Compile(cfg []RuleConfig) (*Rules, error)` — nil, nil for empty input
  - `func (r *Rules) Allowed(pkt []byte) bool` — nil receiver returns true
  - `func (r *Rules) Len() int` — nil receiver returns 0

- [ ] **Step 1: Write the failing tests**

Table-driven, same-package test. Cover:

```go
func TestCompile(t *testing.T) {
    // ok: single rule, defaults filled; multi rules
    // error cases, each asserting the error message mentions the rule index:
    //   bad hex ("zz"), empty magic_hex ("" decodes to 0 bytes),
    //   negative offset, offset+magic > 65536, negative min_length
}

func TestAllowed(t *testing.T) {
    // rule: magic "5763536a" offset 0, min_length omitted (defaults to 4)
    //   8-byte frame starting with those bytes -> true
    //   same frame with one flipped byte -> false
    //   3-byte packet -> false (shorter than default min_length)
    //   empty packet -> false
    // explicit min_length 8: 4-byte magic-only packet -> false; 8-byte -> true
    // offset 2 rule: magic at bytes [2:6] -> true; at [0:4] -> false
    // two rules: packet matching only the second -> true (any-of)
    // nil *Rules: any packet -> true
}
```

- [ ] **Step 2: Run tests, verify they fail** — `go test ./internal/payloadfilter/` fails to build (package missing).

- [ ] **Step 3: Implement**

```go
// Package payloadfilter implements the listener ingress payload gate: an
// immutable, compiled set of magic-byte rules any of which a packet may
// match to be considered legal.
package payloadfilter

import (
    "bytes"
    "encoding/hex"
    "fmt"
)

// maxPacket mirrors pktio.MaxPacketSize without importing it: the gate runs
// on receive-loop buffers whose payloads never exceed this size.
const maxPacket = 65536

// RuleConfig is one raw allowlist rule as spelled in YAML.
type RuleConfig struct {
    MagicHex  string `yaml:"magic_hex"`
    Offset    int    `yaml:"offset"`
    MinLength int    `yaml:"min_length"` // 0 -> offset+len(magic) at compile time
}

type compiledRule struct {
    magic  []byte
    offset int
    minLen int
}

// Rules is an immutable compiled snapshot. The nil *Rules is valid and
// allows every packet (gate off).
type Rules struct {
    rules []compiledRule
}

// Compile validates and compiles rules. An empty or nil input compiles to
// a nil *Rules (gate off). Errors name the offending rule index.
func Compile(cfg []RuleConfig) (*Rules, error) {
    if len(cfg) == 0 {
        return nil, nil
    }
    rules := make([]compiledRule, 0, len(cfg))
    for i, c := range cfg {
        magic, err := hex.DecodeString(c.MagicHex)
        if err != nil {
            return nil, fmt.Errorf("rules[%d]: magic_hex must be hex: %w", i, err)
        }
        if len(magic) == 0 {
            return nil, fmt.Errorf("rules[%d]: magic_hex is required", i)
        }
        if c.Offset < 0 {
            return nil, fmt.Errorf("rules[%d]: offset must be >= 0", i)
        }
        if c.Offset+len(magic) > maxPacket {
            return nil, fmt.Errorf("rules[%d]: offset+magic (%d bytes) exceeds the %d-byte packet bound", i, c.Offset+len(magic), maxPacket)
        }
        if c.MinLength < 0 {
            return nil, fmt.Errorf("rules[%d]: min_length must be >= 0", i)
        }
        minLen := c.MinLength
        if minLen == 0 {
            minLen = c.Offset + len(magic)
        }
        rules = append(rules, compiledRule{magic: magic, offset: c.Offset, minLen: minLen})
    }
    return &Rules{rules: rules}, nil
}

// Allowed reports whether pkt carries at least one rule's magic bytes at
// the rule's offset. A nil receiver allows every packet (gate off). The
// second length check keeps the slice bounds safe when a configured
// min_length is smaller than the rule's own footprint.
func (r *Rules) Allowed(pkt []byte) bool {
    if r == nil {
        return true
    }
    for i := range r.rules {
        c := &r.rules[i]
        if len(pkt) >= c.minLen && len(pkt) >= c.offset+len(c.magic) &&
            bytes.Equal(pkt[c.offset:c.offset+len(c.magic)], c.magic) {
            return true
        }
    }
    return false
}

// Len returns the number of compiled rules (0 for the nil gate).
func (r *Rules) Len() int {
    if r == nil {
        return 0
    }
    return len(r.rules)
}
```

- [ ] **Step 4: Run tests until green** — `go test -race ./internal/payloadfilter/`

- [ ] **Step 5: Commit** — `feat(payloadfilter): compiled any-of magic-byte rules for the ingress payload gate`

---

### Task 2: config section and validation

**Files:**
- Modify: `internal/config/config.go` (Listener struct ~line 46; Validate listener loop ~line 289)
- Test: `internal/config/config_test.go`

**Interfaces:**
- Consumes: `payloadfilter.Compile`, `payloadfilter.RuleConfig` (Task 1).
- Produces: `config.Listener.PayloadFilter PayloadFilter` with `Rules []payloadfilter.RuleConfig` and `LogIllegal bool`; `Listener` gains yaml key `payload_filter`.

- [ ] **Step 1: Write the failing tests**

```go
// Valid config with a payload_filter section parses and passes validation:
//   listeners: [{name: t, bind: "127.0.0.1:0", backends: ["127.0.0.1:65001"],
//     payload_filter: {rules: [{magic_hex: "00112233", offset: 0, min_length: 8}], log_illegal: true}}]
// Rules parse into RuleConfig values; LogIllegal == true.
// min_length omitted -> stays 0 in config (Compile fills it; config does not).
// Absent section -> zero value, validation passes.
// Error cases (assert the message contains `payload_filter` and the listener
// name prefix `listeners[0] (t)`):
//   magic_hex: "zz" (bad hex)
//   offset: -1
//   min_length: -1
```

- [ ] **Step 2: Run, verify fail** — compile error: `PayloadFilter` undefined.

- [ ] **Step 3: Implement**

In `config.go`, add the import `"github.com/fetaoily/udpshunt/internal/payloadfilter"` (after the blocklist import), the type (near Blacklist):

```go
// PayloadFilter configures a listener's ingress payload gate: a datagram is
// forwarded only when it matches at least one rule's magic bytes. The zero
// value (section absent or no rules) disables the gate.
type PayloadFilter struct {
    Rules      []payloadfilter.RuleConfig `yaml:"rules"`
    LogIllegal bool                       `yaml:"log_illegal"`
}
```

Add to `Listener`:

```go
	PayloadFilter  PayloadFilter `yaml:"payload_filter"` // absent = no ingress gating
```

In `Validate()`, inside the listener loop after the `on_down` switch (mirroring the blacklist comment style):

```go
		// Payload filter validation is conditional: the zero value (section
		// absent or rules empty) disables the gate and passes trivially.
		// Compile here so a bad rule rejects the whole config (fail-closed,
		// same as blacklist entries).
		if len(l.PayloadFilter.Rules) > 0 {
			if _, err := payloadfilter.Compile(l.PayloadFilter.Rules); err != nil {
				return fmt.Errorf("listeners[%d] (%s): payload_filter: %w", i, l.Name, err)
			}
		}
```

- [ ] **Step 4: Run config tests + full suite** — `go test -race ./internal/config/ && go test -race ./...`

- [ ] **Step 5: Commit** — `feat(config): per-listener payload_filter section with fail-closed validation`

---

### Task 3: metrics counter and request-log outcome

**Files:**
- Modify: `internal/metrics/metrics.go`
- Modify: `internal/requestlog/requestlog.go` (one constant, line ~29)
- Test: `internal/metrics/metrics_test.go`

**Interfaces:**
- Consumes: nothing new.
- Produces (Task 4/5 depend on these):
  - `Metrics.OnIllegal func(int64)` field; `ListenerMetrics.Illegal(n int)` method; registered counter `udpshunt_illegal_packets_total` (label `listener`).
  - `requestlog.OutcomeIllegal = "illegal"`.

- [ ] **Step 1: Write the failing test** (follow existing `metrics_test.go` patterns): `Illegal(3)` increments `udpshunt_illegal_packets_total{listener="t"}` by 3 and fires `OnIllegal` exactly once with 3; nil `*ListenerMetrics` `Illegal` is a no-op (same as the Blacklisted nil test).

- [ ] **Step 2: Run, verify fail.**

- [ ] **Step 3: Implement** — mirror the `blacklisted` family exactly (metrics.go:27, :33-37, :75-77, :145-152):

```go
	illegal *prometheus.CounterVec
```

```go
	// OnIllegal, when set, is invoked with each illegal-payload count
	// alongside the udpshunt_illegal_packets_total counter, so the app can
	// keep a lock-free /status total. Set once at startup; called only
	// from the packet-drop path.
	OnIllegal func(int64)
```

```go
		illegal: promauto.With(r).NewCounterVec(prometheus.CounterOpts{
			Name: "udpshunt_illegal_packets_total", Help: "Packets dropped by the payload filter.",
		}, []string{"listener"}),
```

```go
func (lm *ListenerMetrics) Illegal(n int) {
	if lm != nil {
		lm.m.illegal.WithLabelValues(lm.name).Add(float64(n))
		if lm.m.OnIllegal != nil {
			lm.m.OnIllegal(int64(n))
		}
	}
}
```

In `requestlog.go`, add to the outcome constants:

```go
	OutcomeIllegal       = "illegal"
```

- [ ] **Step 4: Run metrics tests + full suite.**

- [ ] **Step 5: Commit** — `feat(metrics): illegal-payload counter with lock-free status hook`

---

### Task 4: listener gate

**Files:**
- Modify: `internal/listener/listener.go`
- Test: `internal/listener/listener_test.go`, `internal/listener/integration_test.go`

**Interfaces:**
- Consumes: `payloadfilter.Rules/Compile`, `metrics.Illegal`, `requestlog.OutcomeIllegal` (Tasks 1, 3).
- Produces: `Listener.UpdateRules(*payloadfilter.Rules)`, `Listener.UpdateLogIllegal(bool)` (Task 5 depends on these).

- [ ] **Step 1: Write the failing tests** (reuse the existing `startEcho`/`roundTrip` helpers; build listeners via the existing test constructor with a `config.Listener` carrying `PayloadFilter`):

```go
// Gate on (rules require magic "00112233", min_length 4):
//   - a datagram without the magic: roundTrip client gets NO reply and the
//     echo backend received nothing (probe the echo server's receive count),
//     no session is created for that client.
//   - a datagram with the magic: normal echo reply.
//   - a 3-byte datagram: dropped (below default min_length).
// Multi-rule: datagram matching only the second rule is forwarded.
// Gate off (no section): a garbage datagram is echoed (behavior unchanged).
// Blacklist precedence: blacklisted client sending non-magic garbage lands
// in the blocked path, not the illegal path (assert via the blocked stats
// table, mirroring the existing blacklist listener test).
// UpdateRules(nil) after gate-on: garbage flows again (reload-to-off path).
```

- [ ] **Step 2: Run, verify fail** (gate fields undefined).

- [ ] **Step 3: Implement** in `listener.go`:

Imports: add `"github.com/fetaoily/udpshunt/internal/payloadfilter"`.

Struct fields (next to `logBlocked`, line ~46):

```go
	rules        atomic.Pointer[payloadfilter.Rules]
	logIllegal   atomic.Bool
```

`New()` — the current tail returns a struct literal directly; restructure to a variable so the atomics can be initialized:

```go
	rules, err := payloadfilter.Compile(cfg.PayloadFilter.Rules)
	if err != nil {
		// Unreachable via config.Load (Validate compiles first); guards
		// programmatic callers that skip validation.
		pc.Close()
		return nil, fmt.Errorf("listener %q: compile payload filter: %w", name, err)
	}
	l := &Listener{ /* existing literal fields unchanged */ }
	l.rules.Store(rules)
	l.logIllegal.Store(cfg.PayloadFilter.LogIllegal)
	return l, nil
```

`Run()` — insert between the blacklist block (ends line ~182) and `l.met.PacketsIn(1)` (line ~183):

```go
			// Payload gate: a datagram matching no rule is dropped with the
			// same accounting discipline as blacklisted packets (spec §9 of
			// the blacklist design): silent, dedicated counter only, no
			// session, no general stats, no debug logging.
			if pf := l.rules.Load(); pf != nil && !pf.Allowed(bufs[i][:sizes[i]]) {
				l.met.Illegal(1)
				if l.logIllegal.Load() && l.reqLog != nil {
					l.reqLog.Record(requestlog.Entry{
						Time:     time.Now(),
						Listener: l.name,
						Client:   addrs[i].String(),
						Backend:  "",
						Bytes:    sizes[i],
						Outcome:  requestlog.OutcomeIllegal,
					})
				}
				continue
			}
```

Updaters (next to `UpdateLogBlocked`, line ~124):

```go
// UpdateRules swaps the compiled payload-filter gate (COW). A nil snapshot
// disables the gate.
func (l *Listener) UpdateRules(r *payloadfilter.Rules) { l.rules.Store(r) }

// UpdateLogIllegal toggles whether illegal-payload drops are written to the
// request log. Default off.
func (l *Listener) UpdateLogIllegal(v bool) { l.logIllegal.Store(v) }
```

Integration test in `integration_test.go`: gated listener + echo backend — illegal datagram gets no reply and the backend counter stays put; legal frame round-trips (spec §9 integration row).

- [ ] **Step 4: Run listener package + full suite** — `go test -race ./internal/listener/ && go test -race ./...`

- [ ] **Step 5: Commit** — `feat(listener): ingress payload gate drops non-matching datagrams before accounting`

---

### Task 5: app wiring — status, reload, event

**Files:**
- Modify: `cmd/udpshunt/app.go` (struct ~line 63; `NewApp` ~line 120; `updateListenerLocked` ~line 662; `Status()` ~line 737)
- Modify: `internal/admin/admin.go` (Status struct, line ~63)
- Test: existing app/admin test files (follow their harness)

**Interfaces:**
- Consumes: `Listener.UpdateRules/UpdateLogIllegal` (Task 4), `payloadfilter.Compile/Len`.
- Produces: `/status` top-level `"illegal_packets": int64`; event `payload_filter_reloaded <listener> rules=N`.

- [ ] **Step 1: Write the failing tests** (follow the existing app-test harness — temp config file, start the app, send datagrams to the listener port):

```go
// Start with a gated listener; send one illegal and one legal datagram:
//   GET /status -> illegal_packets == 1; echo backend saw only the legal one.
// Reload with tightened rules (legal datagram no longer matches):
//   subsequent legal datagram is dropped, illegal_packets == 2, and the
//   event list contains "payload_filter_reloaded" with rules=N.
// Reload with no rules: gate off, garbage flows, event rules=0.
// Config with an invalid rule (bad hex): Reload returns error, previous
//   rules keep working (reload_failed path).
```

- [ ] **Step 2: Run, verify fail.**

- [ ] **Step 3: Implement.**

`admin.go` — add to `Status` (top level, plain field, always serialized):

```go
	IllegalPackets int64             `json:"illegal_packets"`
```

`app.go` — struct field next to `blBlocked` (line ~63):

```go
	// illegalPackets totals payload-filter drops for /status (fed by the
	// metrics hook). Lock-free, like blBlocked.
	illegalPackets atomic.Int64
```

`NewApp` — next to line 120:

```go
	met.OnIllegal = func(n int64) { a.illegalPackets.Add(n) }
```

`Status()` — add to the `admin.Status` literal:

```go
		IllegalPackets: a.illegalPackets.Load(),
```

`updateListenerLocked` — after `UpdateTimeout` (line ~662):

```go
	// Payload gate: config.Load already compiled these rules (Validate),
	// so Compile here cannot fail; on the defensive branch keep the old
	// gate rather than dropping packets on a nil snapshot mismatch.
	if rules, err := payloadfilter.Compile(lc.PayloadFilter.Rules); err != nil {
		a.logger.Error("payload filter recompile failed, keeping previous rules", "listener", lc.Name, "err", err)
	} else {
		a.listeners[lc.Name].UpdateRules(rules)
		a.listeners[lc.Name].UpdateLogIllegal(lc.PayloadFilter.LogIllegal)
		a.events.Add("payload_filter_reloaded", fmt.Sprintf("%s rules=%d", lc.Name, rules.Len()))
	}
```

Add the payloadfilter import to app.go. Note: `updateListenerLocked` fires on every reload of a bind-unchanged listener, so `payload_filter_reloaded` accompanies `listener_updated` — one event per listener per reload, matching the existing convention.

- [ ] **Step 4: Run full suite** — `go test -race ./... && go vet ./...`

- [ ] **Step 5: Commit** — `feat(app): payload gate status total, hot reload and event`

---

### Task 6: TUI header

**Files:**
- Modify: `internal/tui/tui.go` (header build, lines ~198-204)
- Test: `internal/tui/tui_test.go`

**Interfaces:**
- Consumes: `admin.Status.IllegalPackets` (Task 5).

- [ ] **Step 1: Write the failing test** — mirror the existing blocked-header test (`headerLine` helper, tui_test.go:252): with `IllegalPackets > 0` the header line contains `illegal <N>`; with 0 it does not.

- [ ] **Step 2: Run, verify fail.**

- [ ] **Step 3: Implement** — next to the `blocked` segment (tui.go:198):

```go
	illegal := ""
	if st.IllegalPackets > 0 {
		illegal = fmt.Sprintf("   illegal %d", st.IllegalPackets)
	}
```

and append `illegal` to the header `Sprintf` argument list after `blocked`.

- [ ] **Step 4: Run tui tests + full suite.**

- [ ] **Step 5: Commit** — `feat(tui): show illegal-payload drops in the dashboard header`

---

### Task 7: docs and packaged config example

**Files:**
- Modify: `README.md` (new subsection after the blacklist section)
- Modify: `packaging/config/udpshunt.yaml` (commented example)

- [ ] **Step 1: README** — new `### Payload filter` subsection (English), covering:
  - What it does: per-listener ingress allowlist on payload magic bytes; any-of semantics; non-matching datagrams are dropped before sessions/accounting, counted in `/status` (`illegal_packets`), Prometheus (`udpshunt_illegal_packets_total`) and the TUI header.
  - Config example with **placeholder** magic `00112233` and the explicit note that the real value comes from the operator's own protocol spec.
  - `min_length` default (`offset + len(magic)`); `offset` for magics not at byte 0.
  - `log_illegal` magnitude warning (same wording class as `log_blocked`: scales with attack pps).
  - Reload: rules hot-swap via `/reload`; tightening rules does not kill existing sessions (they idle out).
  - Operational note: confirm the on-wire magic bytes and the attack profile with a packet capture before enabling in production (`tcpdump -i eth0 -s0 -X -c 30 'udp port <port>'`); the gate only blocks traffic that lacks the magic — forged-but-valid frames are the blacklist's job.

- [ ] **Step 2: packaging/config/udpshunt.yaml** — commented block inside the example listener (English comments, placeholder magic):

```yaml
    # payload_filter:            # optional ingress gate; absent = disabled
    #   rules:
    #     - magic_hex: "00112233"  # example placeholder; use your protocol's
    #       offset: 0              #   magic as wire-order hex
    #       min_length: 8          # default: offset + len(magic)
    #   log_illegal: false         # true: one request-log line per drop
```

- [ ] **Step 3: Verify** — `go test -race ./... && go vet ./...` still green; grep the diff for internal-company identifiers (hostnames, real magic bytes, business names) — must be empty.

- [ ] **Step 4: Commit** — `docs: payload filter usage, ops notes and packaged config example`
