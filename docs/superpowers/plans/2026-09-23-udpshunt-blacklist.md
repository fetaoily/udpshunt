# udpshunt Client IP Blacklist Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Drop packets from blacklisted client IPs before forwarding (no session), close their live sessions on block, manage the list via config (inline + separate file with mtime watch) and admin API, and account blocked traffic in a dedicated stats table.

**Architecture:** An immutable `blocklist.List` (exact-IP map + CIDR slice) behind an atomic-pointer container — the codebase's copy-on-write idiom — checked once per packet in the listener receive loop. Effective list = config entries (inline ∪ file) ∪ runtime adds − runtime dels, rebuilt on reload/API/watch; rebuilds diff-close newly blocked sessions. A second `clientstats.Table` instance (own file prefix) records blocked traffic.

**Tech Stack:** Go 1.25, stdlib only (`net/netip`), existing test deps (goleak).

**Spec:** `docs/superpowers/specs/2026-09-23-udpshunt-blacklist-design.md`

## Global Constraints

- Go 1.25, stdlib only — no new dependencies.
- All code and comments in English; no Chinese in code or commit messages.
- Commit style: conventional subject + markdown unordered-list body, NO Co-Authored-By lines.
- Hot path stays lock-free: the per-packet blacklist check is one atomic load + map probe; no mutex, no allocation.
- Data path never blocks on list changes (COW swap only); blocked packets create no session and (by default) no request-log line.
- Test command: `go test ./...` from repo root. Known machine flake: `internal/metrics` `fork/exec ... Access is denied` (AV) — re-run the package once; only persistent failures count.

## Interface contract (cross-task, exact)

```go
// internal/blocklist
func New(entries []string) (*List, error)          // parse+normalize (bare IP -> /32 or /128, Unmap), dedupe
func (*List) Blocked(a netip.Addr) bool            // exact map probe, then CIDR linear scan
func (*List) Entries() []string                    // canonical CIDR form
func (*List) Len() int
func NewContainer() *Container                     // starts with an empty list
func (*Container) Swap(*List)                      // atomic replace
func (*Container) Load() *List
func (*Container) Blocked(a netip.Addr) bool       // nil-list-safe
func ReadFileEntries(path string) ([]string, error) // comma- and newline-separated, blanks dropped

// internal/session
func (*Manager).CloseClients(pred func(*net.UDPAddr) bool) int

// internal/config
type Blacklist struct {
	Entries       []string  `yaml:"entries"`
	File          string    `yaml:"file"`
	WatchInterval Duration  `yaml:"watch_interval"` // >= 0; 0 = no watch
	LogBlocked    bool      `yaml:"log_blocked"`
}

// internal/metrics (ListenerMetrics)
func (*ListenerMetrics).Blacklisted(n int)          // udpshunt_blacklisted_packets_total{listener}

// internal/requestlog
OutcomeBlacklisted = "blacklisted"

// internal/admin
type BlacklistStatus struct {
	Entries        []string `json:"entries"`
	LogBlocked     bool     `json:"log_blocked"`
	BlockedPackets int64    `json:"blocked_packets"`
}
// Deps gains: Blacklist func() BlacklistStatus
//             BlacklistAdd func(entry string) error   // validation errors -> 400
//             BlacklistDel func(entry string) error   // not-found -> 404
// Deps.Clients widens to: func(sortCol, order string, limit int, blocked bool) ClientsStatus
// Routes: GET /blacklist, POST /blacklist {"entry":"..."}, DELETE /blacklist?entry=...
//         GET /clients honors ?scope=blocked

// listener.New gains two trailing params: bl *blocklist.Container, blockedStats *clientstats.Table
// (nil for either: no check / no accounting)
// (*Listener).UpdateLogBlocked(v bool)  // atomic, reload path
```

---

### Task 1: internal/blocklist — List, Container, file reader

**Files:** Create `internal/blocklist/blocklist.go`; Test `internal/blocklist/blocklist_test.go`

- [ ] **Step 1: failing tests**

```go
package blocklist

import (
	"os"
	"path/filepath"
	"testing"

	"net/netip"
)

func mustNew(t *testing.T, entries ...string) *List {
	t.Helper()
	l, err := New(entries)
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func TestParseNormalizeAndBlock(t *testing.T) {
	l := mustNew(t, "203.0.113.7", "198.51.100.0/24", "2001:db8::/32",
		"203.0.113.7") // duplicate silently dropped
	if l.Len() != 3 {
		t.Fatalf("Len = %d, want 3 (dedupe)", l.Len())
	}
	cases := map[string]bool{
		"203.0.113.7":        true,  // exact
		"198.51.100.9":       true,  // inside CIDR
		"198.51.101.9":       false, // outside CIDR
		"2001:db8::1":        true,  // v6 prefix
		"2001:db9::1":        false,
		"192.0.2.1":          false,
	}
	for ip, want := range cases {
		a, err := netip.ParseAddr(ip)
		if err != nil {
			t.Fatal(err)
		}
		if got := l.Blocked(a.Unmap()); got != want {
			t.Fatalf("Blocked(%s) = %v, want %v", ip, got, want)
		}
	}
}

func TestParseErrors(t *testing.T) {
	for _, bad := range []string{"not-an-ip", "10.0.0.0/99", "10.0.0.1/24", "", "10.0.0.0/8/7"} {
		if _, err := New([]string{bad}); err == nil {
			t.Fatalf("New(%q) must fail", bad)
		}
	}
}

func TestEntriesCanonical(t *testing.T) {
	l := mustNew(t, "203.0.113.7")
	if got := l.Entries(); len(got) != 1 || got[0] != "203.0.113.7/32" {
		t.Fatalf("Entries = %v, want [203.0.113.7/32]", got)
	}
}

func TestContainerSwapAndNilSafety(t *testing.T) {
	c := NewContainer()
	a, _ := netip.ParseAddr("203.0.113.7")
	if c.Blocked(a.Unmap()) {
		t.Fatal("empty container must not block")
	}
	c.Swap(mustNew(t, "203.0.113.7"))
	if !c.Blocked(a.Unmap()) {
		t.Fatal("must block after swap")
	}
}

func TestReadFileEntries(t *testing.T) {
	p := filepath.Join(t.TempDir(), "bl.txt")
	if err := os.WriteFile(p, []byte("203.0.113.7, 198.51.100.0/24\n\n192.0.2.9\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := ReadFileEntries(p)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"203.0.113.7", "198.51.100.0/24", "192.0.2.9"}
	if len(got) != len(want) {
		t.Fatalf("got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v want %v", got, want)
		}
	}
	if _, err := ReadFileEntries(filepath.Join(t.TempDir(), "missing.txt")); err == nil {
		t.Fatal("missing file must error (fail-closed)")
	}
}
```

- [ ] **Step 2:** `go test ./internal/blocklist/ -count=1` → FAIL (no package)
- [ ] **Step 3: implement** — `List{exact map[netip.Addr]struct{}, prefixes []netip.Prefix}`; `New` uses `netip.ParsePrefix`, falling back to `ParseAddr` + `PrefixFrom(addr, addr.BitLen())`; `Is4In6` results `Unmap()`ed; host-length prefixes go in `exact`, shorter in `prefixes`; `Entries()` returns `Prefix.String()` for both. `Container` is `atomic.Pointer[List]` with a non-nil empty default. `ReadFileEntries`: `os.ReadFile` + `strings.FieldsFunc(s, func(r rune) bool { return r == ',' || r == '\n' || r == '\r' || r == ' ' || r == '\t' })`. Doc comments per the spec (§4).
- [ ] **Step 4:** tests PASS; `gofmt -l` clean
- [ ] **Step 5: commit** `feat(blocklist): immutable IP/CIDR list with atomic container and list-file reader` + bullets (parse/normalize/dedupe; exact+prefix matching; COW container; comma/newline file reader with missing-file error)

### Task 2: clientstats — FilePrefix option

**Files:** Modify `internal/clientstats/clientstats.go`; Test `internal/clientstats/clientstats_test.go`

**Consumes:** none. **Produces:** `Options.FilePrefix string` (default `udpshunt-clients-`).

- [ ] **Step 1: failing test** — append:

```go
func TestFilePrefixSeparatesInstances(t *testing.T) {
	dir := t.TempDir()
	c := clock{}
	a := New(Options{Dir: dir, FilePrefix: "a-clients-", SnapshotInterval: 30 * time.Second, TickEvery: time.Hour, Clock: c.Now})
	a.PacketIn(net.IPv4(1, 2, 3, 4), 10)
	a.writeSnapshot(c.Now().Format(dayFormat))
	b := New(Options{Dir: dir, FilePrefix: "b-clients-", SnapshotInterval: 30 * time.Second, TickEvery: time.Hour, Clock: c.Now})
	rows, _, _ := b.Top("requests", true, 10)
	if len(rows) != 0 {
		t.Fatalf("prefixed instance must not load the other prefix's snapshot, got %v", rows)
	}
	// Default prefix keeps the historical name.
	d := New(Options{Dir: dir, SnapshotInterval: time.Hour, TickEvery: time.Hour, Clock: c.Now})
	if d.pathFor("2026-01-02") != filepath.Join(dir, "udpshunt-clients-2026-01-02.jsonl") {
		t.Fatalf("default prefix changed: %s", d.pathFor("2026-01-02"))
	}
}
```

(Use the file's existing fake-clock helper names — check the top of `clientstats_test.go` for `clock`/`c.Now`; adapt names if different.)

- [ ] **Step 2:** run → FAIL (no FilePrefix field)
- [ ] **Step 3: implement** — replace the `filePrefix` const with an `Options.FilePrefix` field defaulting to `"udpshunt-clients-"` in `New`; store on Table; `pathFor`, `Prune`, `SweepStale`, `removeTempFiles` use `t.opts.FilePrefix`.
- [ ] **Step 4:** package tests PASS (existing tests must stay green — they rely on the default name)
- [ ] **Step 5: commit** `feat(clientstats): configurable snapshot file prefix` + bullets

### Task 3: config — blacklist section, fail-closed file load

**Files:** Modify `internal/config/config.go`; Test `internal/config/config_test.go`

**Consumes:** `blocklist.New`, `blocklist.ReadFileEntries`. **Produces:** `Config.Blacklist Blacklist` (yaml `blacklist`).

- [ ] **Step 1: failing test** — table-driven, following the file's existing style (`baseListener` fixture, `writeConfig` helper):

```go
func TestBlacklistValidation(t *testing.T) {
	dir := t.TempDir()
	good := filepath.Join(dir, "bl.txt")
	os.WriteFile(good, []byte("203.0.113.7,198.51.100.0/24"), 0o600)
	bad := filepath.Join(dir, "bad.txt")
	os.WriteFile(bad, []byte("not-an-ip"), 0o600)

	cases := map[string]struct {
		yaml    string
		wantErr bool
	}{
		"absent":               {baseListener + "blacklist:\n  entries: [203.0.113.7]\n", false},
		"file and entries":     {baseListener + "blacklist:\n  entries: [192.0.2.1]\n  file: " + good + "\n", false},
		"invalid entry":        {baseListener + "blacklist:\n  entries: [nope]\n", true},
		"prefix on host bits":  {baseListener + "blacklist:\n  entries: [10.0.0.1/24]\n", true},
		"missing file":         {baseListener + "blacklist:\n  file: " + filepath.Join(dir, "nope.txt") + "\n", true},
		"invalid in file":      {baseListener + "blacklist:\n  file: " + bad + "\n", true},
		"negative watch":       {baseListener + "blacklist:\n  watch_interval: -1s\n", true},
		"watch zero ok":        {baseListener + "blacklist:\n  watch_interval: 0s\n", false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Load(writeConfig(t, tc.yaml))
			if tc.wantErr != (err != nil) {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}
```

- [ ] **Step 2:** run → FAIL
- [ ] **Step 3: implement** — add the `Blacklist` struct (Interface contract) and top-level field `Blacklist Blacklist \`yaml:"blacklist"\``; in `Validate`: if section non-empty → validate every inline entry via `blocklist.New` (error text `blacklist.entries[%d]: %w`); if `File != ""` → `blocklist.ReadFileEntries` (missing/unreadable → `blacklist.file %q: %w`) then validate those entries the same way (error mentions "file entry %q"); `WatchInterval < 0` → error; `WatchInterval == 0` allowed.
- [ ] **Step 4:** tests PASS
- [ ] **Step 5: commit** `feat(config): blacklist section with fail-closed list file` + bullets

### Task 4: session — CloseClients

**Files:** Modify `internal/session/session.go`; Test `internal/session/session_test.go`

- [ ] **Step 1: failing test** (mirror `TestCloseBackendFiltersListenerAndBackend`'s setup helpers):

```go
func TestCloseClientsByPredicate(t *testing.T) {
	mgr := NewManager(0)
	// (use the existing test helpers to put sessions for two client IPs on
	//  one listener/backend; then:)
	n := mgr.CloseClients(func(c *net.UDPAddr) bool { return c.IP.Equal(net.IPv4(203, 0, 113, 7)) })
	if n != 1 {
		t.Fatalf("closed = %d, want 1", n)
	}
	// the other client's session survives (assert via the helpers' count)
}
```

- [ ] **Step 2:** FAIL → **Step 3:** `CloseClients(pred)` = `closeWhere(func(s *Session) bool { return pred(s.Client) })` with the doc comment from the Interface contract → **Step 4:** PASS
- [ ] **Step 5: commit** `feat(session): close sessions by client predicate` + bullets

### Task 5: requestlog outcome + metrics counter + listener enforcement

**Files:** Modify `internal/requestlog/requestlog.go` (one const), `internal/metrics/metrics.go` (counter + method), `internal/listener/listener.go`, `internal/listener/listener_test.go`

**Consumes:** Tasks 1 (blocklist). **Produces:** listener integration incl. `UpdateLogBlocked`.

- [ ] **Step 1: failing listener test** — using the package's `newStack`-style setup where possible (note `newStack` builds its own balancer; a dedicated stack like `TestGracefulShutdown`'s is fine):

```go
func TestBlacklistedPacketDropped(t *testing.T) {
	backend := startEcho(t)
	lc := config.Listener{Name: "bl", Bind: "127.0.0.1:0", Backends: []string{backend}, SessionTimeout: config.Duration(time.Minute)}
	mgr := session.NewManager(0)
	bal := balancer.New(lc.Backends, balancer.Options{})
	bl := blocklist.NewContainer()
	attacker, _ := netip.ParseAddr("203.0.113.7")
	bl.Swap(func() *blocklist.List { l, _ := blocklist.New([]string{"203.0.113.7"}); return l }())
	bstats := clientstats.New(clientstats.Options{Dir: t.TempDir(), TickEvery: time.Hour, SnapshotInterval: time.Hour})
	met := metrics.New()
	lm := met.ForListener("bl")
	l, err := New(lc.Name, lc, bal, mgr, slog.Default(), lm, nil, nil, bl, bstats)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = l.Run(ctx) }()

	attackerSock, _ := net.DialUDP("udp", nil, l.Addr()) // spoofed source is not possible
	// NOTE: a real spoof is impossible; instead bind a client socket, learn
	// its IP, and blacklist THAT address (same code path).
	_ = attackerSock
	cli := testClient(t)
	cliAddr := cli.LocalAddr().(*net.UDPAddr)
	l2, _ := blocklist.New([]string{cliAddr.IP.String()})
	bl.Swap(l2)
	_, _ = cli.WriteToUDP([]byte("evil"), l.Addr())
	time.Sleep(200 * time.Millisecond)
	if mgr.Count() != 0 {
		t.Fatal("blacklisted client must not create a session")
	}
	if got, err := roundTripFrom(t, cli, l.Addr()); err == nil { // no reply ever
		_ = got
	}
	// blocked stats recorded for that IP
	rows, _, _ := bstats.Top("requests", true, 10)
	if len(rows) != 1 || rows[0].IP != cliAddr.IP.String() || rows[0].Requests < 1 {
		t.Fatalf("blocked stats missing: %+v", rows)
	}
	// unblock -> traffic flows again
	empty, _ := blocklist.New(nil)
	bl.Swap(empty)
	if got := roundTrip(t, testClient(t), l.Addr(), "ping"); got != "echo:ping" {
		t.Fatalf("after unblock: %q", got)
	}
}
```

Adapt helpers to the file's real names (`roundTrip`, `testClient`); drop the `roundTripFrom` pseudo-helper — assert "no reply within 300ms" with a deadline read instead. Add a second small test `TestLogBlockedWritesRequestLog` using a `requestlog.New` into `t.TempDir()`, `l.UpdateLogBlocked(true)`, send one packet, read today's file line and assert `"outcome":"blacklisted"` (the package's tests already read log files — mirror them).

- [ ] **Step 2:** FAIL (New signature) → **Step 3: implement**
  - `listener.New` gains trailing `bl *blocklist.Container, blockedStats *clientstats.Table`; store both plus `logBlocked atomic.Bool`.
  - In `Run`'s packet loop, before the `handle` call:
    ```go
    if l.bl != nil {
        if a, ok := netip.AddrFromSlice(addrs[i].IP); ok {
            a = a.Unmap()
            if l.bl.Blocked(a) {
                l.met.Blacklisted(1)
                l.blockedStats.PacketIn(addrs[i].IP, sizes[i])
                if l.logBlocked.Load() && l.reqLog != nil {
                    l.reqLog.Record(requestlog.Entry{Time: time.Now(), Listener: l.name,
                        Client: addrs[i].String(), Backend: "", Bytes: sizes[i], Outcome: requestlog.OutcomeBlacklisted})
                }
                continue
            }
        }
    }
    ```
  - `UpdateLogBlocked(v bool)` mirrors `UpdateTimeout` (atomic store, no mutex needed).
  - metrics: add `blacklisted *prometheus.CounterVec` (`udpshunt_blacklisted_packets_total`, label `listener`) registered like the others, `Blacklisted(n int)` on `ListenerMetrics`.
  - requestlog: `OutcomeBlacklisted = "blacklisted"`.
  - Update all existing `listener.New` callers (`newStack` in `listener_test.go`, `integration_test.go` stacks, `app.go`) with `bl, blockedStats` args (tests pass `blocklist.NewContainer(), nil` or `nil, nil`).
- [ ] **Step 4:** `go test ./internal/listener/ ./internal/metrics/` PASS
- [ ] **Step 5: commit** `feat(listener): drop blacklisted client packets before forwarding` + bullets (silent drop incl. no session; per-IP blocked accounting; optional request-log outcome default off; counter)

### Task 6: app wiring — effective list, admin API, /status, watcher

**Files:** Modify `cmd/udpshunt/app.go`, `cmd/udpshunt/main.go`, `internal/admin/admin.go`; Test `cmd/udpshunt/app_test.go`, `internal/admin/admin_test.go`

**Consumes:** Tasks 1-5. **Produces:** the full runtime surface.

- [ ] **Step 1: failing tests**
  - app: `TestBlacklistRuntimeAPIAndReload` — config A with `blacklist.entries: [203.0.113.7]`; Apply; establish a session from a real test client whose IP is then blocked at runtime via `app.BlacklistAdd(cliIP)`; assert session closed, event `blacklist_added`, `Blacklist()` lists it; `BlacklistDel` restores; reload with config B adding a CIDR → session closed + `blacklist_enforced` event; reload preserves a runtime add (block 192.0.2.1 at runtime, reload config A, still blocked).
  - admin: `TestBlacklistRoutes` — `httptest` against the mux (mirror existing tests): GET returns JSON shape; POST invalid entry → 400; POST valid → 200; DELETE missing → 404; `GET /clients?scope=blocked` returns the blocked table's shape (empty ok).
  - status: assert `app.Status().Blacklist.BlockedPackets` grows after a blocked packet (reuse the listener-level flow through `Apply`).
- [ ] **Step 2:** FAIL → **Step 3: implement**
  - App fields: `bl *blocklist.Container` (init in NewApp), `blockedStats *clientstats.Table` (set in main when `cfg.ClientStats.IsEnabled()` AND `cfg.Blacklist` section present, `FilePrefix: "udpshunt-blocked-"`), `blAdds, blDels map[string]struct{}` (under a.mu), `blBlocked atomic.Int64`, `blWatch atomic.Pointer[blWatchCfg]` (`struct{ path string; interval time.Duration }`).
  - `Apply` (after listeners update): rebuild — `cfgEntries := inline; if file != "" { fileEntries, err := ReadFileEntries(file); on error: fail Apply (reload keeps previous — consistent with fail-closed) }`; `eff := union(cfgEntries, blAdds) − blDels`; build `blocklist.New(eff)`; diff old vs new effective sets → for each newly blocked entry `CloseClients` matching + `blacklist_enforced closed=N` event; `a.bl.Swap(list)`; per-listener `UpdateLogBlocked(cfg.Blacklist.LogBlocked)`; update `blWatch` cfg.
  - `BlacklistAdd(entry string) error`: validate via `blocklist.New([entry])`; add to blAdds / remove from blDels; rebuild+swap (extract `rebuildBlacklistLocked()`); `CloseClients` for the entry; event `blacklist_added`.
  - `BlacklistDel(entry string) error`: if not in effective list → `os.ErrNotExist`; move to blDels (drop from blAdds); rebuild; event `blacklist_removed`.
  - `Blacklist() admin.BlacklistStatus` (under a.mu, reads container.Entries + blBlocked).
  - listener counter hook: increment `a.blBlocked` alongside `met.Blacklisted` — simplest: in `startListenerLocked`, wrap `lm := a.met.ForListener(name)` usage… instead have `Apply`'s per-listener metrics constructor pass a counting wrapper: add to `metrics.ListenerMetrics.Blacklisted` an optional callback? NO — simpler: `App` polls nothing; make the counter authoritative from metrics? Prometheus counters aren't readable back portably here. Pragmatic: `ListenerMetrics.Blacklisted` also takes the count via the listener; the /status number comes from `a.blBlocked` incremented by a tiny `metrics`-free hook: `listener.New` gains nothing — instead `met` wrapper: `a.met.ForListener` returns `*ListenerMetrics`; add `OnBlacklisted func(int64)` field on `Metrics` (set in NewApp to `a.blBlocked.Add`), invoked inside `Blacklisted(n)`. One callback field, no hot-path cost beyond the existing Add.
  - admin `Deps` + routes per the Interface contract; `handleClients` reads `scope` (`blocked` → `Clients(..., true)`); `app.Clients` signature widens (`blocked bool` → use `a.blockedStats` table; nil table → empty status).
  - `/status`: `Status.Blacklist *BlacklistStatus \`json:"blacklist,omitempty"\`` filled when the feature is on.
  - watcher: `func (a *App) WatchBlacklist(ctx context.Context)` — loop: cfg := blWatch.Load(); if path=="" || interval<=0 → sleep 1s continue; `os.Stat` vs stored mtime/size; changed → `ReadFileEntries` + build under a.mu via rebuild (keeping runtime sets), on success `Swap` + event `blacklist_reloaded entries=N source=file`, on parse error keep old + `logger.Error`; main starts `go app.WatchBlacklist(ctx)` next to `runSampler`.
- [ ] **Step 4:** `go test ./cmd/udpshunt/ ./internal/admin/` PASS, then full suite
- [ ] **Step 5: commit** `feat(app): blacklist runtime — config union, admin API, status and file watch` + bullets

### Task 7: TUI — blocked scope toggle and header count

**Files:** Modify `internal/tui/tui.go` (+ fetch URL building), `internal/tui/tui_test.go`; check `web/src/lib/api.js` only if the TUI shares it (it does not — TUI has its own fetch)

**Consumes:** Task 6 API surface.

- [ ] **Step 1: failing test** — model-level: `viewClients` with `m.clientsBlocked = true` renders the blocked-table header line (`blocked clients` marker or the footer hint contains `b:`); key handler for `b` toggles the flag. Follow existing `viewClients`/key-switch test patterns.
- [ ] **Step 2:** FAIL → **Step 3: implement** — model field `clientsBlocked bool`; key `b` in the clients view toggles it and triggers a refetch with `?scope=blocked`; header line appends `   blocked N` when `status.blacklist.blocked_packets > 0`; footer hint gains `b: blocked/normal`.
- [ ] **Step 4:** package tests PASS → **Step 5: commit** `feat(tui): blocked-clients scope toggle and header count` + bullets

### Task 8: docs — README + starter config

**Files:** `README.md`, `packaging/config/udpshunt.yaml`

- [ ] **Step 1:** README new `### Blacklist` section after Client stats: config example (entries + file + watch_interval + log_blocked), semantics summary (silent drop, session close on block, union reload model with runtime adds preserved across reloads, watch best-effort vs startup fail-closed), admin API examples (`curl`), `/clients?scope=blocked`, TUI `b` key, and a **prominent log_blocked volume warning**. `/status` field list gains the blacklist block. Starter config gains a commented-out `blacklist:` example.
- [ ] **Step 2:** full suite once → **Step 3: commit** `docs: client IP blacklist usage and semantics` + bullets

---

## Self-review notes

- Spec coverage: §3→T3, §4→T1, §5→T5(+T2 for the stats table), §6→T4/T6, §7→T6, §8→T5, §9→T5/T6/T7, §10→T1/T5 invariants, §11 embedded per task, §12→T8.
- `listener.New` grows to 10 params — matches the file's existing style (no options-struct refactor in scope; noted for the final review).
- The metrics→App blocked counter uses a callback field on `Metrics` set by `NewApp` — the alternative (reading Prometheus counters back) is worse; flagged for the final review to sanity-check.
- Watcher interacts with a.mu only in its rebuild step (brief); it never runs on the packet path.
- T5's test sketch spoofs nothing: it blacklists the test client's real local IP — the same Blocked() path a genuine entry takes.
