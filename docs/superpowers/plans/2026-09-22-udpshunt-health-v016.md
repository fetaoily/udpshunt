# udpshunt v0.1.6 Trustworthy Health Check Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Two-phase backend down (errors → suspect → confirmed down), error-source attribution end to end, and a per-listener `on_down: close|drain` session policy.

**Architecture:** The balancer state machine (lock-free, atomics + copy-on-write) gains a suspect state between healthy and down; probes confirm or clear suspects, a cooldown window confirms when no prober runs. Sources ride `ReportError`/`ReportSuccess` into events, logs and `/status`. The app supervisor maps down transitions to close-or-drain via an atomic copy-on-write policy map (never `a.mu` — callbacks fire under it during reload).

**Tech Stack:** Go 1.25, stdlib only, `go test` (no external test deps beyond existing goleak).

**Spec:** `docs/superpowers/specs/2026-09-22-udpshunt-health-design.md`

**Planned deviation from spec §4.3 (approved shape, refined plumbing):** the state-change callback is `func(t balancer.Transition)` where `Transition` carries `{Addr, From, To, Source, LastSource, Errors}` instead of `func(addr string, from, to State)`. Same transitions, but events get attribution without a re-entrant `Snapshot()` inside the callback (which risks recursion and lock-order trouble). Everything else follows the spec.

## Global Constraints

- Go 1.25 (`go.mod`); stdlib only — no new dependencies.
- All code and comments in English; no Chinese in generated code (repo rule).
- Commit messages: conventional subject + markdown unordered-list body, one bullet per change; NO Co-Authored-By lines (user rule overrides default attribution).
- Test command: `go test ./...` from repo root (`D:\Development\MyWorkspace\github\fetaoily\udpshunt`). Tests are pure Go — no cgo needed.
- Linux-only E2E tests carry `runtime.GOOS == "windows"` skips and self-skip; do not remove the skips.
- Hot path (`ReportError`/`ReportSuccess`/`Pick`/`Snapshot`) stays lock-free: new backend fields must be atomics.
- State-change callbacks fire: after the state flip, exactly once per transition (CAS winner), after any pool read completes, and never while holding `a.mu`.

## Source-string contract (used across all tasks)

Defined once in `internal/balancer` (Task 1):

```go
const (
	SrcProbe         = "probe"         // active health-check probe
	SrcReply         = "reply"         // relayed backend reply (real traffic)
	SrcUpstreamWrite = "upstream_write"// session-socket write to backend failed
	SrcDial          = "dial"          // DialUDP for a new session failed
	SrcRelayRead     = "relay_read"    // session-socket read from backend failed
	SrcWindow        = "window"        // passive suspect window confirmed the down
)
```

`Transition.Source` meaning per transition:
- `Healthy→Suspect`: the error source that completed the streak.
- `Suspect→Down`: `SrcProbe` or `SrcWindow` (the confirmer).
- `Suspect→Healthy` (cleared): the success source (`SrcProbe`/`SrcReply`).
- `Down→Healthy`: `""` (not attributed).

---

### Task 1: Balancer suspect state machine + API change, all callers compiling, suite green

This is the seam task: the balancer API change breaks `internal/health`, `internal/listener` and `cmd/udpshunt` call sites, and changes down timing semantics for several existing tests. All of it lands in ONE commit so every commit builds and passes.

**Files:**
- Modify: `internal/balancer/balancer.go`
- Modify: `internal/balancer/balancer_test.go` (rewrite ~8 tests, add 5)
- Modify: `internal/health/health.go:72-76` (source tags only)
- Modify: `internal/health/health_test.go:110-118` (one test)
- Modify: `internal/listener/listener.go:190,228,277,290,298` (source tags; G4 removal is Task 2)
- Modify: `internal/listener/integration_test.go:85` (deadline 8s→15s)
- Modify: `cmd/udpshunt/app.go:130-139` (callback adapter only)

**Interfaces (produced for later tasks):**

```go
type State uint8
const ( StateHealthy State = iota; StateSuspect; StateDown )

type Transition struct {
	Addr       string
	From, To   State
	Source     string // per-transition meaning documented above
	LastSource string // most recent error source at transition time ("" if none)
	Errors     int64  // errCount at transition time
}

func (b *Balancer) ReportError(addr, source string)
func (b *Balancer) ReportSuccess(addr, source string)
func (b *Balancer) SetOnStateChange(fn func(t Transition))

type BackendState struct {
	Addr            string
	Healthy         bool // true while suspect (suspect backends serve traffic)
	Suspect         bool
	SuspectSince    int64  // unix nanos, 0 when not suspect
	ErrCount        int64
	LastErrorSource string
	DownConfirmBy   string // "probe" | "window", set when down
}
```

- [ ] **Step 1: Write the new balancer tests (failing)**

Add to `internal/balancer/balancer_test.go`:

```go
func stateOf(b *Balancer, addr string) BackendState {
	for _, st := range b.Snapshot() {
		if st.Addr == addr {
			return st
		}
	}
	return BackendState{}
}

func TestSuspectClearedByReply(t *testing.T) {
	b := New([]string{"a:1"}, Options{Fall: 1})
	b.ReportError("a:1", SrcRelayRead)
	if st := stateOf(b, "a:1"); !st.Suspect || !st.Healthy {
		t.Fatalf("want suspect+healthy after fall errors, got %+v", st)
	}
	b.ReportSuccess("a:1", SrcReply)
	if st := stateOf(b, "a:1"); st.Suspect || st.ErrCount != 0 {
		t.Fatalf("reply must clear suspect and errors, got %+v", st)
	}
}

func TestDataErrorsDoNotConfirmDown(t *testing.T) {
	b := New([]string{"a:1"}, Options{Fall: 1, ActiveChecks: true, Cooldown: time.Hour})
	b.ReportError("a:1", SrcDial) // fall=1: suspect
	for i := 0; i < 10; i++ {
		b.ReportError("a:1", SrcRelayRead) // data-path errors never confirm
	}
	st := stateOf(b, "a:1")
	if !st.Healthy || !st.Suspect {
		t.Fatalf("data-path errors must not confirm down, got %+v", st)
	}
	if st.LastErrorSource != SrcRelayRead {
		t.Fatalf("LastErrorSource = %q, want %q", st.LastErrorSource, SrcRelayRead)
	}
}

func TestProbeErrorConfirmsDown(t *testing.T) {
	b := New([]string{"a:1"}, Options{Fall: 1, Rise: 1, ActiveChecks: true, Cooldown: time.Hour})
	b.ReportError("a:1", SrcDial)  // suspect
	b.ReportError("a:1", SrcProbe) // confirm
	st := stateOf(b, "a:1")
	if st.Healthy {
		t.Fatal("probe error while suspect must confirm down")
	}
	if st.DownConfirmBy != SrcProbe {
		t.Fatalf("DownConfirmBy = %q, want %q", st.DownConfirmBy, SrcProbe)
	}
}

func TestSuspectBackendStillPicked(t *testing.T) {
	b := New([]string{"a:1"}, Options{Fall: 1, Cooldown: time.Hour})
	b.ReportError("a:1", SrcDial) // suspect; cooldown 1h: no window confirm
	for i := 0; i < 5; i++ {
		if got := b.Pick(""); got != "a:1" {
			t.Fatalf("suspect backend must stay pickable, got %q", got)
		}
	}
}

func TestSuspectWindowConfirmTransition(t *testing.T) {
	b := New([]string{"a:1"}, Options{Fall: 1, Cooldown: 25 * time.Millisecond})
	var mu sync.Mutex
	var ts []Transition
	b.SetOnStateChange(func(t Transition) {
		mu.Lock()
		ts = append(ts, t)
		mu.Unlock()
	})
	b.ReportError("a:1", SrcDial) // H->S
	time.Sleep(60 * time.Millisecond)
	b.Snapshot() // window confirm S->D fires here
	time.Sleep(60 * time.Millisecond)
	b.Snapshot() // cooldown recovery D->H fires here
	mu.Lock()
	defer mu.Unlock()
	want := []struct{ from, to State; source string }{
		{StateHealthy, StateSuspect, SrcDial},
		{StateSuspect, StateDown, SrcWindow},
		{StateDown, StateHealthy, ""},
	}
	if len(ts) != len(want) {
		t.Fatalf("transitions = %+v, want %d", ts, len(want))
	}
	for i, w := range want {
		if ts[i].From != w.from || ts[i].To != w.to || ts[i].Source != w.source {
			t.Fatalf("transition %d = %+v, want %+v", i, ts[i], w)
		}
	}
}
```

- [ ] **Step 2: Run the new tests to verify they fail**

Run: `go test ./internal/balancer/ -run 'Suspect|ProbeError|DataErrors' -v`
Expected: FAIL (undefined: SrcRelayRead, wrong signature for ReportError, etc.)

- [ ] **Step 3: Rewrite the semantics-affected existing balancer tests**

In `internal/balancer/balancer_test.go`, replace these tests wholesale (same names, new bodies):

```go
func TestPassiveDownAndCooldown(t *testing.T) {
	b := New([]string{"a:1", "b:1"}, Options{Cooldown: 25 * time.Millisecond})
	for i := 0; i < 3; i++ { // fall default 3
		b.ReportError("b:1", SrcRelayRead)
	}
	if st := stateOf(b, "b:1"); !st.Healthy || !st.Suspect {
		t.Fatalf("fall errors must suspect, not down: %+v", st)
	}
	time.Sleep(60 * time.Millisecond) // > Cooldown: window confirm on next view
	if st := stateOf(b, "b:1"); st.Healthy {
		t.Fatal("window elapsed with no liveness: b:1 should be down")
	}
	time.Sleep(60 * time.Millisecond) // cooldown recovery
	if st := stateOf(b, "b:1"); !st.Healthy {
		t.Fatal("cooldown elapsed: b:1 should be healthy again")
	}
	if got := b.Pick(""); got == "" {
		t.Fatal("pick must work after cooldown")
	}
}

func TestConfigurableFall(t *testing.T) {
	b := New([]string{"a:1"}, Options{Fall: 1, Cooldown: 20 * time.Millisecond})
	b.ReportError("a:1", SrcDial)
	if st := stateOf(b, "a:1"); !st.Suspect {
		t.Fatal("fall=1 must suspect on first error")
	}
	time.Sleep(40 * time.Millisecond)
	if st := stateOf(b, "a:1"); st.Healthy {
		t.Fatal("window confirm must down the backend after cooldown")
	}
}

func TestActiveRiseRecovery(t *testing.T) {
	b := New([]string{"a:1"}, Options{Fall: 1, Rise: 2, ActiveChecks: true, Cooldown: time.Hour})
	b.ReportError("a:1", SrcDial)  // suspect
	b.ReportError("a:1", SrcProbe) // confirmed down
	if stateOf(b, "a:1").Healthy {
		t.Fatal("should be down")
	}
	b.ReportSuccess("a:1", SrcProbe) // rise 1/2
	if stateOf(b, "a:1").Healthy {
		t.Fatal("must stay down until rise successes reached")
	}
	b.ReportSuccess("a:1", SrcProbe) // rise 2/2
	if !stateOf(b, "a:1").Healthy {
		t.Fatal("rise reached: should be up")
	}
}

func TestActiveModeIgnoresCooldown(t *testing.T) {
	b := New([]string{"a:1"}, Options{Fall: 1, Rise: 5, ActiveChecks: true, Cooldown: time.Millisecond})
	b.ReportError("a:1", SrcDial)  // suspect
	b.ReportError("a:1", SrcProbe) // down; window path must never run (active)
	time.Sleep(20 * time.Millisecond)
	for i := 0; i < 10; i++ {
		b.Pick("") // would recover via cooldown in passive mode
	}
	if stateOf(b, "a:1").Healthy {
		t.Fatal("active mode: recovery only via rise, never cooldown or window")
	}
}

func TestSourceHashSkipsUnhealthy(t *testing.T) {
	b := New([]string{"a:1", "b:1"}, Options{Balance: "source_hash", Fall: 1, Cooldown: 20 * time.Millisecond})
	victim := b.Pick("10.0.0.7")
	b.ReportError(victim, SrcDial) // suspect; still pickable until window elapses
	time.Sleep(40 * time.Millisecond)
	for i := 0; i < 3; i++ {
		if got := b.Pick("10.0.0.7"); got == victim {
			t.Fatal("down backend must not be picked")
		}
	}
}

func TestSetHealthLive(t *testing.T) {
	b := New([]string{"a:1"}, Options{Cooldown: time.Hour}) // passive defaults
	b.SetHealth(true, 3, 1) // active, fall 1
	b.ReportError("a:1", SrcDial)  // suspect
	b.ReportError("a:1", SrcProbe) // confirmed down
	if stateOf(b, "a:1").Healthy {
		t.Fatal("live-updated fall=1 must down on suspect+probe error")
	}
	for i := 0; i < 10; i++ {
		b.Pick("") // active now: cooldown must not recover
	}
	if stateOf(b, "a:1").Healthy {
		t.Fatal("live-updated active mode must ignore cooldown")
	}
}

func TestUpdateKeepsStateAndAddsHealthy(t *testing.T) {
	b := New([]string{"a:1", "b:1"}, Options{Fall: 1, ActiveChecks: true, Cooldown: time.Hour})
	b.ReportError("a:1", SrcDial)  // suspect
	b.ReportError("a:1", SrcProbe) // down (survives Update)
	b.Update([]string{"a:1", "c:1"}) // drop b:1, add c:1
	st := b.Snapshot()
	if len(st) != 2 {
		t.Fatalf("want 2 backends, got %d", len(st))
	}
	byAddr := map[string]bool{}
	for _, s := range st {
		byAddr[s.Addr] = s.Healthy
	}
	if byAddr["a:1"] { // still down, state preserved
		t.Fatal("a:1 state must survive Update")
	}
	if !byAddr["c:1"] {
		t.Fatal("new backend c:1 must start healthy")
	}
}

func TestPassiveCooldownRecoveryFiresOnState(t *testing.T) {
	// Full passive cycle observed through Pick/Snapshot callbacks:
	// H->S (error), S->D (window), D->H (cooldown recovery).
	b := New([]string{"b:1"}, Options{Fall: 1, Cooldown: 25 * time.Millisecond})
	var mu sync.Mutex
	var ts []Transition
	b.SetOnStateChange(func(t Transition) {
		mu.Lock()
		ts = append(ts, t)
		mu.Unlock()
	})
	b.ReportError("b:1", SrcDial) // H->S
	time.Sleep(60 * time.Millisecond)
	if b.Pick("") != "b:1" {
		t.Fatal("pick must still serve the suspect backend")
	}
	time.Sleep(60 * time.Millisecond)
	if b.Pick("") != "b:1" {
		t.Fatal("pick must serve the backend again after cooldown")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(ts) != 3 || ts[1].To != StateDown || ts[1].Source != SrcWindow || ts[2].To != StateHealthy {
		t.Fatalf("want [H->S, S->D window, D->H], got %+v", ts)
	}
}

func TestOnStateChangeBothDirections(t *testing.T) {
	b := New([]string{"a:1"}, Options{Fall: 1, Rise: 1, ActiveChecks: true})
	var mu sync.Mutex
	var events []State
	b.SetOnStateChange(func(t Transition) {
		mu.Lock()
		events = append(events, t.To)
		mu.Unlock()
	})
	b.ReportError("a:1", SrcDial)  // -> suspect
	b.ReportError("a:1", SrcProbe) // -> down
	b.ReportSuccess("a:1", SrcProbe) // rise=1 -> up
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		done := len(events) == 3
		mu.Unlock()
		if done {
			mu.Lock()
			want := []State{StateSuspect, StateDown, StateHealthy}
			for i := range want {
				if events[i] != want[i] {
					t.Fatalf("want %v, got %v", want, events)
				}
			}
			mu.Unlock()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("expected 3 state-change events, got %v", events)
}

func TestFailOpenAlternatesAcrossAllDownBackends(t *testing.T) {
	b := New([]string{"a:1", "b:1"}, Options{Fall: 2, ActiveChecks: true, Cooldown: time.Hour})
	for _, addr := range []string{"a:1", "b:1"} {
		b.ReportError(addr, SrcDial)  // errCount=1
		b.ReportError(addr, SrcProbe) // =fall: suspect... probe while NOT yet suspect
	}
	// fall=2: the probe error above only completed the streak (suspect), it
	// did not confirm — confirming needs a probe error WHILE suspect.
	got := map[string]int{}
	for i := 0; i < 6; i++ {
		got[b.Pick("")]++
	}
	if got["a:1"] == 0 || got["b:1"] == 0 {
		t.Fatalf("fail-open pinned to one backend: %v", got)
	}
}
```

Note the comment inside `TestFailOpenAlternatesAcrossAllDownBackends`: with `Fall: 2`, `ReportError(dial)` then `ReportError(probe)` reaches the streak but does not confirm. To keep the test's intent (both backends down, fail-open alternates), fix the loop to add one confirming error per backend after the two streak errors — use this final body instead of the one above:

```go
func TestFailOpenAlternatesAcrossAllDownBackends(t *testing.T) {
	b := New([]string{"a:1", "b:1"}, Options{Fall: 2, ActiveChecks: true, Cooldown: time.Hour})
	for _, addr := range []string{"a:1", "b:1"} {
		b.ReportError(addr, SrcDial)  // errCount=1
		b.ReportError(addr, SrcDial)  // =fall: suspect
		b.ReportError(addr, SrcProbe) // confirm: down
	}
	got := map[string]int{}
	for i := 0; i < 6; i++ {
		got[b.Pick("")]++
	}
	if got["a:1"] == 0 || got["b:1"] == 0 {
		t.Fatalf("fail-open pinned to one backend: %v", got)
	}
}
```

Also update `TestConcurrentReportsKeepConsistentState`: every `b.ReportError(addr)` becomes `b.ReportError(addr, SrcRelayRead)` and every `b.ReportSuccess(addr)` becomes `b.ReportSuccess(addr, SrcReply)`.

- [ ] **Step 4: Implement the balancer state machine**

In `internal/balancer/balancer.go`:

4a. Add the State type, source constants and Transition struct (exact code from the Interfaces block above, with doc comments: `State` — "coarse health state of a backend; StateHealthy covers suspect-free serving", `Transition` — per-transition Source semantics from the contract section).

4b. Extend `backend`:

```go
type backend struct {
	addr         string
	healthy      atomic.Bool
	errCount     atomic.Int64
	successCount atomic.Int64
	downSince    atomic.Int64  // unix nanoseconds
	suspectSince atomic.Int64  // unix nanoseconds; 0 = not suspect
	lastErrSrc   atomic.Pointer[string]
	downConfirm  atomic.Pointer[string] // "probe" | "window", set on down entry
}
```

4c. Replace `SetOnStateChange` and `fire`:

```go
// SetOnStateChange registers a callback fired on every state transition
// (Healthy<->Suspect<->Down). The callback runs outside any lock, exactly
// once per transition.
func (b *Balancer) SetOnStateChange(fn func(t Transition)) {
	b.onState.Store(&fn)
}

func (b *Balancer) fire(fn *func(t Transition), t Transition) {
	if fn != nil && *fn != nil {
		(*fn)(t)
	}
}

func (b *Balancer) onStateFn() *func(t Transition) { return b.onState.Load() }
```

And the `Balancer.onState` field becomes `atomic.Pointer[func(t Transition)]`.

4d. Replace `ReportError`:

```go
// ReportError records a failure against addr from source (Src* constants).
// Fall errors enter the suspect state; only a probe-source error while
// suspect confirms the down — data-path bursts alone never kill a backend
// (spec: alive = any liveness evidence; dead = errors + independent
// confirmation with no evidence in between).
func (b *Balancer) ReportError(addr, source string) {
	o := b.opts.Load()
	p := b.pool.Load()
	if o == nil || p == nil {
		return
	}
	be := p.byAddr[addr]
	if be == nil {
		return
	}
	n := be.errCount.Add(1)
	be.successCount.Store(0)
	be.lastErrSrc.Store(&source)
	if !be.healthy.Load() {
		return
	}
	if be.suspectSince.Load() == 0 {
		if n >= int64(o.Fall) {
			// Exactly-once entry per streak: the CAS from 0 is the single
			// entry point even if SetHealth moved Fall mid-streak.
			now := time.Now().UnixNano()
			if be.suspectSince.CompareAndSwap(0, now) {
				b.fire(b.onStateFn(), Transition{
					Addr: addr, From: StateHealthy, To: StateSuspect,
					Source: source, LastSource: source, Errors: n,
				})
			}
		}
		return
	}
	// Suspect: only the prober may confirm the down. Metadata lands before
	// the flip; the CAS winner fires, so the transition fires exactly once.
	// suspectSince is cleared AFTER the flip: Suspect is only meaningful
	// while healthy, and clearing first would let a concurrent error
	// re-enter suspect on a backend already being downed.
	if source == SrcProbe {
		confirm := SrcProbe
		be.downConfirm.Store(&confirm)
		be.downSince.Store(time.Now().UnixNano())
		if be.healthy.CompareAndSwap(true, false) {
			be.suspectSince.Store(0)
			b.fire(b.onStateFn(), Transition{
				Addr: addr, From: StateSuspect, To: StateDown,
				Source: SrcProbe, LastSource: source, Errors: n,
			})
		}
	}
}
```

4e. Replace the healthy branch of `ReportSuccess`:

```go
	if be.healthy.Load() {
		be.errCount.Store(0)
		// Swap makes the clear exactly-once: only the caller that observed
		// a nonzero suspect stamp fires the Suspect->Healthy transition.
		if prev := be.suspectSince.Swap(0); prev != 0 {
			b.fire(b.onStateFn(), Transition{
				Addr: addr, From: StateSuspect, To: StateHealthy, Source: source,
			})
		}
		return
	}
```

(Keep the existing down/rise branch below it logically unchanged, but its `fire` call — `b.fire(b.onStateFn(), addr, true)` — must become `b.fire(b.onStateFn(), Transition{Addr: addr, From: StateDown, To: StateHealthy})`: the compiler forces this once `fire`'s signature changes.)

4f. Rework `usable` + collectors for the passive window confirm. Replace `usable`/`usableTrack`/`fireRecovered` with:

```go
// windowConfirm flips a suspect backend (passive mode only) whose suspect
// stamp is older than Cooldown with no liveness since. The caller collects
// the transition and fires it after its pool read completes. Reports whether
// the flip happened (CAS winner).
func (b *Balancer) windowConfirm(be *backend, o *Options) (down bool, t Transition) {
	if o.ActiveChecks {
		return false, Transition{} // the prober confirms or clears instead
	}
	s := be.suspectSince.Load()
	if s == 0 || time.Since(time.Unix(0, s)) < o.Cooldown {
		return false, Transition{}
	}
	confirm := SrcWindow
	be.downConfirm.Store(&confirm)
	be.downSince.Store(time.Now().UnixNano())
	if be.healthy.CompareAndSwap(true, false) {
		be.suspectSince.Store(0)
		last := ""
		if v := be.lastErrSrc.Load(); v != nil {
			last = *v
		}
		return true, Transition{
			Addr: be.addr, From: StateSuspect, To: StateDown,
			Source: SrcWindow, LastSource: last, Errors: be.errCount.Load(),
		}
	}
	return false, Transition{}
}

// usable reports whether the backend may serve traffic, applying passive
// recovery. A window-confirmed backend reports (and becomes) down exactly as
// the next Pick would treat it. Transitions are collected by the caller and
// fired after the pool read (same discipline as recovery).
func (b *Balancer) usable(be *backend, o *Options, fired *[]Transition) bool {
	if be.healthy.Load() {
		if down, t := b.windowConfirm(be, o); down {
			*fired = append(*fired, t)
			return false
		}
		return true
	}
	if o.ActiveChecks {
		return false // recovery only via ReportSuccess rise path
	}
	if time.Since(time.Unix(0, be.downSince.Load())) >= o.Cooldown {
		be.errCount.Store(0)
		if be.healthy.CompareAndSwap(false, true) {
			*fired = append(*fired, Transition{Addr: be.addr, From: StateDown, To: StateHealthy})
			return true
		}
		return false
	}
	return false
}
```

Keep the metadata-before-flip and CAS-winner comments from the original `usable` (copy them into the new bodies). Update `pick`'s `usableTrack` call sites to pass `&recovered` (rename the variable `recovered` to `fired`), and replace `fireRecovered` with:

```go
// fireTransitions publishes pool-read transitions (passive recoveries and
// window confirms) after that read completes and in pool order.
func (b *Balancer) fireTransitions(fn *func(t Transition), ts []Transition) {
	for _, t := range ts {
		b.fire(fn, t)
	}
}
```

Update `Pick` and `Snapshot` accordingly (`fireRecovered` → `fireTransitions`).

4g. Extend `Snapshot`'s output:

```go
	suspect := be.suspectSince.Load() != 0 && be.healthy.Load()
	last, confirm := "", ""
	if v := be.lastErrSrc.Load(); v != nil {
		last = *v
	}
	if v := be.downConfirm.Load(); v != nil {
		confirm = *v
	}
	out = append(out, BackendState{
		Addr: be.addr, Healthy: be.healthy.Load(), Suspect: suspect,
		SuspectSince: be.suspectSince.Load(), ErrCount: be.errCount.Load(),
		LastErrorSource: last, DownConfirmBy: confirm,
	})
```

(Keep the existing `usable` call for recovery/window reflection in the view.)

- [ ] **Step 5: Update callers mechanically so the build passes**

- `internal/health/health.go:73` → `bal.ReportError(addr, balancer.SrcProbe)`; `:75` → `bal.ReportSuccess(addr, balancer.SrcProbe)`.
- `internal/listener/listener.go:190` → `l.bal.ReportError(s.Backend, balancer.SrcUpstreamWrite)`
- `internal/listener/listener.go:228` → `l.bal.ReportError(backendAddr, balancer.SrcDial)`
- `internal/listener/listener.go:277` → `l.bal.ReportError(s.Backend, balancer.SrcRelayRead)`
- `internal/listener/listener.go:298` → `l.bal.ReportSuccess(s.Backend, balancer.SrcReply)`
- `cmd/udpshunt/app.go:130-139` callback adapter (behavior identical to today; Task 4 replaces it):

```go
	bal.SetOnStateChange(func(t balancer.Transition) {
		healthy := t.To != balancer.StateDown
		a.met.SetBackendHealthy(lc.Name, t.Addr, healthy)
		if healthy {
			a.events.Add("backend_up", lc.Name+" "+t.Addr)
			return
		}
		n := a.mgr.CloseBackend(lc.Name, t.Addr)
		a.events.Add("backend_down", fmt.Sprintf("%s %s closed=%d", lc.Name, t.Addr, n))
		a.logger.Info("backend marked down, sessions closed", "listener", lc.Name, "backend", t.Addr, "sessions", n)
	})
```

- `internal/health/health_test.go` `TestDNSProbeRespondsToAnyReply` push-down block (lines ~114-118) becomes:

```go
	// push it down first (fall=2: two errors suspect, a probe error
	// confirms), then prove the dns probe recovers it
	bal.ReportError(addr, balancer.SrcDial)
	bal.ReportError(addr, balancer.SrcDial)
	bal.ReportError(addr, balancer.SrcProbe)
	if healthy(bal, addr) {
		t.Fatal("should be down after suspect + probe confirm")
	}
```

- `internal/listener/integration_test.go:85`: change `deadline := time.Now().Add(8 * time.Second)` to `15 * time.Second` and extend the comment above it: passive down now needs fall errors (suspect) PLUS the default 10s cooldown window before Snapshot confirms.

- [ ] **Step 6: Run the full suite**

Run: `go test ./...`
Expected: PASS everywhere (Windows skips apply). If `internal/metrics` fails with `fork/exec ... Access is denied`, re-run `go test ./internal/metrics/` — known flaky AV interception on this machine, unrelated to changes.

- [ ] **Step 7: Commit**

```bash
git add internal/balancer/ internal/health/ internal/listener/ cmd/udpshunt/app.go
git commit -F - <<'EOF'
feat(balancer): two-phase down with suspect state and error attribution

- errors reaching fall enter a suspect state instead of downing the backend; suspect backends keep serving (pickable, Healthy=true)
- only a probe-source error confirms suspect->down in active mode; a cooldown window confirms it in passive mode (Pick/Snapshot pool-read discipline)
- any ReportSuccess clears suspect (Suspect->Healthy), zeroing the error streak
- ReportError/ReportSuccess carry a source (probe/reply/upstream_write/dial/relay_read); Snapshot exposes Suspect/ErrCount/LastErrorSource/DownConfirmBy
- state-change callback becomes func(Transition) carrying from/to/source/errors for event attribution
- rewrite affected balancer/health/listener tests for the new semantics; extend the passive failover E2E deadline past the default 10s window
EOF
```

---

### Task 2: Listener G4 fix — client-direction write failures no longer count against the backend

**Files:**
- Modify: `internal/listener/listener.go:285-293`
- Test: `internal/listener/integration_test.go` (append)

**Interfaces:**
- Consumes: `balancer.SrcRelayRead`, `BackendState.LastErrorSource` (Task 1).
- Produces: listener behavior — client write failures report nothing to the balancer (no test seam needed downstream).

- [ ] **Step 1: Write the attribution + regression tests (failing for the attribution one)**

Append to `internal/listener/integration_test.go`:

```go
func stateOf(bal *balancer.Balancer, addr string) balancer.BackendState {
	for _, st := range bal.Snapshot() {
		if st.Addr == addr {
			return st
		}
	}
	return balancer.BackendState{}
}

// TestRelayReadErrorAttributed: a session socket read error on a dead
// backend surfaces as LastErrorSource=relay_read (Linux: ICMP-derived).
func TestRelayReadErrorAttributed(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows UDP sockets never surface ICMP port-unreachable as read errors")
	}
	dead := deadBackendAddr(t)
	lc := config.Listener{Name: "attr", Bind: "127.0.0.1:0", Backends: []string{dead}, SessionTimeout: config.Duration(time.Minute)}
	l, bal, _ := newStack(t, lc, 0)

	client := testClient(t)
	for i := 0; i < 3; i++ { // feed errors until suspect so the streak is recorded
		_, _ = client.WriteToUDP([]byte("ping"), l.Addr())
		time.Sleep(50 * time.Millisecond)
	}
	eventually(t, 5*time.Second, "relay_read error attributed", func() bool {
		return stateOf(bal, dead).LastErrorSource == balancer.SrcRelayRead
	})
}
```

(`TestRelayReadErrorAttributed` passes already after Task 1 — it locks the attribution in. The G4 change itself has no portable failure-inducing test: a `WriteToUDP` to an arbitrary client address virtually never fails synchronously on either OS. Verified by inspection + code comment.)

- [ ] **Step 2: Run the new test**

Run: `go test ./internal/listener/ -run TestRelayReadErrorAttributed -v`
Expected: PASS (Linux semantics; skips on Windows). CI (Linux) runs it for real.

- [ ] **Step 3: Apply the G4 fix**

In `internal/listener/listener.go`, replace the client-write failure branch of `downstream` (currently lines 285-292):

```go
		if werr != nil {
			if errors.Is(werr, net.ErrClosed) {
				return
			}
			// Client-direction failure (typically a gone NAT mapping): the
			// client is unreachable, not the backend — never bill it to
			// backend health (spec G4). Drop the session; nothing else.
			l.logger.Debug("client write failed, dropping session", "client", s.Client, "err", werr)
			l.mgr.Remove(l.name, s.Client)
			return
		}
```

(The `l.met.BackendError(s.Backend)` and `l.bal.ReportError(...)` calls are removed with it.)

- [ ] **Step 4: Run the listener package**

Run: `go test ./internal/listener/ ./internal/health/`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/listener/
git commit -F - <<'EOF'
fix(listener): stop billing client-direction write failures to backend health

- downstream WriteToUDP failures to the client no longer call ReportError or count as backend errors; the session is dropped with a debug log
- client NAT loss is routine and must not feed the health state machine
- add relay_read attribution regression test (Linux CI)
EOF
```

---

### Task 3: Config — per-listener `on_down` policy

**Files:**
- Modify: `internal/config/config.go` (Listener struct ~line 43, validation ~line 263)
- Test: `internal/config/config_test.go` (append)

**Interfaces:**
- Produces: `config.Listener.OnDown string` (yaml `on_down`; "" | "close" | "drain"; "" means close). Task 4 consumes it.

- [ ] **Step 1: Write the failing validation test**

Append to `internal/config/config_test.go` (reuse the file's existing table-test idiom and its `baseListener` fixture if present — the file already has listener table tests around line 391; if `baseListener` is named differently, use the local name):

```go
func TestOnDownValidation(t *testing.T) {
	cases := map[string]struct {
		yaml     string
		wantErr  bool
		wantPol  string
	}{
		"absent defaults to close": {yaml: baseListener, wantErr: false, wantPol: ""},
		"close accepted":           {yaml: baseListener + "    on_down: close\n", wantErr: false, wantPol: "close"},
		"drain accepted":           {yaml: baseListener + "    on_down: drain\n", wantErr: false, wantPol: "drain"},
		"invalid rejected":         {yaml: baseListener + "    on_down: nuke\n", wantErr: true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			c, err := Load(writeTemp(t, tc.yaml))
			if tc.wantErr {
				if err == nil {
					t.Fatal("invalid on_down must be rejected")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got := c.Listeners[0].OnDown; got != tc.wantPol {
				t.Fatalf("OnDown = %q, want %q", got, tc.wantPol)
			}
		})
	}
}
```

Adapt `writeTemp` to the file's actual temp-config helper name; if the file has no such helper, write one:

```go
func writeTemp(t *testing.T, src string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "cfg.yaml")
	if err := os.WriteFile(p, []byte(src), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./internal/config/ -run TestOnDownValidation -v`
Expected: FAIL (unknown field accepted silently / OnDown missing)

- [ ] **Step 3: Implement**

In `internal/config/config.go`, add to `Listener` (after `ReadBuffer`):

```go
	OnDown string `yaml:"on_down"` // close (default) | drain: keep live sessions until timeout on down
```

In the listener validation loop (after the `read_buffer` check), add:

```go
		switch l.OnDown {
		case "", "close", "drain":
		default:
			return fmt.Errorf("listeners[%d] (%s): on_down must be close or drain, got %q", i, l.Name, l.OnDown)
		}
```

- [ ] **Step 4: Run config tests**

Run: `go test ./internal/config/`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/config/
git commit -F - <<'EOF'
feat(config): per-listener on_down session policy

- new optional listeners[].on_down: close (default, current behavior) or drain
- drain keeps a downed backend's live sessions until session_timeout instead of closing them
- validation rejects any other value
EOF
```

---

### Task 4: App wiring — drain policy, attribution events, /status fields

**Files:**
- Modify: `cmd/udpshunt/app.go` (App struct, NewApp, startListenerLocked, updateListenerLocked, stopListenerLocked, Status)
- Modify: `internal/admin/admin.go:102-106` (BackendStatus fields)
- Test: `cmd/udpshunt/app_test.go` (append)

**Interfaces:**
- Consumes: `balancer.Transition/State/Src*` (Task 1), `config.Listener.OnDown` (Task 3), `session.Manager.BackendCount(listener, addr string) int64` (existing).
- Produces: `/status` backends gain `suspect`, `err_count`, `last_error`, `confirmed_by`; events `backend_suspect` / `backend_suspect_cleared` / extended `backend_down`.

- [ ] **Step 1: Write the failing app tests**

Append to `cmd/udpshunt/app_test.go`:

```go
// startEchoCtl is an echo backend the test can kill on demand (unlike the
// t.Cleanup-managed startEcho).
func startEchoCtl(t *testing.T) (addr string, stop func()) {
	t.Helper()
	pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, 65536)
		for {
			n, from, err := pc.ReadFromUDP(buf)
			if err != nil {
				return
			}
			_, _ = pc.WriteToUDP(buf[:n], from)
		}
	}()
	return pc.LocalAddr().String(), func() { pc.Close(); <-done }
}

func onDownCfg(t *testing.T, backend, onDown string, timeout time.Duration) string {
	t.Helper()
	pol := ""
	if onDown != "" {
		pol = "    on_down: " + onDown + "\n"
	}
	return writeCfg(t, fmt.Sprintf(`
listeners:
  - name: L1
    bind: 127.0.0.1:0
    backends: [%s]
%s    session_timeout: %s
    health_check:
      mode: raw
      payload: "70696e67"
      interval: 100ms
      timeout: 60ms
      rise: 1
      fall: 2
`, backend, pol, timeout))
}

// waitBackendDown polls /status-derived state until addr is down.
func waitBackendDown(t *testing.T, app *App, addr string) admin.Status {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		st := app.Status()
		for _, l := range st.Listeners {
			for _, b := range l.Backends {
				if b.Addr == addr && !b.Healthy {
					return st
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("backend did not go down in time")
	return admin.Status{}
}

func TestOnDownDrainKeepsSessions(t *testing.T) {
	backend, kill := startEchoCtl(t)
	p := onDownCfg(t, backend, "drain", 400*time.Millisecond)
	app, _ := newApp(t, p)
	if err := app.Apply(context.Background(), mustLoad(t, p)); err != nil {
		t.Fatal(err)
	}
	if got, err := roundTripUDP(t, app.listeners["L1"].Addr(), "ping"); err != nil || got != "echo:ping" {
		t.Fatalf("setup round trip: %q %v", got, err)
	}
	kill() // probes start failing: suspect at fall, probe-confirm down

	// app tests bypass main(): no idle reaper runs unless the test starts
	// one. Drain expiry depends on it (session_timeout must be enforced).
	reapCtx, stopReaper := context.WithCancel(context.Background())
	defer stopReaper()
	app.mgr.Start(reapCtx, 20*time.Millisecond)

	st := waitBackendDown(t, app, backend)
	for _, b := range st.Listeners[0].Backends {
		if b.Addr == backend && b.Sessions == 0 {
			t.Fatal("drain must keep live sessions after down")
		}
	}
	// Sessions expire via session_timeout, not by eviction.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && app.mgr.Count() > 0 {
		time.Sleep(50 * time.Millisecond)
	}
	if app.mgr.Count() != 0 {
		t.Fatal("drained sessions must expire via session timeout")
	}
	evs := app.events.List()
	sawDrain := false
	for _, e := range evs {
		if e.Kind == "backend_down" && strings.Contains(e.Detail, "policy=drain") {
			sawDrain = true
		}
	}
	if !sawDrain {
		t.Fatalf("backend_down event must carry policy=drain, events: %+v", evs)
	}
}

func TestOnDownCloseClosesSessions(t *testing.T) {
	backend, kill := startEchoCtl(t)
	p := onDownCfg(t, backend, "", time.Minute) // absent = close
	app, _ := newApp(t, p)
	if err := app.Apply(context.Background(), mustLoad(t, p)); err != nil {
		t.Fatal(err)
	}
	if _, err := roundTripUDP(t, app.listeners["L1"].Addr(), "ping"); err != nil {
		t.Fatal(err)
	}
	kill()
	waitBackendDown(t, app, backend)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && app.mgr.Count() > 0 {
		time.Sleep(20 * time.Millisecond)
	}
	if app.mgr.Count() != 0 {
		t.Fatal("close policy must close sessions on down")
	}
}

func TestOnDownHotReloadSwitchesPolicy(t *testing.T) {
	backend, kill := startEchoCtl(t)
	pDrain := onDownCfg(t, backend, "drain", 30*time.Second)
	app, _ := newApp(t, pDrain)
	if err := app.Apply(context.Background(), mustLoad(t, pDrain)); err != nil {
		t.Fatal(err)
	}
	if _, err := roundTripUDP(t, app.listeners["L1"].Addr(), "ping"); err != nil {
		t.Fatal(err)
	}
	// Same bind: reload takes the updateListenerLocked path (no restart).
	pClose := onDownCfg(t, backend, "close", 30*time.Second)
	if err := app.Apply(context.Background(), mustLoad(t, pClose)); err != nil {
		t.Fatal(err)
	}
	kill()
	waitBackendDown(t, app, backend)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && app.mgr.Count() > 0 {
		time.Sleep(20 * time.Millisecond)
	}
	if app.mgr.Count() != 0 {
		t.Fatal("policy must follow the reloaded config (close), not the startup config")
	}
}

func TestStatusAttributionFields(t *testing.T) {
	dead := deadAddr(t)
	p := onDownCfg(t, dead, "", time.Minute)
	app, _ := newApp(t, p)
	if err := app.Apply(context.Background(), mustLoad(t, p)); err != nil {
		t.Fatal(err)
	}
	st := waitBackendDown(t, app, dead)
	var b admin.BackendStatus
	for _, bb := range st.Listeners[0].Backends {
		if bb.Addr == dead {
			b = bb
		}
	}
	if b.DownConfirmBy != balancer.SrcProbe {
		t.Fatalf("confirmed_by = %q, want probe", b.DownConfirmBy)
	}
	if b.ErrCount < 3 { // fall 2 + 1 confirming probe failure
		t.Fatalf("err_count = %d, want >= 3", b.ErrCount)
	}
	// Events: suspect then attributed down.
	kinds := map[string]bool{}
	for _, e := range app.events.List() {
		kinds[e.Kind] = true
	}
	if !kinds["backend_suspect"] {
		t.Fatal("backend_suspect event missing")
	}
	sawConfirm := false
	for _, e := range app.events.List() {
		if e.Kind == "backend_down" && strings.Contains(e.Detail, "confirmed_by=probe") {
			sawConfirm = true
		}
	}
	if !sawConfirm {
		t.Fatal("backend_down event must carry confirmed_by=probe")
	}
}
```

Verified helpers and types: `writeCfg`, `newApp`, `mustLoad`, `roundTripUDP`, `deadAddr` all exist in `app_test.go` (see `TestHealthCheckFailoverE2E`); `admin.Event` fields are `Kind`/`Detail` (`admin.go:22-26`) — the assertions above already use them. Add imports as needed (`strings`, `net`, `time`, `admin`, `balancer`, `context` if absent).

- [ ] **Step 2: Run to verify they fail**

Run: `go test ./cmd/udpshunt/ -run 'OnDown|StatusAttribution' -v`
Expected: FAIL (no OnDown wiring, no policy map, events lack fields)

- [ ] **Step 3: Implement admin.BackendStatus fields**

In `internal/admin/admin.go`:

```go
type BackendStatus struct {
	Addr            string `json:"addr"`
	Healthy         bool   `json:"healthy"`
	Suspect         bool   `json:"suspect"`
	Sessions        int64  `json:"sessions"`
	ErrCount        int64  `json:"err_count"`
	LastErrorSource string `json:"last_error,omitempty"`
	DownConfirmBy   string `json:"confirmed_by,omitempty"`
}
```

- [ ] **Step 4: Implement the app policy map and full callback**

In `cmd/udpshunt/app.go`:

4a. Add to `App` (outside the `a.mu` group, with this comment):

```go
	// onDown maps listener name -> on_down policy. Copy-on-write atomic:
	// state-change callbacks fire while Apply/updateListenerLocked holds
	// a.mu (bal.Snapshot may flip a suspect window mid-reload), so the
	// callback MUST read policies here and never take a.mu.
	onDown atomic.Pointer[map[string]string]
```

Add `"sync/atomic"` to imports if absent.

4b. Helper (place near startListenerLocked):

```go
// setOnDownLocked stores name's policy in the copy-on-write map. Callers
// hold a.mu (single writer); readers are state-change callbacks.
func (a *App) setOnDownLocked(name, policy string) {
	m := *a.onDown.Load()
	next := make(map[string]string, len(m)+1)
	for k, v := range m {
		next[k] = v
	}
	if policy == "" {
		policy = "close"
	}
	next[name] = policy
	a.onDown.Store(&next)
}

func (a *App) deleteOnDownLocked(name string) {
	m := *a.onDown.Load()
	next := make(map[string]string, len(m))
	for k, v := range m {
		if k != name {
			next[k] = v
		}
	}
	a.onDown.Store(&next)
}
```

Initialize in `NewApp`: `onDown: func() *map[string]string { m := map[string]string{}; return &m }(),` — or simpler, a `newOnDown()` helper; and in `startListenerLocked` call `a.setOnDownLocked(lc.Name, lc.OnDown)`, in `updateListenerLocked` likewise, and `a.deleteOnDownLocked(name)` in `stopListenerLocked`.

4c. Replace the Task 1 adapter callback in `startListenerLocked`:

```go
	bal.SetOnStateChange(func(t balancer.Transition) {
		// Runs on data-path and admin goroutines, and under a.mu during
		// reloads (Snapshot fires passive transitions). No a.mu here.
		a.met.SetBackendHealthy(lc.Name, t.Addr, t.To != balancer.StateDown)
		switch {
		case t.To == balancer.StateSuspect:
			a.events.Add("backend_suspect", fmt.Sprintf(
				"%s %s errors=%d last_error=%s", lc.Name, t.Addr, t.Errors, t.LastSource))
			a.logger.Warn("backend suspect", "listener", lc.Name, "backend", t.Addr,
				"errors", t.Errors, "last_error", t.LastSource)
		case t.To == balancer.StateDown:
			if pol, ok := (*a.onDown.Load())[lc.Name]; ok && pol == "drain" {
				n := a.mgr.BackendCount(lc.Name, t.Addr)
				a.events.Add("backend_down", fmt.Sprintf(
					"%s %s policy=drain draining=%d confirmed_by=%s errors=%d last_error=%s",
					lc.Name, t.Addr, n, t.Source, t.Errors, t.LastSource))
				a.logger.Info("backend marked down, sessions draining", "listener", lc.Name,
					"backend", t.Addr, "draining", n, "confirmed_by", t.Source)
				return
			}
			n := a.mgr.CloseBackend(lc.Name, t.Addr)
			a.events.Add("backend_down", fmt.Sprintf(
				"%s %s closed=%d confirmed_by=%s errors=%d last_error=%s",
				lc.Name, t.Addr, n, t.Source, t.Errors, t.LastSource))
			a.logger.Info("backend marked down, sessions closed", "listener", lc.Name,
				"backend", t.Addr, "sessions", n, "confirmed_by", t.Source)
		case t.From == balancer.StateSuspect && t.To == balancer.StateHealthy:
			a.events.Add("backend_suspect_cleared", fmt.Sprintf("%s %s by=%s", lc.Name, t.Addr, t.Source))
		default: // Down -> Healthy recovery
			a.events.Add("backend_up", lc.Name+" "+t.Addr)
		}
	})
```

4d. Extend the `Status` backend mapping (currently `admin.BackendStatus{Addr: bt.Addr, Healthy: bt.Healthy, Sessions: n}`):

```go
			bs = append(bs, admin.BackendStatus{
				Addr: bt.Addr, Healthy: bt.Healthy, Suspect: bt.Suspect, Sessions: n,
				ErrCount: bt.ErrCount, LastErrorSource: bt.LastErrorSource, DownConfirmBy: bt.DownConfirmBy,
			})
```

- [ ] **Step 5: Run the app tests and the full suite**

Run: `go test ./cmd/udpshunt/ -run 'OnDown|StatusAttribution' -v` then `go test ./...`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add cmd/udpshunt/ internal/admin/
git commit -F - <<'EOF'
feat(app): on_down drain policy, attribution events and status fields

- per-listener on_down: drain keeps a downed backend's sessions until session_timeout; policy read from a copy-on-write atomic map (callbacks fire under a.mu during reloads and must not take it)
- events: backend_suspect (errors + last source), backend_suspect_cleared (by probe/reply), backend_down extended with confirmed_by/errors/last_error and policy=drain draining=N
- /status backends gain suspect, err_count, last_error and confirmed_by
EOF
```

---

### Task 5: Documentation — README + starter config

**Files:**
- Modify: `README.md` (health-check section ~line 263, config reference)
- Modify: `packaging/config/udpshunt.yaml`

**Interfaces:** none (docs only).

- [ ] **Step 1: Update README**

Find the paragraph beginning "A backend marked down stops receiving new sessions and its live sessions are" (~line 263) and replace it with:

```markdown
A backend's health is judged by evidence, not by raw error counts. Errors
(data-path failures or probe timeouts) reaching `fall` put the backend in a
*suspect* state — it keeps serving traffic. Only an independent confirmation
marks it down: the next failed health probe (active mode), or a cooldown
window of silence with no probe reply and no relayed backend reply (passive
mode, no `health_check`). Any real backend reply or successful probe clears
the suspect immediately. Detection of a truly dead backend therefore takes
about one extra `interval` (active) or `cooldown` (passive) compared to a
naive error counter — in exchange, error bursts from busy-but-alive backends
never evict sessions.

Every error carries a source (`probe`, `upstream_write`, `dial`,
`relay_read`); `backend_down` events and `/status` report who confirmed the
down (`confirmed_by`) and the error sources behind it, so a panel is enough
to answer "who killed this backend and why". Client-direction failures (a
gone NAT mapping) never count against a backend.

`on_down` (per listener, default `close`) decides what happens to a downed
backend's live sessions: `close` terminates them immediately; `drain` lets
them run until `session_timeout`, trading up to one timeout of extra
black-hole time on true failures for zero eviction cost on false positives.
Draining sessions still count against `sessions.max` until they expire.
Removing a backend from the config always closes its sessions, regardless of
`on_down`.
```

Also update the listener config reference (the yaml block in the config
section) to show `on_down: close` with a one-line comment, and the `/status`
endpoint description (~line 282) to append: per-backend `suspect`, `err_count`,
`last_error` and `confirmed_by`.

- [ ] **Step 2: Update the starter config**

In `packaging/config/udpshunt.yaml`, inside the `listeners:` entry after
`session_timeout: 60s`, add:

```yaml
    # What happens to a downed backend's live sessions:
    # close (default): terminate them immediately; drain: let them
    # expire via session_timeout.
    on_down: close
```

- [ ] **Step 3: Run the full suite one last time**

Run: `go test ./...`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add README.md packaging/config/udpshunt.yaml
git commit -F - <<'EOF'
docs: two-phase health semantics, attribution and on_down

- README: evidence-based health judgment, suspect state, error sources, confirmed_by attribution
- document listeners[].on_down close|drain and its sessions.max interaction
- starter config gains a commented on_down: close line
EOF
```

---

## Self-review notes

- Spec §4.1–4.3 → Task 1; §5 (+G4) → Tasks 1–2; §6 → Tasks 3–4; §7 → Task 4; §8 → Task 1 (comments and CAS discipline preserved); §9 → tests embedded per task; §10 → Task 5.
- Known untestable portably: the G4 client-write branch (no synchronous failure to induce on either OS) — covered by inspection and a code comment; the a.mu deadlock hazard is covered by the copy-on-write design plus the race detector in `TestOnDownHotReloadSwitchesPolicy` (which exercises Snapshot-under-a.mu via reload), not a dedicated deadlock test.
- `TestFailOpenAlternatesAcrossAllDownBackends` has two candidate bodies in Task 1 Step 3; use the second (final) one — it correctly confirms down after suspect.
- `admin.Event` field names in Task 4 Step 1 assertions (`Kind`/`Text`) must be verified against `internal/admin`'s actual struct before running.
