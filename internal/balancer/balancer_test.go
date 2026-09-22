package balancer

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func stateOf(b *Balancer, addr string) BackendState {
	for _, st := range b.Snapshot() {
		if st.Addr == addr {
			return st
		}
	}
	return BackendState{}
}

func TestRoundRobinSequence(t *testing.T) {
	b := New([]string{"a:1", "b:1", "c:1"}, Options{})
	want := []string{"a:1", "b:1", "c:1", "a:1", "b:1", "c:1"}
	for i, w := range want {
		if got := b.Pick(""); got != w {
			t.Fatalf("pick %d: want %s, got %s", i, w, got)
		}
	}
}

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

func TestLeastSessions(t *testing.T) {
	counts := map[string]*atomic.Int64{
		"a:1": {}, "b:1": {}, "c:1": {},
	}
	b := New([]string{"a:1", "b:1", "c:1"}, Options{
		Balance: "least_sessions",
		Counts:  func(addr string) int64 { return counts[addr].Load() },
	})
	counts["b:1"].Store(5)
	counts["c:1"].Store(9)
	for i := 0; i < 6; i++ { // 6 adds take a:1 from 0 past b:1's 5 (ties keep a:1)
		if got := b.Pick(""); got != "a:1" {
			t.Fatalf("least-loaded is a:1, got %s", got)
		}
		counts["a:1"].Add(1)
	}
	if got := b.Pick(""); got != "b:1" {
		t.Fatalf("after a catches up, b:1 is least loaded, got %s", got)
	}
}

func TestSourceHashDeterministicAndStable(t *testing.T) {
	addrs := []string{"a:1", "b:1", "c:1"}
	b := New(addrs, Options{Balance: "source_hash"})
	first := b.Pick("10.0.0.1")
	for i := 0; i < 5; i++ {
		if got := b.Pick("10.0.0.1"); got != first {
			t.Fatalf("same IP must map to same backend: %s vs %s", got, first)
		}
	}
	// Minimal disruption: removing one backend keeps other keys' mapping.
	saw := map[string]string{}
	for i := 1; i <= 50; i++ {
		ip := fmt.Sprintf("10.1.%d.%d", i/256, i%256)
		saw[ip] = b.Pick(ip)
	}
	b.Update([]string{"a:1", "c:1"})
	moved := 0
	for ip, old := range saw {
		if b.Pick(ip) != old {
			moved++
		}
	}
	if moved > len(saw)/2 {
		t.Fatalf("removing one backend moved %d/%d keys", moved, len(saw))
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

func TestEmptyPoolGuard(t *testing.T) {
	b := New(nil, Options{})
	if got := b.Pick(""); got != "" {
		t.Fatalf("empty pool must return empty string, got %q", got)
	}
}

func TestUpdateKeepsStateAndAddsHealthy(t *testing.T) {
	b := New([]string{"a:1", "b:1"}, Options{Fall: 1, ActiveChecks: true, Cooldown: time.Hour})
	b.ReportError("a:1", SrcDial)    // suspect
	b.ReportError("a:1", SrcProbe)   // down (survives Update)
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

func TestSetBalanceLive(t *testing.T) {
	counts := map[string]*atomic.Int64{"a:1": {}, "b:1": {}}
	b := New([]string{"a:1", "b:1"}, Options{Counts: func(a string) int64 { return counts[a].Load() }})
	counts["a:1"].Store(9)
	if b.Pick("") != "a:1" { // round-robin starts at a:1
		t.Fatal("round_robin first pick is a:1")
	}
	b.SetBalance("least_sessions")
	if got := b.Pick(""); got != "b:1" {
		t.Fatalf("after SetBalance least_sessions must pick b:1, got %s", got)
	}
}

func TestSetHealthLive(t *testing.T) {
	b := New([]string{"a:1"}, Options{Cooldown: time.Hour}) // passive defaults
	b.SetHealth(true, 3, 1)                                 // active, fall 1
	b.ReportError("a:1", SrcDial)                           // suspect
	b.ReportError("a:1", SrcProbe)                          // confirmed down
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

func TestConcurrentReportsKeepConsistentState(t *testing.T) {
	b := New([]string{"a:1", "b:1"}, Options{Fall: 3, Rise: 2, ActiveChecks: true, Cooldown: time.Hour})
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				addr := "a:1"
				if i%2 == g%2 {
					addr = "b:1"
				}
				if i%3 == 0 {
					b.ReportSuccess(addr, SrcReply)
				} else {
					b.ReportError(addr, SrcRelayRead)
				}
				if i%50 == 0 {
					_ = b.Pick("")
					_ = b.Snapshot()
				}
			}
		}(g)
	}
	wg.Wait()
	for _, st := range b.Snapshot() {
		if st.Addr != "a:1" && st.Addr != "b:1" {
			t.Fatalf("unexpected backend %q", st.Addr)
		}
	}
	// Whatever the interleaving, a Pick must still return a backend (fail-open).
	if got := b.Pick(""); got != "a:1" && got != "b:1" {
		t.Fatalf("Pick returned %q", got)
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
	b.ReportError("a:1", SrcDial)    // -> suspect
	b.ReportError("a:1", SrcProbe)   // -> down
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
	want := []struct {
		from, to State
		source   string
	}{
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
