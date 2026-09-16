package balancer

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

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
	b := New([]string{"a:1", "b:1"}, Options{Cooldown: 20 * time.Millisecond})
	for i := 0; i < 3; i++ { // fall default 3
		b.ReportError("b:1")
	}
	for _, st := range b.Snapshot() {
		if st.Addr == "b:1" && st.Healthy {
			t.Fatal("b:1 should be down after 3 errors")
		}
	}
	time.Sleep(30 * time.Millisecond)
	if got := b.Pick(""); got == "" {
		t.Fatal("pick must work after cooldown")
	}
	if !b.Snapshot()[1].Healthy {
		t.Fatal("cooldown elapsed: b:1 should be healthy again")
	}
}

func TestConfigurableFall(t *testing.T) {
	b := New([]string{"a:1"}, Options{Fall: 1})
	b.ReportError("a:1")
	if b.Snapshot()[0].Healthy {
		t.Fatal("fall=1 should mark down on first error")
	}
}

func TestActiveRiseRecovery(t *testing.T) {
	b := New([]string{"a:1"}, Options{Fall: 1, Rise: 2, ActiveChecks: true, Cooldown: time.Hour})
	b.ReportError("a:1")
	if b.Snapshot()[0].Healthy {
		t.Fatal("should be down")
	}
	b.ReportSuccess("a:1") // rise 1/2
	if b.Snapshot()[0].Healthy {
		t.Fatal("must stay down until rise successes reached")
	}
	b.ReportSuccess("a:1") // rise 2/2
	if !b.Snapshot()[0].Healthy {
		t.Fatal("rise reached: should be up")
	}
}

func TestActiveModeIgnoresCooldown(t *testing.T) {
	b := New([]string{"a:1"}, Options{Fall: 1, Rise: 5, ActiveChecks: true, Cooldown: time.Millisecond})
	b.ReportError("a:1")
	time.Sleep(10 * time.Millisecond)
	for i := 0; i < 10; i++ {
		b.Pick("") // would recover via cooldown in passive mode
	}
	if b.Snapshot()[0].Healthy {
		t.Fatal("active mode: recovery only via rise, never cooldown")
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
	b := New([]string{"a:1", "b:1"}, Options{Balance: "source_hash", Fall: 1})
	victim := b.Pick("10.0.0.7")
	b.ReportError(victim)
	for i := 0; i < 3; i++ {
		if got := b.Pick("10.0.0.7"); got == victim {
			t.Fatal("unhealthy backend must not be picked")
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
	b := New([]string{"a:1", "b:1"}, Options{Fall: 1})
	b.ReportError("a:1")
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
	b.ReportError("a:1")
	if b.Snapshot()[0].Healthy {
		t.Fatal("live-updated fall=1 must mark down on first error")
	}
	for i := 0; i < 10; i++ {
		b.Pick("") // active now: cooldown must not recover
	}
	if b.Snapshot()[0].Healthy {
		t.Fatal("live-updated active mode must ignore cooldown")
	}
}

func TestPassiveCooldownRecoveryFiresOnState(t *testing.T) {
	// Recovery observed through Pick.
	b := New([]string{"b:1"}, Options{Fall: 1, Cooldown: 20 * time.Millisecond})
	var mu sync.Mutex
	var ups []string
	b.SetOnStateChange(func(addr string, healthy bool) {
		mu.Lock()
		if healthy {
			ups = append(ups, addr)
		}
		mu.Unlock()
	})
	b.ReportError("b:1") // fall=1: down (fires the down transition)
	time.Sleep(30 * time.Millisecond)
	if b.Pick("") != "b:1" {
		t.Fatal("pick must serve the backend again after cooldown")
	}
	mu.Lock()
	if len(ups) != 1 || ups[0] != "b:1" {
		mu.Unlock()
		t.Fatalf("Pick must fire onState(b:1, true) on cooldown recovery, got %v", ups)
	}
	mu.Unlock()

	// Recovery observed through Snapshot on a fresh balancer, still with no
	// ReportSuccess anywhere: only the passive cooldown recovers.
	b2 := New([]string{"a:1"}, Options{Fall: 1, Cooldown: 20 * time.Millisecond})
	var ups2 []string
	b2.SetOnStateChange(func(addr string, healthy bool) {
		mu.Lock()
		if healthy {
			ups2 = append(ups2, addr)
		}
		mu.Unlock()
	})
	b2.ReportError("a:1")
	time.Sleep(30 * time.Millisecond)
	b2.Snapshot()
	mu.Lock()
	defer mu.Unlock()
	if len(ups2) != 1 || ups2[0] != "a:1" {
		t.Fatalf("Snapshot must fire onState(a:1, true) on cooldown recovery, got %v", ups2)
	}
}

func TestOnStateChangeBothDirections(t *testing.T) {
	b := New([]string{"a:1"}, Options{Fall: 1, Rise: 1, ActiveChecks: true})
	var mu sync.Mutex
	events := []bool{}
	b.SetOnStateChange(func(addr string, healthy bool) {
		mu.Lock()
		events = append(events, healthy)
		mu.Unlock()
	})
	b.ReportError("a:1")
	b.ReportSuccess("a:1")
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		if len(events) == 2 {
			mu.Unlock()
			if events[0] || !events[1] {
				t.Fatalf("want [down up], got %v", events)
			}
			return
		}
		mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("expected 2 state-change events, got %v", events)
}
