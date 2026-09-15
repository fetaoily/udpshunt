package balancer

import (
	"sync"
	"testing"
	"time"
)

func TestRoundRobinSequence(t *testing.T) {
	b := New([]string{"a:1", "b:1", "c:1"}, 0)
	want := []string{"a:1", "b:1", "c:1", "a:1", "b:1", "c:1"}
	for i, w := range want {
		if got := b.Pick(); got != w {
			t.Fatalf("pick %d: want %s, got %s", i, w, got)
		}
	}
}

func TestSkipsUnhealthyBackend(t *testing.T) {
	b := New([]string{"a:1", "b:1", "c:1"}, time.Hour)
	for i := 0; i < 3; i++ {
		b.ReportError("b:1")
	}
	for _, st := range b.Snapshot() {
		if st.Addr == "b:1" && st.Healthy {
			t.Fatal("b:1 should be down")
		}
	}
	seen := map[string]bool{}
	for i := 0; i < 4; i++ {
		seen[b.Pick()] = true
	}
	if seen["b:1"] {
		t.Fatal("picker returned the down backend")
	}
	if !seen["a:1"] || !seen["c:1"] {
		t.Fatalf("healthy backends not used: %v", seen)
	}
}

func TestErrorThresholdRequiresConsecutiveErrors(t *testing.T) {
	b := New([]string{"a:1"}, time.Hour)
	b.ReportError("a:1")
	b.ReportError("a:1")
	b.ReportSuccess("a:1") // resets the streak
	b.ReportError("a:1")
	b.ReportError("a:1")
	if !b.Snapshot()[0].Healthy {
		t.Fatal("two non-consecutive pairs of errors must not mark down")
	}
	b.ReportError("a:1")
	if b.Snapshot()[0].Healthy {
		t.Fatal("third consecutive error must mark down")
	}
}

func TestFailOpenWhenAllDown(t *testing.T) {
	b := New([]string{"a:1", "b:1"}, time.Hour)
	for _, a := range []string{"a:1", "b:1"} {
		for i := 0; i < 3; i++ {
			b.ReportError(a)
		}
	}
	if b.Pick() == "" {
		t.Fatal("fail-open: Pick must still return a backend")
	}
}

func TestCooldownRecovery(t *testing.T) {
	b := New([]string{"a:1"}, 20*time.Millisecond)
	for i := 0; i < 3; i++ {
		b.ReportError("a:1")
	}
	if b.Snapshot()[0].Healthy {
		t.Fatal("should be down")
	}
	time.Sleep(30 * time.Millisecond)
	if got := b.Pick(); got != "a:1" {
		t.Fatalf("after cooldown Pick should retry the backend, got %q", got)
	}
	if !b.Snapshot()[0].Healthy {
		t.Fatal("cooldown elapsed: backend should be healthy again")
	}
}

func TestOnDownCallbackFiresOnce(t *testing.T) {
	b := New([]string{"a:1"}, time.Hour)
	var mu sync.Mutex
	calls := 0
	b.SetOnDown(func(addr string) {
		mu.Lock()
		calls++
		mu.Unlock()
	})
	for i := 0; i < 6; i++ {
		b.ReportError("a:1")
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		if calls == 1 {
			mu.Unlock()
			return
		}
		mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("OnDown should fire exactly once, fired %d times", calls)
}

func TestUnknownBackendIgnored(t *testing.T) {
	b := New([]string{"a:1"}, 0)
	b.ReportError("nope:1") // must not panic
	b.ReportSuccess("nope:1")
}
