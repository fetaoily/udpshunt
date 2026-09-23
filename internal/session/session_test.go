package session

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func testAddr(t *testing.T, s string) *net.UDPAddr {
	t.Helper()
	a, err := net.ResolveUDPAddr("udp", s)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func testUpstream(t *testing.T) *net.UDPConn {
	t.Helper()
	pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pc.Close() })
	return pc
}

func TestPutGetRemove(t *testing.T) {
	mgr := NewManager(0)
	cli := testAddr(t, "127.0.0.1:1111")
	s := NewSession("L", cli, "10.0.0.1:53", testUpstream(t), time.Minute)
	if !mgr.Put("L", cli, s) {
		t.Fatal("Put should succeed")
	}
	if mgr.Get("L", cli) != s {
		t.Fatal("Get should return the stored session")
	}
	if mgr.Get("L", testAddr(t, "127.0.0.1:2222")) != nil {
		t.Fatal("different client must not match")
	}
	if mgr.Get("other", cli) != nil {
		t.Fatal("different listener must not match")
	}
	if mgr.Count() != 1 {
		t.Fatalf("Count = %d, want 1", mgr.Count())
	}
	mgr.Remove("L", cli)
	if mgr.Get("L", cli) != nil || mgr.Count() != 0 {
		t.Fatal("session not removed")
	}
}

func TestPutDuplicateReturnsFalse(t *testing.T) {
	mgr := NewManager(0)
	cli := testAddr(t, "127.0.0.1:1111")
	s1 := NewSession("L", cli, "b:1", testUpstream(t), time.Minute)
	s2 := NewSession("L", cli, "b:1", testUpstream(t), time.Minute)
	if !mgr.Put("L", cli, s1) {
		t.Fatal("first Put should succeed")
	}
	if mgr.Put("L", cli, s2) {
		t.Fatal("duplicate Put must return false so the caller closes s2")
	}
	if mgr.Get("L", cli) != s1 {
		t.Fatal("original session must survive a duplicate Put")
	}
}

func TestCapRejectsNewSessions(t *testing.T) {
	mgr := NewManager(1)
	a := testAddr(t, "127.0.0.1:1111")
	b := testAddr(t, "127.0.0.1:2222")
	if !mgr.Put("L", a, NewSession("L", a, "b:1", testUpstream(t), time.Minute)) {
		t.Fatal("first Put under cap should succeed")
	}
	if mgr.Put("L", b, NewSession("L", b, "b:1", testUpstream(t), time.Minute)) {
		t.Fatal("Put over cap must fail")
	}
	if mgr.Rejected() != 1 {
		t.Fatalf("Rejected = %d, want 1", mgr.Rejected())
	}
}

func TestRemoveClosesUpstream(t *testing.T) {
	mgr := NewManager(0)
	cli := testAddr(t, "127.0.0.1:1111")
	up := testUpstream(t)
	mgr.Put("L", cli, NewSession("L", cli, "b:1", up, time.Minute))
	mgr.Remove("L", cli)
	buf := make([]byte, 1)
	up.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	if _, err := up.Read(buf); err == nil {
		t.Fatal("upstream socket should be closed after Remove")
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	cli := testAddr(t, "127.0.0.1:1111")
	s := NewSession("L", cli, "b:1", testUpstream(t), time.Minute)
	s.Close()
	s.Close() // must not panic
}

func TestCloseBackendFiltersListenerAndBackend(t *testing.T) {
	mgr := NewManager(0)
	a := testAddr(t, "127.0.0.1:1111")
	b := testAddr(t, "127.0.0.1:2222")
	mgr.Put("L1", a, NewSession("L1", a, "b:1", testUpstream(t), time.Minute))
	mgr.Put("L1", b, NewSession("L1", b, "b:2", testUpstream(t), time.Minute))
	mgr.Put("L2", a, NewSession("L2", a, "b:1", testUpstream(t), time.Minute))
	if n := mgr.CloseBackend("L1", "b:1"); n != 1 {
		t.Fatalf("CloseBackend closed %d, want 1 (only L1/b:1)", n)
	}
	if mgr.Count() != 2 {
		t.Fatalf("Count = %d, want 2 remaining", mgr.Count())
	}
	if mgr.Get("L1", a) != nil {
		t.Fatal("L1/b:1 session should be gone")
	}
	if mgr.Get("L2", a) == nil {
		t.Fatal("L2/b:1 session must survive (different listener)")
	}
}

func TestCloseClientsByPredicate(t *testing.T) {
	mgr := NewManager(0)
	a := testAddr(t, "203.0.113.7:1111")
	b := testAddr(t, "203.0.113.8:2222")
	mgr.Put("L", a, NewSession("L", a, "b:1", testUpstream(t), time.Minute))
	mgr.Put("L", b, NewSession("L", b, "b:1", testUpstream(t), time.Minute))
	n := mgr.CloseClients(func(c *net.UDPAddr) bool { return c.IP.Equal(net.IPv4(203, 0, 113, 7)) })
	if n != 1 {
		t.Fatalf("closed = %d, want 1", n)
	}
	if mgr.Count() != 1 {
		t.Fatalf("Count = %d, want 1 remaining", mgr.Count())
	}
	if mgr.Get("L", a) != nil {
		t.Fatal("blocked client's session should be gone")
	}
	if mgr.Get("L", b) == nil {
		t.Fatal("other client's session must survive")
	}
}

func TestIdleAging(t *testing.T) {
	mgr := NewManager(0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mgr.Start(ctx, 20*time.Millisecond)

	cli := testAddr(t, "127.0.0.1:1111")
	mgr.Put("L", cli, NewSession("L", cli, "b:1", testUpstream(t), 80*time.Millisecond))
	if mgr.Count() != 1 {
		t.Fatal("session should exist")
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && mgr.Count() > 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if mgr.Count() != 0 {
		t.Fatal("idle session should be reaped")
	}
}

func TestTouchPreventsAging(t *testing.T) {
	mgr := NewManager(0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mgr.Start(ctx, 20*time.Millisecond)

	cli := testAddr(t, "127.0.0.1:1111")
	s := NewSession("L", cli, "b:1", testUpstream(t), 150*time.Millisecond)
	mgr.Put("L", cli, s)
	deadline := time.Now().Add(600 * time.Millisecond)
	for time.Now().Before(deadline) {
		s.Touch()
		time.Sleep(50 * time.Millisecond)
	}
	if mgr.Count() != 1 {
		t.Fatal("touched session must survive past its timeout")
	}
}

func TestExactCapUnderConcurrency(t *testing.T) {
	const max = 50
	mgr := NewManager(max)
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				cli := testAddr(t, fmt.Sprintf("127.0.0.1:%d", 10000+w*100+i))
				s := NewSession("L", cli, "b:1", testUpstream(t), time.Minute)
				if !mgr.Put("L", cli, s) {
					s.Close()
				}
			}
		}(w)
	}
	wg.Wait()
	if mgr.Count() > max {
		t.Fatalf("cap violated: %d > %d", mgr.Count(), max)
	}
	if mgr.Count() < max {
		t.Fatalf("expected the pool to fill to the cap, got %d", mgr.Count())
	}
}

func TestBackendCountsLifecycle(t *testing.T) {
	mgr := NewManager(0)
	a := testAddr(t, "127.0.0.1:1111")
	b := testAddr(t, "127.0.0.1:2222")
	mgr.Put("L", a, NewSession("L", a, "b:1", testUpstream(t), time.Minute))
	mgr.Put("L", b, NewSession("L", b, "b:2", testUpstream(t), time.Minute))
	mgr.Put("L2", a, NewSession("L2", a, "b:1", testUpstream(t), time.Minute))
	if got := mgr.BackendCount("L", "b:1"); got != 1 {
		t.Fatalf("BackendCount(L,b:1) = %d", got)
	}
	all := mgr.BackendCounts("L")
	if len(all) != 2 || all["b:1"] != 1 || all["b:2"] != 1 {
		t.Fatalf("BackendCounts(L) = %v", all)
	}
	mgr.Remove("L", a)
	if got := mgr.BackendCount("L", "b:1"); got != 0 {
		t.Fatalf("after Remove = %d", got)
	}
	if got := mgr.BackendCount("L2", "b:1"); got != 1 {
		t.Fatalf("L2 unaffected = %d", got)
	}
}

func TestBackendCountsExpireWithSweep(t *testing.T) {
	mgr := NewManager(0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mgr.Start(ctx, 20*time.Millisecond)
	cli := testAddr(t, "127.0.0.1:1111")
	mgr.Put("L", cli, NewSession("L", cli, "b:9", testUpstream(t), 80*time.Millisecond))
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && mgr.BackendCount("L", "b:9") != 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if got := mgr.BackendCount("L", "b:9"); got != 0 {
		t.Fatalf("sweep must decrement backend counts, got %d", got)
	}
}

func TestCloseListenerScoped(t *testing.T) {
	mgr := NewManager(0)
	a := testAddr(t, "127.0.0.1:1111")
	mgr.Put("L1", a, NewSession("L1", a, "b:1", testUpstream(t), time.Minute))
	mgr.Put("L2", a, NewSession("L2", a, "b:1", testUpstream(t), time.Minute))
	if n := mgr.CloseListener("L1"); n != 1 {
		t.Fatalf("closed %d, want 1", n)
	}
	if mgr.Count() != 1 || mgr.Get("L2", a) == nil {
		t.Fatal("L2 must survive")
	}
}

func TestSetMaxLive(t *testing.T) {
	mgr := NewManager(0)
	a := testAddr(t, "127.0.0.1:1111")
	b := testAddr(t, "127.0.0.1:2222")
	mgr.Put("L", a, NewSession("L", a, "b:1", testUpstream(t), time.Minute))
	mgr.SetMax(1)
	if mgr.Put("L", b, NewSession("L", b, "b:1", testUpstream(t), time.Minute)) {
		t.Fatal("live max must reject")
	}
}

func TestLifecycleCounters(t *testing.T) {
	mgr := NewManager(1)
	a := testAddr(t, "127.0.0.1:1111")
	mgr.Put("L", a, NewSession("L", a, "b:1", testUpstream(t), time.Minute))
	if mgr.Created() != 1 {
		t.Fatalf("Created = %d", mgr.Created())
	}
	mgr.Remove("L", a)
	if mgr.Expired() != 1 {
		t.Fatalf("Expired = %d", mgr.Expired())
	}
	if mgr.Rejected() < 0 || mgr.Created() < 1 {
		t.Fatal("counters must be monotonic")
	}
}
