package listener

import (
	"context"
	"log/slog"
	"net"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/fetaoily/udpshunt/internal/balancer"
	"github.com/fetaoily/udpshunt/internal/config"
	"github.com/fetaoily/udpshunt/internal/session"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// deadBackendAddr returns a loopback address that nothing listens on.
func deadBackendAddr(t *testing.T) string {
	t.Helper()
	pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	addr := pc.LocalAddr().String()
	pc.Close()
	return addr
}

func eventually(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("condition not met within %v: %s", timeout, what)
}

func TestIdleAgingRemovesSession(t *testing.T) {
	backend := startEcho(t)
	lc := config.Listener{Name: "age", Bind: "127.0.0.1:0", Backends: []string{backend}, SessionTimeout: config.Duration(120 * time.Millisecond)}
	l, _, mgr := newStack(t, lc, 0)

	reapCtx, stopReaper := context.WithCancel(context.Background())
	defer stopReaper()
	mgr.Start(reapCtx, 20*time.Millisecond)

	client := testClient(t)
	if got := roundTrip(t, client, l.Addr(), "ping"); got != "echo:ping" {
		t.Fatalf("reply = %q", got)
	}
	if mgr.Count() != 1 {
		t.Fatalf("count = %d, want 1", mgr.Count())
	}
	eventually(t, 3*time.Second, "idle session reaped", func() bool { return mgr.Count() == 0 })
}

func TestBackendFailover(t *testing.T) {
	dead := deadBackendAddr(t)
	alive := startEcho(t)
	lc := config.Listener{Name: "fail", Bind: "127.0.0.1:0", Backends: []string{dead, alive}, SessionTimeout: config.Duration(time.Minute)}
	l, bal, _ := newStack(t, lc, 0)

	client := testClient(t)
	buf := make([]byte, 65536)
	// The dead backend produces ICMP port-unreachable (Linux) or
	// WSAECONNRESET (Windows) on its session socket; either surfaces as an
	// error, feeds passive health, and kills the session. The client keeps
	// retransmitting until round-robin lands on the live backend.
	deadline := time.Now().Add(8 * time.Second)
	ok := false
	for time.Now().Before(deadline) && !ok {
		_, _ = client.WriteToUDP([]byte("ping"), l.Addr())
		client.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
		n, _, err := client.ReadFromUDP(buf)
		if err == nil && string(buf[:n]) == "echo:ping" {
			ok = true
		}
	}
	if !ok {
		t.Fatal("failover to the healthy backend did not happen within deadline")
	}
	eventually(t, 8*time.Second, "dead backend marked down", func() bool {
		for _, st := range bal.Snapshot() {
			if st.Addr == dead && !st.Healthy {
				return true
			}
		}
		return false
	})
}

func TestSessionCapDropsNewClients(t *testing.T) {
	backend := startEcho(t)
	lc := config.Listener{Name: "cap", Bind: "127.0.0.1:0", Backends: []string{backend}, SessionTimeout: config.Duration(time.Minute)}
	l, _, mgr := newStack(t, lc, 1)

	first := testClient(t)
	if got := roundTrip(t, first, l.Addr(), "one"); got != "echo:one" {
		t.Fatalf("first client reply = %q", got)
	}
	second := testClient(t)
	_, _ = second.WriteToUDP([]byte("two"), l.Addr())
	second.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	buf := make([]byte, 65536)
	if n, _, err := second.ReadFromUDP(buf); err == nil {
		t.Fatalf("capped client must not be served, got %q", buf[:n])
	}
	if mgr.Rejected() < 1 {
		t.Fatalf("Rejected = %d, want >= 1", mgr.Rejected())
	}
}

func TestGracefulShutdown(t *testing.T) {
	// Dedicated stack (not newStack) so its context can be cancelled
	// independently of newStack's t.Cleanup.
	backend := startEcho(t)
	mgr := session.NewManager(0)
	bal := balancer.New([]string{backend}, 0)
	lc := config.Listener{Name: "stop", Bind: "127.0.0.1:0", Backends: []string{backend}, SessionTimeout: config.Duration(time.Minute)}
	l, err := New(lc.Name, lc, bal, mgr, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- l.Run(ctx) }()

	client := testClient(t)
	if got := roundTrip(t, client, l.Addr(), "bye"); got != "echo:bye" {
		t.Fatalf("reply = %q", got)
	}

	cancel()
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
	eventually(t, 2*time.Second, "sessions drained after shutdown", func() bool { return mgr.Count() == 0 })
	l.WaitDownstream(500 * time.Millisecond)
}
