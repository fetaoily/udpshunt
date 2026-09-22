package listener

import (
	"context"
	"log/slog"
	"net"
	"runtime"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/fetaoily/udpshunt/internal/balancer"
	"github.com/fetaoily/udpshunt/internal/config"
	"github.com/fetaoily/udpshunt/internal/metrics"
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
	if runtime.GOOS == "windows" {
		t.Skip("Windows UDP sockets never surface ICMP port-unreachable as read errors " +
			"(Go disables SIO_UDP_CONNRESET); passive-health failover E2E runs on Linux CI")
	}
	dead := deadBackendAddr(t)
	alive := startEcho(t)
	lc := config.Listener{Name: "fail", Bind: "127.0.0.1:0", Backends: []string{dead, alive}, SessionTimeout: config.Duration(time.Minute)}
	l, bal, _ := newStack(t, lc, 0)

	buf := make([]byte, 65536)
	// On Linux the dead backend's session socket gets ECONNREFUSED from the
	// ICMP port-unreachable on read; each error feeds passive health and
	// kills the session. Sessions are sticky, so retransmitting from one
	// client port would pin to the first healthy backend and stop feeding
	// errors to the dead one; each retransmit therefore comes from a fresh
	// client port (a new client flow), keeping round-robin offering the
	// dead backend until it is evicted (3 consecutive errors). Since the
	// two-phase down, those errors only make the backend suspect; passive
	// down additionally needs the default 10s cooldown window (no prober
	// here) before a Pick/Snapshot view confirms it. Keep going until BOTH
	// the client is served AND the dead backend is unhealthy.
	deadline := time.Now().Add(15 * time.Second)
	ok := false
	down := false
	for time.Now().Before(deadline) && !(ok && down) {
		client := testClient(t)
		_, _ = client.WriteToUDP([]byte("ping"), l.Addr())
		client.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
		n, _, err := client.ReadFromUDP(buf)
		if err == nil && string(buf[:n]) == "echo:ping" {
			ok = true
		}
		down = backendDown(bal, dead)
	}
	if !ok {
		t.Fatal("failover to the healthy backend did not happen within deadline")
	}
	if !down {
		t.Fatal("dead backend was not marked down within deadline")
	}
}

// backendDown reports whether bal shows addr as unhealthy.
func backendDown(bal *balancer.Balancer, addr string) bool {
	for _, st := range bal.Snapshot() {
		if st.Addr == addr {
			return !st.Healthy
		}
	}
	return false
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
	// Drain sequencing (spec §10): cancellation stops the receive loop but
	// leaves the frontend socket writable for the flush window; CloseAll
	// unblocks downstream readers; Close then releases the socket.
	// Dedicated stack (not newStack) so its context can be cancelled
	// independently of newStack's t.Cleanup.
	backend := startEcho(t)
	mgr := session.NewManager(0)
	bal := balancer.New([]string{backend}, balancer.Options{})
	lc := config.Listener{Name: "stop", Bind: "127.0.0.1:0", Backends: []string{backend}, SessionTimeout: config.Duration(time.Minute)}
	l, err := New(lc.Name, lc, bal, mgr, slog.Default(), nil, nil, nil)
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
	// The drain window requires the frontend socket to stay open (writable)
	// after Run returned.
	if _, err := l.pc.WriteToUDP([]byte("drain"), client.LocalAddr().(*net.UDPAddr)); err != nil {
		t.Fatalf("frontend socket not writable after Run returned: %v", err)
	}
	mgr.CloseAll()
	if mgr.Count() != 0 {
		t.Fatalf("count = %d, want 0 after CloseAll", mgr.Count())
	}
	l.WaitDownstream(500 * time.Millisecond)
	if err := l.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("second Close returned error (must be idempotent): %v", err)
	}
}

func TestMetricsCountTraffic(t *testing.T) {
	backend := startEcho(t)
	lc := config.Listener{Name: "met", Bind: "127.0.0.1:0", Backends: []string{backend}, SessionTimeout: config.Duration(time.Minute)}
	met := metrics.New()
	lm := met.ForListener("met")
	mgr := session.NewManager(0)
	bal := balancer.New(lc.Backends, balancer.Options{})
	l, err := New(lc.Name, lc, bal, mgr, slog.Default(), lm, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	// Same LIFO teardown as newStack: cancel (stop receiving) -> CloseAll
	// (unblock downstream readers) -> Close (release the frontend socket);
	// without it goleak flags the session's downstream goroutine.
	defer func() { _ = l.Close() }()
	defer mgr.CloseAll()
	defer cancel()
	go func() { _ = l.Run(ctx) }()

	client := testClient(t)
	if got := roundTrip(t, client, l.Addr(), "x"); got != "echo:x" {
		t.Fatalf("reply = %q", got)
	}
	fams, err := met.Registry().Gather()
	if err != nil {
		t.Fatal(err)
	}
	vals := map[string]float64{}
	for _, f := range fams {
		for _, mv := range f.GetMetric() {
			vals[f.GetName()] = mv.GetCounter().GetValue()
		}
	}
	if vals["udpshunt_packets_in_total"] < 1 {
		t.Fatalf("packets_in = %v", vals)
	}
	if vals["udpshunt_backend_packets_out_total"] < 1 {
		t.Fatalf("backend_packets_out = %v", vals)
	}
	if vals["udpshunt_sessions_created_total"] < 1 {
		t.Fatalf("sessions_created = %v", vals)
	}
}

func TestUpdateTimeoutAppliesToNewSessions(t *testing.T) {
	backend := startEcho(t)
	lc := config.Listener{Name: "tt", Bind: "127.0.0.1:0", Backends: []string{backend}, SessionTimeout: config.Duration(time.Minute)}
	mgr := session.NewManager(0)
	bal := balancer.New(lc.Backends, balancer.Options{})
	l, err := New(lc.Name, lc, bal, mgr, slog.Default(), nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	// Same LIFO teardown as newStack: cancel (stop receiving) -> CloseAll
	// (unblock downstream readers) -> Close (release the frontend socket);
	// without it goleak flags the session's downstream goroutine.
	defer func() { _ = l.Close() }()
	defer mgr.CloseAll()
	defer cancel()
	go func() { _ = l.Run(ctx) }()
	l.UpdateTimeout(100 * time.Millisecond)

	reapCtx, stop := context.WithCancel(context.Background())
	defer stop()
	mgr.Start(reapCtx, 20*time.Millisecond)

	client := testClient(t)
	if got := roundTrip(t, client, l.Addr(), "p"); got != "echo:p" {
		t.Fatalf("reply = %q", got)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && mgr.Count() > 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if mgr.Count() != 0 {
		t.Fatal("new timeout must apply to sessions created after UpdateTimeout")
	}
}

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
