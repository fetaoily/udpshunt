package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/fetaoily/udpshunt/internal/admin"
	"github.com/fetaoily/udpshunt/internal/balancer"
	"github.com/fetaoily/udpshunt/internal/blocklist"
	"github.com/fetaoily/udpshunt/internal/config"
	"github.com/fetaoily/udpshunt/internal/webui"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

func startEcho(t *testing.T) string {
	t.Helper()
	pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		buf := make([]byte, 65536)
		for {
			n, from, err := pc.ReadFromUDP(buf)
			if err != nil {
				return
			}
			_, _ = pc.WriteToUDP(append([]byte("echo:"), buf[:n]...), from)
		}
	}()
	t.Cleanup(func() { pc.Close() })
	return pc.LocalAddr().String()
}

func writeCfg(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "udpshunt.yaml")
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func cfgYAML(listeners string, extra string) string {
	return fmt.Sprintf("listeners:\n%s%s\n", listeners, extra)
}

func newApp(t *testing.T, cfgPath string) (*App, context.CancelFunc) {
	t.Helper()
	app := NewApp(cfgPath, slog.Default())
	// The cancel is wired into t.Cleanup; no test needs the ctx itself.
	_, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		app.Shutdown(500 * time.Millisecond)
	})
	return app, cancel
}

func roundTripUDP(t *testing.T, dst *net.UDPAddr, payload string) (string, error) {
	t.Helper()
	pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	_, _ = pc.WriteToUDP([]byte(payload), dst)
	pc.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 65536)
	n, _, err := pc.ReadFromUDP(buf)
	if err != nil {
		return "", err
	}
	return string(buf[:n]), nil
}

func mustLoad(t *testing.T, path string) config.Config {
	t.Helper()
	c, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestReloadListenerLifecycle(t *testing.T) {
	b1 := startEcho(t)
	b2 := startEcho(t)
	p1 := writeCfg(t, cfgYAML(fmt.Sprintf(
		"  - name: L1\n    bind: 127.0.0.1:0\n    backends: [%s]\n", b1), ""))
	app, _ := newApp(t, p1)
	if err := app.Apply(context.Background(), mustLoad(t, p1)); err != nil {
		t.Fatal(err)
	}
	l1 := app.listeners["L1"].Addr()
	if got, err := roundTripUDP(t, l1, "hi"); err != nil || got != "echo:hi" {
		t.Fatalf("L1 round trip: %q %v", got, err)
	}

	p2 := writeCfg(t, cfgYAML(fmt.Sprintf(
		"  - name: L2\n    bind: 127.0.0.1:0\n    backends: [%s]\n", b2), ""))
	if err := app.Apply(context.Background(), mustLoad(t, p2)); err != nil {
		t.Fatal(err)
	}
	app.mu.Lock()
	_, l1Alive := app.listeners["L1"]
	l2 := app.listeners["L2"]
	app.mu.Unlock()
	if l1Alive {
		t.Fatal("L1 must be removed after reload")
	}
	if l2 == nil {
		t.Fatal("L2 must be running after reload")
	}
	if got, err := roundTripUDP(t, l2.Addr(), "yo"); err != nil || got != "echo:yo" {
		t.Fatalf("L2 round trip: %q %v", got, err)
	}
	// BackendCounts is not used here: its underlying counter map keeps
	// zero-valued keys, so an empty result must be asserted per backend.
	if n := app.mgr.BackendCount("L1", b1); n != 0 {
		t.Fatalf("L1 sessions must be closed, got %d", n)
	}
}

func TestReloadInvalidConfigKeepsServing(t *testing.T) {
	b1 := startEcho(t)
	p := writeCfg(t, cfgYAML(fmt.Sprintf(
		"  - name: L1\n    bind: 127.0.0.1:0\n    backends: [%s]\n", b1), ""))
	app, _ := newApp(t, p)
	if err := app.Apply(context.Background(), mustLoad(t, p)); err != nil {
		t.Fatal(err)
	}
	addr := app.listeners["L1"].Addr()

	bad := writeCfg(t, "listeners: [broken\n")
	app.cfgPath = bad
	if err := app.Reload(); err == nil {
		t.Fatal("reload of invalid config must fail")
	}
	if got, err := roundTripUDP(t, addr, "ok"); err != nil || got != "echo:ok" {
		t.Fatal("previous config must keep serving after failed reload")
	}
}

func TestReloadBackendChangeEvictsSessions(t *testing.T) {
	b1 := startEcho(t)
	b2 := startEcho(t)
	p := writeCfg(t, cfgYAML(fmt.Sprintf(
		"  - name: L1\n    bind: 127.0.0.1:0\n    backends: [%s]\n", b1), ""))
	app, _ := newApp(t, p)
	_ = app.Apply(context.Background(), mustLoad(t, p))
	l1 := app.listeners["L1"].Addr()
	if got, err := roundTripUDP(t, l1, "x"); err != nil || got != "echo:x" {
		t.Fatalf("setup round trip: %q %v", got, err)
	}
	if n := app.mgr.BackendCount("L1", b1); n != 1 {
		t.Fatalf("one session on b1 expected, got %d", n)
	}

	p2 := writeCfg(t, cfgYAML(fmt.Sprintf(
		"  - name: L1\n    bind: 127.0.0.1:0\n    backends: [%s]\n", b2), ""))
	if err := app.Apply(context.Background(), mustLoad(t, p2)); err != nil {
		t.Fatal(err)
	}
	if n := app.mgr.BackendCount("L1", b1); n != 0 {
		t.Fatalf("removed backend sessions must be evicted, got %d", n)
	}
	if got, err := roundTripUDP(t, l1, "y"); err != nil || got != "echo:y" {
		t.Fatalf("traffic must move to b2: %q %v", got, err)
	}
}

func TestHealthCheckFailoverE2E(t *testing.T) {
	if runtime.GOOS == "windows" {
		// Active checks use timeouts (cross-platform), but the failover
		// retransmission relies on errors from session sockets, which
		// Windows does not surface (see M1). Probe state itself is tested
		// in internal/health; here we assert failover, which is Linux CI.
		t.Skip("failover E2E runs on Linux CI")
	}
	dead := deadAddr(t)
	alive := startEcho(t)
	p := writeCfg(t, fmt.Sprintf(`
listeners:
  - name: L1
    bind: 127.0.0.1:0
    backends: [%s, %s]
    session_timeout: 60s
    health_check:
      mode: raw
      payload: "70696e67"
      interval: 100ms
      timeout: 60ms
      rise: 1
      fall: 2
`, dead, alive))
	app, _ := newApp(t, p)
	if err := app.Apply(context.Background(), mustLoad(t, p)); err != nil {
		t.Fatal(err)
	}
	l1 := app.listeners["L1"].Addr()
	deadline := time.Now().Add(8 * time.Second)
	ok := false
	for time.Now().Before(deadline) && !ok {
		if got, err := roundTripUDP(t, l1, "ping"); err == nil && got == "echo:ping" {
			ok = true
		}
	}
	if !ok {
		t.Fatal("did not fail over to the healthy backend in time")
	}
	deadline = time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		app.mu.Lock()
		bal := app.balancers["L1"]
		app.mu.Unlock()
		down := false
		for _, st := range bal.Snapshot() {
			if st.Addr == dead && !st.Healthy {
				down = true
			}
		}
		if down {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("prober did not mark the dead backend down")
}

func deadAddr(t *testing.T) string {
	t.Helper()
	pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	addr := pc.LocalAddr().String()
	pc.Close()
	return addr
}

// sendUDP fires one datagram at dst without waiting for a reply (used to
// probe session-cap rejection, where no reply ever comes).
func sendUDP(t *testing.T, dst *net.UDPAddr, payload string) {
	t.Helper()
	pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	if _, err := pc.WriteToUDP([]byte(payload), dst); err != nil {
		t.Fatal(err)
	}
}

func TestFailedReloadKeepsSessionsCap(t *testing.T) {
	b1 := startEcho(t)
	// Occupied port: config B's new listener L2 fails to bind there, so
	// Apply fails mid-way (the way a real bind conflict does).
	hold, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { hold.Close() })
	occupied := hold.LocalAddr().String()

	pA := writeCfg(t, fmt.Sprintf(`
sessions:
  max: 1
listeners:
  - name: L1
    bind: 127.0.0.1:0
    backends: [%s]
`, b1))
	app, _ := newApp(t, pA)
	if err := app.Reload(); err != nil { // startup path: sets the cap, then applies
		t.Fatal(err)
	}
	l1 := app.listeners["L1"].Addr()
	if got, err := roundTripUDP(t, l1, "a"); err != nil || got != "echo:a" {
		t.Fatalf("setup: %q %v", got, err)
	}
	if got := app.mgr.Rejected(); got != 0 {
		t.Fatalf("no rejections expected under config A, got %d", got)
	}

	pB := writeCfg(t, fmt.Sprintf(`
sessions:
  max: 999
listeners:
  - name: L1
    bind: 127.0.0.1:0
    backends: [%s]
  - name: L2
    bind: %s
    backends: [%s]
`, b1, occupied, b1))
	app.cfgPath = pB
	if err := app.Reload(); err == nil {
		t.Fatal("reload must fail on the L2 bind conflict")
	}
	// A second client must still be rejected under config A's cap of 1; the
	// failed reload must not have moved the cap to config B's 999.
	sendUDP(t, l1, "b")
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && app.mgr.Rejected() == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if got := app.mgr.Rejected(); got != 1 {
		t.Fatalf("sessions cap must stay at config A's 1 after a failed reload, rejected = %d", got)
	}
}

func TestPoolChangeKeepsSurvivingBackendSessions(t *testing.T) {
	b1 := startEcho(t)
	b2 := startEcho(t)
	p := writeCfg(t, cfgYAML(fmt.Sprintf(
		"  - name: L1\n    bind: 127.0.0.1:0\n    backends: [%s, %s]\n", b1, b2), ""))
	app, _ := newApp(t, p)
	if err := app.Apply(context.Background(), mustLoad(t, p)); err != nil {
		t.Fatal(err)
	}
	l1 := app.listeners["L1"].Addr()
	// First round-robin pick lands on b1 (pool order): one session there.
	if got, err := roundTripUDP(t, l1, "x"); err != nil || got != "echo:x" {
		t.Fatalf("setup: %q %v", got, err)
	}
	if n := app.mgr.BackendCount("L1", b1); n != 1 {
		t.Fatalf("one session on b1 expected, got %d", n)
	}
	expired := app.mgr.Expired()

	// Change the OTHER backend (drop b2): b1's session must survive the
	// pool update untouched.
	p2 := writeCfg(t, cfgYAML(fmt.Sprintf(
		"  - name: L1\n    bind: 127.0.0.1:0\n    backends: [%s]\n", b1), ""))
	if err := app.Apply(context.Background(), mustLoad(t, p2)); err != nil {
		t.Fatal(err)
	}
	if n := app.mgr.BackendCount("L1", b1); n != 1 {
		t.Fatalf("surviving backend must keep its session, got %d", n)
	}
	if n := app.mgr.BackendCount("L1", b2); n != 0 {
		t.Fatalf("dropped backend must have no sessions, got %d", n)
	}
	if app.mgr.Expired() != expired {
		t.Fatalf("no eviction may hit the surviving backend, expired %d -> %d",
			expired, app.mgr.Expired())
	}
}

func TestStatusAndAdminEndpoints(t *testing.T) {
	b1 := startEcho(t)
	p := writeCfg(t, cfgYAML(fmt.Sprintf(
		"  - name: L1\n    bind: 127.0.0.1:0\n    backends: [%s]\n", b1), ""))
	app, _ := newApp(t, p)
	_ = app.Apply(context.Background(), mustLoad(t, p))
	if got, err := roundTripUDP(t, app.listeners["L1"].Addr(), "s"); err != nil || got != "echo:s" {
		t.Fatalf("setup: %q %v", got, err)
	}

	raw, err := json.Marshal(app.Status())
	if err != nil {
		t.Fatal(err)
	}
	var st admin.Status
	if err := json.Unmarshal(raw, &st); err != nil {
		t.Fatal(err)
	}
	if len(st.Listeners) != 1 || st.Listeners[0].Name != "L1" {
		t.Fatalf("status listeners: %+v", st.Listeners)
	}
	if len(st.Listeners[0].Backends) != 1 || !st.Listeners[0].Backends[0].Healthy {
		t.Fatalf("status backends: %+v", st.Listeners[0].Backends)
	}
	if st.Sessions.Created < 1 || st.Sessions.Active < 1 {
		t.Fatalf("status sessions: %+v", st.Sessions)
	}

	// POST /reload must 500 only when the file is invalid; the brief's
	// draft left cfgPath pointing at the valid config (which reloads 204),
	// so point it at a broken one first.
	app.cfgPath = writeCfg(t, "listeners: [broken\n")
	ts := httptest.NewServer(adminHandlerForTest(app))
	defer ts.Close()
	resp, err := http.Get(ts.URL + "/metrics")
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("metrics: %v %v", err, resp)
	}
	resp.Body.Close()
	if resp2, err := http.Post(ts.URL+"/reload", "", nil); err != nil || resp2.StatusCode != http.StatusInternalServerError {
		t.Fatalf("reload of invalid path must 500: %v %v", err, resp2)
	} else {
		resp2.Body.Close()
	}
	kinds := map[string]bool{}
	for _, ev := range app.events.List() {
		kinds[ev.Kind] = true
	}
	if !kinds["listener_started"] || !kinds["reload_failed"] {
		t.Fatalf("events must record listener_started and reload_failed, got %+v", app.events.List())
	}
}

func TestReloadKeepsSurvivingBackendSessions(t *testing.T) {
	b1 := startEcho(t)
	b2 := startEcho(t)
	b3 := startEcho(t)
	p := writeCfg(t, cfgYAML(fmt.Sprintf(
		"  - name: L1\n    bind: 127.0.0.1:0\n    backends: [%s, %s]\n", b1, b2), ""))
	app, _ := newApp(t, p)
	if err := app.Apply(context.Background(), mustLoad(t, p)); err != nil {
		t.Fatal(err)
	}
	l1 := app.listeners["L1"].Addr()
	if got, err := roundTripUDP(t, l1, "x"); err != nil || got != "echo:x" {
		t.Fatalf("setup: %q %v", got, err)
	}

	app.mu.Lock()
	counts := app.mgr.BackendCounts("L1")
	app.mu.Unlock()
	keeper, evictee := "", ""
	for _, b := range []string{b1, b2} {
		if counts[b] > 0 {
			keeper = b
		} else {
			evictee = b
		}
	}
	if keeper == "" {
		t.Fatal("no session found on either backend")
	}

	p2 := writeCfg(t, cfgYAML(fmt.Sprintf(
		"  - name: L1\n    bind: 127.0.0.1:0\n    backends: [%s, %s]\n", keeper, b3), ""))
	if err := app.Apply(context.Background(), mustLoad(t, p2)); err != nil {
		t.Fatal(err)
	}
	if n := app.mgr.BackendCount("L1", keeper); n != 1 {
		t.Fatalf("surviving backend session was evicted, count = %d", n)
	}
	if n := app.mgr.BackendCount("L1", evictee); n != 0 {
		t.Fatalf("evicted backend still holds sessions, count = %d", n)
	}
}

func TestStatusCarriesRateHistory(t *testing.T) {
	b1 := startEcho(t)
	p := writeCfg(t, cfgYAML(fmt.Sprintf(
		"  - name: L1\n    bind: 127.0.0.1:0\n    backends: [%s]\n", b1), ""))
	app, _ := newApp(t, p)
	if err := app.Apply(context.Background(), mustLoad(t, p)); err != nil {
		t.Fatal(err)
	}
	if got, err := roundTripUDP(t, app.listeners["L1"].Addr(), "h"); err != nil || got != "echo:h" {
		t.Fatalf("setup: %q %v", got, err)
	}
	// Two manual ticks: the sampler is driven by run() in production, so
	// tests drive the collector directly.
	app.rates.Tick(time.Now())
	time.Sleep(10 * time.Millisecond)
	app.rates.Tick(time.Now())

	st := app.Status()
	var hist []admin.Sample
	for _, l := range st.Listeners {
		if l.Name == "L1" {
			hist = l.History
		}
	}
	if len(hist) != 2 {
		t.Fatalf("history len = %d, want 2", len(hist))
	}
	if hist[1].In < hist[0].In || hist[1].In == 0 {
		t.Fatalf("cumulative packets_in must grow: %+v", hist)
	}
}

// adminHandlerForTest builds the admin mux wired to an app's live state the
// way run() wires it in production, the embedded /ui included.
func adminHandlerForTest(app *App) http.Handler {
	return admin.New("127.0.0.1:0", admin.Deps{
		Registry: app.met.Registry(),
		Status:   app.Status,
		Reload:   app.Reload,
		Logger:   slog.Default(),
		UI:       webui.Handler(),
	}).Handler()
}

func TestSamplerPopulatesHistoryOverTime(t *testing.T) {
	b1 := startEcho(t)
	p := writeCfg(t, cfgYAML(fmt.Sprintf(
		"  - name: L1\n    bind: 127.0.0.1:0\n    backends: [%s]\n", b1), ""))
	app, _ := newApp(t, p)
	if err := app.Apply(context.Background(), mustLoad(t, p)); err != nil {
		t.Fatal(err)
	}
	// Drive the collector the way run()'s sampler does, but fast.
	for i := 0; i < 3; i++ {
		if got, err := roundTripUDP(t, app.listeners["L1"].Addr(), "s"); err != nil || got != "echo:s" {
			t.Fatalf("traffic %d: %q %v", i, got, err)
		}
		app.rates.Tick(time.Now())
		time.Sleep(5 * time.Millisecond)
	}
	st := app.Status()
	for _, l := range st.Listeners {
		if l.Name == "L1" && len(l.History) >= 2 {
			return
		}
	}
	t.Fatal("expected L1 history with >= 2 samples")
}

func TestUIServesBuiltSPA(t *testing.T) {
	b1 := startEcho(t)
	p := writeCfg(t, cfgYAML(fmt.Sprintf(
		"  - name: L1\n    bind: 127.0.0.1:0\n    backends: [%s]\n", b1), ""))
	app, _ := newApp(t, p)
	_ = app.Apply(context.Background(), mustLoad(t, p))

	ts := httptest.NewServer(adminHandlerForTest(app))
	defer ts.Close()
	resp, err := http.Get(ts.URL + "/ui/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/ui/ = %d", resp.StatusCode)
	}
	if !strings.Contains(string(body), "<div id=\"app\">") {
		t.Fatalf("/ui/ does not serve the SPA shell: %.200s", body)
	}
}

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
			_, _ = pc.WriteToUDP(append([]byte("echo:"), buf[:n]...), from)
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
	p := onDownCfg(t, backend, "drain", time.Second)
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
	addrBefore := app.listeners["L1"].Addr()
	// Same bind: reload takes the updateListenerLocked path (no restart).
	pClose := onDownCfg(t, backend, "close", 30*time.Second)
	if err := app.Apply(context.Background(), mustLoad(t, pClose)); err != nil {
		t.Fatal(err)
	}
	if got := app.listeners["L1"].Addr(); got != addrBefore {
		t.Fatalf("reload must update in place (bind unchanged), got %v then %v", addrBefore, got)
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

// waitFor polls cond until it holds or a 2s deadline passes.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not reached in time")
}

// testClientUDP returns a persistent loopback UDP socket, so one client
// (one IP:port session) can be followed across block/unblock transitions.
func testClientUDP(t *testing.T) *net.UDPConn {
	t.Helper()
	pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pc.Close() })
	return pc
}

// cliTrip sends payload via the persistent client and returns the echo
// reply; a blocked client sees the read-deadtime error instead.
func cliTrip(t *testing.T, cli *net.UDPConn, dst *net.UDPAddr, payload string) (string, error) {
	t.Helper()
	if _, err := cli.WriteToUDP([]byte(payload), dst); err != nil {
		return "", err
	}
	buf := make([]byte, 65536)
	cli.SetReadDeadline(time.Now().Add(time.Second))
	n, _, err := cli.ReadFromUDP(buf)
	if err != nil {
		return "", err
	}
	return string(buf[:n]), nil
}

func TestBlacklistRuntimeAPIAndReload(t *testing.T) {
	b1 := startEcho(t)
	pA := writeCfg(t, cfgYAML(fmt.Sprintf(
		"  - name: L1\n    bind: 127.0.0.1:0\n    backends: [%s]\n", b1),
		"blacklist:\n  entries: [203.0.113.7]\n"))
	app, _ := newApp(t, pA)
	if err := app.Apply(context.Background(), mustLoad(t, pA)); err != nil {
		t.Fatal(err)
	}
	app.mu.Lock()
	l1 := app.listeners["L1"].Addr()
	app.mu.Unlock()

	cli := testClientUDP(t)
	cliIP := cli.LocalAddr().(*net.UDPAddr).IP.String()
	if got, err := cliTrip(t, cli, l1, "hi"); err != nil || got != "echo:hi" {
		t.Fatalf("setup round trip: %q %v", got, err)
	}
	waitFor(t, func() bool { return app.mgr.Count() == 1 })

	// Runtime add: closes the client's live session, records the event and
	// lists the entry (canonical host-length form).
	if err := app.BlacklistAdd(cliIP); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return app.mgr.Count() == 0 })
	sawAdded := false
	for _, e := range app.events.List() {
		if e.Kind == "blacklist_added" && strings.Contains(e.Detail, "closed=1") {
			sawAdded = true
		}
	}
	if !sawAdded {
		t.Fatalf("blacklist_added closed=1 event missing: %+v", app.events.List())
	}
	if !slices.Contains(app.Blacklist().Entries, cliIP+"/32") {
		t.Fatalf("Blacklist() must list %s/32, got %v", cliIP, app.Blacklist().Entries)
	}
	// Blocked traffic gets no reply and creates no session.
	if got, err := cliTrip(t, cli, l1, "evil"); err == nil {
		t.Fatalf("blocked client must get no reply, got %q", got)
	}
	if n := app.mgr.Count(); n != 0 {
		t.Fatalf("blocked packet must not create a session, count = %d", n)
	}

	// Runtime del unblocks: a fresh round trip works again.
	if err := app.BlacklistDel(cliIP); err != nil {
		t.Fatal(err)
	}
	if got, err := cliTrip(t, cli, l1, "back"); err != nil || got != "echo:back" {
		t.Fatalf("unblocked round trip: %q %v", got, err)
	}
	waitFor(t, func() bool { return app.mgr.Count() == 1 })

	// Reload that adds a CIDR over the client's live session: the reload
	// enforcement sweep closes the session and fires blacklist_enforced.
	pB := writeCfg(t, cfgYAML(fmt.Sprintf(
		"  - name: L1\n    bind: 127.0.0.1:0\n    backends: [%s]\n", b1),
		"blacklist:\n  entries: [203.0.113.7, 127.0.0.0/8]\n"))
	if err := app.Apply(context.Background(), mustLoad(t, pB)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return app.mgr.Count() == 0 })
	sawEnforced := false
	for _, e := range app.events.List() {
		if e.Kind == "blacklist_enforced" && strings.Contains(e.Detail, "closed=1") {
			sawEnforced = true
		}
	}
	if !sawEnforced {
		t.Fatalf("blacklist_enforced closed=1 event missing: %+v", app.events.List())
	}

	// A runtime add survives a reload that does not mention it (union
	// model), while config B's CIDR leaves with config B.
	if err := app.BlacklistAdd("192.0.2.1"); err != nil {
		t.Fatal(err)
	}
	if err := app.Apply(context.Background(), mustLoad(t, pA)); err != nil {
		t.Fatal(err)
	}
	entries := app.Blacklist().Entries
	if !slices.Contains(entries, "192.0.2.1/32") {
		t.Fatalf("runtime add must survive reload, got %v", entries)
	}
	if slices.Contains(entries, "127.0.0.0/8") {
		t.Fatalf("config B's CIDR must be gone after config A reapplies, got %v", entries)
	}

	// Deleting an entry that is not effective is a not-found error.
	if err := app.BlacklistDel("198.51.100.9"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("del of unknown entry = %v, want os.ErrNotExist", err)
	}
}

// Ruled delete semantics: a runtime delete of a config-sourced entry lasts
// only until the next config-source refresh (Apply or watcher reload) �?the
// entry still present in config comes back; a runtime add survives reloads.
func TestBlacklistDeleteUntilNextRefresh(t *testing.T) {
	b1 := startEcho(t)
	pA := writeCfg(t, cfgYAML(fmt.Sprintf(
		"  - name: L1\n    bind: 127.0.0.1:0\n    backends: [%s]\n", b1),
		"blacklist:\n  entries: [203.0.113.7]\n"))
	app, _ := newApp(t, pA)
	if err := app.Apply(context.Background(), mustLoad(t, pA)); err != nil {
		t.Fatal(err)
	}

	// Delete via the exact string GET /blacklist reports (canonical form;
	// the config spelled the entry bare): it leaves the effective list.
	if err := app.BlacklistDel("203.0.113.7/32"); err != nil {
		t.Fatal(err)
	}
	if slices.Contains(app.Blacklist().Entries, "203.0.113.7/32") {
		t.Fatal("deleted entry must leave the list")
	}

	// The same config applied again resurrects it, while a runtime add made
	// before that reload survives it.
	if err := app.BlacklistAdd("192.0.2.77"); err != nil {
		t.Fatal(err)
	}
	if err := app.Apply(context.Background(), mustLoad(t, pA)); err != nil {
		t.Fatal(err)
	}
	entries := app.Blacklist().Entries
	if !slices.Contains(entries, "203.0.113.7/32") {
		t.Fatalf("config-sourced entry must come back on reload, got %v", entries)
	}
	if !slices.Contains(entries, "192.0.2.77/32") {
		t.Fatalf("runtime add must survive the reload, got %v", entries)
	}

	// Removing the entry from config keeps it away across reloads.
	pB := writeCfg(t, cfgYAML(fmt.Sprintf(
		"  - name: L1\n    bind: 127.0.0.1:0\n    backends: [%s]\n", b1),
		"blacklist:\n  entries: [198.51.100.7]\n"))
	if err := app.Apply(context.Background(), mustLoad(t, pB)); err != nil {
		t.Fatal(err)
	}
	entries = app.Blacklist().Entries
	if slices.Contains(entries, "203.0.113.7/32") {
		t.Fatalf("entry removed from config must stay away, got %v", entries)
	}
	if !slices.Contains(entries, "198.51.100.7/32") || !slices.Contains(entries, "192.0.2.77/32") {
		t.Fatalf("new config entry and runtime add must be effective, got %v", entries)
	}
}

// A rebuild failure (a host-bit entry like "10.0.0.1/24" canonicalizes but
// blocklist.New rejects it) must leave the runtime state untouched: the
// swapped list and the armed runtime deletes survive, so a later API
// rebuild still honors the delete. config.Load rejects such entries, so the
// test hands Apply a hand-built config �?the same rebuild the watcher's
// file path can hit, driven synchronously.
func TestBlacklistFailedRebuildKeepsRuntimeState(t *testing.T) {
	b1 := startEcho(t)
	p := writeCfg(t, cfgYAML(fmt.Sprintf(
		"  - name: L1\n    bind: 127.0.0.1:0\n    backends: [%s]\n", b1),
		"blacklist:\n  entries: [203.0.113.7, 198.51.100.9]\n"))
	app, _ := newApp(t, p)
	if err := app.Apply(context.Background(), mustLoad(t, p)); err != nil {
		t.Fatal(err)
	}
	contains := func(entry string) bool {
		return slices.Contains(app.Blacklist().Entries, entry)
	}
	if !contains("203.0.113.7/32") || !contains("198.51.100.9/32") {
		t.Fatalf("setup: %v", app.Blacklist().Entries)
	}

	// Arm a runtime delete, then fail a refresh rebuild with an entry that
	// canonicalizes but fails blocklist.New. Same bind as the running
	// listener, so Apply reaches the blacklist rebuild without churn.
	if err := app.BlacklistDel("203.0.113.7"); err != nil {
		t.Fatal(err)
	}
	app.mu.Lock()
	bind := app.binds["L1"]
	app.mu.Unlock()
	bad := config.Config{
		Listeners: []config.Listener{{Name: "L1", Bind: bind, Backends: []string{b1}}},
		Blacklist: config.Blacklist{Entries: []string{"10.0.0.1/24"}},
	}
	if err := app.Apply(context.Background(), bad); err == nil {
		t.Fatal("host-bit entry must fail the rebuild")
	}

	// The failed rebuild swapped nothing and kept the runtime delete armed:
	// an API rebuild (no refresh) must not resurrect the config entry.
	if got := app.Blacklist().Entries; len(got) != 1 || !contains("198.51.100.9/32") || contains("10.0.0.1/24") {
		t.Fatalf("failed rebuild must keep the previous list, got %v", got)
	}
	if err := app.BlacklistAdd("192.0.2.50"); err != nil {
		t.Fatal(err)
	}
	if contains("203.0.113.7/32") {
		t.Fatal("runtime delete must survive a failed rebuild")
	}
	if !contains("192.0.2.50/32") || !contains("198.51.100.9/32") {
		t.Fatalf("runtime add must land on the surviving list, got %v", app.Blacklist().Entries)
	}
}

func TestStatusBlacklistBlockedPackets(t *testing.T) {
	b1 := startEcho(t)
	p := writeCfg(t, cfgYAML(fmt.Sprintf(
		"  - name: L1\n    bind: 127.0.0.1:0\n    backends: [%s]\n", b1),
		"blacklist:\n  entries: [203.0.113.7]\n"))
	app, _ := newApp(t, p)
	if err := app.Apply(context.Background(), mustLoad(t, p)); err != nil {
		t.Fatal(err)
	}
	app.mu.Lock()
	l1 := app.listeners["L1"].Addr()
	app.mu.Unlock()
	// Blacklist the test client's own IP, then send one packet: /status
	// must report it under blacklist.blocked_packets.
	if err := app.BlacklistAdd("127.0.0.1"); err != nil {
		t.Fatal(err)
	}
	sendUDP(t, l1, "dropped")
	waitFor(t, func() bool {
		st := app.Status()
		return st.Blacklist != nil && st.Blacklist.BlockedPackets >= 1
	})
}

func TestBlacklistWatch(t *testing.T) {
	b1 := startEcho(t)
	blFile := filepath.Join(t.TempDir(), "bl.txt")
	if err := os.WriteFile(blFile, []byte("203.0.113.7"), 0o644); err != nil {
		t.Fatal(err)
	}
	// Forward slashes keep the YAML scalar and the Windows path valid.
	yamlPath := strings.ReplaceAll(blFile, "\\", "/")
	p := writeCfg(t, cfgYAML(fmt.Sprintf(
		"  - name: L1\n    bind: 127.0.0.1:0\n    backends: [%s]\n", b1),
		fmt.Sprintf("blacklist:\n  file: %s\n  watch_interval: 100ms\n", yamlPath)))
	app, _ := newApp(t, p)
	if err := app.Apply(context.Background(), mustLoad(t, p)); err != nil {
		t.Fatal(err)
	}
	wctx, wcancel := context.WithCancel(context.Background())
	t.Cleanup(wcancel)
	go app.WatchBlacklist(wctx)

	contains := func(entry string) bool {
		return slices.Contains(app.Blacklist().Entries, entry)
	}
	if !contains("203.0.113.7/32") {
		t.Fatalf("initial file entries missing: %v", app.Blacklist().Entries)
	}

	// Rewriting the file swaps the list within a couple of ticks and
	// records the reload event.
	if err := os.WriteFile(blFile, []byte("198.51.100.5"), 0o644); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return contains("198.51.100.5/32") && !contains("203.0.113.7/32") })
	sawReload := false
	for _, e := range app.events.List() {
		if e.Kind == "blacklist_reloaded" && strings.Contains(e.Detail, "source=file") {
			sawReload = true
		}
	}
	if !sawReload {
		t.Fatal("blacklist_reloaded event missing")
	}

	// A runtime delete of a file-sourced entry lasts until the next
	// watcher refresh that still carries it: it comes back when the file
	// includes it again (delete here uses the bare spelling GET never
	// shows, so this also pins the canonical DELETE lookup).
	if err := app.BlacklistDel("198.51.100.5"); err != nil {
		t.Fatal(err)
	}
	if contains("198.51.100.5/32") {
		t.Fatal("deleted file entry must leave the list")
	}
	if err := os.WriteFile(blFile, []byte("198.51.100.5,192.0.2.8"), 0o644); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return contains("198.51.100.5/32") && contains("192.0.2.8/32") })

	// A broken file keeps the previous list (best-effort watch).
	if err := os.WriteFile(blFile, []byte("not-an-ip"), 0o644); err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond)
	if !contains("198.51.100.5/32") || !contains("192.0.2.8/32") || len(app.Blacklist().Entries) != 2 {
		t.Fatalf("broken file must not change the list, got %v", app.Blacklist().Entries)
	}

	// Fixing the file recovers on a later tick.
	if err := os.WriteFile(blFile, []byte("192.0.2.9"), 0o644); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return contains("192.0.2.9/32") })
}

// A stopped listener's balancer must have its state-change callback detached:
// an in-flight probe from the old prober can report up to one timeout after
// Stop, and the old callback �?keyed by listener name �?would close the
// sessions of a same-named RESTARTED listener.
func TestStoppedListenerCallbackDetached(t *testing.T) {
	backend, kill := startEchoCtl(t)
	defer kill()
	p := writeCfg(t, fmt.Sprintf(`
listeners:
  - name: L1
    bind: 127.0.0.1:0
    backends: [%s]
    session_timeout: 60s
`, backend))
	app, _ := newApp(t, p)
	if err := app.Apply(context.Background(), mustLoad(t, p)); err != nil {
		t.Fatal(err)
	}
	app.mu.Lock()
	bal := app.balancers["L1"]
	app.mu.Unlock()

	// Stop L1 by applying a config that replaces it with L2 (config
	// validation requires at least one listener); the stale-prober window
	// is exactly here: an in-flight probe reports on bal after this
	// returns, and a same-named restart would be the victim.
	replace := writeCfg(t, fmt.Sprintf(`
listeners:
  - name: L2
    bind: 127.0.0.1:0
    backends: [%s]
    session_timeout: 60s
`, backend))
	if err := app.Apply(context.Background(), mustLoad(t, replace)); err != nil {
		t.Fatal(err)
	}

	// Simulate the late probe report: suspect + probe error confirms down,
	// which would fire the (formerly live) callback.
	bal.ReportError(backend, balancer.SrcDial)
	bal.ReportError(backend, balancer.SrcDial)
	bal.ReportError(backend, balancer.SrcDial)
	bal.ReportError(backend, balancer.SrcProbe)

	for _, e := range app.events.List() {
		if e.Kind == "backend_suspect" || e.Kind == "backend_down" || e.Kind == "backend_up" {
			t.Fatalf("stopped listener's balancer must not fire callbacks: %+v", e)
		}
	}
}

// blacklistFileCfg writes a config with one listener and a blacklist file.
func blacklistFileCfg(t *testing.T, backend string, blFile string) string {
	t.Helper()
	yamlPath := strings.ReplaceAll(blFile, "\\", "/")
	return writeCfg(t, cfgYAML(fmt.Sprintf(
		"  - name: L1\n    bind: 127.0.0.1:0\n    backends: [%s]\n", backend),
		fmt.Sprintf("blacklist:\n  file: %s\n", yamlPath)))
}

func blFileContains(t *testing.T, path, canonical string) bool {
	t.Helper()
	entries, err := blocklist.ReadFileEntries(path)
	if err != nil {
		t.Fatal(err)
	}
	can, err := canonicalizeEntries(entries)
	if err != nil {
		t.Fatal(err)
	}
	return slices.Contains(can, canonical)
}

// With a list file configured, a runtime add persists to the file and thus
// survives a restart (a fresh App on the same config).
func TestBlacklistAddPersistsToFile(t *testing.T) {
	backend := startEcho(t)
	blFile := filepath.Join(t.TempDir(), "bl.txt")
	if err := os.WriteFile(blFile, []byte("203.0.113.7"), 0o644); err != nil {
		t.Fatal(err)
	}
	p := blacklistFileCfg(t, backend, blFile)
	app, _ := newApp(t, p)
	if err := app.Apply(context.Background(), mustLoad(t, p)); err != nil {
		t.Fatal(err)
	}
	if err := app.BlacklistAdd("198.51.100.9"); err != nil {
		t.Fatal(err)
	}
	if !blFileContains(t, blFile, "198.51.100.9/32") {
		t.Fatal("runtime add must persist to the list file")
	}
	// Restart: a fresh app on the same config loads the added entry.
	app2, _ := newApp(t, p)
	if err := app2.Apply(context.Background(), mustLoad(t, p)); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(app2.Blacklist().Entries, "198.51.100.9/32") {
		t.Fatalf("added entry must survive restart: %v", app2.Blacklist().Entries)
	}
}

// With a list file configured, a runtime delete removes the entry from the
// file: the unblock is permanent, not reverted by the next reload.
func TestBlacklistDelRemovesFromFile(t *testing.T) {
	backend := startEcho(t)
	blFile := filepath.Join(t.TempDir(), "bl.txt")
	if err := os.WriteFile(blFile, []byte("203.0.113.7,198.51.100.9"), 0o644); err != nil {
		t.Fatal(err)
	}
	p := blacklistFileCfg(t, backend, blFile)
	app, _ := newApp(t, p)
	if err := app.Apply(context.Background(), mustLoad(t, p)); err != nil {
		t.Fatal(err)
	}
	if err := app.BlacklistDel("198.51.100.9"); err != nil {
		t.Fatal(err)
	}
	if blFileContains(t, blFile, "198.51.100.9/32") {
		t.Fatal("runtime delete must remove the entry from the list file")
	}
	// Reload resurrects nothing: the file no longer carries the entry.
	if err := app.Apply(context.Background(), mustLoad(t, p)); err != nil {
		t.Fatal(err)
	}
	if slices.Contains(app.Blacklist().Entries, "198.51.100.9/32") {
		t.Fatal("deleted file entry must stay deleted after reload")
	}
}

// A failed file write must not change the effective list: the file is the
// source of truth, so an unpersistable add is refused entirely.
func TestBlacklistAddPersistFailureKeepsState(t *testing.T) {
	backend := startEcho(t)
	dir := t.TempDir()
	blFile := filepath.Join(dir, "bl.txt")
	if err := os.WriteFile(blFile, []byte("203.0.113.7"), 0o644); err != nil {
		t.Fatal(err)
	}
	p := blacklistFileCfg(t, backend, blFile)
	app, _ := newApp(t, p)
	if err := app.Apply(context.Background(), mustLoad(t, p)); err != nil {
		t.Fatal(err)
	}
	// Replace the list file with a directory: reading it fails on every OS
	// (Windows ignores the read-only attribute on directories, so chmod
	// tricks are not portable here).
	if err := os.Remove(blFile); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(blFile, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Remove(blFile) })
	err := app.BlacklistAdd("198.51.100.9")
	if err == nil {
		t.Fatal("add must fail when the list file cannot be written")
	}
	if errors.Is(err, blocklist.ErrInvalidEntry) {
		t.Fatalf("persist failure misreported as validation error: %v", err)
	}
	if slices.Contains(app.Blacklist().Entries, "198.51.100.9/32") {
		t.Fatal("failed persist must leave the effective list unchanged")
	}
}

// Repeated adds of the same entry must not pile up duplicate lines in the
// list file: the append is idempotent.
func TestBlacklistAddIdempotent(t *testing.T) {
	backend := startEcho(t)
	blFile := filepath.Join(t.TempDir(), "bl.txt")
	if err := os.WriteFile(blFile, []byte("203.0.113.7"), 0o644); err != nil {
		t.Fatal(err)
	}
	p := blacklistFileCfg(t, backend, blFile)
	app, _ := newApp(t, p)
	if err := app.Apply(context.Background(), mustLoad(t, p)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := app.BlacklistAdd("198.51.100.9"); err != nil {
			t.Fatal(err)
		}
	}
	entries, err := blocklist.ReadFileEntries(blFile)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, e := range entries {
		if e == "198.51.100.9/32" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("file carries %d copies of the entry, want 1: %v", count, entries)
	}
}

// API writes keep the list file sorted by IP (numeric order, so
// 10.0.0.2 < 10.0.0.9 < 10.0.0.10, not lexicographic).
func TestBlacklistFileSortedOnWrite(t *testing.T) {
	backend := startEcho(t)
	blFile := filepath.Join(t.TempDir(), "bl.txt")
	if err := os.WriteFile(blFile, []byte("10.0.0.9,192.0.2.1"), 0o644); err != nil {
		t.Fatal(err)
	}
	p := blacklistFileCfg(t, backend, blFile)
	app, _ := newApp(t, p)
	if err := app.Apply(context.Background(), mustLoad(t, p)); err != nil {
		t.Fatal(err)
	}
	if err := app.BlacklistAdd("10.0.0.10"); err != nil {
		t.Fatal(err)
	}
	if err := app.BlacklistAdd("10.0.0.2"); err != nil {
		t.Fatal(err)
	}
	want := []string{"10.0.0.2/32", "10.0.0.9/32", "10.0.0.10/32", "192.0.2.1/32"}
	got, err := blocklist.ReadFileEntries(blFile)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("file order = %v, want %v", got, want)
	}
	// Delete keeps the order.
	if err := app.BlacklistDel("10.0.0.9"); err != nil {
		t.Fatal(err)
	}
	got, err = blocklist.ReadFileEntries(blFile)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, []string{"10.0.0.2/32", "10.0.0.10/32", "192.0.2.1/32"}) {
		t.Fatalf("file order after delete = %v", got)
	}
}

// payloadListenerCfg writes a one-listener config whose ingress gate allows
// payloads starting with the given hex magic at offset 0 (empty magic = no
// payload_filter section = gate off).
func payloadListenerCfg(t *testing.T, backend, magicHex string) string {
	t.Helper()
	pf := ""
	if magicHex != "" {
		pf = fmt.Sprintf("    payload_filter:\n      rules:\n        - magic_hex: \"%s\"\n          offset: 0\n", magicHex)
	}
	return writeCfg(t, cfgYAML(fmt.Sprintf(
		"  - name: L1\n    bind: 127.0.0.1:0\n    backends: [%s]\n", backend), pf))
}

// The payload gate end to end: /status counts drops, a reload swaps the gate
// in place (same bind -> updateListenerLocked), and a rule-less reload turns
// the gate off with a rules=0 event.
func TestPayloadGateStatusAndReload(t *testing.T) {
	b1 := startEcho(t)
	pA := payloadListenerCfg(t, b1, "aabb")
	app, _ := newApp(t, pA)
	if err := app.Apply(context.Background(), mustLoad(t, pA)); err != nil {
		t.Fatal(err)
	}
	l1 := app.listeners["L1"].Addr()

	legal := "\xaa\xbbhi"
	if got, err := roundTripUDP(t, l1, legal); err != nil || got != "echo:"+legal {
		t.Fatalf("legal datagram must pass the gate: %q %v", got, err)
	}
	if got, err := roundTripUDP(t, l1, "junk"); err == nil {
		t.Fatalf("illegal datagram must get no reply, got %q", got)
	}
	waitFor(t, func() bool { return app.Status().IllegalPackets == 1 })
	// The json tag and always-serialized shape are pinned through the raw
	// /status JSON, not just the Go struct.
	raw, err := json.Marshal(app.Status())
	if err != nil {
		t.Fatal(err)
	}
	var top map[string]any
	if err := json.Unmarshal(raw, &top); err != nil {
		t.Fatal(err)
	}
	if n, ok := top["illegal_packets"].(float64); !ok || n != 1 {
		t.Fatalf("illegal_packets = %v in %s, want 1", top["illegal_packets"], raw)
	}

	// Tightened rules: the old legal payload no longer matches, so it now
	// drops, and the reload records the new rules count.
	pB := payloadListenerCfg(t, b1, "ccdd")
	if err := app.Apply(context.Background(), mustLoad(t, pB)); err != nil {
		t.Fatal(err)
	}
	if got, err := roundTripUDP(t, l1, legal); err == nil {
		t.Fatalf("tightened gate must drop the old legal datagram, got %q", got)
	}
	waitFor(t, func() bool { return app.Status().IllegalPackets == 2 })
	sawTightened := false
	for _, e := range app.events.List() {
		if e.Kind == "payload_filter_reloaded" && strings.Contains(e.Detail, "L1 rules=1") {
			sawTightened = true
		}
	}
	if !sawTightened {
		t.Fatalf("payload_filter_reloaded rules=1 event missing: %+v", app.events.List())
	}

	// No rules: gate off, garbage flows, event carries rules=0.
	pC := payloadListenerCfg(t, b1, "")
	if err := app.Apply(context.Background(), mustLoad(t, pC)); err != nil {
		t.Fatal(err)
	}
	if got, err := roundTripUDP(t, l1, "junk"); err != nil || got != "echo:junk" {
		t.Fatalf("gate must be off after the rule-less reload: %q %v", got, err)
	}
	sawOff := false
	for _, e := range app.events.List() {
		if e.Kind == "payload_filter_reloaded" && strings.Contains(e.Detail, "L1 rules=0") {
			sawOff = true
		}
	}
	if !sawOff {
		t.Fatalf("payload_filter_reloaded rules=0 event missing: %+v", app.events.List())
	}
}

// A config whose payload rule does not compile fails the reload (reload_failed
// path) and leaves the running gate exactly as it was.
func TestReloadInvalidPayloadRuleKeepsGate(t *testing.T) {
	b1 := startEcho(t)
	pA := payloadListenerCfg(t, b1, "aabb")
	app, _ := newApp(t, pA)
	if err := app.Apply(context.Background(), mustLoad(t, pA)); err != nil {
		t.Fatal(err)
	}
	l1 := app.listeners["L1"].Addr()
	if got, err := roundTripUDP(t, l1, "\xaa\xbbok"); err != nil || got != "echo:\xaa\xbbok" {
		t.Fatalf("setup round trip: %q %v", got, err)
	}

	pB := writeCfg(t, cfgYAML(fmt.Sprintf(
		"  - name: L1\n    bind: 127.0.0.1:0\n    backends: [%s]\n", b1),
		"    payload_filter:\n      rules:\n        - magic_hex: \"zz\"\n"))
	app.cfgPath = pB
	if err := app.Reload(); err == nil {
		t.Fatal("reload with an uncompilable payload rule must fail")
	}
	sawFailed := false
	for _, e := range app.events.List() {
		if e.Kind == "reload_failed" {
			sawFailed = true
		}
	}
	if !sawFailed {
		t.Fatalf("reload_failed event missing: %+v", app.events.List())
	}
	// The running gate is untouched: the old legal payload still passes and
	// garbage still drops.
	if got, err := roundTripUDP(t, l1, "\xaa\xbbback"); err != nil || got != "echo:\xaa\xbbback" {
		t.Fatalf("previous rules must keep serving after the failed reload: %q %v", got, err)
	}
	if got, err := roundTripUDP(t, l1, "junk"); err == nil {
		t.Fatalf("gate must still drop non-matching datagrams, got %q", got)
	}
}
