package listener

import (
	"bufio"
	"context"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fetaoily/udpshunt/internal/balancer"
	"github.com/fetaoily/udpshunt/internal/blocklist"
	"github.com/fetaoily/udpshunt/internal/clientstats"
	"github.com/fetaoily/udpshunt/internal/config"
	"github.com/fetaoily/udpshunt/internal/metrics"
	"github.com/fetaoily/udpshunt/internal/payloadfilter"
	"github.com/fetaoily/udpshunt/internal/requestlog"
	"github.com/fetaoily/udpshunt/internal/session"
)

// startEcho runs a UDP server on 127.0.0.1:0 that replies "echo:<payload>".
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
			resp := append([]byte("echo:"), buf[:n]...)
			_, _ = pc.WriteToUDP(resp, from)
		}
	}()
	t.Cleanup(func() { pc.Close() })
	return pc.LocalAddr().String()
}

// startRecordingEcho behaves like startEcho and also counts the distinct
// client addresses that reached it.
func startRecordingEcho(t *testing.T) (addr string, sources func() int) {
	t.Helper()
	pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	seen := map[string]bool{}
	go func() {
		buf := make([]byte, 65536)
		for {
			n, from, err := pc.ReadFromUDP(buf)
			if err != nil {
				return
			}
			mu.Lock()
			seen[from.String()] = true
			mu.Unlock()
			resp := append([]byte("echo:"), buf[:n]...)
			_, _ = pc.WriteToUDP(resp, from)
		}
	}()
	t.Cleanup(func() { pc.Close() })
	return pc.LocalAddr().String(), func() int {
		mu.Lock()
		defer mu.Unlock()
		return len(seen)
	}
}

func newStack(t *testing.T, lc config.Listener, maxSessions int64) (*Listener, *balancer.Balancer, *session.Manager) {
	t.Helper()
	mgr := session.NewManager(maxSessions)
	bal := balancer.New(lc.Backends, balancer.Options{})
	l, err := New(lc.Name, lc, bal, mgr, slog.Default(), nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	// Cleanups run LIFO: cancel (stop receiving) -> CloseAll (unblock
	// downstream readers) -> Close (release the frontend socket). Run no
	// longer closes the socket itself, so Close must happen here.
	t.Cleanup(func() { _ = l.Close() })
	t.Cleanup(func() { mgr.CloseAll() })
	t.Cleanup(cancel)
	go func() { _ = l.Run(ctx) }()
	return l, bal, mgr
}

func testClient(t *testing.T) *net.UDPConn {
	t.Helper()
	pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pc.Close() })
	return pc
}

func roundTrip(t *testing.T, client *net.UDPConn, dst *net.UDPAddr, payload string) string {
	t.Helper()
	if _, err := client.WriteToUDP([]byte(payload), dst); err != nil {
		t.Fatal(err)
	}
	client.SetReadDeadline(time.Now().Add(3 * time.Second))
	buf := make([]byte, 65536)
	n, _, err := client.ReadFromUDP(buf)
	if err != nil {
		t.Fatalf("no reply: %v", err)
	}
	return string(buf[:n])
}

func TestFullProxyRoundTrip(t *testing.T) {
	backend := startEcho(t)
	lc := config.Listener{Name: "t", Bind: "127.0.0.1:0", Backends: []string{backend}, SessionTimeout: config.Duration(time.Minute)}
	l, _, _ := newStack(t, lc, 0)

	client := testClient(t)
	if got := roundTrip(t, client, l.Addr(), "hello"); got != "echo:hello" {
		t.Fatalf("reply = %q", got)
	}
}

func TestSessionReuseSameUpstreamPort(t *testing.T) {
	backend, sources := startRecordingEcho(t)
	lc := config.Listener{Name: "t", Bind: "127.0.0.1:0", Backends: []string{backend}, SessionTimeout: config.Duration(time.Minute)}
	l, _, mgr := newStack(t, lc, 0)

	client := testClient(t)
	for i := 0; i < 5; i++ {
		if got := roundTrip(t, client, l.Addr(), "pkt"); got != "echo:pkt" {
			t.Fatalf("reply %d = %q", i, got)
		}
	}
	if n := sources(); n != 1 {
		t.Fatalf("backend saw %d distinct client ports, want 1 (session reuse broken)", n)
	}
	if mgr.Count() != 1 {
		t.Fatalf("session count = %d, want 1", mgr.Count())
	}
}

// waitLines polls the request log file until it holds at least n lines
// (entries are written asynchronously from the receive loop).
func waitLines(t *testing.T, path string, n int) []string {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if lines := readLogLines(t, path); len(lines) >= n {
			return lines
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("request log %s never reached %d lines", path, n)
	return nil
}

func readLogLines(t *testing.T, path string) []string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()
	var lines []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		lines = append(lines, sc.Text())
	}
	return lines
}

func TestRequestLogOutcomes(t *testing.T) {
	dir := t.TempDir()
	rl := requestlog.New(requestlog.Options{Dir: dir, RetentionDays: 30})
	t.Cleanup(rl.Stop)
	// Entries are written asynchronously from the receive loop; the file
	// accumulates across subtests, so each waits for its cumulative line.
	logPath := filepath.Join(dir, "udpshunt-requests-"+time.Now().Format("2006-01-02")+".log")

	t.Run("forwarded", func(t *testing.T) {
		backend := startEcho(t)
		lc := config.Listener{Name: "rl-ok", Bind: "127.0.0.1:0", Backends: []string{backend}, SessionTimeout: config.Duration(time.Minute)}
		l, _, _ := newStackWithLog(t, lc, 0, rl)
		client := testClient(t)
		if got := roundTrip(t, client, l.Addr(), "hello"); got != "echo:hello" {
			t.Fatalf("reply = %q", got)
		}
		lines := waitLines(t, logPath, 1)
		last := lines[len(lines)-1]
		for _, want := range []string{`"listener":"rl-ok"`, `"outcome":"forwarded"`, `"backend":`, `"bytes":5`} {
			if !strings.Contains(last, want) {
				t.Fatalf("line %q missing %s", last, want)
			}
		}
	})

	t.Run("no_backend", func(t *testing.T) {
		// Pick fails open on an all-down pool, so no_backend happens only
		// with an empty pool (constructed here, bypassing config.Validate).
		lc := config.Listener{Name: "rl-none", Bind: "127.0.0.1:0", SessionTimeout: config.Duration(time.Minute)}
		l, _, _ := newStackWithLog(t, lc, 0, rl)
		client := testClient(t)
		if _, err := client.WriteToUDP([]byte("x"), l.Addr()); err != nil {
			t.Fatal(err)
		}
		lines := waitLines(t, logPath, 2)
		if !strings.Contains(lines[len(lines)-1], `"outcome":"no_backend"`) {
			t.Fatalf("want no_backend outcome, got %s", lines[len(lines)-1])
		}
	})

	t.Run("rejected", func(t *testing.T) {
		backend := startEcho(t)
		lc := config.Listener{Name: "rl-cap", Bind: "127.0.0.1:0", Backends: []string{backend}, SessionTimeout: config.Duration(time.Minute)}
		l, _, _ := newStackWithLog(t, lc, 1, rl) // session cap 1
		a := testClient(t)
		if got := roundTrip(t, a, l.Addr(), "one"); got != "echo:one" {
			t.Fatalf("first reply = %q", got)
		}
		b := testClient(t)
		if _, err := b.WriteToUDP([]byte("two"), l.Addr()); err != nil {
			t.Fatal(err)
		}
		lines := waitLines(t, logPath, 3)
		if !strings.Contains(lines[len(lines)-1], `"outcome":"rejected"`) {
			t.Fatalf("want rejected outcome, got %s", lines[len(lines)-1])
		}
	})
}

// newStackWithLog is newStack with a request logger attached.
func newStackWithLog(t *testing.T, lc config.Listener, maxSessions int64, rl *requestlog.Logger) (*Listener, *balancer.Balancer, *session.Manager) {
	t.Helper()
	mgr := session.NewManager(maxSessions)
	bal := balancer.New(lc.Backends, balancer.Options{})
	l, err := New(lc.Name, lc, bal, mgr, slog.Default(), nil, rl, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() { _ = l.Close() })
	t.Cleanup(func() { mgr.CloseAll() })
	t.Cleanup(cancel)
	go func() { _ = l.Run(ctx) }()
	return l, bal, mgr
}

func TestClientStatsCounting(t *testing.T) {
	stats := clientstats.New(clientstats.Options{Dir: t.TempDir(), TickEvery: time.Hour, SnapshotInterval: time.Hour})
	t.Cleanup(stats.Stop)

	backend := startEcho(t)
	lc := config.Listener{Name: "cs", Bind: "127.0.0.1:0", Backends: []string{backend}, SessionTimeout: config.Duration(time.Minute)}
	mgr := session.NewManager(0)
	bal := balancer.New(lc.Backends, balancer.Options{})
	l, err := New(lc.Name, lc, bal, mgr, slog.Default(), nil, nil, stats, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() { _ = l.Close() })
	t.Cleanup(func() { mgr.CloseAll() })
	t.Cleanup(cancel)
	go func() { _ = l.Run(ctx) }()

	client := testClient(t)
	if got := roundTrip(t, client, l.Addr(), "hello"); got != "echo:hello" {
		t.Fatalf("reply = %q", got)
	}

	// PacketOut is recorded by the downstream goroutine after the client
	// already saw the reply, so poll briefly for the row to complete.
	var row clientstats.Row
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		rows, tracked, _ := stats.Top("requests", false, 0)
		if tracked == 1 && len(rows) == 1 && rows[0].Responses >= 1 {
			row = rows[0]
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if row.IP != "127.0.0.1" || row.Requests != 1 || row.Responses != 1 ||
		row.BytesIn != 5 || row.BytesOut != int64(len("echo:hello")) {
		t.Fatalf("stats row wrong: %+v", row)
	}
}

// blacklistStack is a dedicated stack (like TestGracefulShutdown's) with a
// blocklist container, a live metrics registry and BOTH client-stats tables
// (general + blocked), so tests can swap lists while the listener runs and
// observe where each packet was accounted.
func blacklistStack(t *testing.T, name string, rl *requestlog.Logger) (*Listener, *session.Manager, *blocklist.Container, *metrics.Metrics, *clientstats.Table, *clientstats.Table) {
	t.Helper()
	backend := startEcho(t)
	lc := config.Listener{Name: name, Bind: "127.0.0.1:0", Backends: []string{backend}, SessionTimeout: config.Duration(time.Minute)}
	mgr := session.NewManager(0)
	bal := balancer.New(lc.Backends, balancer.Options{})
	bl := blocklist.NewContainer()
	met := metrics.New()
	stats := clientstats.New(clientstats.Options{Dir: t.TempDir(), TickEvery: time.Hour, SnapshotInterval: time.Hour})
	bstats := clientstats.New(clientstats.Options{Dir: t.TempDir(), TickEvery: time.Hour, SnapshotInterval: time.Hour})
	t.Cleanup(stats.Stop)
	t.Cleanup(bstats.Stop)
	l, err := New(lc.Name, lc, bal, mgr, slog.Default(), met.ForListener(name), rl, stats, bl, bstats)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	// Same LIFO teardown as newStack: cancel (stop receiving) -> CloseAll
	// (unblock downstream readers) -> Close (release the frontend socket).
	t.Cleanup(func() { _ = l.Close() })
	t.Cleanup(func() { mgr.CloseAll() })
	t.Cleanup(cancel)
	go func() { _ = l.Run(ctx) }()
	return l, mgr, bl, met, stats, bstats
}

// counterValue sums one counter metric across all its label values (0 when
// the counter has no children yet).
func counterValue(t *testing.T, m *metrics.Metrics, name string) float64 {
	t.Helper()
	fams, err := m.Registry().Gather()
	if err != nil {
		t.Fatal(err)
	}
	var sum float64
	for _, f := range fams {
		if f.GetName() == name {
			for _, mv := range f.GetMetric() {
				sum += mv.GetCounter().GetValue()
			}
		}
	}
	return sum
}

// waitCounter polls until the named counter reaches at least v (the receive
// loop processes packets asynchronously from the client's send).
func waitCounter(t *testing.T, m *metrics.Metrics, name string, v float64) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if counterValue(t, m, name) >= v {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("counter %s never reached %v", name, v)
}

// blockClientAddr builds a list holding the client's own local address: a
// real source spoof is impossible in a test, but blacklisting the client's
// IP exercises the same code path a spoofed source would hit.
func blockClientAddr(t *testing.T, cli *net.UDPConn) *blocklist.List {
	t.Helper()
	l, err := blocklist.New([]string{cli.LocalAddr().(*net.UDPAddr).IP.String()})
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func TestBlacklistedPacketDropped(t *testing.T) {
	l, mgr, bl, met, stats, bstats := blacklistStack(t, "bl", nil)

	cli := testClient(t)
	cliIP := cli.LocalAddr().(*net.UDPAddr).IP.String()
	bl.Swap(blockClientAddr(t, cli))

	if _, err := cli.WriteToUDP([]byte("evil"), l.Addr()); err != nil {
		t.Fatal(err)
	}
	cli.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	buf := make([]byte, 65536)
	if n, _, err := cli.ReadFromUDP(buf); err == nil {
		t.Fatalf("blacklisted client must get no reply, got %q", buf[:n])
	}
	// The receive loop has accounted the packet as blocked by now.
	waitCounter(t, met, "udpshunt_blacklisted_packets_total", 1)

	// A blocked packet must bypass the general accounting entirely: no
	// traffic counters, no row in the general client-stats table.
	if got := counterValue(t, met, "udpshunt_packets_in_total"); got != 0 {
		t.Fatalf("packets_in counted a blocked packet: %v", got)
	}
	if rows, _, _ := stats.Top("requests", true, 10); len(rows) != 0 {
		t.Fatalf("general client stats must not see a blocked packet: %+v", rows)
	}
	if mgr.Count() != 0 {
		t.Fatal("blacklisted client must not create a session")
	}

	// Blocked stats recorded for that IP.
	var row clientstats.Row
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		brows, _, _ := bstats.Top("requests", true, 10)
		if len(brows) == 1 && brows[0].IP == cliIP && brows[0].Requests >= 1 {
			row = brows[0]
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if row.IP != cliIP || row.Requests < 1 {
		t.Fatalf("blocked stats missing for %s: %+v", cliIP, row)
	}

	// Unblock -> traffic flows again and lands in the general accounting.
	empty, err := blocklist.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	bl.Swap(empty)
	if got := roundTrip(t, cli, l.Addr(), "ping"); got != "echo:ping" {
		t.Fatalf("after unblock: %q", got)
	}
	if got := counterValue(t, met, "udpshunt_packets_in_total"); got < 1 {
		t.Fatalf("packets_in must count unblocked packets, got %v", got)
	}
	if got := counterValue(t, met, "udpshunt_blacklisted_packets_total"); got != 1 {
		t.Fatalf("blacklisted must stay at the blocked packet only, got %v", got)
	}
}

func TestLogBlockedWritesRequestLog(t *testing.T) {
	dir := t.TempDir()
	rl := requestlog.New(requestlog.Options{Dir: dir, RetentionDays: 30, FlushEvery: 5 * time.Millisecond})
	t.Cleanup(rl.Stop)
	logPath := filepath.Join(dir, "udpshunt-requests-"+time.Now().Format("2006-01-02")+".log")
	l, _, bl, _, _, _ := blacklistStack(t, "bl-log", rl)

	cli := testClient(t)
	cliIP := cli.LocalAddr().(*net.UDPAddr).IP.String()
	bl.Swap(blockClientAddr(t, cli))

	l.UpdateLogBlocked(true)
	if _, err := cli.WriteToUDP([]byte("evil"), l.Addr()); err != nil {
		t.Fatal(err)
	}
	lines := waitLines(t, logPath, 1)
	last := lines[len(lines)-1]
	// Backend is empty (no backend was selected), so the omitempty field is
	// absent from the line entirely.
	for _, want := range []string{`"listener":"bl-log"`, `"outcome":"blacklisted"`, `"client":"` + cliIP + `:`} {
		if !strings.Contains(last, want) {
			t.Fatalf("line %q missing %s", last, want)
		}
	}
	if strings.Contains(last, `"backend"`) {
		t.Fatalf("blocked line must have no backend, got %s", last)
	}

	// Default off again: another blocked packet must not log.
	l.UpdateLogBlocked(false)
	before := len(readLogLines(t, logPath))
	if _, err := cli.WriteToUDP([]byte("evil2"), l.Addr()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond) // a few flush cycles
	if after := len(readLogLines(t, logPath)); after != before {
		t.Fatalf("log_blocked=false must not log: %d -> %d lines", before, after)
	}
}

// startCountingEcho behaves like startEcho and also counts every datagram
// it receives, so tests can prove a dropped packet never reached a backend.
func startCountingEcho(t *testing.T) (addr string, received func() int) {
	t.Helper()
	pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	var n atomic.Int64
	go func() {
		buf := make([]byte, 65536)
		for {
			m, from, err := pc.ReadFromUDP(buf)
			if err != nil {
				return
			}
			n.Add(1)
			resp := append([]byte("echo:"), buf[:m]...)
			_, _ = pc.WriteToUDP(resp, from)
		}
	}()
	t.Cleanup(func() { pc.Close() })
	return pc.LocalAddr().String(), func() int { return int(n.Load()) }
}

// gatedStack is blacklistStack with an ingress payload filter: the listener
// config carries pf, and the echo backend counts every datagram it receives,
// so tests can observe which drop path a packet took and whether an illegal
// packet ever reached a backend.
func gatedStack(t *testing.T, name string, pf config.PayloadFilter) (*Listener, *session.Manager, *blocklist.Container, *metrics.Metrics, *clientstats.Table, *clientstats.Table, func() int) {
	t.Helper()
	backend, received := startCountingEcho(t)
	lc := config.Listener{Name: name, Bind: "127.0.0.1:0", Backends: []string{backend}, SessionTimeout: config.Duration(time.Minute), PayloadFilter: pf}
	mgr := session.NewManager(0)
	bal := balancer.New(lc.Backends, balancer.Options{})
	bl := blocklist.NewContainer()
	met := metrics.New()
	stats := clientstats.New(clientstats.Options{Dir: t.TempDir(), TickEvery: time.Hour, SnapshotInterval: time.Hour})
	bstats := clientstats.New(clientstats.Options{Dir: t.TempDir(), TickEvery: time.Hour, SnapshotInterval: time.Hour})
	t.Cleanup(stats.Stop)
	t.Cleanup(bstats.Stop)
	l, err := New(lc.Name, lc, bal, mgr, slog.Default(), met.ForListener(name), nil, nil, bl, bstats)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	// Same LIFO teardown as newStack: cancel (stop receiving) -> CloseAll
	// (unblock downstream readers) -> Close (release the frontend socket).
	t.Cleanup(func() { _ = l.Close() })
	t.Cleanup(func() { mgr.CloseAll() })
	t.Cleanup(cancel)
	go func() { _ = l.Run(ctx) }()
	return l, mgr, bl, met, stats, bstats, received
}

// gatedStackWithLog is gatedStack with a request logger attached.
func gatedStackWithLog(t *testing.T, name string, pf config.PayloadFilter, rl *requestlog.Logger) (*Listener, *session.Manager, *blocklist.Container, *metrics.Metrics, *clientstats.Table, *clientstats.Table, func() int) {
	t.Helper()
	backend, received := startCountingEcho(t)
	lc := config.Listener{Name: name, Bind: "127.0.0.1:0", Backends: []string{backend}, SessionTimeout: config.Duration(time.Minute), PayloadFilter: pf}
	mgr := session.NewManager(0)
	bal := balancer.New(lc.Backends, balancer.Options{})
	bl := blocklist.NewContainer()
	met := metrics.New()
	stats := clientstats.New(clientstats.Options{Dir: t.TempDir(), TickEvery: time.Hour, SnapshotInterval: time.Hour})
	bstats := clientstats.New(clientstats.Options{Dir: t.TempDir(), TickEvery: time.Hour, SnapshotInterval: time.Hour})
	t.Cleanup(stats.Stop)
	t.Cleanup(bstats.Stop)
	l, err := New(lc.Name, lc, bal, mgr, slog.Default(), met.ForListener(name), rl, nil, bl, bstats)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	// Same LIFO teardown as newStack: cancel (stop receiving) -> CloseAll
	// (unblock downstream readers) -> Close (release the frontend socket).
	t.Cleanup(func() { _ = l.Close() })
	t.Cleanup(func() { mgr.CloseAll() })
	t.Cleanup(cancel)
	go func() { _ = l.Run(ctx) }()
	return l, mgr, bl, met, stats, bstats, received
}

// gateRule is the canonical single-rule gate used by the tests: magic
// 00112233 at offset 0, min_length 4.
var gateRule = payloadfilter.RuleConfig{MagicHex: "00112233", Offset: 0, MinLength: 4}

func TestPayloadGateDropsNonMatchingDatagrams(t *testing.T) {
	l, mgr, _, met, _, _, received := gatedStack(t, "gate", config.PayloadFilter{Rules: []payloadfilter.RuleConfig{gateRule}})

	cli := testClient(t)
	// No magic: dropped with no reply, no session, no backend receipt.
	if _, err := cli.WriteToUDP([]byte("hello"), l.Addr()); err != nil {
		t.Fatal(err)
	}
	cli.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	buf := make([]byte, 65536)
	if n, _, err := cli.ReadFromUDP(buf); err == nil {
		t.Fatalf("illegal datagram must get no reply, got %q", buf[:n])
	}
	waitCounter(t, met, "udpshunt_illegal_packets_total", 1)
	if got := received(); got != 0 {
		t.Fatalf("backend received %d datagrams from an illegal packet", got)
	}
	if mgr.Count() != 0 {
		t.Fatal("illegal datagram must not create a session")
	}

	// With the magic: normal echo reply.
	if got := roundTrip(t, cli, l.Addr(), "\x00\x11\x22\x33payload"); got != "echo:\x00\x11\x22\x33payload" {
		t.Fatalf("legal datagram reply = %q", got)
	}

	// Below min_length (3 bytes < 4): dropped too.
	if _, err := cli.WriteToUDP([]byte("abc"), l.Addr()); err != nil {
		t.Fatal(err)
	}
	cli.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	if n, _, err := cli.ReadFromUDP(buf); err == nil {
		t.Fatalf("short datagram must be dropped, got %q", buf[:n])
	}
	waitCounter(t, met, "udpshunt_illegal_packets_total", 2)
}

func TestPayloadGateMultiRuleAnyOf(t *testing.T) {
	// Second rule: magic aabb at offset 2, so "xy\xaa\xbb" matches only it.
	pf := config.PayloadFilter{Rules: []payloadfilter.RuleConfig{
		gateRule,
		{MagicHex: "aabb", Offset: 2, MinLength: 4},
	}}
	l, _, _, met, _, _, _ := gatedStack(t, "gate2", pf)

	cli := testClient(t)
	// Matching only the second rule is enough: forwarded.
	if got := roundTrip(t, cli, l.Addr(), "xy\xaa\xbb"); got != "echo:xy\xaa\xbb" {
		t.Fatalf("second-rule datagram reply = %q", got)
	}
	if got := counterValue(t, met, "udpshunt_illegal_packets_total"); got != 0 {
		t.Fatalf("forwarded datagram must not count as illegal, got %v", got)
	}
}

func TestPayloadGateOffEchoesGarbage(t *testing.T) {
	// No payload_filter section: a garbage datagram is echoed (unchanged).
	backend := startEcho(t)
	lc := config.Listener{Name: "nogate", Bind: "127.0.0.1:0", Backends: []string{backend}, SessionTimeout: config.Duration(time.Minute)}
	l, _, _ := newStack(t, lc, 0)
	client := testClient(t)
	if got := roundTrip(t, client, l.Addr(), "garbage"); got != "echo:garbage" {
		t.Fatalf("reply = %q", got)
	}
}

func TestBlacklistBeatsPayloadGate(t *testing.T) {
	l, mgr, bl, met, stats, bstats, received := gatedStack(t, "gate-bl", config.PayloadFilter{Rules: []payloadfilter.RuleConfig{gateRule}})

	cli := testClient(t)
	cliIP := cli.LocalAddr().(*net.UDPAddr).IP.String()
	bl.Swap(blockClientAddr(t, cli))

	// Non-magic garbage from a blacklisted client: the blacklist check runs
	// first, so the packet must land in the blocked path, not the gate.
	if _, err := cli.WriteToUDP([]byte("evil"), l.Addr()); err != nil {
		t.Fatal(err)
	}
	cli.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	buf := make([]byte, 65536)
	if n, _, err := cli.ReadFromUDP(buf); err == nil {
		t.Fatalf("blacklisted client must get no reply, got %q", buf[:n])
	}
	// The receive loop has accounted the packet as blocked by now.
	waitCounter(t, met, "udpshunt_blacklisted_packets_total", 1)

	// Blocked-path accounting only: no general counters, no general row,
	// no session — and never the illegal path or a backend receipt.
	if got := counterValue(t, met, "udpshunt_packets_in_total"); got != 0 {
		t.Fatalf("packets_in counted a blocked packet: %v", got)
	}
	if rows, _, _ := stats.Top("requests", true, 10); len(rows) != 0 {
		t.Fatalf("general client stats must not see a blocked packet: %+v", rows)
	}
	if mgr.Count() != 0 {
		t.Fatal("blacklisted client must not create a session")
	}
	if got := counterValue(t, met, "udpshunt_illegal_packets_total"); got != 0 {
		t.Fatalf("blacklisted packet must bypass the payload gate, illegal = %v", got)
	}
	if got := received(); got != 0 {
		t.Fatalf("backend received %d datagrams from a blocked packet", got)
	}

	// Blocked stats recorded for that IP.
	var row clientstats.Row
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		brows, _, _ := bstats.Top("requests", true, 10)
		if len(brows) == 1 && brows[0].IP == cliIP && brows[0].Requests >= 1 {
			row = brows[0]
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if row.IP != cliIP || row.Requests < 1 {
		t.Fatalf("blocked stats missing for %s: %+v", cliIP, row)
	}
}

func TestUpdateRulesNilReopensGate(t *testing.T) {
	l, _, _, met, _, _, received := gatedStack(t, "gate-off", config.PayloadFilter{Rules: []payloadfilter.RuleConfig{gateRule}})

	cli := testClient(t)
	// Gate on: garbage dropped.
	if _, err := cli.WriteToUDP([]byte("garbage"), l.Addr()); err != nil {
		t.Fatal(err)
	}
	waitCounter(t, met, "udpshunt_illegal_packets_total", 1)

	// Reload to no rules: the gate opens and garbage flows again.
	l.UpdateRules(nil)
	if got := roundTrip(t, cli, l.Addr(), "garbage"); got != "echo:garbage" {
		t.Fatalf("after UpdateRules(nil): %q", got)
	}
	if got := counterValue(t, met, "udpshunt_illegal_packets_total"); got != 1 {
		t.Fatalf("illegal must stay at the pre-reload packet, got %v", got)
	}
	if got := received(); got != 1 {
		t.Fatalf("backend received %d datagrams, want 1 (only the post-reload one)", got)
	}
}

func TestLogIllegalWritesRequestLog(t *testing.T) {
	dir := t.TempDir()
	rl := requestlog.New(requestlog.Options{Dir: dir, RetentionDays: 30, FlushEvery: 5 * time.Millisecond})
	t.Cleanup(rl.Stop)
	logPath := filepath.Join(dir, "udpshunt-requests-"+time.Now().Format("2006-01-02")+".log")
	l, _, _, met, _, _, _ := gatedStackWithLog(t, "gate-log", config.PayloadFilter{Rules: []payloadfilter.RuleConfig{gateRule}}, rl)

	cli := testClient(t)
	cliIP := cli.LocalAddr().(*net.UDPAddr).IP.String()

	// ON: the illegal drop is logged with its dedicated outcome.
	l.UpdateLogIllegal(true)
	if _, err := cli.WriteToUDP([]byte("evil"), l.Addr()); err != nil {
		t.Fatal(err)
	}
	lines := waitLines(t, logPath, 1)
	if len(lines) != 1 {
		t.Fatalf("want exactly one log line, got %d", len(lines))
	}
	last := lines[0]
	for _, want := range []string{`"listener":"gate-log"`, `"outcome":"illegal"`, `"client":"` + cliIP + `:`} {
		if !strings.Contains(last, want) {
			t.Fatalf("line %q missing %s", last, want)
		}
	}
	// Backend is empty (no backend was selected), so the omitempty field is
	// absent from the line entirely.
	if strings.Contains(last, `"backend"`) {
		t.Fatalf("illegal line must have no backend, got %s", last)
	}

	// OFF (the default): another illegal drop must not log.
	l.UpdateLogIllegal(false)
	before := len(readLogLines(t, logPath))
	if _, err := cli.WriteToUDP([]byte("evil2"), l.Addr()); err != nil {
		t.Fatal(err)
	}
	// The drop itself was processed; the log staying silent is the point.
	waitCounter(t, met, "udpshunt_illegal_packets_total", 2)
	time.Sleep(200 * time.Millisecond) // a few flush cycles
	if after := len(readLogLines(t, logPath)); after != before {
		t.Fatalf("log_illegal=false must not log: %d -> %d lines", before, after)
	}
}
