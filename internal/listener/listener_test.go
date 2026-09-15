package listener

import (
	"context"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/fetaoily/udpshunt/internal/balancer"
	"github.com/fetaoily/udpshunt/internal/config"
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
	bal := balancer.New(lc.Backends, 0)
	l, err := New(lc.Name, lc, bal, mgr, slog.Default())
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
