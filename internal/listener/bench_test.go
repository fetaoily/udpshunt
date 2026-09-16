package listener

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/fetaoily/udpshunt/internal/balancer"
	"github.com/fetaoily/udpshunt/internal/config"
	"github.com/fetaoily/udpshunt/internal/session"
)

// discardLogger silences session-establishment logging from the hot path.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// benchStack wires one listener over one echo backend and returns a client
// socket plus the listener address.
func benchStack(b *testing.B) (client *net.UDPConn, laddr *net.UDPAddr) {
	b.Helper()
	pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		b.Fatal(err)
	}
	go func() {
		buf := make([]byte, 65536)
		for {
			n, from, err := pc.ReadFromUDP(buf)
			if err != nil {
				return
			}
			_, _ = pc.WriteToUDP(buf[:n], from)
		}
	}()
	b.Cleanup(func() { pc.Close() })

	mgr := session.NewManager(0)
	bal := balancer.New([]string{pc.LocalAddr().String()}, balancer.Options{})
	lc := config.Listener{
		Name: "bench", Bind: "127.0.0.1:0",
		Backends:       []string{pc.LocalAddr().String()},
		SessionTimeout: config.Duration(time.Minute),
	}
	l, err := New(lc.Name, lc, bal, mgr, discardLogger(), nil)
	if err != nil {
		b.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	// Cleanups run LIFO: cancel (stop the receive loop) -> CloseAll (unblock
	// the downstream relays; the package's goleak TestMain would otherwise
	// flag them) -> Close (release the frontend socket).
	b.Cleanup(func() { _ = l.Close() })
	b.Cleanup(func() { mgr.CloseAll() })
	b.Cleanup(cancel)
	go func() { _ = l.Run(ctx) }()

	cli, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { cli.Close() })
	return cli, l.Addr()
}

// BenchmarkProxyRoundTrip measures one full ping-pong through the proxy per
// op (send -> proxy -> backend echo -> proxy -> client read).
func BenchmarkProxyRoundTrip(b *testing.B) {
	client, laddr := benchStack(b)
	buf := make([]byte, 64)
	payload := make([]byte, 64)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := client.WriteToUDP(payload, laddr); err != nil {
			b.Fatal(err)
		}
		if err := client.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			b.Fatal(err)
		}
		if _, _, err := client.ReadFromUDP(buf); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkProxyPipelined keeps ~256 requests in flight and measures
// per-packet proxy cost under load (closer to the pps regime).
func BenchmarkProxyPipelined(b *testing.B) {
	client, laddr := benchStack(b)
	const inFlight = 256
	payload := make([]byte, 512)
	buf := make([]byte, 1500)

	acked := 0
	sent := 0
	client.SetReadDeadline(time.Time{}) // no deadline; we poll
	b.ResetTimer()
	for sent < b.N {
		// top up the window
		for sent < b.N && sent-acked < inFlight {
			if _, err := client.WriteToUDP(payload, laddr); err != nil {
				b.Fatal(err)
			}
			sent++
		}
		if err := client.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			b.Fatal(err)
		}
		if _, _, err := client.ReadFromUDP(buf); err != nil {
			b.Fatalf("read at sent=%d acked=%d: %v", sent, acked, err)
		}
		acked++
	}
}
