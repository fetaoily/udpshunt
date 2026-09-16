package health

import (
	"context"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/fetaoily/udpshunt/internal/balancer"
	"github.com/fetaoily/udpshunt/internal/config"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

// startResponder echoes every packet back.
func startResponder(t *testing.T) string {
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
			_, _ = pc.WriteToUDP(buf[:n], from)
		}
	}()
	t.Cleanup(func() { pc.Close() })
	return pc.LocalAddr().String()
}

// startSilent swallows packets without replying.
func startSilent(t *testing.T) string {
	t.Helper()
	pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		buf := make([]byte, 65536)
		for {
			if _, _, err := pc.ReadFromUDP(buf); err != nil {
				return
			}
		}
	}()
	t.Cleanup(func() { pc.Close() })
	return pc.LocalAddr().String()
}

func testHC(mode, payload string) config.HealthCheck {
	return config.HealthCheck{
		Mode: mode, Payload: payload,
		Interval: config.Duration(50 * time.Millisecond),
		Timeout:  config.Duration(80 * time.Millisecond),
		Rise:     1, Fall: 2,
	}
}

func healthy(bal *balancer.Balancer, addr string) bool {
	for _, st := range bal.Snapshot() {
		if st.Addr == addr {
			return st.Healthy
		}
	}
	return false
}

func eventually(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("condition not met within %v: %s", timeout, what)
}

func TestRawProbeKeepsHealthyBackendUp(t *testing.T) {
	addr := startResponder(t)
	bal := balancer.New([]string{addr}, balancer.Options{Fall: 1, Rise: 1, ActiveChecks: true})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := Start(ctx, bal, []string{addr}, testHC("raw", "ab"), slog.Default())
	defer p.Stop()
	time.Sleep(300 * time.Millisecond)
	if !healthy(bal, addr) {
		t.Fatal("responding backend must stay healthy")
	}
}

func TestRawProbeMarksSilentBackendDown(t *testing.T) {
	addr := startSilent(t)
	bal := balancer.New([]string{addr}, balancer.Options{Fall: 2, Rise: 1, ActiveChecks: true})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := Start(ctx, bal, []string{addr}, testHC("raw", "ab"), slog.Default())
	defer p.Stop()
	eventually(t, 3*time.Second, "silent backend marked down", func() bool { return !healthy(bal, addr) })
}

func TestDNSProbeRespondsToAnyReply(t *testing.T) {
	addr := startResponder(t) // echoes the DNS query verbatim: still a reply
	bal := balancer.New([]string{addr}, balancer.Options{Fall: 2, Rise: 1, ActiveChecks: true})
	// push it down first, then prove the dns probe recovers it
	bal.ReportError(addr)
	bal.ReportError(addr)
	if healthy(bal, addr) {
		t.Fatal("should be down after 2 errors")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := Start(ctx, bal, []string{addr}, testHC("dns", ""), slog.Default())
	defer p.Stop()
	eventually(t, 3*time.Second, "dns probe recovers backend via rise", func() bool { return healthy(bal, addr) })
}

func TestStopHaltsProbing(t *testing.T) {
	addr := startSilent(t)
	bal := balancer.New([]string{addr}, balancer.Options{Fall: 2, Rise: 1, ActiveChecks: true})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := Start(ctx, bal, []string{addr}, testHC("raw", "ab"), slog.Default())
	p.Stop()
	time.Sleep(200 * time.Millisecond)
	if !healthy(bal, addr) {
		t.Fatal("probes stopped before fall threshold: must remain healthy")
	}
}
