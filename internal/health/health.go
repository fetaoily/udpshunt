// Package health actively probes backends and feeds the shared balancer
// state machine (spec §5: any response within the window counts).
package health

import (
	"context"
	"encoding/hex"
	"log/slog"
	"net"
	"time"

	"github.com/fetaoily/udpshunt/internal/balancer"
	"github.com/fetaoily/udpshunt/internal/config"
)

// dnsQuery is a wire-format A query for "health.udpshunt.invalid" (RD set).
// Any response — including NXDOMAIN — proves the backend is answering.
var dnsQuery = []byte{
	0x00, 0x00, // ID
	0x01, 0x00, // flags: recursion desired
	0x00, 0x01, // QDCOUNT
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	6, 'h', 'e', 'a', 'l', 't', 'h',
	8, 'u', 'd', 'p', 's', 'h', 'u', 'n', 't',
	7, 'i', 'n', 'v', 'a', 'l', 'i', 'd',
	0x00,
	0x00, 0x01, // QTYPE A
	0x00, 0x01, // QCLASS IN
}

// Prober runs one probe goroutine per backend address.
type Prober struct {
	cancel context.CancelFunc
}

// Start launches probes for every backend. Cancel ctx (or call Stop) to end
// them. The balancer's Options must have ActiveChecks=true so Rise recovery
// applies.
func Start(ctx context.Context, bal *balancer.Balancer, addrs []string, hc config.HealthCheck, logger *slog.Logger) *Prober {
	pctx, cancel := context.WithCancel(ctx)
	p := &Prober{cancel: cancel}
	interval := time.Duration(hc.Interval)
	timeout := time.Duration(hc.Timeout)
	var payload []byte
	if hc.Mode == "dns" {
		payload = dnsQuery
	} else {
		payload, _ = hex.DecodeString(hc.Payload) // validated at config load
	}
	for _, addr := range addrs {
		go p.probeLoop(pctx, bal, addr, payload, interval, timeout, logger)
	}
	return p
}

// Stop cancels every probe goroutine. Idempotent.
func (p *Prober) Stop() { p.cancel() }

func (p *Prober) probeLoop(ctx context.Context, bal *balancer.Balancer, addr string, payload []byte, interval, timeout time.Duration, logger *slog.Logger) {
	raddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		logger.Error("health check cannot resolve backend", "backend", addr, "err", err)
		return
	}
	buf := make([]byte, 65536)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		if err := probeOnce(raddr, payload, timeout, buf); err != nil {
			bal.ReportError(addr)
		} else {
			bal.ReportSuccess(addr)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// probeOnce sends the payload on a fresh connected socket and waits for any
// response until the deadline. A fresh socket per probe keeps a stale ICMP
// error from poisoning the next round.
func probeOnce(raddr *net.UDPAddr, payload []byte, timeout time.Duration, buf []byte) error {
	conn, err := net.DialUDP("udp", nil, raddr)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := conn.Write(payload); err != nil {
		return err
	}
	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return err
	}
	if _, err := conn.Read(buf); err != nil {
		return err // includes timeout (i/o timeout) and ICMP-derived errors
	}
	return nil
}
