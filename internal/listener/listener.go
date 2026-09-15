// Package listener implements the frontend UDP receive loop and the
// per-session upstream forwarding path (full proxy mode).
package listener

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/fetaoily/udpshunt/internal/balancer"
	"github.com/fetaoily/udpshunt/internal/config"
	"github.com/fetaoily/udpshunt/internal/session"
)

const maxPacketSize = 65536

type Listener struct {
	name    string
	pc      *net.UDPConn
	bal     *balancer.Balancer
	mgr     *session.Manager
	timeout time.Duration
	logger  *slog.Logger
	wg      sync.WaitGroup // downstream goroutines
}

// New binds the frontend UDP socket described by cfg.Bind.
func New(name string, cfg config.Listener, bal *balancer.Balancer, mgr *session.Manager, logger *slog.Logger) (*Listener, error) {
	addr, err := net.ResolveUDPAddr("udp", cfg.Bind)
	if err != nil {
		return nil, err
	}
	pc, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, err
	}
	return &Listener{
		name:    name,
		pc:      pc,
		bal:     bal,
		mgr:     mgr,
		timeout: time.Duration(cfg.SessionTimeout),
		logger:  logger.With("listener", name),
	}, nil
}

// Addr returns the bound address (useful with :0 binds in tests).
func (l *Listener) Addr() *net.UDPAddr {
	return l.pc.LocalAddr().(*net.UDPAddr)
}

// Run receives packets until ctx is cancelled (which closes the socket).
func (l *Listener) Run(ctx context.Context) error {
	go func() {
		<-ctx.Done()
		l.pc.Close()
	}()
	buf := make([]byte, maxPacketSize)
	for {
		n, client, err := l.pc.ReadFromUDP(buf)
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			l.logger.Warn("read from frontend failed", "err", err)
			continue
		}
		l.handle(client, buf[:n])
	}
}

// handle routes one client packet: find or create the session, then forward
// to the session's backend.
func (l *Listener) handle(client *net.UDPAddr, pkt []byte) {
	s := l.mgr.Get(l.name, client)
	if s == nil {
		s = l.createSession(client)
		if s == nil {
			return
		}
	}
	s.Touch()
	if _, err := s.Upstream().Write(pkt); err != nil {
		l.bal.ReportError(s.Backend)
		l.mgr.Remove(l.name, client)
		return
	}
	l.bal.ReportSuccess(s.Backend)
}

// createSession picks a backend, dials the upstream socket and starts the
// downstream relay goroutine. It returns nil when the session could not be
// established or stored.
func (l *Listener) createSession(client *net.UDPAddr) *session.Session {
	backendAddr := l.bal.Pick()
	raddr, err := net.ResolveUDPAddr("udp", backendAddr)
	if err != nil {
		l.logger.Error("unresolvable backend address", "backend", backendAddr, "err", err)
		return nil
	}
	up, err := net.DialUDP("udp", nil, raddr)
	if err != nil {
		l.bal.ReportError(backendAddr)
		l.logger.Warn("dial upstream failed", "backend", backendAddr, "err", err)
		return nil
	}
	s := session.NewSession(l.name, client, backendAddr, up, l.timeout)
	if !l.mgr.Put(l.name, client, s) {
		s.Close()
		l.logger.Debug("session rejected (cap or duplicate), dropping packet", "client", client)
		return nil
	}
	l.wg.Add(1)
	go l.downstream(s)
	l.logger.Debug("session created", "client", client, "backend", backendAddr)
	return s
}

// downstream relays backend replies back to the client through the
// listener socket.
func (l *Listener) downstream(s *session.Session) {
	defer l.wg.Done()
	buf := make([]byte, maxPacketSize)
	for {
		n, err := s.Upstream().Read(buf)
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				l.bal.ReportError(s.Backend)
			}
			l.mgr.Remove(l.name, s.Client)
			return
		}
		s.Touch()
		if _, err := l.pc.WriteToUDP(buf[:n], s.Client); err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			l.bal.ReportError(s.Backend)
			l.mgr.Remove(l.name, s.Client)
			return
		}
		l.bal.ReportSuccess(s.Backend)
	}
}

// WaitDownstream waits up to maxWait for downstream goroutines to finish
// (the graceful-shutdown drain window, spec §10).
func (l *Listener) WaitDownstream(maxWait time.Duration) {
	done := make(chan struct{})
	go func() {
		l.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(maxWait):
	}
}
