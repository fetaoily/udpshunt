// Package listener implements the frontend UDP receive loop and the
// per-session upstream forwarding path (full proxy mode).
package listener

import (
	"context"
	"errors"
	"fmt"
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
	name     string
	pc       *net.UDPConn
	bal      *balancer.Balancer
	mgr      *session.Manager
	timeout  time.Duration
	logger   *slog.Logger
	resolved map[string]*net.UDPAddr // backend address -> pre-resolved address

	mu        sync.Mutex // guards stopped and the wg.Add in startDownstream
	stopped   bool       // set once draining: no new downstream goroutines
	wg        sync.WaitGroup
	closeOnce sync.Once
	closeErr  error
}

// New binds the frontend UDP socket described by cfg.Bind and resolves every
// backend address once so the receive loop never pays for DNS on new sessions.
func New(name string, cfg config.Listener, bal *balancer.Balancer, mgr *session.Manager, logger *slog.Logger) (*Listener, error) {
	addr, err := net.ResolveUDPAddr("udp", cfg.Bind)
	if err != nil {
		return nil, err
	}
	pc, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, err
	}
	resolved := make(map[string]*net.UDPAddr, len(cfg.Backends))
	for _, b := range cfg.Backends {
		ra, err := net.ResolveUDPAddr("udp", b)
		if err != nil {
			pc.Close()
			return nil, fmt.Errorf("resolve backend %q: %w", b, err)
		}
		resolved[b] = ra
	}
	return &Listener{
		name:     name,
		pc:       pc,
		bal:      bal,
		mgr:      mgr,
		timeout:  time.Duration(cfg.SessionTimeout),
		logger:   logger.With("listener", name),
		resolved: resolved,
	}, nil
}

// Addr returns the bound address (useful with :0 binds in tests).
func (l *Listener) Addr() *net.UDPAddr {
	return l.pc.LocalAddr().(*net.UDPAddr)
}

// Close closes the frontend socket (single close point). It is safe to call
// after Run has returned and safe to call more than once.
func (l *Listener) Close() error {
	l.closeOnce.Do(func() { l.closeErr = l.pc.Close() })
	return l.closeErr
}

// Run receives packets until ctx is cancelled. Cancellation stops the receive
// loop without closing the socket: it must stay writable for the downstream
// drain window; call Close once draining is done (spec §10).
func (l *Listener) Run(ctx context.Context) error {
	go func() {
		<-ctx.Done()
		// Unblock the pending ReadFromUDP via an already-expired read
		// deadline instead of closing the socket.
		_ = l.pc.SetReadDeadline(time.Now())
	}()
	buf := make([]byte, maxPacketSize)
	for {
		n, client, err := l.pc.ReadFromUDP(buf)
		if err != nil {
			if ctx.Err() != nil {
				// Cancellation path: the watcher's read deadline unblocked
				// the pending read; stop receiving cleanly.
				return nil
			}
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			l.logger.Warn("read from frontend failed", "err", err)
			continue
		}
		if ctx.Err() != nil {
			// A buffered packet slipped through as cancellation fired; drop
			// it instead of creating sessions during shutdown.
			return nil
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
	// No ReportSuccess here: a locally successful UDP write says nothing
	// about backend liveness; only a relayed reply (downstream) is a
	// liveness signal.
}

// createSession picks a backend, dials the upstream socket and starts the
// downstream relay goroutine. It returns nil when the session could not be
// established or stored.
func (l *Listener) createSession(client *net.UDPAddr) *session.Session {
	backendAddr := l.bal.Pick(client.IP.String())
	if backendAddr == "" {
		return nil
	}
	raddr, ok := l.resolved[backendAddr]
	if !ok {
		// Should not happen (New pre-resolves every configured backend);
		// resolve on demand rather than silently drop traffic.
		var err error
		raddr, err = net.ResolveUDPAddr("udp", backendAddr)
		if err != nil {
			l.logger.Error("unresolvable backend address", "backend", backendAddr, "err", err)
			return nil
		}
		l.logger.Error("backend address missing from startup resolution, resolved on demand", "backend", backendAddr)
		l.resolved[backendAddr] = raddr
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
	if !l.startDownstream(s) {
		l.mgr.Remove(l.name, client)
		return nil
	}
	l.logger.Debug("session created", "client", client, "backend", backendAddr)
	return s
}

// startDownstream spawns the downstream relay for s. The stopped check and
// the wg.Add share the mutex so a drain-time wg.Wait cannot race a late
// wg.Add; it returns false once the listener is draining.
func (l *Listener) startDownstream(s *session.Session) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.stopped {
		return false
	}
	l.wg.Add(1)
	go l.downstream(s)
	return true
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
// (the graceful-shutdown drain window, spec §10). Once called, no new
// downstream goroutines are spawned.
func (l *Listener) WaitDownstream(maxWait time.Duration) {
	l.mu.Lock()
	l.stopped = true
	l.mu.Unlock()
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
