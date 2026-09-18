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
	"github.com/fetaoily/udpshunt/internal/clientstats"
	"github.com/fetaoily/udpshunt/internal/config"
	"github.com/fetaoily/udpshunt/internal/metrics"
	"github.com/fetaoily/udpshunt/internal/pktio"
	"github.com/fetaoily/udpshunt/internal/requestlog"
	"github.com/fetaoily/udpshunt/internal/session"
)

const maxPacketSize = 65536

const defaultReadBuffer = 4 << 20 // 4 MiB

type Listener struct {
	name     string
	pc       *net.UDPConn
	io       *pktio.Conn
	bal      *balancer.Balancer
	mgr      *session.Manager
	met      *metrics.ListenerMetrics
	reqLog   *requestlog.Logger
	stats    *clientstats.Table
	timeout  time.Duration
	logger   *slog.Logger
	resolved map[string]*net.UDPAddr // backend address -> pre-resolved address

	mu        sync.Mutex // guards stopped, waiters, timeout and the wg.Add in startDownstream
	stopped   bool       // set once draining: no new downstream goroutines
	waiters   []chan struct{}
	wg        sync.WaitGroup
	closeOnce sync.Once
	closeErr  error
}

// New binds the frontend UDP socket described by cfg.Bind and resolves every
// backend address once so the receive loop never pays for DNS on new sessions.
// met may be nil to run without metrics; reqLog may be nil to run without
// the per-request file log; stats may be nil to run without client stats.
func New(name string, cfg config.Listener, bal *balancer.Balancer, mgr *session.Manager, logger *slog.Logger, met *metrics.ListenerMetrics, reqLog *requestlog.Logger, stats *clientstats.Table) (*Listener, error) {
	addr, err := net.ResolveUDPAddr("udp", cfg.Bind)
	if err != nil {
		return nil, err
	}
	pc, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, err
	}
	readBuf := cfg.ReadBuffer
	if readBuf <= 0 {
		readBuf = defaultReadBuffer // spec §7: default above the OS baseline
	}
	if err := pc.SetReadBuffer(readBuf); err != nil {
		logger.Warn("set read buffer failed", "listener", cfg.Name, "size", readBuf, "err", err)
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
		io:       pktio.Wrap(pc),
		bal:      bal,
		mgr:      mgr,
		met:      met,
		reqLog:   reqLog,
		stats:    stats,
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

// UpdateTimeout changes the idle timeout applied to newly created sessions.
func (l *Listener) UpdateTimeout(d time.Duration) {
	l.mu.Lock()
	l.timeout = d
	l.mu.Unlock()
}

// Run receives packets until ctx is cancelled. Cancellation stops the receive
// loop without closing the socket: it must stay writable for the downstream
// drain window; call Close once draining is done (spec §10).
func (l *Listener) Run(ctx context.Context) error {
	go func() {
		<-ctx.Done()
		// Unblock the pending batch read via an already-expired read
		// deadline instead of closing the socket.
		_ = l.pc.SetReadDeadline(time.Now())
	}()
	// Batch size comes from the platform receiver — 64 on Linux, 1 elsewhere
	// — so non-Linux listeners stop allocating a 4 MiB arena they never use.
	n := l.io.MaxBatch()
	raw := make([]byte, n*pktio.MaxPacketSize)
	bufs := make([][]byte, n)
	for i := range bufs {
		bufs[i] = raw[i*pktio.MaxPacketSize : (i+1)*pktio.MaxPacketSize]
	}
	addrs := make([]*net.UDPAddr, n)
	sizes := make([]int, n)
	for {
		n, err := l.io.ReceiveBatch(bufs, addrs, sizes)
		if ctx.Err() != nil {
			// A buffered packet slipped through as cancellation fired; drop
			// it instead of creating sessions during shutdown.
			return nil
		}
		// Drain the packets already received before handling the error: a
		// partial batch (deadline expiry during shutdown) must not be dropped.
		for i := 0; i < n; i++ {
			l.met.PacketsIn(1)
			l.met.BytesIn(sizes[i])
			l.stats.PacketIn(addrs[i].IP, sizes[i])
			l.handle(addrs[i], bufs[i][:sizes[i]])
		}
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			l.logger.Warn("read from frontend failed", "err", err)
			continue
		}
	}
}

// handle routes one client packet: find or create the session, then forward
// to the session's backend. Every packet lands in the request log with its
// outcome (the log swallows nothing on the hot path: Record never blocks).
func (l *Listener) handle(client *net.UDPAddr, pkt []byte) {
	backend, outcome := l.forward(client, pkt)
	if l.reqLog != nil {
		l.reqLog.Record(requestlog.Entry{
			Time:     time.Now(),
			Listener: l.name,
			Client:   client.String(),
			Backend:  backend,
			Bytes:    len(pkt),
			Outcome:  outcome,
		})
	}
}

// forward sends one client packet to its session's backend and reports the
// backend involved ("" when none was selected) and the outcome.
func (l *Listener) forward(client *net.UDPAddr, pkt []byte) (backend, outcome string) {
	s := l.mgr.Get(l.name, client)
	if s == nil {
		var err string
		s, err = l.createSession(client)
		if s == nil {
			return "", err
		}
	}
	s.Touch()
	if _, err := s.Upstream().Write(pkt); err != nil {
		l.met.BackendError(s.Backend)
		l.bal.ReportError(s.Backend)
		l.mgr.Remove(l.name, client)
		return s.Backend, requestlog.OutcomeUpstreamError
	}
	l.met.BackendIn(s.Backend, 1)
	// No ReportSuccess here: a locally successful UDP write says nothing
	// about backend liveness; only a relayed reply (downstream) is a
	// liveness signal.
	return s.Backend, requestlog.OutcomeForwarded
}

// createSession picks a backend, dials the upstream socket and starts the
// downstream relay goroutine. It returns a nil session when one could not
// be established or stored, together with the outcome describing why
// (requestlog.OutcomeNoBackend / OutcomeRejected / OutcomeUpstreamError).
func (l *Listener) createSession(client *net.UDPAddr) (*session.Session, string) {
	l.mu.Lock()
	timeout := l.timeout
	l.mu.Unlock()
	backendAddr := l.bal.Pick(client.IP.String())
	if backendAddr == "" {
		return nil, requestlog.OutcomeNoBackend
	}
	raddr, ok := l.resolved[backendAddr]
	if !ok {
		// Should not happen (New pre-resolves every configured backend);
		// resolve on demand rather than silently drop traffic.
		var err error
		raddr, err = net.ResolveUDPAddr("udp", backendAddr)
		if err != nil {
			l.logger.Error("unresolvable backend address", "backend", backendAddr, "err", err)
			return nil, requestlog.OutcomeUpstreamError
		}
		l.logger.Error("backend address missing from startup resolution, resolved on demand", "backend", backendAddr)
		l.resolved[backendAddr] = raddr
	}
	up, err := net.DialUDP("udp", nil, raddr)
	if err != nil {
		l.bal.ReportError(backendAddr)
		l.logger.Warn("dial upstream failed", "backend", backendAddr, "err", err)
		return nil, requestlog.OutcomeUpstreamError
	}
	s := session.NewSession(l.name, client, backendAddr, up, timeout)
	if !l.mgr.Put(l.name, client, s) {
		s.Close()
		l.logger.Debug("session rejected (cap or duplicate), dropping packet", "client", client)
		return nil, requestlog.OutcomeRejected
	}
	if !l.startDownstream(s) {
		l.mgr.Remove(l.name, client)
		return nil, requestlog.OutcomeUpstreamError
	}
	l.met.SessionCreated()
	l.logger.Debug("session created", "client", client, "backend", backendAddr)
	return s, ""
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

// downstreamBufs recycles relay buffers: a buffer is held only for one
// read->write span, so memory scales with in-flight packets rather than
// with live sessions.
var downstreamBufs = sync.Pool{New: func() any { return make([]byte, maxPacketSize) }}

// downstream relays backend replies back to the client through the
// listener socket.
func (l *Listener) downstream(s *session.Session) {
	defer l.wg.Done()
	for {
		buf := downstreamBufs.Get().([]byte)
		n, err := s.Upstream().Read(buf)
		if err != nil {
			downstreamBufs.Put(buf)
			if !errors.Is(err, net.ErrClosed) {
				l.met.BackendError(s.Backend)
				l.bal.ReportError(s.Backend)
			}
			l.mgr.Remove(l.name, s.Client)
			return
		}
		s.Touch()
		_, werr := l.pc.WriteToUDP(buf[:n], s.Client)
		downstreamBufs.Put(buf)
		if werr != nil {
			if errors.Is(werr, net.ErrClosed) {
				return
			}
			l.met.BackendError(s.Backend)
			l.bal.ReportError(s.Backend)
			l.mgr.Remove(l.name, s.Client)
			return
		}
		l.met.PacketsOut(1)
		l.met.BytesOut(n)
		l.stats.PacketOut(s.Client.IP, n)
		l.met.BackendOut(s.Backend, 1)
		l.bal.ReportSuccess(s.Backend)
	}
}

// WaitDownstream waits up to maxWait for downstream goroutines to finish.
// Once called, no new downstream goroutines are spawned. The internal waiter
// is tracked so WaitWaiters can join it later (no goroutine leaks).
func (l *Listener) WaitDownstream(maxWait time.Duration) {
	l.mu.Lock()
	l.stopped = true
	done := make(chan struct{})
	l.waiters = append(l.waiters, done)
	l.mu.Unlock()
	go func() {
		l.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(maxWait):
	}
}

// WaitWaiters joins every tracked waiter goroutine (call after CloseAll so
// pending waiters terminate).
func (l *Listener) WaitWaiters(maxWait time.Duration) {
	deadline := time.Now().Add(maxWait)
	l.mu.Lock()
	waiters := append([]chan struct{}{}, l.waiters...)
	l.mu.Unlock()
	for _, done := range waiters {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return
		}
		select {
		case <-done:
		case <-time.After(remaining):
			return
		}
	}
}
