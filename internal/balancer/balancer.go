// Package balancer picks a backend for new sessions and tracks health state.
// The reporting path (ReportError/ReportSuccess) and Pick/Snapshot are
// lock-free: per-backend counters and state are atomics, the pool and
// options are copy-on-write atomic pointers (spec §7: no hot-path locks).
package balancer

import (
	"sync/atomic"
	"time"
)

const (
	defaultCooldown = 10 * time.Second
	defaultFall     = 3
	defaultRise     = 2
)

// Options configures the balancing algorithm and health state machine.
// The zero value behaves like M1: round_robin, fall 3, passive cooldown
// recovery.
type Options struct {
	Balance      string        // round_robin | least_sessions | source_hash
	Fall         int           // consecutive errors before down (default 3)
	Rise         int           // consecutive successes to recover (default 2)
	Cooldown     time.Duration // passive recovery cooldown (default 10s)
	ActiveChecks bool          // true: recovery needs Rise successes (no cooldown path)
	// Counts reports the live session count per backend (least_sessions
	// only). It is called during Pick and must not re-enter the Balancer.
	Counts func(addr string) int64
}

// BackendState is a point-in-time view of one backend.
type BackendState struct {
	Addr    string
	Healthy bool
}

type backend struct {
	addr         string
	healthy      atomic.Bool
	errCount     atomic.Int64
	successCount atomic.Int64
	downSince    atomic.Int64 // unix nanoseconds
}

type pool struct {
	list   []*backend
	byAddr map[string]*backend
}

type Balancer struct {
	pool    atomic.Pointer[pool]
	opts    atomic.Pointer[Options]
	next    atomic.Uint64
	onState atomic.Pointer[func(addr string, healthy bool)]
}

func (b *Balancer) fire(fn *func(addr string, healthy bool), addr string, healthy bool) {
	if fn != nil && *fn != nil {
		(*fn)(addr, healthy)
	}
}

func (b *Balancer) onStateFn() *func(addr string, healthy bool) { return b.onState.Load() }

// New creates a balancer over addrs.
func New(addrs []string, opts Options) *Balancer {
	if opts.Fall <= 0 {
		opts.Fall = defaultFall
	}
	if opts.Rise <= 0 {
		opts.Rise = defaultRise
	}
	if opts.Cooldown <= 0 {
		opts.Cooldown = defaultCooldown
	}
	if opts.Balance == "" {
		opts.Balance = "round_robin"
	}
	b := &Balancer{}
	o := opts
	b.opts.Store(&o)
	b.Update(addrs)
	return b
}

// Update replaces the backend pool, preserving the state of backends that
// stay; new backends start healthy with zeroed counters.
func (b *Balancer) Update(addrs []string) {
	var prev map[string]*backend
	if old := b.pool.Load(); old != nil {
		prev = make(map[string]*backend, len(old.list))
		for _, be := range old.list {
			prev[be.addr] = be
		}
	}
	list := make([]*backend, 0, len(addrs))
	for _, a := range addrs {
		if be, ok := prev[a]; ok {
			list = append(list, be)
		} else {
			be := &backend{addr: a}
			be.healthy.Store(true)
			list = append(list, be)
		}
	}
	byAddr := make(map[string]*backend, len(list))
	for _, be := range list {
		byAddr[be.addr] = be
	}
	b.pool.Store(&pool{list: list, byAddr: byAddr})
}

// SetBalance switches the algorithm live.
func (b *Balancer) SetBalance(mode string) {
	o := *b.opts.Load()
	o.Balance = mode
	b.opts.Store(&o)
}

// SetHealth adjusts the state machine thresholds live (reload path).
// rise/fall <= 0 leave the current values unchanged.
func (b *Balancer) SetHealth(activeChecks bool, rise, fall int) {
	o := *b.opts.Load()
	o.ActiveChecks = activeChecks
	if rise > 0 {
		o.Rise = rise
	}
	if fall > 0 {
		o.Fall = fall
	}
	b.opts.Store(&o)
}

// SetOnStateChange registers a callback fired on every UP<->DOWN transition.
// The callback runs outside any lock, exactly once per transition.
func (b *Balancer) SetOnStateChange(fn func(addr string, healthy bool)) {
	b.onState.Store(&fn)
}

// Pick returns a healthy backend for a new session from clientIP (used only
// by source_hash). It fails open when every backend is down (spec
// assumption B) and returns "" for an empty pool.
func (b *Balancer) Pick(clientIP string) string {
	p := b.pool.Load()
	if p == nil || len(p.list) == 0 {
		return ""
	}
	o := b.opts.Load()
	var recovered []string
	addr, ok := b.pick(clientIP, p, o, true, &recovered)
	if !ok {
		// fail-open: same algorithm ignoring health
		addr, _ = b.pick(clientIP, p, o, false, &recovered)
	}
	b.fireRecovered(b.onStateFn(), recovered)
	return addr
}

func (b *Balancer) pick(clientIP string, p *pool, o *Options, needHealthy bool, recovered *[]string) (string, bool) {
	n := uint64(len(p.list))
	switch o.Balance {
	case "least_sessions":
		if o.Counts != nil {
			best := -1
			var bestCount int64
			for i, be := range p.list {
				if needHealthy && !b.usableTrack(be, o, recovered) {
					continue
				}
				c := o.Counts(be.addr)
				if best == -1 || c < bestCount {
					best, bestCount = i, c
				}
			}
			if best >= 0 {
				return p.list[best].addr, true
			}
			return "", false
		}
		fallthrough
	case "source_hash":
		if clientIP != "" {
			best := -1
			var bestHash uint64
			for i, be := range p.list {
				if needHealthy && !b.usableTrack(be, o, recovered) {
					continue
				}
				h := rendezvous(clientIP, be.addr)
				if best == -1 || h > bestHash {
					best, bestHash = i, h
				}
			}
			if best >= 0 {
				return p.list[best].addr, true
			}
			return "", false
		}
	}
	// round_robin (and fallback for empty clientIP)
	start := (b.next.Add(1) - 1) % n
	for i := uint64(0); i < n; i++ {
		be := p.list[(start+i)%n]
		if !needHealthy || b.usableTrack(be, o, recovered) {
			return be.addr, true
		}
	}
	return p.list[start].addr, !needHealthy
}

// usable reports whether the backend may serve traffic, applying
// passive cooldown recovery. recovered reports whether this call flipped the
// backend from down to healthy (cooldown elapsed); the caller must fire the
// state-change callback for it after its pool read completes. The flip is a
// CompareAndSwap so concurrent observers fire the callback exactly once.
func (b *Balancer) usable(be *backend, o *Options) (usable, recovered bool) {
	if be.healthy.Load() {
		return true, false
	}
	if o.ActiveChecks {
		return false, false // recovery only via ReportSuccess rise path
	}
	if time.Since(time.Unix(0, be.downSince.Load())) >= o.Cooldown {
		// errCount is zeroed before the flip, which removes the systematic
		// window: an observer of the flipped-up state no longer routinely
		// sees the stale >= Fall streak. Residual: a ReportError exactly
		// concurrent with the recovery can still observe the stale count
		// and re-down the backend — fail-closed, same pathological-scheduler
		// family as the documented CAS-loser residuals; a CAS loser's
		// zeroing is inert.
		be.errCount.Store(0)
		if be.healthy.CompareAndSwap(false, true) {
			return true, true
		}
		return false, false
	}
	return false, false
}

// usableTrack is usable plus collection of recovered addresses.
func (b *Balancer) usableTrack(be *backend, o *Options, recovered *[]string) bool {
	usable, rec := b.usable(be, o)
	if rec {
		*recovered = append(*recovered, be.addr)
	}
	return usable
}

// fireRecovered publishes cooldown recoveries collected during a Pick or
// Snapshot pool read, after that read completes and in pool order — same
// discipline as ReportError and ReportSuccess.
func (b *Balancer) fireRecovered(fn *func(addr string, healthy bool), recovered []string) {
	for _, addr := range recovered {
		b.fire(fn, addr, true)
	}
}

// rendezvous implements highest-random-weight hashing: the (client, backend)
// pair with the highest hash wins, so removing one backend only remaps the
// keys that preferred it (spec: minimal disruption).
func rendezvous(clientIP, backendAddr string) uint64 {
	h := uint64(14695981039346656037)
	for i := 0; i < len(clientIP); i++ {
		h ^= uint64(clientIP[i])
		h *= 1099511628211
	}
	h ^= ':'
	h *= 1099511628211
	for i := 0; i < len(backendAddr); i++ {
		h ^= uint64(backendAddr[i])
		h *= 1099511628211
	}
	return h
}

// ReportError records a failure against a backend. Fall consecutive errors
// mark it down.
func (b *Balancer) ReportError(addr string) {
	o := b.opts.Load()
	p := b.pool.Load()
	if o == nil || p == nil {
		return
	}
	be := p.byAddr[addr]
	if be == nil {
		return
	}
	n := be.errCount.Add(1)
	be.successCount.Store(0)
	if be.healthy.Load() && n >= int64(o.Fall) {
		// Metadata lands before the flip: an observer of the flipped-down
		// state must never see a stale (cooldown-elapsed) stamp. A CAS
		// loser's write is inert — stamps are only read while down.
		be.downSince.Store(time.Now().UnixNano())
		if be.healthy.CompareAndSwap(true, false) {
			// Fired after the state flip so ordering between the down and a
			// following up transition is deterministic; never called under a
			// lock. The CAS winner fires, so a transition fires exactly once.
			b.fire(b.onStateFn(), addr, false)
		}
	}
}

// ReportSuccess records a liveness signal. When healthy it clears the error
// streak; when down with ActiveChecks it counts toward Rise recovery.
func (b *Balancer) ReportSuccess(addr string) {
	o := b.opts.Load()
	p := b.pool.Load()
	if o == nil || p == nil {
		return
	}
	be := p.byAddr[addr]
	if be == nil {
		return
	}
	if be.healthy.Load() {
		be.errCount.Store(0)
		return
	}
	if !o.ActiveChecks {
		return
	}
	if be.successCount.Add(1) >= int64(o.Rise) {
		// Counters are zeroed before the flip, which removes the systematic
		// window: an observer of the flipped-up state no longer routinely
		// sees the stale >= Fall error streak. Residual: a ReportError
		// exactly concurrent with the recovery's completing success can
		// still observe the stale count and re-down a fresh recovery —
		// fail-closed, same pathological-scheduler family as the documented
		// CAS-loser residuals. A CAS loser's zeroing is inert — the
		// counters restart on the next down entry.
		be.errCount.Store(0)
		be.successCount.Store(0)
		if be.healthy.CompareAndSwap(false, true) {
			b.fire(b.onStateFn(), addr, true)
		}
	}
}

// Snapshot returns the current state of every backend. A backend whose
// passive cooldown has elapsed reports (and becomes) healthy again, exactly
// as the next Pick would treat it, and that recovery is fired through the
// state-change callback like any other transition.
func (b *Balancer) Snapshot() []BackendState {
	p := b.pool.Load()
	if p == nil {
		return nil
	}
	o := b.opts.Load()
	var recovered []string
	out := make([]BackendState, 0, len(p.list))
	for _, be := range p.list {
		if _, rec := b.usable(be, o); rec { // reflect elapsed-cooldown recovery in the view
			recovered = append(recovered, be.addr)
		}
		out = append(out, BackendState{Addr: be.addr, Healthy: be.healthy.Load()})
	}
	b.fireRecovered(b.onStateFn(), recovered)
	return out
}
