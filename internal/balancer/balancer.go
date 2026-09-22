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

// Error sources reported through ReportError/ReportSuccess. Only SrcProbe
// (active checks) and SrcWindow (passive cooldown) may confirm a suspect
// backend down; data-path sources alone never do. SrcProbeDial is a probe
// that failed before leaving this process (dial/write under fd or port
// exhaustion): proxy-local evidence that counts toward the streak but never
// confirms.
const (
	SrcProbe         = "probe"          // active health-check probe
	SrcProbeDial     = "probe_dial"     // probe dial/write failed locally (proxy resource pressure, not backend)
	SrcReply         = "reply"          // relayed backend reply (real traffic)
	SrcUpstreamWrite = "upstream_write" // session-socket write to backend failed
	SrcDial          = "dial"           // DialUDP for a new session failed
	SrcRelayRead     = "relay_read"     // session-socket read from backend failed
	SrcWindow        = "window"         // passive suspect window confirmed the down
)

// State is the coarse health state of a backend; StateHealthy covers
// suspect-free serving.
type State uint8

const (
	StateHealthy State = iota
	StateSuspect
	StateDown
)

// Transition is one state-machine move. Source means different things per
// transition: Healthy->Suspect carries the error source that completed the
// streak; Suspect->Down carries SrcProbe or SrcWindow (the confirmer);
// Suspect->Healthy (cleared) carries the success source (SrcProbe/SrcReply);
// Down->Healthy leaves it "" (not attributed).
type Transition struct {
	Addr       string
	From, To   State
	Source     string // per-transition meaning documented above
	LastSource string // most recent error source at transition time ("" if none)
	Errors     int64  // errCount at transition time
}

// Options configures the balancing algorithm and health state machine.
// The zero value behaves like M1: round_robin, fall 3, passive cooldown
// recovery.
type Options struct {
	Balance      string        // round_robin | least_sessions | source_hash
	Fall         int           // consecutive errors before suspect (default 3)
	Rise         int           // consecutive successes to recover (default 2)
	Cooldown     time.Duration // passive recovery cooldown (default 10s)
	ActiveChecks bool          // true: recovery needs Rise successes (no cooldown path)
	// Counts reports the live session count per backend (least_sessions
	// only). It is called during Pick and must not re-enter the Balancer.
	Counts func(addr string) int64
}

// BackendState is a point-in-time view of one backend.
type BackendState struct {
	Addr            string
	Healthy         bool  // true while suspect (suspect backends serve traffic)
	Suspect         bool  // fall errors reached, awaiting confirmation
	SuspectSince    int64 // unix nanos, 0 when not suspect
	ErrCount        int64
	LastErrorSource string
	DownConfirmBy   string // "probe" | "window", set when down
}

type backend struct {
	addr         string
	healthy      atomic.Bool
	errCount     atomic.Int64
	successCount atomic.Int64
	downSince    atomic.Int64 // unix nanoseconds
	suspectSince atomic.Int64 // unix nanoseconds; 0 = not suspect
	lastErrSrc   atomic.Pointer[string]
	downConfirm  atomic.Pointer[string] // "probe" | "window", set on down entry
}

type pool struct {
	list   []*backend
	byAddr map[string]*backend
}

type Balancer struct {
	pool    atomic.Pointer[pool]
	opts    atomic.Pointer[Options]
	next    atomic.Uint64
	onState atomic.Pointer[func(t Transition)]
}

func (b *Balancer) fire(fn *func(t Transition), t Transition) {
	if fn != nil && *fn != nil {
		(*fn)(t)
	}
}

func (b *Balancer) onStateFn() *func(t Transition) { return b.onState.Load() }

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
// stay; new backends start healthy with zeroed counters. Concurrent calls
// are last-writer-wins per call; production callers hold the supervisor
// lock, making reloads single-writer.
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

// SetBalance switches the algorithm live. Concurrent calls are
// last-writer-wins per call; production callers hold the supervisor lock,
// making reloads single-writer.
func (b *Balancer) SetBalance(mode string) {
	o := *b.opts.Load()
	o.Balance = mode
	b.opts.Store(&o)
}

// SetHealth adjusts the state machine thresholds live (reload path).
// rise/fall <= 0 leave the current values unchanged. Concurrent calls are
// last-writer-wins per call; production callers hold the supervisor lock,
// making reloads single-writer.
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

// SetOnStateChange registers a callback fired on every state transition
// (Healthy<->Suspect<->Down). The callback runs outside any lock, exactly
// once per transition.
func (b *Balancer) SetOnStateChange(fn func(t Transition)) {
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
	// One cursor advance per Pick: the fail-open pass re-evaluates the SAME
	// rotation position. Advancing twice (once per pass) pinned even-sized
	// all-down pools to a single backend — every fail-open start landed on
	// the same index.
	start := (b.next.Add(1) - 1) % uint64(len(p.list))
	var fired []Transition
	addr, ok := b.pick(clientIP, p, o, start, true, &fired)
	if !ok {
		// fail-open: same algorithm, same rotation start, ignoring health
		addr, _ = b.pick(clientIP, p, o, start, false, &fired)
	}
	b.fireTransitions(b.onStateFn(), fired)
	return addr
}

func (b *Balancer) pick(clientIP string, p *pool, o *Options, start uint64, needHealthy bool, fired *[]Transition) (string, bool) {
	n := uint64(len(p.list))
	switch o.Balance {
	case "least_sessions":
		if o.Counts != nil {
			best := -1
			var bestCount int64
			for i, be := range p.list {
				if needHealthy && !b.usable(be, o, fired) {
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
				if needHealthy && !b.usable(be, o, fired) {
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
	for i := uint64(0); i < n; i++ {
		be := p.list[(start+i)%n]
		if !needHealthy || b.usable(be, o, fired) {
			return be.addr, true
		}
	}
	return p.list[start].addr, !needHealthy
}

// windowConfirm flips a suspect backend (passive mode only) whose suspect
// stamp is older than Cooldown with no liveness since. The caller collects
// the transition and fires it after its pool read completes. Reports whether
// the flip happened (CAS winner).
func (b *Balancer) windowConfirm(be *backend, o *Options) (down bool, t Transition) {
	if o.ActiveChecks {
		return false, Transition{} // the prober confirms or clears instead
	}
	s := be.suspectSince.Load()
	if s == 0 || time.Since(time.Unix(0, s)) < o.Cooldown {
		return false, Transition{}
	}
	// Metadata lands before the flip: an observer of the flipped-down state
	// must never see a stale or missing confirmation stamp. The CAS winner
	// fires, so the transition fires exactly once.
	confirm := SrcWindow
	be.downConfirm.Store(&confirm)
	be.downSince.Store(time.Now().UnixNano())
	if be.healthy.CompareAndSwap(true, false) {
		be.suspectSince.Store(0)
		last := ""
		if v := be.lastErrSrc.Load(); v != nil {
			last = *v
		}
		return true, Transition{
			Addr: be.addr, From: StateSuspect, To: StateDown,
			Source: SrcWindow, LastSource: last, Errors: be.errCount.Load(),
		}
	}
	return false, Transition{}
}

// usable reports whether the backend may serve traffic, applying passive
// recovery. A window-confirmed backend reports (and becomes) down exactly as
// the next Pick would treat it. Transitions are collected by the caller and
// fired after the pool read (same discipline as recovery). The flips are
// CompareAndSwaps so concurrent observers fire each callback exactly once.
func (b *Balancer) usable(be *backend, o *Options, fired *[]Transition) bool {
	if be.healthy.Load() {
		if down, t := b.windowConfirm(be, o); down {
			*fired = append(*fired, t)
			return false
		}
		return true
	}
	if o.ActiveChecks {
		return false // recovery only via ReportSuccess rise path
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
			*fired = append(*fired, Transition{Addr: be.addr, From: StateDown, To: StateHealthy})
			return true
		}
		return false
	}
	return false
}

// fireTransitions publishes pool-read transitions (passive recoveries and
// window confirms) after that read completes and in pool order.
func (b *Balancer) fireTransitions(fn *func(t Transition), ts []Transition) {
	for _, t := range ts {
		b.fire(fn, t)
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

// ReportError records a failure against addr from source (Src* constants).
// Fall errors enter the suspect state; only a probe-source error while
// suspect confirms the down — data-path bursts alone never kill a backend
// (spec: alive = any liveness evidence; dead = errors + independent
// confirmation with no evidence in between).
func (b *Balancer) ReportError(addr, source string) {
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
	be.lastErrSrc.Store(&source)
	if !be.healthy.Load() {
		return
	}
	if be.suspectSince.Load() == 0 {
		if n >= int64(o.Fall) {
			// Exactly-once entry per streak: the CAS from 0 is the single
			// entry point even if SetHealth moved Fall mid-streak.
			now := time.Now().UnixNano()
			if be.suspectSince.CompareAndSwap(0, now) {
				b.fire(b.onStateFn(), Transition{
					Addr: addr, From: StateHealthy, To: StateSuspect,
					Source: source, LastSource: source, Errors: n,
				})
			}
		}
		return
	}
	// Suspect: only the prober may confirm the down. Metadata lands before
	// the flip; the CAS winner fires, so the transition fires exactly once.
	// suspectSince is cleared AFTER the flip: Suspect is only meaningful
	// while healthy, and clearing first would let a concurrent error
	// re-enter suspect on a backend already being downed.
	if source == SrcProbe {
		confirm := SrcProbe
		be.downConfirm.Store(&confirm)
		be.downSince.Store(time.Now().UnixNano())
		if be.healthy.CompareAndSwap(true, false) {
			be.suspectSince.Store(0)
			b.fire(b.onStateFn(), Transition{
				Addr: addr, From: StateSuspect, To: StateDown,
				Source: SrcProbe, LastSource: source, Errors: n,
			})
		}
	}
}

// ReportSuccess records a liveness signal from source (Src* constants). When
// healthy it clears the error streak and any suspect state; when down with
// ActiveChecks it counts toward Rise recovery.
func (b *Balancer) ReportSuccess(addr, source string) {
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
		// Swap makes the clear exactly-once: only the caller that observed
		// a nonzero suspect stamp fires the Suspect->Healthy transition.
		if prev := be.suspectSince.Swap(0); prev != 0 {
			b.fire(b.onStateFn(), Transition{
				Addr: addr, From: StateSuspect, To: StateHealthy, Source: source,
			})
		}
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
			b.fire(b.onStateFn(), Transition{Addr: addr, From: StateDown, To: StateHealthy})
		}
	}
}

// Snapshot returns the current state of every backend. A backend whose
// passive cooldown has elapsed reports (and becomes) healthy again, and a
// suspect backend whose window elapsed reports (and becomes) down — exactly
// as the next Pick would treat each. Those transitions are fired through the
// state-change callback like any other transition.
func (b *Balancer) Snapshot() []BackendState {
	p := b.pool.Load()
	if p == nil {
		return nil
	}
	o := b.opts.Load()
	var fired []Transition
	out := make([]BackendState, 0, len(p.list))
	for _, be := range p.list {
		b.usable(be, o, &fired) // reflect recovery / window confirm in the view
		suspect := be.suspectSince.Load() != 0 && be.healthy.Load()
		last, confirm := "", ""
		if v := be.lastErrSrc.Load(); v != nil {
			last = *v
		}
		if v := be.downConfirm.Load(); v != nil {
			confirm = *v
		}
		var suspectSince int64
		if suspect {
			suspectSince = be.suspectSince.Load()
		}
		out = append(out, BackendState{
			Addr: be.addr, Healthy: be.healthy.Load(), Suspect: suspect,
			SuspectSince: suspectSince, ErrCount: be.errCount.Load(),
			LastErrorSource: last, DownConfirmBy: confirm,
		})
	}
	b.fireTransitions(b.onStateFn(), fired)
	return out
}
