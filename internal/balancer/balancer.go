// Package balancer picks a backend for new sessions and tracks health state.
package balancer

import (
	"sync"
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
	// only). It is called under the balancer lock and must not re-enter
	// the Balancer.
	Counts func(addr string) int64
}

// BackendState is a point-in-time view of one backend.
type BackendState struct {
	Addr    string
	Healthy bool
}

type backend struct {
	addr         string
	healthy      bool
	errCount     int
	successCount int
	downSince    time.Time
}

type Balancer struct {
	mu       sync.Mutex
	backends []*backend
	next     uint64
	opts     Options
	onState  func(addr string, healthy bool)
}

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
	b := &Balancer{opts: opts}
	b.Update(addrs)
	return b
}

// Update replaces the backend pool, preserving the state of backends that
// stay; new backends start healthy with zeroed counters.
func (b *Balancer) Update(addrs []string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	old := make(map[string]*backend, len(b.backends))
	for _, be := range b.backends {
		old[be.addr] = be
	}
	next := make([]*backend, 0, len(addrs))
	for _, a := range addrs {
		if be, ok := old[a]; ok {
			next = append(next, be)
		} else {
			next = append(next, &backend{addr: a, healthy: true})
		}
	}
	b.backends = next
}

// SetBalance switches the algorithm live.
func (b *Balancer) SetBalance(mode string) {
	b.mu.Lock()
	b.opts.Balance = mode
	b.mu.Unlock()
}

// SetHealth adjusts the state machine thresholds live (reload path).
// rise/fall <= 0 leave the current values unchanged.
func (b *Balancer) SetHealth(activeChecks bool, rise, fall int) {
	b.mu.Lock()
	b.opts.ActiveChecks = activeChecks
	if rise > 0 {
		b.opts.Rise = rise
	}
	if fall > 0 {
		b.opts.Fall = fall
	}
	b.mu.Unlock()
}

// SetOnStateChange registers a callback fired on every UP<->DOWN transition.
// The callback runs outside the balancer mutex, in transition order.
func (b *Balancer) SetOnStateChange(fn func(addr string, healthy bool)) {
	b.mu.Lock()
	b.onState = fn
	b.mu.Unlock()
}

// Pick returns a healthy backend for a new session from clientIP (used only
// by source_hash). It fails open when every backend is down (spec
// assumption B) and returns "" for an empty pool.
func (b *Balancer) Pick(clientIP string) string {
	b.mu.Lock()
	var recovered []string
	if len(b.backends) == 0 {
		b.mu.Unlock()
		return ""
	}
	addr, ok := b.pickHealthyLocked(clientIP, &recovered)
	if !ok {
		// fail-open: same algorithm ignoring health
		addr, _ = b.pickLocked(clientIP, false, &recovered)
	}
	fn := b.onState
	b.mu.Unlock()
	b.fireRecovered(fn, recovered)
	return addr
}

func (b *Balancer) pickHealthyLocked(clientIP string, recovered *[]string) (string, bool) {
	return b.pickLocked(clientIP, true, recovered)
}

func (b *Balancer) pickLocked(clientIP string, needHealthy bool, recovered *[]string) (string, bool) {
	n := uint64(len(b.backends))
	switch b.opts.Balance {
	case "least_sessions":
		if b.opts.Counts != nil {
			best := -1
			var bestCount int64
			for i, be := range b.backends {
				if needHealthy && !b.usableTrackLocked(be, recovered) {
					continue
				}
				c := b.opts.Counts(be.addr)
				if best == -1 || c < bestCount {
					best, bestCount = i, c
				}
			}
			if best >= 0 {
				return b.backends[best].addr, true
			}
			return "", false
		}
		fallthrough
	case "source_hash":
		if clientIP != "" {
			best := -1
			var bestHash uint64
			for i, be := range b.backends {
				if needHealthy && !b.usableTrackLocked(be, recovered) {
					continue
				}
				h := rendezvous(clientIP, be.addr)
				if best == -1 || h > bestHash {
					best, bestHash = i, h
				}
			}
			if best >= 0 {
				return b.backends[best].addr, true
			}
			return "", false
		}
	}
	// round_robin (and fallback for empty clientIP)
	start := b.next % n
	b.next++
	for i := uint64(0); i < n; i++ {
		be := b.backends[(start+i)%n]
		if !needHealthy || b.usableTrackLocked(be, recovered) {
			return be.addr, true
		}
	}
	return b.backends[start].addr, !needHealthy
}

// usableLocked reports whether the backend may serve traffic, applying
// passive cooldown recovery. recovered reports whether this call flipped the
// backend from down to healthy (cooldown elapsed); the caller must fire the
// state-change callback for it after unlocking b.mu. Caller must hold b.mu.
func (b *Balancer) usableLocked(be *backend) (usable, recovered bool) {
	if be.healthy {
		return true, false
	}
	if b.opts.ActiveChecks {
		return false, false // recovery only via ReportSuccess rise path
	}
	if time.Since(be.downSince) >= b.opts.Cooldown {
		be.healthy = true
		be.errCount = 0
		return true, true
	}
	return false, false
}

// usableTrackLocked is usableLocked plus collection of recovered addresses.
// Caller must hold b.mu.
func (b *Balancer) usableTrackLocked(be *backend, recovered *[]string) bool {
	usable, rec := b.usableLocked(be)
	if rec {
		*recovered = append(*recovered, be.addr)
	}
	return usable
}

// fireRecovered publishes cooldown recoveries collected under the lock,
// after unlocking it and in pool order — same discipline as ReportError and
// ReportSuccess.
func (b *Balancer) fireRecovered(fn func(addr string, healthy bool), recovered []string) {
	if fn == nil {
		return
	}
	for _, addr := range recovered {
		fn(addr, true)
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
	b.mu.Lock()
	be := b.find(addr)
	if be == nil {
		b.mu.Unlock()
		return
	}
	be.errCount++
	be.successCount = 0
	wasHealthy := be.healthy
	if be.healthy && be.errCount >= b.opts.Fall {
		be.healthy = false
		be.downSince = time.Now()
	}
	down := wasHealthy && !be.healthy
	fn := b.onState
	b.mu.Unlock()
	if down && fn != nil {
		// Fired after unlock so ordering between the down and a following
		// up transition is deterministic; never called under b.mu.
		fn(addr, false)
	}
}

// ReportSuccess records a liveness signal. When healthy it clears the error
// streak; when down with ActiveChecks it counts toward Rise recovery.
func (b *Balancer) ReportSuccess(addr string) {
	b.mu.Lock()
	be := b.find(addr)
	if be == nil {
		b.mu.Unlock()
		return
	}
	if be.healthy {
		be.errCount = 0
		b.mu.Unlock()
		return
	}
	if !b.opts.ActiveChecks {
		b.mu.Unlock()
		return
	}
	be.successCount++
	up := be.successCount >= b.opts.Rise
	if up {
		be.healthy = true
		be.errCount = 0
		be.successCount = 0
	}
	fn := b.onState
	b.mu.Unlock()
	if up && fn != nil {
		fn(addr, true)
	}
}

func (b *Balancer) find(addr string) *backend {
	for _, be := range b.backends {
		if be.addr == addr {
			return be
		}
	}
	return nil
}

// Snapshot returns the current state of every backend. A backend whose
// passive cooldown has elapsed reports (and becomes) healthy again, exactly
// as the next Pick would treat it, and that recovery is fired through the
// state-change callback like any other transition.
func (b *Balancer) Snapshot() []BackendState {
	b.mu.Lock()
	var recovered []string
	out := make([]BackendState, 0, len(b.backends))
	for _, be := range b.backends {
		if _, rec := b.usableLocked(be); rec { // reflect elapsed-cooldown recovery in the view
			recovered = append(recovered, be.addr)
		}
		out = append(out, BackendState{Addr: be.addr, Healthy: be.healthy})
	}
	fn := b.onState
	b.mu.Unlock()
	b.fireRecovered(fn, recovered)
	return out
}
