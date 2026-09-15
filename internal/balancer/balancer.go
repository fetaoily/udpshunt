// Package balancer picks a backend for new sessions and tracks passive health.
package balancer

import (
	"sync"
	"time"
)

const (
	errorThreshold  = 3                // consecutive errors before a backend goes down
	defaultCooldown = 10 * time.Second // down time before passive retry
)

// BackendState is a point-in-time view of one backend.
type BackendState struct {
	Addr    string
	Healthy bool
}

type backend struct {
	addr      string
	healthy   bool
	errCount  int
	downSince time.Time
}

type Balancer struct {
	mu       sync.Mutex
	backends []*backend
	next     uint64
	cooldown time.Duration
	onDown   func(addr string)
}

// New creates a round-robin balancer over addrs. cooldown <= 0 selects the
// default passive-recovery cooldown.
func New(addrs []string, cooldown time.Duration) *Balancer {
	if cooldown <= 0 {
		cooldown = defaultCooldown
	}
	b := &Balancer{cooldown: cooldown}
	for _, a := range addrs {
		b.backends = append(b.backends, &backend{addr: a, healthy: true})
	}
	return b
}

// SetOnDown registers a callback fired (asynchronously) once each time a
// backend transitions to the down state.
func (b *Balancer) SetOnDown(fn func(addr string)) {
	b.mu.Lock()
	b.onDown = fn
	b.mu.Unlock()
}

// Pick returns the next healthy backend address. When every backend is down
// it fails open and returns the next address anyway (spec assumption B).
func (b *Balancer) Pick() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := uint64(len(b.backends))
	start := b.next % n
	b.next++
	for i := uint64(0); i < n; i++ {
		be := b.backends[(start+i)%n]
		if b.recoverIfDue(be) {
			return be.addr
		}
	}
	return b.backends[start].addr // fail-open
}

// recoverIfDue reports whether the backend is usable, clearing the down
// state once the cooldown has elapsed. Caller must hold b.mu.
func (b *Balancer) recoverIfDue(be *backend) bool {
	if be.healthy {
		return true
	}
	if time.Since(be.downSince) >= b.cooldown {
		be.healthy = true
		be.errCount = 0
		return true
	}
	return false
}

// ReportError records an I/O error against a backend. errorThreshold
// consecutive errors mark it down.
func (b *Balancer) ReportError(addr string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	be := b.find(addr)
	if be == nil {
		return
	}
	be.errCount++
	if be.healthy && be.errCount >= errorThreshold {
		be.healthy = false
		be.downSince = time.Now()
		if b.onDown != nil {
			go b.onDown(addr)
		}
	}
}

// ReportSuccess resets the consecutive-error counter of a backend.
func (b *Balancer) ReportSuccess(addr string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if be := b.find(addr); be != nil {
		be.errCount = 0
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

// Snapshot returns the current state of every backend.
func (b *Balancer) Snapshot() []BackendState {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]BackendState, 0, len(b.backends))
	for _, be := range b.backends {
		out = append(out, BackendState{Addr: be.addr, Healthy: be.healthy})
	}
	return out
}
