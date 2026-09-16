// Package session tracks per-client UDP proxy sessions in a sharded table.
package session

import (
	"context"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const shardCount = 256

// Session is one client flow: a listener's client address routed over one
// upstream socket to a single backend.
type Session struct {
	Listener string
	Client   *net.UDPAddr
	Backend  string
	Timeout  time.Duration

	upstream  *net.UDPConn
	lastSeen  atomic.Int64 // unix nanoseconds
	closeOnce sync.Once
}

// NewSession creates a session. The upstream socket must already be
// connected to backend.
func NewSession(listener string, client *net.UDPAddr, backend string, upstream *net.UDPConn, timeout time.Duration) *Session {
	s := &Session{
		Listener: listener,
		Client:   client,
		Backend:  backend,
		Timeout:  timeout,
		upstream: upstream,
	}
	s.lastSeen.Store(time.Now().UnixNano())
	return s
}

func (s *Session) Upstream() *net.UDPConn { return s.upstream }

// Touch marks the session as active.
func (s *Session) Touch() { s.lastSeen.Store(time.Now().UnixNano()) }

// Close closes the upstream socket exactly once (spec §10: single close point).
func (s *Session) Close() {
	s.closeOnce.Do(func() { _ = s.upstream.Close() })
}

// Manager is a sharded session table keyed by listener + client address.
type Manager struct {
	shards   [shardCount]shard
	count    atomic.Int64
	rejected atomic.Int64
	created  atomic.Int64
	expired  atomic.Int64
	max      atomic.Int64
	bcounts  sync.Map // "listener|backend" -> *atomic.Int64
}

type shard struct {
	mu sync.Mutex
	m  map[string]*Session
}

// NewManager creates a session table. max <= 0 means unlimited sessions.
func NewManager(max int64) *Manager {
	mgr := &Manager{}
	mgr.max.Store(max)
	for i := range mgr.shards {
		mgr.shards[i].m = make(map[string]*Session)
	}
	return mgr
}

func key(listener string, client *net.UDPAddr) string {
	return listener + "|" + client.String()
}

// shardFor maps a key to a shard with FNV-1a.
func (m *Manager) shardFor(k string) *shard {
	var h uint32 = 2166136261
	for i := 0; i < len(k); i++ {
		h ^= uint32(k[i])
		h *= 16777619
	}
	return &m.shards[h%shardCount]
}

// Get returns the session for the key, or nil.
func (m *Manager) Get(listener string, client *net.UDPAddr) *Session {
	k := key(listener, client)
	sh := m.shardFor(k)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	return sh.m[k]
}

// Put inserts a session. It returns false when the session cap is reached or
// the key already exists; the caller must then Close the session it tried to
// insert.
func (m *Manager) Put(listener string, client *net.UDPAddr, s *Session) bool {
	if max := m.max.Load(); max > 0 {
		// Reserve a slot atomically: the cap can never be exceeded even
		// under concurrent Puts (reserve, roll back on any failure).
		if m.count.Add(1) > max {
			m.count.Add(-1)
			m.rejected.Add(1)
			return false
		}
	} else {
		m.count.Add(1)
	}
	k := key(listener, client)
	sh := m.shardFor(k)
	sh.mu.Lock()
	if _, dup := sh.m[k]; dup {
		sh.mu.Unlock()
		m.count.Add(-1)
		return false
	}
	sh.m[k] = s
	sh.mu.Unlock()
	m.created.Add(1)
	m.bcount(listener, s.Backend).Add(1)
	return true
}

// Remove deletes and closes the session for the key if present.
func (m *Manager) Remove(listener string, client *net.UDPAddr) {
	k := key(listener, client)
	sh := m.shardFor(k)
	sh.mu.Lock()
	s, ok := sh.m[k]
	if ok {
		delete(sh.m, k)
	}
	sh.mu.Unlock()
	if ok {
		s.Close()
		m.count.Add(-1)
		m.expired.Add(1)
		m.bcount(listener, s.Backend).Add(-1)
	}
}

// CloseBackend closes every session of one listener routed to the given
// backend (spec §3: backend leaving the pool terminates its sessions) and
// returns how many were closed.
func (m *Manager) CloseBackend(listener, backend string) int {
	return m.closeWhere(func(s *Session) bool {
		return s.Listener == listener && s.Backend == backend
	})
}

// CloseAll closes and drops every session.
func (m *Manager) CloseAll() int {
	return m.closeWhere(func(*Session) bool { return true })
}

func (m *Manager) closeWhere(match func(*Session) bool) int {
	closed := 0
	for i := range m.shards {
		sh := &m.shards[i]
		sh.mu.Lock()
		for k, s := range sh.m {
			if match(s) {
				delete(sh.m, k)
				s.Close()
				m.expired.Add(1)
				m.bcount(s.Listener, s.Backend).Add(-1)
				closed++
			}
		}
		sh.mu.Unlock()
	}
	m.count.Add(int64(-closed))
	return closed
}

// Start runs the idle-session reaper. Each tick sweeps shardCount/8 shards,
// so a full sweep takes eight ticks (spec §3). Cancel ctx to stop it.
func (m *Manager) Start(ctx context.Context, sweepEvery time.Duration) {
	go func() {
		t := time.NewTicker(sweepEvery)
		defer t.Stop()
		offset := 0
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				m.sweep(offset, shardCount/8)
				offset = (offset + shardCount/8) % shardCount
			}
		}
	}()
}

// sweep scans n shards starting at shard offset and expires idle sessions.
func (m *Manager) sweep(offset, n int) {
	now := time.Now()
	for i := offset; i < offset+n; i++ {
		sh := &m.shards[i%shardCount]
		sh.mu.Lock()
		for k, s := range sh.m {
			if now.Sub(time.Unix(0, s.lastSeen.Load())) > s.Timeout {
				delete(sh.m, k)
				s.Close()
				m.count.Add(-1)
				m.expired.Add(1)
				m.bcount(s.Listener, s.Backend).Add(-1)
			}
		}
		sh.mu.Unlock()
	}
}

func (m *Manager) Count() int      { return int(m.count.Load()) }
func (m *Manager) Rejected() int64 { return m.rejected.Load() }

// SetMax applies a new session cap live. max <= 0 means unlimited.
func (m *Manager) SetMax(max int64) { m.max.Store(max) }
func (m *Manager) Created() int64   { return m.created.Load() }
func (m *Manager) Expired() int64   { return m.expired.Load() }

func (m *Manager) bcount(listener, backend string) *atomic.Int64 {
	k := listener + "|" + backend
	if v, ok := m.bcounts.Load(k); ok {
		return v.(*atomic.Int64)
	}
	v, _ := m.bcounts.LoadOrStore(k, &atomic.Int64{})
	return v.(*atomic.Int64)
}

// BackendCount returns the live session count for one listener+backend.
func (m *Manager) BackendCount(listener, backend string) int64 {
	return m.bcount(listener, backend).Load()
}

// BackendCounts returns live session counts per backend for one listener.
func (m *Manager) BackendCounts(listener string) map[string]int64 {
	out := make(map[string]int64)
	m.bcounts.Range(func(k, v any) bool {
		key := k.(string)
		if strings.HasPrefix(key, listener+"|") {
			out[strings.TrimPrefix(key, listener+"|")] = v.(*atomic.Int64).Load()
		}
		return true
	})
	return out
}

// CloseListener closes every session of one listener (reload removal path).
func (m *Manager) CloseListener(listener string) int {
	return m.closeWhere(func(s *Session) bool { return s.Listener == listener })
}
