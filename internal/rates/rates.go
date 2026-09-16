// Package rates samples per-listener traffic counters into ring buffers
// backing the /status history (spec §8: 5 minutes at 1-second samples).
package rates

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// Rates is one cumulative reading of a listener's traffic counters.
type Rates struct {
	In       uint64
	Out      uint64
	BytesIn  uint64
	BytesOut uint64
}

// Sample is a timestamped cumulative reading.
type Sample struct {
	T int64 // unix milliseconds
	Rates
}

// Ring is a fixed-capacity FIFO of samples.
type Ring struct {
	cap      int
	all      []Sample
	lastSeen int64 // unix millis of the most recent snapshot containing this listener
}

func (r *Ring) Push(s Sample) {
	r.all = append(r.all, s)
	if len(r.all) > r.cap {
		r.all = r.all[len(r.all)-r.cap:]
	}
}

func (r *Ring) last() []Sample {
	out := make([]Sample, len(r.all))
	copy(out, r.all)
	return out
}

// Collector polls a source function and keeps one ring per listener.
type Collector struct {
	cap    int
	source func() (map[string]Rates, error)

	mu    sync.Mutex
	rings map[string]*Ring
}

// NewCollector creates a collector whose rings hold cap samples each.
func NewCollector(cap int, source func() (map[string]Rates, error)) *Collector {
	return &Collector{cap: cap, source: source, rings: map[string]*Ring{}}
}

func (c *Collector) ringFor(name string) *Ring {
	r, ok := c.rings[name]
	if !ok {
		r = &Ring{cap: c.cap}
		c.rings[name] = r
	}
	return r
}

// Tick takes one sample round at now. A failed source round is skipped
// (rings keep their previous contents).
func (c *Collector) Tick(now time.Time) {
	snap, err := c.source()
	if err != nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	s := Sample{T: now.UnixMilli()}
	for name, r := range snap {
		s.Rates = r
		ring := c.ringFor(name)
		ring.lastSeen = now.UnixMilli()
		ring.Push(s)
	}
}

// Prune drops rings whose listener has not appeared in a source snapshot
// for longer than olderThan (spec §9: frontends only render configured
// listeners; stopped listeners must not accumulate forever). Returns the
// number of rings removed.
func (c *Collector) Prune(now time.Time, olderThan time.Duration) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	cutoff := now.Add(-olderThan).UnixMilli()
	removed := 0
	for name, r := range c.rings {
		if r.lastSeen < cutoff {
			delete(c.rings, name)
			removed++
		}
	}
	return removed
}

// History returns a copy of one listener's ring (oldest first).
func (c *Collector) History(name string) []Sample {
	c.mu.Lock()
	defer c.mu.Unlock()
	if r, ok := c.rings[name]; ok {
		return r.last()
	}
	return nil
}

// All returns a copy of every listener's ring.
func (c *Collector) All() map[string][]Sample {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string][]Sample, len(c.rings))
	for name, r := range c.rings {
		out[name] = r.last()
	}
	return out
}

// FromRegistry builds a source that reads the udpshunt traffic counters
// from a Prometheus registry, summing by listener label.
func FromRegistry(reg *prometheus.Registry) func() (map[string]Rates, error) {
	return func() (map[string]Rates, error) {
		fams, err := reg.Gather()
		if err != nil {
			return nil, err
		}
		out := map[string]Rates{}
		for _, f := range fams {
			var dst func(r *Rates, v float64)
			switch f.GetName() {
			case "udpshunt_packets_in_total":
				dst = func(r *Rates, v float64) { r.In = uint64(v) }
			case "udpshunt_packets_out_total":
				dst = func(r *Rates, v float64) { r.Out = uint64(v) }
			case "udpshunt_bytes_in_total":
				dst = func(r *Rates, v float64) { r.BytesIn = uint64(v) }
			case "udpshunt_bytes_out_total":
				dst = func(r *Rates, v float64) { r.BytesOut = uint64(v) }
			default:
				continue
			}
			for _, m := range f.GetMetric() {
				name := listenerLabel(m)
				if name == "" {
					continue
				}
				r := out[name]
				dst(&r, m.GetCounter().GetValue())
				out[name] = r
			}
		}
		return out, nil
	}
}

func listenerLabel(m *dto.Metric) string {
	for _, l := range m.GetLabel() {
		if l.GetName() == "listener" {
			return l.GetValue()
		}
	}
	return ""
}
