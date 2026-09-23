// Package metrics owns the private Prometheus registry and per-listener
// counter bundles for udpshunt.
package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type Metrics struct {
	registry *prometheus.Registry

	packetsIn  *prometheus.CounterVec
	bytesIn    *prometheus.CounterVec
	packetsOut *prometheus.CounterVec
	bytesOut   *prometheus.CounterVec

	backendIn    *prometheus.CounterVec
	backendOut   *prometheus.CounterVec
	backendErrs  *prometheus.CounterVec
	backendUp    *prometheus.GaugeVec
	backendFlips *prometheus.CounterVec

	sessionsCreated *prometheus.CounterVec
	blacklisted     *prometheus.CounterVec
	sessionStatsSet bool
	reloads         prometheus.Counter
	reloadFailures  prometheus.Counter
	startedAtSet    bool

	// OnBlacklisted, when set, is invoked with each blacklisted-packet
	// count alongside the udpshunt_blacklisted_packets_total counter, so
	// the app can keep a lock-free /status total. Set once at startup;
	// called only from the packet-drop path.
	OnBlacklisted func(int64)
}

// New creates the registry and registers every metric family.
func New() *Metrics {
	r := prometheus.NewRegistry()
	m := &Metrics{
		registry: r,
		packetsIn: promauto.With(r).NewCounterVec(prometheus.CounterOpts{
			Name: "udpshunt_packets_in_total", Help: "Packets received from clients.",
		}, []string{"listener"}),
		bytesIn: promauto.With(r).NewCounterVec(prometheus.CounterOpts{
			Name: "udpshunt_bytes_in_total", Help: "Bytes received from clients.",
		}, []string{"listener"}),
		packetsOut: promauto.With(r).NewCounterVec(prometheus.CounterOpts{
			Name: "udpshunt_packets_out_total", Help: "Packets relayed to clients.",
		}, []string{"listener"}),
		bytesOut: promauto.With(r).NewCounterVec(prometheus.CounterOpts{
			Name: "udpshunt_bytes_out_total", Help: "Bytes relayed to clients.",
		}, []string{"listener"}),
		backendIn: promauto.With(r).NewCounterVec(prometheus.CounterOpts{
			Name: "udpshunt_backend_packets_in_total", Help: "Packets forwarded to backends.",
		}, []string{"listener", "backend"}),
		backendOut: promauto.With(r).NewCounterVec(prometheus.CounterOpts{
			Name: "udpshunt_backend_packets_out_total", Help: "Packets received from backends.",
		}, []string{"listener", "backend"}),
		backendErrs: promauto.With(r).NewCounterVec(prometheus.CounterOpts{
			Name: "udpshunt_backend_errors_total", Help: "Backend I/O errors.",
		}, []string{"listener", "backend"}),
		backendUp: promauto.With(r).NewGaugeVec(prometheus.GaugeOpts{
			Name: "udpshunt_backend_healthy", Help: "1 if the backend is healthy, 0 if down.",
		}, []string{"listener", "backend"}),
		backendFlips: promauto.With(r).NewCounterVec(prometheus.CounterOpts{
			Name: "udpshunt_backend_state_changes_total", Help: "Backend health state transitions (spec §8).",
		}, []string{"listener", "backend"}),
		sessionsCreated: promauto.With(r).NewCounterVec(prometheus.CounterOpts{
			Name: "udpshunt_sessions_created_total", Help: "Sessions created.",
		}, []string{"listener"}),
		blacklisted: promauto.With(r).NewCounterVec(prometheus.CounterOpts{
			Name: "udpshunt_blacklisted_packets_total", Help: "Packets dropped from blacklisted clients.",
		}, []string{"listener"}),
		reloads: promauto.With(r).NewCounter(prometheus.CounterOpts{
			Name: "udpshunt_reloads_total", Help: "Successful config reloads.",
		}),
		reloadFailures: promauto.With(r).NewCounter(prometheus.CounterOpts{
			Name: "udpshunt_reload_failures_total", Help: "Failed config reloads.",
		}),
	}
	return m
}

func (m *Metrics) Registry() *prometheus.Registry { return m.registry }

// ForListener returns the labeled counter bundle for one listener.
func (m *Metrics) ForListener(name string) *ListenerMetrics {
	return &ListenerMetrics{
		m:    m,
		name: name,
	}
}

// ListenerMetrics is a per-listener view; a nil receiver is a no-op so
// callers without metrics can pass nil.
type ListenerMetrics struct {
	m    *Metrics
	name string
}

func (lm *ListenerMetrics) PacketsIn(n int) {
	if lm != nil {
		lm.m.packetsIn.WithLabelValues(lm.name).Add(float64(n))
	}
}
func (lm *ListenerMetrics) BytesIn(n int) {
	if lm != nil {
		lm.m.bytesIn.WithLabelValues(lm.name).Add(float64(n))
	}
}
func (lm *ListenerMetrics) PacketsOut(n int) {
	if lm != nil {
		lm.m.packetsOut.WithLabelValues(lm.name).Add(float64(n))
	}
}
func (lm *ListenerMetrics) BytesOut(n int) {
	if lm != nil {
		lm.m.bytesOut.WithLabelValues(lm.name).Add(float64(n))
	}
}
func (lm *ListenerMetrics) BackendIn(backend string, n int) {
	if lm != nil {
		lm.m.backendIn.WithLabelValues(lm.name, backend).Add(float64(n))
	}
}
func (lm *ListenerMetrics) BackendOut(backend string, n int) {
	if lm != nil {
		lm.m.backendOut.WithLabelValues(lm.name, backend).Add(float64(n))
	}
}
func (lm *ListenerMetrics) BackendError(backend string) {
	if lm != nil {
		lm.m.backendErrs.WithLabelValues(lm.name, backend).Inc()
	}
}
func (lm *ListenerMetrics) SessionCreated() {
	if lm != nil {
		lm.m.sessionsCreated.WithLabelValues(lm.name).Inc()
	}
}
func (lm *ListenerMetrics) Blacklisted(n int) {
	if lm != nil {
		lm.m.blacklisted.WithLabelValues(lm.name).Add(float64(n))
		if lm.m.OnBlacklisted != nil {
			lm.m.OnBlacklisted(int64(n))
		}
	}
}

// SetBackendHealthy updates the per-backend health gauge and counts the
// state transition. Call it only on real UP<->DOWN transitions; use
// SeedBackendHealthy to initialize without counting.
func (m *Metrics) SetBackendHealthy(listener, backend string, healthy bool) {
	m.backendUp.WithLabelValues(listener, backend).Set(healthyValue(healthy))
	m.backendFlips.WithLabelValues(listener, backend).Inc()
}

// SeedBackendHealthy initializes the per-backend health gauge without
// counting a state transition (listener start, reseeding after a pool
// update).
func (m *Metrics) SeedBackendHealthy(listener, backend string, healthy bool) {
	m.backendUp.WithLabelValues(listener, backend).Set(healthyValue(healthy))
}

func healthyValue(healthy bool) float64 {
	if healthy {
		return 1
	}
	return 0
}

func (m *Metrics) IncReload()        { m.reloads.Inc() }
func (m *Metrics) IncReloadFailure() { m.reloadFailures.Inc() }

// SetSessionStats wires the manager-level counters; call once at startup.
func (m *Metrics) SetSessionStats(created, expired, rejected, active func() int64) {
	if m.sessionStatsSet {
		return
	}
	m.sessionStatsSet = true
	promauto.With(m.registry).NewCounterFunc(prometheus.CounterOpts{
		Name: "udpshunt_sessions_expired_total", Help: "Sessions expired or evicted.",
	}, func() float64 { return float64(expired()) })
	promauto.With(m.registry).NewCounterFunc(prometheus.CounterOpts{
		Name: "udpshunt_sessions_rejected_total", Help: "Sessions rejected at the cap.",
	}, func() float64 { return float64(rejected()) })
	promauto.With(m.registry).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "udpshunt_sessions_active", Help: "Currently active sessions.",
	}, func() float64 { return float64(active()) })
}

// SetStartedAt wires the uptime gauge; call once at startup.
func (m *Metrics) SetStartedAt(t time.Time) {
	if m.startedAtSet {
		return
	}
	m.startedAtSet = true
	promauto.With(m.registry).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "udpshunt_uptime_seconds", Help: "Seconds since process start.",
	}, func() float64 { return time.Since(t).Seconds() })
}
