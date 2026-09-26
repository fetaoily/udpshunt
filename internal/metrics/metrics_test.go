package metrics

import (
	"testing"
	"time"
)

func TestRegistryExposesMetrics(t *testing.T) {
	m := New()
	lm := m.ForListener("t")
	lm.PacketsIn(2)
	lm.BytesIn(100)
	lm.PacketsOut(1)
	lm.BytesOut(50)
	lm.BackendIn("b:1", 2)
	lm.BackendOut("b:1", 1)
	lm.BackendError("b:1")
	lm.SessionCreated()
	lm.Blacklisted(1)
	m.SetBackendHealthy("t", "b:1", true)
	m.IncReload()
	m.IncReloadFailure()
	m.SetStartedAt(time.Now().Add(-time.Second))
	m.SetSessionStats(
		func() int64 { return 10 },
		func() int64 { return 8 },
		func() int64 { return 2 },
		func() int64 { return 1 },
	)
	want := []string{
		"udpshunt_packets_in_total",
		"udpshunt_bytes_in_total",
		"udpshunt_packets_out_total",
		"udpshunt_bytes_out_total",
		"udpshunt_backend_packets_in_total",
		"udpshunt_backend_packets_out_total",
		"udpshunt_backend_errors_total",
		"udpshunt_backend_healthy",
		"udpshunt_backend_state_changes_total",
		"udpshunt_sessions_created_total",
		"udpshunt_blacklisted_packets_total",
		"udpshunt_sessions_expired_total",
		"udpshunt_sessions_rejected_total",
		"udpshunt_sessions_active",
		"udpshunt_reloads_total",
		"udpshunt_reload_failures_total",
		"udpshunt_uptime_seconds",
	}
	fams, err := m.Registry().Gather()
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, f := range fams {
		got[f.GetName()] = true
	}
	for _, w := range want {
		if !got[w] {
			t.Errorf("missing metric %s (have %v)", w, got)
		}
	}
}

func TestSeedBackendHealthyDoesNotCountTransition(t *testing.T) {
	m := New()
	m.backendFlips.WithLabelValues("t", "b:1") // materialize the counter at 0
	m.SeedBackendHealthy("t", "b:1", true)
	if got := metricValue(t, m, "udpshunt_backend_healthy", "t", "b:1"); got != 1 {
		t.Fatalf("seeded gauge = %v, want 1", got)
	}
	if got := metricValue(t, m, "udpshunt_backend_state_changes_total", "t", "b:1"); got != 0 {
		t.Fatalf("seed must not count a state transition, got %v", got)
	}
	m.SetBackendHealthy("t", "b:1", false)
	if got := metricValue(t, m, "udpshunt_backend_healthy", "t", "b:1"); got != 0 {
		t.Fatalf("gauge = %v, want 0", got)
	}
	if got := metricValue(t, m, "udpshunt_backend_state_changes_total", "t", "b:1"); got != 1 {
		t.Fatalf("real transition must count, got %v", got)
	}
}

// metricValue reads one labeled sample from the private registry.
func metricValue(t *testing.T, m *Metrics, name, listener, backend string) float64 {
	t.Helper()
	fams, err := m.Registry().Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range fams {
		if f.GetName() != name {
			continue
		}
		for _, mf := range f.GetMetric() {
			var l, b string
			for _, lbl := range mf.GetLabel() {
				switch lbl.GetName() {
				case "listener":
					l = lbl.GetValue()
				case "backend":
					b = lbl.GetValue()
				}
			}
			if l != listener || b != backend {
				continue
			}
			if mf.GetGauge() != nil {
				return mf.GetGauge().GetValue()
			}
			return mf.GetCounter().GetValue()
		}
	}
	t.Fatalf("metric %s{listener=%q,backend=%q} not found", name, listener, backend)
	return 0
}

func TestNilListenerMetricsSafe(t *testing.T) {
	var lm *ListenerMetrics
	lm.PacketsIn(1) // must not panic
	lm.BackendError("b:1")
	lm.SessionCreated()
	lm.Blacklisted(1)
	lm.Illegal(1)
}

func TestOnBlacklistedHook(t *testing.T) {
	m := New()
	var total int64
	m.OnBlacklisted = func(n int64) { total += n }
	lm := m.ForListener("t")
	lm.Blacklisted(1)
	lm.Blacklisted(4)
	if total != 5 {
		t.Fatalf("OnBlacklisted total = %d, want 5", total)
	}
	// The zero value (hook unset) must not panic.
	New().ForListener("t").Blacklisted(1)
}

func TestIllegalCounterAndHook(t *testing.T) {
	m := New()
	var calls []int64
	m.OnIllegal = func(n int64) { calls = append(calls, n) }
	lm := m.ForListener("t")
	lm.Illegal(3)
	if got := metricValue(t, m, "udpshunt_illegal_packets_total", "t", ""); got != 3 {
		t.Fatalf("illegal counter = %v, want 3", got)
	}
	if len(calls) != 1 || calls[0] != 3 {
		t.Fatalf("OnIllegal calls = %v, want exactly one call with 3", calls)
	}
	// The zero value (hook unset) must not panic.
	New().ForListener("t").Illegal(1)
}

func TestForListenerReusesChildren(t *testing.T) {
	m := New()
	m.ForListener("t").PacketsIn(1)
	m.ForListener("t").PacketsIn(1) // same labels: no duplicate registration panic
	fams, _ := m.Registry().Gather()
	for _, f := range fams {
		if f.GetName() == "udpshunt_packets_in_total" {
			if got := f.GetMetric()[0].GetCounter().GetValue(); got != 2 {
				t.Fatalf("value = %f, want 2", got)
			}
		}
	}
}
