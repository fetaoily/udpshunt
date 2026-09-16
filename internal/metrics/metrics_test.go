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

func TestNilListenerMetricsSafe(t *testing.T) {
	var lm *ListenerMetrics
	lm.PacketsIn(1) // must not panic
	lm.BackendError("b:1")
	lm.SessionCreated()
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
