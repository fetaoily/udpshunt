package rates

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

func TestRingCapsAndOrders(t *testing.T) {
	c := NewCollector(3, func() (map[string]Rates, error) { return nil, nil })
	base := time.Unix(1700000000, 0)
	for i := 0; i < 5; i++ {
		c.rings["L"] = c.ringFor("L")
		c.rings["L"].Push(Sample{T: base.Add(time.Duration(i) * time.Second).UnixMilli(), Rates: Rates{In: uint64(i)}})
	}
	h := c.History("L")
	if len(h) != 3 {
		t.Fatalf("len = %d, want 3 (cap)", len(h))
	}
	if h[0].In != 2 || h[2].In != 4 {
		t.Fatalf("oldest-first window wrong: %+v", h)
	}
}

func TestTickSamplesKnownAndNewListeners(t *testing.T) {
	seq := []map[string]Rates{
		{"L1": {In: 10, Out: 5, BytesIn: 100, BytesOut: 50}},
		{"L1": {In: 20, Out: 9, BytesIn: 200, BytesOut: 90}, "L2": {In: 1}},
	}
	i := 0
	c := NewCollector(10, func() (map[string]Rates, error) {
		s := seq[i]
		i++
		return s, nil
	})
	base := time.Unix(1700000000, 0)
	c.Tick(base)
	c.Tick(base.Add(time.Second))
	if got := c.History("L1"); len(got) != 2 || got[1].In != 20 {
		t.Fatalf("L1 history = %+v", got)
	}
	if got := c.History("L2"); len(got) != 1 || got[0].In != 1 {
		t.Fatalf("L2 history = %+v", got)
	}
	all := c.All()
	if len(all) != 2 {
		t.Fatalf("All() = %d listeners", len(all))
	}
}

func TestTickSurvivesSourceError(t *testing.T) {
	c := NewCollector(10, func() (map[string]Rates, error) { return nil, errBoom{} })
	c.Tick(time.Now()) // must not panic and must not create rings
	if len(c.All()) != 0 {
		t.Fatal("no rings should exist after a failed source")
	}
}

type errBoom struct{}

func (errBoom) Error() string { return "boom" }

func TestFromRegistrySumsListenerLabels(t *testing.T) {
	reg := prometheus.NewRegistry()
	factory := promauto.With(reg)
	pin := factory.NewCounterVec(prometheus.CounterOpts{Name: "udpshunt_packets_in_total"}, []string{"listener"})
	pout := factory.NewCounterVec(prometheus.CounterOpts{Name: "udpshunt_packets_out_total"}, []string{"listener"})
	bin := factory.NewCounterVec(prometheus.CounterOpts{Name: "udpshunt_bytes_in_total"}, []string{"listener"})
	bout := factory.NewCounterVec(prometheus.CounterOpts{Name: "udpshunt_bytes_out_total"}, []string{"listener"})
	pin.WithLabelValues("a").Add(7)
	pin.WithLabelValues("a").Add(3)
	pout.WithLabelValues("a").Add(2)
	bin.WithLabelValues("a").Add(700)
	bout.WithLabelValues("a").Add(200)

	src := FromRegistry(reg)
	got, err := src()
	if err != nil {
		t.Fatal(err)
	}
	r := got["a"]
	if r.In != 10 || r.Out != 2 || r.BytesIn != 700 || r.BytesOut != 200 {
		t.Fatalf("rates = %+v", r)
	}
	if _, ok := got["missing"]; ok {
		t.Fatal("listeners without samples must not appear")
	}
}
