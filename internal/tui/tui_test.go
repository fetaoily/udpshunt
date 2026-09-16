package tui

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/fetaoily/udpshunt/internal/admin"
)

func TestSparklineShapes(t *testing.T) {
	if got := sparkline(nil, 10); got != "" {
		t.Fatalf("empty input = %q", got)
	}
	s := sparkline([]float64{0, 1, 2, 3, 4, 5, 6, 7, 8}, 9)
	if len([]rune(s)) != 9 {
		t.Fatalf("width = %q", s)
	}
	if s == sparkline([]float64{8, 7, 6, 5, 4, 3, 2, 1, 0}, 9) {
		t.Fatal("rising and falling series must render differently")
	}
	constant := sparkline([]float64{5, 5, 5, 5}, 4)
	if constant == "" {
		t.Fatal("constant series must still render")
	}
}

func TestHealthText(t *testing.T) {
	up, down := healthText(true), healthText(false)
	if up == down {
		t.Fatal("up and down must render differently")
	}
	// Plain-text content (color codes aside) must identify the state.
	if !contains(up, "UP") || !contains(down, "DOWN") {
		t.Fatalf("up=%q down=%q", up, down)
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (func() bool {
		for i := 0; i+len(sub) <= len(s); i++ {
			if s[i:i+len(sub)] == sub {
				return true
			}
		}
		return false
	})()
}

func TestTotalsSumsListeners(t *testing.T) {
	prev := map[string][]admin.Sample{
		"a": {{T: 1000, In: 10, Out: 5, BytesIn: 100, BytesOut: 50}},
		"b": {{T: 1000, In: 1, Out: 1, BytesIn: 10, BytesOut: 10}},
	}
	cur := map[string][]admin.Sample{
		"a": {{T: 2000, In: 20, Out: 15, BytesIn: 200, BytesOut: 150}},
		"b": {{T: 2000, In: 3, Out: 2, BytesIn: 30, BytesOut: 20}},
	}
	tv := totals(prev, cur, time.Second)
	if tv.InPPS != 12 || tv.OutPPS != 11 {
		t.Fatalf("pps = %+v", tv)
	}
	if tv.InBPS != 120 || tv.OutBPS != 110 {
		t.Fatalf("bps = %+v", tv)
	}
}

func TestFetchStatus(t *testing.T) {
	want := admin.Status{Uptime: "1s"}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(want)
	}))
	defer ts.Close()
	got, err := fetchStatus(ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	if got.Uptime != "1s" {
		t.Fatalf("uptime = %q", got.Uptime)
	}
}

func TestFetchStatusError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()
	if _, err := fetchStatus(ts.URL); err == nil {
		t.Fatal("expected error on 500")
	}
}
