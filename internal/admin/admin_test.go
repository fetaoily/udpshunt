package admin

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/fetaoily/udpshunt/internal/clientstats"
)

func testServer(t *testing.T, reloadErr error) *httptest.Server {
	t.Helper()
	reg := prometheus.NewRegistry()
	reg.MustRegister(prometheus.NewCounter(prometheus.CounterOpts{Name: "udpshunt_packets_in_total"}))
	s := New("127.0.0.1:0", Deps{
		Registry: reg,
		Status: func() Status {
			return Status{
				Uptime: "1s",
				Listeners: []ListenerStatus{{
					Name: "L", Bind: "127.0.0.1:1", Balance: "round_robin",
					Backends: []BackendStatus{{Addr: "b:1", Healthy: true, Sessions: 3}},
					Sessions: 3,
				}},
				Sessions: SessionsStatus{Active: 3, Created: 10, Expired: 7},
				Events:   []Event{{Time: time.Now(), Kind: "reload", Detail: "ok"}},
			}
		},
		Reload: func() error { return reloadErr },
	})
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(ts.Close)
	return ts
}

func TestStatusEndpoint(t *testing.T) {
	ts := testServer(t, nil)
	resp, err := http.Get(ts.URL + "/status")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	for _, want := range []string{`"listeners"`, `"b:1"`, `"healthy":true`, `"active":3`, `"reload"`} {
		if !strings.Contains(string(body), want) {
			t.Fatalf("status body missing %q: %s", want, body)
		}
	}
}

func TestMetricsEndpoint(t *testing.T) {
	ts := testServer(t, nil)
	resp, err := http.Get(ts.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("metrics = %d", resp.StatusCode)
	}
}

func TestReloadEndpoint(t *testing.T) {
	ts := testServer(t, nil)
	resp, err := http.Post(ts.URL+"/reload", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("reload = %d, want 204", resp.StatusCode)
	}
}

func TestReloadEndpointError(t *testing.T) {
	ts := testServer(t, fmt.Errorf("bad config"))
	resp, err := http.Post(ts.URL+"/reload", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("reload = %d, want 500", resp.StatusCode)
	}
}

func TestUIRoute(t *testing.T) {
	s := New("127.0.0.1:0", Deps{
		Registry: prometheus.NewRegistry(),
		Status:   func() Status { return Status{} },
		Reload:   func() error { return nil },
		Logger:   slog.Default(),
		UI: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			_, _ = w.Write([]byte("<html><body>ui:" + r.URL.Path + "</body></html>"))
		}),
	})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/ui/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), "ui:/") {
		t.Fatalf("GET /ui/ = %d %q", resp.StatusCode, body)
	}

	// nil UI must 404
	s2 := New("127.0.0.1:0", Deps{Registry: prometheus.NewRegistry(), Logger: slog.Default()})
	ts2 := httptest.NewServer(s2.Handler())
	defer ts2.Close()
	resp3, err := http.Get(ts2.URL + "/ui/")
	if err != nil {
		t.Fatal(err)
	}
	resp3.Body.Close()
	if resp3.StatusCode != http.StatusNotFound {
		t.Fatalf("nil UI: GET /ui/ = %d, want 404", resp3.StatusCode)
	}
}

func TestClientsEndpoint(t *testing.T) {
	var gotSort, gotOrder string
	var gotLimit int
	s := New("127.0.0.1:0", Deps{
		Registry: prometheus.NewRegistry(),
		Status:   func() Status { return Status{} },
		Reload:   func() error { return nil },
		Clients: func(sortCol, order string, limit int) ClientsStatus {
			gotSort, gotOrder, gotLimit = sortCol, order, limit
			return ClientsStatus{
				Enabled: true,
				Tracked: 2,
				Evicted: 1,
				Rows: []clientstats.Row{{
					IP: "10.0.0.1", Requests: 5, Responses: 4,
					BytesIn: 100, BytesOut: 200, PPSIn: 1.5, LastSeen: 12345,
				}},
			}
		},
		Logger: slog.Default(),
	})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/clients?sort=bytes_out&order=asc&limit=50")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("clients = %d", resp.StatusCode)
	}
	for _, want := range []string{`"enabled":true`, `"tracked":2`, `"evicted":1`, `"ip":"10.0.0.1"`, `"requests":5`, `"pps_in":1.5`} {
		if !strings.Contains(string(body), want) {
			t.Fatalf("clients body missing %q: %s", want, body)
		}
	}
	if gotSort != "bytes_out" || gotOrder != "asc" || gotLimit != 50 {
		t.Fatalf("params not passed: sort=%q order=%q limit=%d", gotSort, gotOrder, gotLimit)
	}

	// Defaults: requests, desc, 200.
	resp2, err := http.Get(ts.URL + "/clients")
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if gotSort != "requests" || gotOrder != "desc" || gotLimit != 200 {
		t.Fatalf("defaults wrong: sort=%q order=%q limit=%d", gotSort, gotOrder, gotLimit)
	}

	// Limit is clamped.
	resp3, err := http.Get(ts.URL + "/clients?limit=9999999")
	if err != nil {
		t.Fatal(err)
	}
	resp3.Body.Close()
	if gotLimit != 10000 {
		t.Fatalf("limit not clamped: %d", gotLimit)
	}

	// nil Clients must serve the disabled shape, not panic.
	s2 := New("127.0.0.1:0", Deps{Registry: prometheus.NewRegistry(), Logger: slog.Default()})
	ts2 := httptest.NewServer(s2.Handler())
	defer ts2.Close()
	resp4, err := http.Get(ts2.URL + "/clients")
	if err != nil {
		t.Fatal(err)
	}
	defer resp4.Body.Close()
	body4, _ := io.ReadAll(resp4.Body)
	if !strings.Contains(string(body4), `"enabled":false`) {
		t.Fatalf("nil Clients must be disabled shape: %s", body4)
	}
}

func TestRecorderBoundsAndOrder(t *testing.T) {
	r := NewRecorder(3)
	for i := 0; i < 5; i++ {
		r.Add("k", fmt.Sprintf("%d", i))
	}
	all := r.List()
	if len(all) != 3 {
		t.Fatalf("len = %d, want 3", len(all))
	}
	if all[0].Detail != "2" || all[2].Detail != "4" {
		t.Fatalf("kept newest 3 in order, got %v", all)
	}
}

func TestRunGracefulStop(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()

	s := New(fmt.Sprintf("127.0.0.1:%d", port), Deps{
		Registry: prometheus.NewRegistry(),
		Status:   func() Status { return Status{} },
		Reload:   func() error { return nil },
		Logger:   slog.Default(),
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()

	deadline := time.Now().Add(2 * time.Second)
	up := false
	for time.Now().Before(deadline) && !up {
		resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/status", port))
		if err == nil {
			resp.Body.Close()
			up = true
		} else {
			time.Sleep(10 * time.Millisecond)
		}
	}
	if !up {
		t.Fatal("server did not come up within 2s")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}
