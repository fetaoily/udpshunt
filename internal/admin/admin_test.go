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
