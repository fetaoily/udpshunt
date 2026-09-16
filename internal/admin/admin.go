// Package admin serves the management HTTP endpoint: /metrics, /status and
// /reload on a single port (spec §6).
package admin

import (
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Event is one entry in the bounded recent-events log.
type Event struct {
	Time   time.Time `json:"time"`
	Kind   string    `json:"kind"`
	Detail string    `json:"detail"`
}

// Recorder is a bounded, concurrency-safe recent-events log.
type Recorder struct {
	mu  sync.Mutex
	cap int
	all []Event
}

func NewRecorder(capacity int) *Recorder {
	if capacity <= 0 {
		capacity = 100
	}
	return &Recorder{cap: capacity}
}

func (r *Recorder) Add(kind, detail string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.all = append(r.all, Event{Time: time.Now().UTC(), Kind: kind, Detail: detail})
	if len(r.all) > r.cap {
		r.all = r.all[len(r.all)-r.cap:]
	}
}

func (r *Recorder) List() []Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Event, len(r.all))
	copy(out, r.all)
	return out
}

// Status is the JSON shape of GET /status (spec §8).
type Status struct {
	Uptime    string           `json:"uptime"`
	Listeners []ListenerStatus `json:"listeners"`
	Sessions  SessionsStatus   `json:"sessions"`
	Events    []Event          `json:"events"`
}

type ListenerStatus struct {
	Name     string          `json:"name"`
	Bind     string          `json:"bind"`
	Balance  string          `json:"balance"`
	Backends []BackendStatus `json:"backends"`
	Sessions int64           `json:"sessions"`
}

type BackendStatus struct {
	Addr     string `json:"addr"`
	Healthy  bool   `json:"healthy"`
	Sessions int64  `json:"sessions"`
}

type SessionsStatus struct {
	Active   int64 `json:"active"`
	Created  int64 `json:"created"`
	Expired  int64 `json:"expired"`
	Rejected int64 `json:"rejected"`
}

// Deps are the injected behaviors of the server.
type Deps struct {
	Registry *prometheus.Registry
	Status   func() Status
	Reload   func() error
	Logger   *slog.Logger
}

// Server is the admin HTTP endpoint.
type Server struct {
	srv  *http.Server
	deps Deps
}

func New(bind string, deps Deps) *Server {
	s := &Server{deps: deps}
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(deps.Registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("GET /status", s.handleStatus)
	mux.HandleFunc("POST /reload", s.handleReload)
	s.srv = &http.Server{Addr: bind, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	return s
}

// Handler returns the mux (tests and embedding).
func (s *Server) Handler() http.Handler { return s.srv.Handler }

func (s *Server) handleStatus(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(s.deps.Status()); err != nil {
		s.deps.Logger.Warn("status encode failed", "err", err)
	}
}

func (s *Server) handleReload(w http.ResponseWriter, _ *http.Request) {
	if err := s.deps.Reload(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// Run serves until ctx is cancelled, then shuts down gracefully.
func (s *Server) Run(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.srv.Addr)
	if err != nil {
		return err
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = s.srv.Shutdown(shutdownCtx)
	}()
	if err := s.srv.Serve(ln); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}
