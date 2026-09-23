// Package admin serves the management HTTP endpoint: /metrics, /status and
// /reload on a single port (spec §6).
package admin

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/fetaoily/udpshunt/internal/clientstats"
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
	Uptime     string            `json:"uptime"`
	Listeners  []ListenerStatus  `json:"listeners"`
	Sessions   SessionsStatus    `json:"sessions"`
	RequestLog *RequestLogStatus `json:"request_log,omitempty"`
	Blacklist  *BlacklistStatus  `json:"blacklist,omitempty"`
	Events     []Event           `json:"events"`
}

// BlacklistStatus is the JSON shape of GET /blacklist and of the /status
// blacklist field.
type BlacklistStatus struct {
	Entries        []string `json:"entries"`
	LogBlocked     bool     `json:"log_blocked"`
	BlockedPackets int64    `json:"blocked_packets"`
}

// RequestLogStatus is the /status view of the per-request file log.
type RequestLogStatus struct {
	Enabled bool   `json:"enabled"`
	Dir     string `json:"dir,omitempty"`
	Dropped int64  `json:"dropped"`
}

// ClientsStatus is the JSON shape of GET /clients: the top client IPs by
// the requested sort column.
type ClientsStatus struct {
	Enabled bool              `json:"enabled"`
	Tracked int64             `json:"tracked"`
	Evicted int64             `json:"evicted"`
	Rows    []clientstats.Row `json:"rows"`
}

type ListenerStatus struct {
	Name     string          `json:"name"`
	Bind     string          `json:"bind"`
	Balance  string          `json:"balance"`
	Backends []BackendStatus `json:"backends"`
	Sessions int64           `json:"sessions"`
	History  []Sample        `json:"history,omitempty"`
}

// Sample is one point of a listener's rate history (cumulative counters).
type Sample struct {
	T        int64  `json:"t"` // unix milliseconds
	In       uint64 `json:"in"`
	Out      uint64 `json:"out"`
	BytesIn  uint64 `json:"bytes_in"`
	BytesOut uint64 `json:"bytes_out"`
}

type BackendStatus struct {
	Addr            string `json:"addr"`
	Healthy         bool   `json:"healthy"`
	Suspect         bool   `json:"suspect"`
	Sessions        int64  `json:"sessions"`
	ErrCount        int64  `json:"err_count"`
	LastErrorSource string `json:"last_error,omitempty"`
	DownConfirmBy   string `json:"confirmed_by,omitempty"`
}

type SessionsStatus struct {
	Active   int64 `json:"active"`
	Created  int64 `json:"created"`
	Expired  int64 `json:"expired"`
	Rejected int64 `json:"rejected"`
}

// Deps are the injected behaviors of the server.
type Deps struct {
	Registry     *prometheus.Registry
	Status       func() Status
	Reload       func() error
	Clients      func(sortCol, order string, limit int, blocked bool) ClientsStatus
	Blacklist    func() BlacklistStatus
	BlacklistAdd func(entry string) error
	BlacklistDel func(entry string) error
	Logger       *slog.Logger
	UI           http.Handler
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
	mux.HandleFunc("GET /clients", s.handleClients)
	mux.HandleFunc("POST /reload", s.handleReload)
	mux.HandleFunc("GET /blacklist", s.handleBlacklistGet)
	mux.HandleFunc("POST /blacklist", s.handleBlacklistAdd)
	mux.HandleFunc("DELETE /blacklist", s.handleBlacklistDel)
	if deps.UI != nil {
		ui := http.StripPrefix("/ui", deps.UI)
		mux.Handle("GET /ui", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, "/ui/", http.StatusFound)
		}))
		mux.Handle("GET /ui/", ui)
	}
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

// handleClients serves the per-client-IP table: ?sort=<column>&order=
// asc|desc&limit=<n>&scope=normal|blocked. Defaults match the UIs: requests,
// descending, 200, general scope.
func (s *Server) handleClients(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	sortCol := q.Get("sort")
	if sortCol == "" {
		sortCol = "requests"
	}
	order := q.Get("order")
	if order != "asc" {
		order = "desc"
	}
	limit := 200
	if v, err := strconv.Atoi(q.Get("limit")); err == nil && v > 0 {
		limit = v
	}
	if limit > 10000 {
		limit = 10000
	}
	w.Header().Set("Content-Type", "application/json")
	var st ClientsStatus
	if s.deps.Clients != nil {
		st = s.deps.Clients(sortCol, order, limit, q.Get("scope") == "blocked")
	}
	if err := json.NewEncoder(w).Encode(st); err != nil {
		s.deps.Logger.Warn("clients encode failed", "err", err)
	}
}

// handleBlacklistGet serves the current blacklist state; 503 when the app
// did not wire the feature.
func (s *Server) handleBlacklistGet(w http.ResponseWriter, _ *http.Request) {
	if s.deps.Blacklist == nil {
		http.Error(w, "blacklist not available", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(s.deps.Blacklist()); err != nil {
		s.deps.Logger.Warn("blacklist encode failed", "err", err)
	}
}

// handleBlacklistAdd blocks one entry: invalid JSON or an empty entry is a
// 400, a validation error a 400 carrying the error text, success a 204.
func (s *Server) handleBlacklistAdd(w http.ResponseWriter, r *http.Request) {
	if s.deps.BlacklistAdd == nil {
		http.Error(w, "blacklist not available", http.StatusServiceUnavailable)
		return
	}
	var req struct {
		Entry string `json:"entry"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}
	if req.Entry == "" {
		http.Error(w, "entry is required", http.StatusBadRequest)
		return
	}
	if err := s.deps.BlacklistAdd(req.Entry); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleBlacklistDel unblocks the entry given as the ?entry= query
// parameter; an unknown entry (os.ErrNotExist from the app) is a 404.
func (s *Server) handleBlacklistDel(w http.ResponseWriter, r *http.Request) {
	if s.deps.BlacklistDel == nil {
		http.Error(w, "blacklist not available", http.StatusServiceUnavailable)
		return
	}
	entry := r.URL.Query().Get("entry")
	if entry == "" {
		http.Error(w, "entry query parameter is required", http.StatusBadRequest)
		return
	}
	if err := s.deps.BlacklistDel(entry); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
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
