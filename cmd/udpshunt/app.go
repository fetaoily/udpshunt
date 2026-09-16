package main

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/fetaoily/udpshunt/internal/admin"
	"github.com/fetaoily/udpshunt/internal/balancer"
	"github.com/fetaoily/udpshunt/internal/config"
	"github.com/fetaoily/udpshunt/internal/health"
	"github.com/fetaoily/udpshunt/internal/listener"
	"github.com/fetaoily/udpshunt/internal/metrics"
	"github.com/fetaoily/udpshunt/internal/session"
)

// App supervises listeners, balancers and health probes across config
// reloads (spec §6).
type App struct {
	cfgPath string
	logger  *slog.Logger
	met     *metrics.Metrics
	mgr     *session.Manager
	events  *admin.Recorder

	mu        sync.Mutex
	cfg       config.Config
	started   time.Time
	listeners map[string]*listener.Listener
	balancers map[string]*balancer.Balancer
	probes    map[string]*health.Prober
	cancels   map[string]context.CancelFunc
	runDone   map[string]chan struct{}
}

func NewApp(cfgPath string, logger *slog.Logger) *App {
	return &App{
		cfgPath:   cfgPath,
		logger:    logger,
		met:       metrics.New(),
		mgr:       session.NewManager(0),
		events:    admin.NewRecorder(100),
		started:   time.Now(),
		listeners: map[string]*listener.Listener{},
		balancers: map[string]*balancer.Balancer{},
		probes:    map[string]*health.Prober{},
		cancels:   map[string]context.CancelFunc{},
		runDone:   map[string]chan struct{}{},
	}
}

func healthEnabled(hc config.HealthCheck) bool { return hc.Mode == "raw" || hc.Mode == "dns" }

// Apply diffs cfg against the running set. A start failure mid-apply
// returns the error; already-removed listeners stay removed (correct) and
// the previous config keeps serving the rest.
func (a *App) Apply(ctx context.Context, cfg config.Config) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	for name := range a.listeners {
		if !configHasListener(cfg, name) {
			a.stopListenerLocked(name)
		}
	}
	for _, lc := range cfg.Listeners {
		oldBind := boundOf(a.cfg, lc.Name)
		if _, exists := a.listeners[lc.Name]; exists && oldBind == lc.Bind {
			a.updateListenerLocked(ctx, lc)
			continue
		}
		if _, exists := a.listeners[lc.Name]; exists {
			a.stopListenerLocked(lc.Name)
		}
		if err := a.startListenerLocked(ctx, lc); err != nil {
			return fmt.Errorf("start listener %s: %w", lc.Name, err)
		}
	}
	a.cfg = cfg
	return nil
}

func configHasListener(cfg config.Config, name string) bool {
	for _, l := range cfg.Listeners {
		if l.Name == name {
			return true
		}
	}
	return false
}

func boundOf(cfg config.Config, name string) string {
	for _, l := range cfg.Listeners {
		if l.Name == name {
			return l.Bind
		}
	}
	return ""
}

func (a *App) startListenerLocked(ctx context.Context, lc config.Listener) error {
	bal := balancer.New(lc.Backends, balancer.Options{
		Balance:      lc.Balance,
		ActiveChecks: healthEnabled(lc.HealthCheck),
		Rise:         lc.HealthCheck.Rise,
		Fall:         lc.HealthCheck.Fall,
		Counts:       func(addr string) int64 { return a.mgr.BackendCount(lc.Name, addr) },
	})
	bal.SetOnStateChange(func(addr string, healthy bool) {
		a.met.SetBackendHealthy(lc.Name, addr, healthy)
		if healthy {
			a.events.Add("backend_up", lc.Name+" "+addr)
			return
		}
		n := a.mgr.CloseBackend(lc.Name, addr)
		a.events.Add("backend_down", fmt.Sprintf("%s %s closed=%d", lc.Name, addr, n))
		a.logger.Info("backend marked down, sessions closed", "listener", lc.Name, "backend", addr, "sessions", n)
	})
	l, err := listener.New(lc.Name, lc, bal, a.mgr, a.logger, a.met.ForListener(lc.Name))
	if err != nil {
		return err
	}
	lctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	a.listeners[lc.Name] = l
	a.balancers[lc.Name] = bal
	a.cancels[lc.Name] = cancel
	a.runDone[lc.Name] = done
	go func() {
		defer close(done)
		if err := l.Run(lctx); err != nil {
			a.logger.Error("listener stopped with error", "listener", lc.Name, "err", err)
		}
	}()
	a.startProbeLocked(lctx, lc)
	a.events.Add("listener_started", lc.Name+" "+lc.Bind)
	return nil
}

func (a *App) startProbeLocked(ctx context.Context, lc config.Listener) {
	if p, ok := a.probes[lc.Name]; ok {
		p.Stop()
		delete(a.probes, lc.Name)
	}
	if !healthEnabled(lc.HealthCheck) {
		return
	}
	a.probes[lc.Name] = health.Start(ctx, a.balancers[lc.Name], lc.Backends, lc.HealthCheck, a.logger)
}

func (a *App) updateListenerLocked(ctx context.Context, lc config.Listener) {
	bal := a.balancers[lc.Name]
	bal.Update(lc.Backends)
	bal.SetBalance(lc.Balance)
	bal.SetHealth(healthEnabled(lc.HealthCheck), lc.HealthCheck.Rise, lc.HealthCheck.Fall)
	a.listeners[lc.Name].UpdateTimeout(time.Duration(lc.SessionTimeout))
	a.startProbeLocked(ctx, lc)
	a.events.Add("listener_updated", lc.Name)
}

func (a *App) stopListenerLocked(name string) {
	if cancel, ok := a.cancels[name]; ok {
		cancel()
	}
	if done, ok := a.runDone[name]; ok {
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			a.logger.Warn("listener Run did not stop in time", "listener", name)
		}
	}
	if p, ok := a.probes[name]; ok {
		p.Stop()
		delete(a.probes, name)
	}
	if l, ok := a.listeners[name]; ok {
		l.WaitDownstream(500 * time.Millisecond)
		n := a.mgr.CloseListener(name)
		_ = l.Close()
		l.WaitWaiters(time.Second)
		delete(a.listeners, name)
		a.events.Add("listener_stopped", fmt.Sprintf("%s closed=%d", name, n))
	}
	delete(a.balancers, name)
	delete(a.cancels, name)
	delete(a.runDone, name)
}

// Reload loads the config file and applies it. On failure the previous
// config keeps running (spec §6).
func (a *App) Reload() error {
	cfg, err := config.Load(a.cfgPath)
	if err != nil {
		a.met.IncReloadFailure()
		a.events.Add("reload_failed", err.Error())
		a.logger.Error("reload failed, keeping previous config", "err", err)
		return err
	}
	a.mgr.SetMax(int64(cfg.Sessions.Max))
	ctx := context.Background()
	if err := a.Apply(ctx, cfg); err != nil {
		a.met.IncReloadFailure()
		a.events.Add("reload_failed", err.Error())
		a.logger.Error("reload failed, keeping previous config", "err", err)
		return err
	}
	a.met.IncReload()
	a.events.Add("reload", "ok")
	a.logger.Info("config reloaded", "listeners", len(cfg.Listeners))
	return nil
}

// Status assembles the /status snapshot.
func (a *App) Status() admin.Status {
	a.mu.Lock()
	defer a.mu.Unlock()
	st := admin.Status{
		Uptime: time.Since(a.started).Round(time.Second).String(),
		Sessions: admin.SessionsStatus{
			Active:   int64(a.mgr.Count()),
			Created:  a.mgr.Created(),
			Expired:  a.mgr.Expired(),
			Rejected: a.mgr.Rejected(),
		},
		Events: a.events.List(),
	}
	for _, lc := range a.cfg.Listeners {
		bal := a.balancers[lc.Name]
		counts := a.mgr.BackendCounts(lc.Name)
		var total int64
		bs := make([]admin.BackendStatus, 0, len(counts))
		for _, bt := range bal.Snapshot() {
			n := counts[bt.Addr]
			total += n
			bs = append(bs, admin.BackendStatus{Addr: bt.Addr, Healthy: bt.Healthy, Sessions: n})
		}
		st.Listeners = append(st.Listeners, admin.ListenerStatus{
			Name: lc.Name, Bind: lc.Bind, Balance: lc.Balance, Backends: bs, Sessions: total,
		})
	}
	return st
}

// Shutdown stops everything: receive loops first, then a parallel drain
// sharing one budget (spec §10), then session/socket close.
func (a *App) Shutdown(drainWindow time.Duration) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, cancel := range a.cancels {
		cancel()
	}
	for name, done := range a.runDone {
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			a.logger.Warn("listener Run did not stop", "listener", name)
		}
	}
	deadline := time.Now().Add(drainWindow)
	var wg sync.WaitGroup
	for _, l := range a.listeners {
		wg.Add(1)
		go func(l *listener.Listener) {
			defer wg.Done()
			l.WaitDownstream(time.Until(deadline))
		}(l)
	}
	wg.Wait()
	a.mgr.CloseAll()
	for _, l := range a.listeners {
		_ = l.Close()
		l.WaitWaiters(time.Second)
	}
}
