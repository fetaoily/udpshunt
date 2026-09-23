package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fetaoily/udpshunt/internal/admin"
	"github.com/fetaoily/udpshunt/internal/balancer"
	"github.com/fetaoily/udpshunt/internal/blocklist"
	"github.com/fetaoily/udpshunt/internal/clientstats"
	"github.com/fetaoily/udpshunt/internal/config"
	"github.com/fetaoily/udpshunt/internal/health"
	"github.com/fetaoily/udpshunt/internal/listener"
	"github.com/fetaoily/udpshunt/internal/metrics"
	"github.com/fetaoily/udpshunt/internal/rates"
	"github.com/fetaoily/udpshunt/internal/requestlog"
	"github.com/fetaoily/udpshunt/internal/session"
)

// App supervises listeners, balancers and health probes across config
// reloads (spec §6).
type App struct {
	cfgPath     string
	logger      *slog.Logger
	met         *metrics.Metrics
	mgr         *session.Manager
	events      *admin.Recorder
	rates       *rates.Collector
	reqLog      *requestlog.Logger
	clientStats *clientstats.Table

	// Blacklist (spec §7): one shared container behind every listener plus
	// the dedicated blocked-traffic table (nil without a blacklist
	// section). blAdds/blDels are the runtime deltas over the config
	// source; blEffective mirrors the swapped list for reload diffing;
	// blCfgEntries is the config portion (inline + file) the watcher
	// rebuilds from. All four live under a.mu — none sit on the packet
	// path.
	bl           *blocklist.Container
	blockedStats *clientstats.Table
	blAdds       map[string]struct{}
	blDels       map[string]struct{}
	blEffective  map[string]struct{}
	blCfgEntries []string

	// blBlocked totals dropped packets for /status (fed by the metrics
	// hook) and blWatch carries the file-watch config for the watch loop.
	// Both lock-free.
	blBlocked atomic.Int64
	blWatch   atomic.Pointer[blWatchCfg]

	// onDown maps listener name -> on_down policy. Copy-on-write atomic:
	// state-change callbacks fire while Apply/updateListenerLocked holds
	// a.mu (bal.Snapshot may flip a suspect window mid-reload), so the
	// callback MUST read policies here and never take a.mu.
	onDown atomic.Pointer[map[string]string]

	mu        sync.Mutex
	cfg       config.Config
	started   time.Time
	listeners map[string]*listener.Listener
	balancers map[string]*balancer.Balancer
	// binds records the bind of each running listener as configured at
	// start time, so Apply compares against what is actually running
	// instead of a.cfg, which a partially failed Apply may leave stale.
	binds   map[string]string
	probes  map[string]*health.Prober
	cancels map[string]context.CancelFunc
	runDone map[string]chan struct{}
	// lctxs holds each listener's derived ctx: probes must die with their
	// listener, so they take this ctx — never the Apply ctx (Background
	// under Reload), which nothing cancels.
	lctxs map[string]context.Context
}

// blWatchCfg is the blacklist file-watch configuration as of the last
// applied config, read by the watch loop without a.mu.
type blWatchCfg struct {
	path     string
	interval time.Duration
}

func NewApp(cfgPath string, logger *slog.Logger) *App {
	met := metrics.New()
	a := &App{
		cfgPath:     cfgPath,
		logger:      logger,
		met:         met,
		mgr:         session.NewManager(0),
		events:      admin.NewRecorder(100),
		started:     time.Now(),
		rates:       rates.NewCollector(300, rates.FromRegistry(met.Registry())),
		bl:          blocklist.NewContainer(),
		blAdds:      map[string]struct{}{},
		blDels:      map[string]struct{}{},
		blEffective: map[string]struct{}{},
		listeners:   map[string]*listener.Listener{},
		balancers:   map[string]*balancer.Balancer{},
		binds:       map[string]string{},
		probes:      map[string]*health.Prober{},
		cancels:     map[string]context.CancelFunc{},
		runDone:     map[string]chan struct{}{},
		lctxs:       map[string]context.Context{},
	}
	a.onDown.Store(&map[string]string{})
	met.OnBlacklisted = func(n int64) { a.blBlocked.Add(n) }
	return a
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
		// Compare against the bind actually running, not a.cfg: after a
		// partially failed Apply, a.cfg is stale and the diff would restart
		// an already correctly-bound listener, evicting its sessions.
		if _, exists := a.listeners[lc.Name]; exists && a.binds[lc.Name] == lc.Bind {
			a.updateListenerLocked(lc)
			continue
		}
		if _, exists := a.listeners[lc.Name]; exists {
			a.stopListenerLocked(lc.Name)
		}
		if err := a.startListenerLocked(ctx, lc); err != nil {
			return fmt.Errorf("start listener %s: %w", lc.Name, err)
		}
	}
	// Blacklist rebuild (spec §7 union model) before a.cfg lands: a failure
	// here fails the Apply with the previous config still authoritative,
	// matching the listener-start failure path. Config entries are inline
	// plus the list file; the file read is fail-closed like config.Load —
	// it only fails when the file vanished between validation and now, and
	// then the reload keeps the previous config.
	cfgEntries := slices.Clone(cfg.Blacklist.Entries)
	if cfg.Blacklist.File != "" {
		fileEntries, err := blocklist.ReadFileEntries(cfg.Blacklist.File)
		if err != nil {
			return fmt.Errorf("blacklist.file %q: %w", cfg.Blacklist.File, err)
		}
		cfgEntries = append(cfgEntries, fileEntries...)
	}
	if err := a.rebuildBlacklistLocked(cfgEntries); err != nil {
		return fmt.Errorf("blacklist: %w", err)
	}
	for _, l := range a.listeners {
		l.UpdateLogBlocked(cfg.Blacklist.LogBlocked)
	}
	a.blWatch.Store(&blWatchCfg{path: cfg.Blacklist.File, interval: time.Duration(cfg.Blacklist.WatchInterval)})
	a.cfg = cfg
	return nil
}

// rebuildBlacklistLocked recomputes the effective blacklist — cfgEntries ∪
// blAdds − blDels — swaps it into the container and closes the live sessions
// of every newly blocked entry (diff over the previous effective set, spec
// §6). It also records cfgEntries as the config portion the watcher rebuilds
// from. Callers hold a.mu; never on the packet path.
func (a *App) rebuildBlacklistLocked(cfgEntries []string) error {
	eff := make(map[string]struct{}, len(cfgEntries)+len(a.blAdds))
	for _, e := range cfgEntries {
		eff[e] = struct{}{}
	}
	for e := range a.blAdds {
		eff[e] = struct{}{}
	}
	for e := range a.blDels {
		delete(eff, e)
	}
	entries := make([]string, 0, len(eff))
	for e := range eff {
		entries = append(entries, e)
	}
	list, err := blocklist.New(entries)
	if err != nil {
		return err
	}
	// Evict sessions only for entries that are newly effective; removals
	// never close anything and unchanged entries were swept when they
	// landed.
	closed, changed := 0, 0
	for e := range eff {
		if _, ok := a.blEffective[e]; ok {
			continue
		}
		changed++
		p, perr := blacklistEntryPrefix(e)
		if perr != nil {
			continue // unreachable: eff passed blocklist.New above
		}
		closed += closeBlacklistClients(a.mgr, p)
	}
	if closed > 0 {
		a.events.Add("blacklist_enforced", fmt.Sprintf("closed=%d entries=%d", closed, changed))
	}
	a.bl.Swap(list)
	a.blEffective = eff
	a.blCfgEntries = cfgEntries
	return nil
}

// blacklistEntryPrefix parses one blacklist entry (bare IP or CIDR) into a
// canonical prefix for session-eviction predicates, mirroring blocklist's
// normalization: bare IPs become host-length prefixes, IPv4-mapped
// addresses are unmapped. Entries reaching it were already validated by
// blocklist.New, so an error is defensive only.
func blacklistEntryPrefix(entry string) (netip.Prefix, error) {
	if p, err := netip.ParsePrefix(entry); err == nil {
		a := p.Addr()
		if a.Is4In6() && p.Bits() >= 96 {
			return netip.PrefixFrom(a.Unmap(), p.Bits()-96), nil
		}
		return p, nil
	}
	a, err := netip.ParseAddr(entry)
	if err != nil {
		return netip.Prefix{}, err
	}
	a = a.Unmap()
	return netip.PrefixFrom(a, a.BitLen()), nil
}

// closeBlacklistClients closes every session whose client IP falls in p and
// returns the count. Off the packet path.
func closeBlacklistClients(mgr *session.Manager, p netip.Prefix) int {
	return mgr.CloseClients(func(c *net.UDPAddr) bool {
		ca, ok := netip.AddrFromSlice(c.IP)
		if !ok {
			return false
		}
		return p.Contains(ca.Unmap())
	})
}

// BlacklistAdd blocks entry at runtime (spec §7): it validates, folds into
// the union and closes the entry's live sessions immediately. The closure
// runs before the rebuild so the count is attributed to blacklist_added;
// the rebuild's diff sweep then finds nothing left to close for it. The
// whole sequence holds a.mu and ends in the same state either way (list
// swapped, sessions gone), so the sub-moment ordering is not observable.
func (a *App) BlacklistAdd(entry string) error {
	if _, err := blocklist.New([]string{entry}); err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	p, err := blacklistEntryPrefix(entry)
	if err != nil {
		return err // unreachable after the blocklist.New validation above
	}
	n := closeBlacklistClients(a.mgr, p)
	delete(a.blDels, entry)
	a.blAdds[entry] = struct{}{}
	if err := a.rebuildBlacklistLocked(a.blCfgEntries); err != nil {
		// Only possible for a set that already passed validation; roll the
		// runtime delta back and keep the previous container list.
		delete(a.blAdds, entry)
		return err
	}
	a.events.Add("blacklist_added", fmt.Sprintf("%s closed=%d", entry, n))
	a.logger.Info("blacklist entry added", "entry", entry, "closed", n)
	return nil
}

// BlacklistDel removes entry from the effective blacklist (spec §7: a
// runtime delete wins over config entries and runtime adds until a reload
// that still carries the entry). An entry that is not effective is an
// os.ErrNotExist so the admin route can map it to 404.
func (a *App) BlacklistDel(entry string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, ok := a.blEffective[entry]; !ok {
		return fmt.Errorf("blacklist: entry %q: %w", entry, os.ErrNotExist)
	}
	delete(a.blAdds, entry)
	a.blDels[entry] = struct{}{}
	if err := a.rebuildBlacklistLocked(a.blCfgEntries); err != nil {
		return err
	}
	a.events.Add("blacklist_removed", entry)
	a.logger.Info("blacklist entry removed", "entry", entry)
	return nil
}

// Blacklist reports the current blacklist state for the admin API.
func (a *App) Blacklist() admin.BlacklistStatus {
	a.mu.Lock()
	defer a.mu.Unlock()
	var entries []string
	if l := a.bl.Load(); l != nil {
		entries = l.Entries()
	}
	return admin.BlacklistStatus{
		Entries:        entries,
		LogBlocked:     a.cfg.Blacklist.LogBlocked,
		BlockedPackets: a.blBlocked.Load(),
	}
}

func configHasListener(cfg config.Config, name string) bool {
	for _, l := range cfg.Listeners {
		if l.Name == name {
			return true
		}
	}
	return false
}

func hasBackend(addrs []string, addr string) bool {
	for _, a := range addrs {
		if a == addr {
			return true
		}
	}
	return false
}

// setOnDownLocked stores name's policy in the copy-on-write map. Callers
// hold a.mu (single writer); readers are state-change callbacks.
func (a *App) setOnDownLocked(name, policy string) {
	m := *a.onDown.Load()
	next := make(map[string]string, len(m)+1)
	for k, v := range m {
		next[k] = v
	}
	if policy == "" {
		policy = "close"
	}
	next[name] = policy
	a.onDown.Store(&next)
}

func (a *App) deleteOnDownLocked(name string) {
	m := *a.onDown.Load()
	next := make(map[string]string, len(m))
	for k, v := range m {
		if k != name {
			next[k] = v
		}
	}
	a.onDown.Store(&next)
}

func (a *App) startListenerLocked(ctx context.Context, lc config.Listener) error {
	bal := balancer.New(lc.Backends, balancer.Options{
		Balance:      lc.Balance,
		ActiveChecks: healthEnabled(lc.HealthCheck),
		Rise:         lc.HealthCheck.Rise,
		Fall:         lc.HealthCheck.Fall,
		Counts:       func(addr string) int64 { return a.mgr.BackendCount(lc.Name, addr) },
	})
	a.setOnDownLocked(lc.Name, lc.OnDown)
	bal.SetOnStateChange(func(t balancer.Transition) {
		// Runs on data-path and admin goroutines, and under a.mu during
		// reloads (Snapshot fires passive transitions). No a.mu here.
		a.met.SetBackendHealthy(lc.Name, t.Addr, t.To != balancer.StateDown)
		switch {
		case t.To == balancer.StateSuspect:
			a.events.Add("backend_suspect", fmt.Sprintf(
				"%s %s errors=%d last_error=%s", lc.Name, t.Addr, t.Errors, t.LastSource))
			a.logger.Warn("backend suspect", "listener", lc.Name, "backend", t.Addr,
				"errors", t.Errors, "last_error", t.LastSource)
		case t.To == balancer.StateDown:
			if pol, ok := (*a.onDown.Load())[lc.Name]; ok && pol == "drain" {
				n := a.mgr.BackendCount(lc.Name, t.Addr)
				a.events.Add("backend_down", fmt.Sprintf(
					"%s %s policy=drain draining=%d confirmed_by=%s errors=%d last_error=%s",
					lc.Name, t.Addr, n, t.Source, t.Errors, t.LastSource))
				a.logger.Info("backend marked down, sessions draining", "listener", lc.Name,
					"backend", t.Addr, "draining", n, "confirmed_by", t.Source)
				return
			}
			n := a.mgr.CloseBackend(lc.Name, t.Addr)
			a.events.Add("backend_down", fmt.Sprintf(
				"%s %s closed=%d confirmed_by=%s errors=%d last_error=%s",
				lc.Name, t.Addr, n, t.Source, t.Errors, t.LastSource))
			a.logger.Info("backend marked down, sessions closed", "listener", lc.Name,
				"backend", t.Addr, "sessions", n, "confirmed_by", t.Source)
		case t.From == balancer.StateSuspect && t.To == balancer.StateHealthy:
			a.events.Add("backend_suspect_cleared", fmt.Sprintf("%s %s by=%s", lc.Name, t.Addr, t.Source))
		default: // Down -> Healthy recovery
			a.events.Add("backend_up", lc.Name+" "+t.Addr)
		}
	})
	// The shared blacklist container is app-wide; blockedStats is nil (and
	// the blocked path metrics-only) without a blacklist section.
	l, err := listener.New(lc.Name, lc, bal, a.mgr, a.logger, a.met.ForListener(lc.Name), a.reqLog, a.clientStats, a.bl, a.blockedStats)
	if err != nil {
		return err
	}
	lctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	a.listeners[lc.Name] = l
	a.balancers[lc.Name] = bal
	a.cancels[lc.Name] = cancel
	a.runDone[lc.Name] = done
	a.lctxs[lc.Name] = lctx
	a.binds[lc.Name] = lc.Bind
	// Seed every backend's gauge before the receive loop starts, so
	// /metrics reports health from the first scrape — a bind-change reload
	// builds a fresh balancer under the same Prometheus labels and would
	// otherwise inherit a stale value.
	for _, addr := range lc.Backends {
		a.met.SeedBackendHealthy(lc.Name, addr, true)
	}
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

func (a *App) updateListenerLocked(lc config.Listener) {
	// Publish the (possibly changed) policy before Snapshot below: it can
	// fire suspect/down transitions, which must see the new config's policy.
	a.setOnDownLocked(lc.Name, lc.OnDown)
	bal := a.balancers[lc.Name]
	// spec §3: a backend leaving the pool terminates its sessions. Repool
	// FIRST, so Pick can no longer select a departing backend, and only
	// then evict: a session that slipped onto a departing backend before
	// the swap is still closed by this sweep. Diffed against the live pool
	// (not a.cfg, which a failed Apply may have left stale).
	var departing []string
	for _, old := range bal.Snapshot() {
		if !hasBackend(lc.Backends, old.Addr) {
			departing = append(departing, old.Addr)
		}
	}
	bal.Update(lc.Backends)
	for _, addr := range departing {
		if n := a.mgr.CloseBackend(lc.Name, addr); n > 0 {
			a.events.Add("backend_evicted", fmt.Sprintf("%s %s closed=%d", lc.Name, addr, n))
		}
	}
	// Re-seed the gauges for the new pool: new backends start healthy and
	// survivors keep their state, so seed from the actual post-Update view.
	for _, st := range bal.Snapshot() {
		a.met.SeedBackendHealthy(lc.Name, st.Addr, st.Healthy)
	}
	bal.SetBalance(lc.Balance)
	bal.SetHealth(healthEnabled(lc.HealthCheck), lc.HealthCheck.Rise, lc.HealthCheck.Fall)
	a.listeners[lc.Name].UpdateTimeout(time.Duration(lc.SessionTimeout))
	a.startProbeLocked(a.lctxs[lc.Name], lc)
	a.events.Add("listener_updated", lc.Name)
}

func (a *App) stopListenerLocked(name string) {
	// Detach the state-change callback first: an in-flight probe from the
	// old prober can report up to one timeout after Stop, and this callback
	// — keyed by listener name — would close the sessions of a same-named
	// restarted listener. Joining the prober instead would block under a.mu
	// for up to the probe timeout; detaching is O(1) and makes the late
	// report inert.
	if bal, ok := a.balancers[name]; ok {
		bal.SetOnStateChange(nil)
	}
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
	delete(a.binds, name)
	delete(a.cancels, name)
	delete(a.runDone, name)
	delete(a.lctxs, name)
	a.deleteOnDownLocked(name)
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
	ctx := context.Background()
	if err := a.Apply(ctx, cfg); err != nil {
		a.met.IncReloadFailure()
		a.events.Add("reload_failed", err.Error())
		a.logger.Error("reload failed, keeping previous config", "err", err)
		return err
	}
	// spec §6: a failed reload changes nothing, so the sessions cap moves
	// only after Apply has succeeded.
	a.mgr.SetMax(int64(cfg.Sessions.Max))
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
	if a.reqLog != nil {
		st.RequestLog = &admin.RequestLogStatus{
			Enabled: a.reqLog.Enabled(),
			Dir:     a.reqLog.Dir(),
			Dropped: a.reqLog.Dropped(),
		}
	}
	// The blacklist snapshot is always present (the feature ships with an
	// empty list, not a disabled one); same lock as the rest of the status.
	var blEntries []string
	if l := a.bl.Load(); l != nil {
		blEntries = l.Entries()
	}
	st.Blacklist = &admin.BlacklistStatus{
		Entries:        blEntries,
		LogBlocked:     a.cfg.Blacklist.LogBlocked,
		BlockedPackets: a.blBlocked.Load(),
	}
	for _, lc := range a.cfg.Listeners {
		bal := a.balancers[lc.Name]
		if bal == nil {
			// A failed reload can stop a listener without updating a.cfg;
			// it is genuinely down, so do not list it (and never Snapshot
			// a nil balancer).
			continue
		}
		counts := a.mgr.BackendCounts(lc.Name)
		var total int64
		bs := make([]admin.BackendStatus, 0, len(counts))
		for _, bt := range bal.Snapshot() {
			n := counts[bt.Addr]
			total += n
			bs = append(bs, admin.BackendStatus{
				Addr: bt.Addr, Healthy: bt.Healthy, Suspect: bt.Suspect, Sessions: n,
				ErrCount: bt.ErrCount, LastErrorSource: bt.LastErrorSource, DownConfirmBy: bt.DownConfirmBy,
			})
		}
		// Report the bind actually running when known; a.cfg may be stale
		// after a partially failed Apply.
		bind := lc.Bind
		if actual, ok := a.binds[lc.Name]; ok {
			bind = actual
		}
		var hist []admin.Sample
		for _, s := range a.rates.History(lc.Name) {
			hist = append(hist, admin.Sample{T: s.T, In: s.In, Out: s.Out, BytesIn: s.BytesIn, BytesOut: s.BytesOut})
		}
		st.Listeners = append(st.Listeners, admin.ListenerStatus{
			Name: lc.Name, Bind: bind, Balance: lc.Balance, Backends: bs, Sessions: total, History: hist,
		})
	}
	return st
}

// Clients serves the /clients endpoint: today's per-client-IP rows sorted
// by the requested column. blocked selects the dedicated blocked-traffic
// table (scope=blocked) instead of the general one; a missing table
// reports the disabled shape.
func (a *App) Clients(sortCol, order string, limit int, blocked bool) admin.ClientsStatus {
	t := a.clientStats
	if blocked {
		t = a.blockedStats
	}
	if t == nil {
		return admin.ClientsStatus{}
	}
	rows, tracked, evicted := t.Top(sortCol, order == "asc", limit)
	return admin.ClientsStatus{
		Enabled: t.Enabled(),
		Tracked: tracked,
		Evicted: evicted,
		Rows:    rows,
	}
}

// runSampler feeds the rate rings once per second until ctx is cancelled.
func (a *App) runSampler(ctx context.Context) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case ts := <-t.C:
			a.rates.Tick(ts)
			a.rates.Prune(ts, 10*time.Minute) // double the history window: a listener silent for 10 min is gone
		}
	}
}

// WatchBlacklist polls the configured blacklist file for changes until ctx
// is cancelled (spec §3). The cadence is the configured watch_interval;
// with no file or interval <= 0 it parks on a 1s standby tick and re-reads
// the config each wake. The watch is best-effort by design, unlike the
// fail-closed loader: a file that is missing, half-written or invalid keeps
// the previous list, logs an error and is retried on the next tick. The
// change check compares against the last successfully loaded stat, so a
// load that failed keeps being retried even though mtime and size stopped
// moving. Stat bookkeeping (path + last good mtime/size) stays local to
// the loop. A newly configured path forces one load — Apply already read
// the file fail-closed, but it may have changed since; the content check in
// pollBlacklistFile keeps that first load from swapping or emitting an
// event when nothing actually changed.
func (a *App) WatchBlacklist(ctx context.Context) {
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	var curPath string
	var goodMod time.Time
	var goodSize int64
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		interval := time.Second
		cfg := a.blWatch.Load()
		if cfg != nil && cfg.path != "" && cfg.interval > 0 {
			interval = cfg.interval
			if cfg.path != curPath {
				// New (or first) path: a zero stat forces one load.
				curPath = cfg.path
				goodMod, goodSize = time.Time{}, 0
			}
			a.pollBlacklistFile(cfg.path, &goodMod, &goodSize)
		}
		timer.Reset(interval)
	}
}

// pollBlacklistFile swaps in a fresh list when the file differs from the
// last successfully loaded stat. See WatchBlacklist for the contract.
func (a *App) pollBlacklistFile(path string, goodMod *time.Time, goodSize *int64) {
	fi, err := os.Stat(path)
	if err != nil {
		a.logger.Error("blacklist watch: file stat failed, keeping previous list", "file", path, "err", err)
		return
	}
	if fi.ModTime().Equal(*goodMod) && fi.Size() == *goodSize {
		return
	}
	entries, err := blocklist.ReadFileEntries(path)
	if err != nil {
		a.logger.Error("blacklist watch: read failed, keeping previous list", "file", path, "err", err)
		return
	}
	a.mu.Lock()
	// The fresh file replaces the previous file portion of the config
	// source; inline entries and runtime adds/dels carry over.
	combined := append(slices.Clone(a.cfg.Blacklist.Entries), entries...)
	if sameStringSet(combined, a.blCfgEntries) {
		// Content already applied (the watcher's first sight of the file
		// after Apply): remember the stat without re-swapping or eventing.
		a.mu.Unlock()
		*goodMod, *goodSize = fi.ModTime(), fi.Size()
		return
	}
	err = a.rebuildBlacklistLocked(combined)
	a.mu.Unlock()
	if err != nil {
		a.logger.Error("blacklist watch: reload failed, keeping previous list", "file", path, "err", err)
		return // last-good stat untouched: retried next tick
	}
	*goodMod, *goodSize = fi.ModTime(), fi.Size()
	if l := a.bl.Load(); l != nil {
		a.events.Add("blacklist_reloaded", fmt.Sprintf("entries=%d source=file", l.Len()))
		a.logger.Info("blacklist file reloaded", "file", path, "entries", l.Len())
	}
}

// sameStringSet reports whether a and b hold the same elements (order and
// duplicates ignored).
func sameStringSet(a, b []string) bool {
	sa := make(map[string]struct{}, len(a))
	for _, s := range a {
		sa[s] = struct{}{}
	}
	sb := make(map[string]struct{}, len(b))
	for _, s := range b {
		sb[s] = struct{}{}
	}
	if len(sa) != len(sb) {
		return false
	}
	for s := range sa {
		if _, ok := sb[s]; !ok {
			return false
		}
	}
	return true
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
