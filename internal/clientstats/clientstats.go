// Package clientstats tracks per-client-IP request/response counts and
// traffic, aggregated across listeners. Counters are per-day: at rollover
// the day's snapshot file is finalized (gzip, retention) and the in-memory
// table resets. A snapshot file is rewritten in place every interval, so a
// restart recovers the current day's totals.
//
// The proxy hot path never blocks on stats: PacketIn/PacketOut take one
// shard lock and do a few integer adds, the same shape as the session
// table. Rates, eviction and all file state belong to the background
// goroutine.
package clientstats

import (
	"bufio"
	"bytes"
	"encoding/json"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fetaoily/udpshunt/internal/dailyfile"
)

const (
	dayFormat  = "2006-01-02"
	filePrefix = "udpshunt-clients-"
	fileSuffix = ".jsonl"
	shardCount = 256
	// rateAlpha is the EWMA smoothing factor: half of each new 1s sample.
	rateAlpha = 0.5
)

// Options configure the stats table.
type Options struct {
	Dir              string
	RetentionDays    int           // default 30
	MaxIPs           int           // default 65536
	SnapshotInterval time.Duration // default 60s
	TickEvery        time.Duration // default 1s (rates, eviction, rollover)
	Clock            func() time.Time
	Logger           *slog.Logger // nil silences the package's warnings
}

// Row is one client IP's stats as exposed by Top (and the /clients API).
// Field names are a public contract; do not rename casually.
type Row struct {
	IP        string  `json:"ip"`
	Requests  int64   `json:"requests"`
	Responses int64   `json:"responses"`
	BytesIn   int64   `json:"bytes_in"`
	BytesOut  int64   `json:"bytes_out"`
	PPSIn     float64 `json:"pps_in"`
	BPSIn     float64 `json:"bps_in"`
	BPSOut    float64 `json:"bps_out"`
	LastSeen  int64   `json:"last_seen"` // unix nanoseconds
}

// wireRow is the JSON shape of one snapshot line.
type wireRow struct {
	IP        string `json:"ip"`
	Requests  int64  `json:"requests"`
	Responses int64  `json:"responses"`
	BytesIn   int64  `json:"bytes_in"`
	BytesOut  int64  `json:"bytes_out"`
	LastSeen  int64  `json:"last_seen"`
}

// key is a zero-alloc comparable IP: 4 bytes + flag for IPv4, otherwise 16.
type key struct {
	b   [16]byte
	is4 bool
}

func keyOf(ip net.IP) (key, bool) {
	var k key
	if v4 := ip.To4(); v4 != nil {
		k.is4 = true
		copy(k.b[:], v4)
		return k, true
	}
	if v6 := ip.To16(); v6 != nil {
		copy(k.b[:], v6)
		return k, true
	}
	return k, false
}

func ipString(k key) string {
	if k.is4 {
		return net.IP(k.b[:4]).String()
	}
	return net.IP(k.b[:]).String()
}

// ipLess orders keys deterministically (IP bytes, then IPv4 before the
// IPv6 form with the same bytes).
func ipLess(a, b key) bool {
	if c := bytes.Compare(a.b[:], b.b[:]); c != 0 {
		return c < 0
	}
	return !a.is4 && b.is4
}

func (t *Table) pathFor(day string) string {
	return filepath.Join(t.opts.Dir, filePrefix+day+fileSuffix)
}

// stat holds one IP's counters. All fields are plain values guarded by the
// owning shard's mutex.
type stat struct {
	requests, responses int64
	bytesIn, bytesOut   int64
	lastSeen            int64 // unix nanoseconds

	ppsIn, bpsIn, bpsOut float64

	// Rate scratch: counters at the previous rate tick.
	havePrev                  bool
	prevRequests, prevBytesIn int64
	prevBytesOut              int64
}

type shard struct {
	mu sync.Mutex
	m  map[key]*stat
}

// rowItem is a key plus a copy of its stat, used while sorting.
type rowItem struct {
	k key
	s stat
}

// Table is the per-client-IP stats table. A nil *Table is valid: every
// method is a no-op, so callers can run without stats.
type Table struct {
	opts   Options
	logger *slog.Logger
	now    func() time.Time

	shards   [shardCount]shard
	tracked  atomic.Int64
	evicted  atomic.Int64
	disabled atomic.Bool

	stop     chan struct{}
	stopOnce sync.Once
	done     chan struct{}

	// Day and tick bookkeeping below is owned by the background goroutine
	// once run() starts (New writes the initial values first).
	day      string
	prevTick time.Time
	prevSnap time.Time
}

// New creates the stats directory, recovers today's snapshot if one exists,
// and starts the background goroutine. If the directory cannot be created
// the returned Table is disabled: every method is a no-op and the proxy
// keeps running (a broken stats sink must never take the data path down).
func New(opts Options) *Table {
	if opts.RetentionDays <= 0 {
		opts.RetentionDays = 30
	}
	if opts.MaxIPs <= 0 {
		opts.MaxIPs = 65536
	}
	if opts.SnapshotInterval <= 0 {
		opts.SnapshotInterval = time.Minute
	}
	if opts.TickEvery <= 0 {
		opts.TickEvery = time.Second
	}
	if opts.Clock == nil {
		opts.Clock = time.Now
	}
	t := &Table{
		opts: opts,
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
	t.now = opts.Clock
	if opts.Logger != nil {
		t.logger = opts.Logger
	}
	for i := range t.shards {
		t.shards[i].m = make(map[key]*stat)
	}
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		t.warn("client stats disabled: cannot create directory", "dir", opts.Dir, "err", err)
		t.disabled.Store(true)
		close(t.done)
		return t
	}
	now := opts.Clock()
	t.day = now.Format(dayFormat)
	t.prevTick = now
	t.prevSnap = now
	// Prune first: expired files must be deleted, not swept into fresh
	// gzip copies. Then compress what a crash left behind.
	dailyfile.Prune(opts.Dir, filePrefix, fileSuffix, t.day, opts.RetentionDays, t.logger)
	dailyfile.SweepStale(opts.Dir, filePrefix, fileSuffix, t.day, t.logger)
	t.removeTempFiles()
	t.loadSnapshot(t.day)
	go t.run()
	return t
}

func (t *Table) warn(msg string, args ...any) {
	if t.logger != nil {
		t.logger.Warn(msg, args...)
	}
}

// PacketIn counts one client packet (any outcome: a request is a request
// even when it is rejected or has no backend). ip may be nil (uncounted).
func (t *Table) PacketIn(ip net.IP, bytes int) { t.record(ip, bytes, true) }

// PacketOut counts one reply packet relayed to a client.
func (t *Table) PacketOut(ip net.IP, bytes int) { t.record(ip, bytes, false) }

func (t *Table) record(ip net.IP, bytes int, in bool) {
	if t == nil || t.disabled.Load() {
		return
	}
	k, ok := keyOf(ip)
	if !ok {
		return
	}
	now := t.now().UnixNano()
	sh := t.shardFor(k)
	sh.mu.Lock()
	s := sh.m[k]
	if s == nil {
		// The soft cap is enforced by the ticker's eviction; this hard cap
		// on the map-miss path only bounds memory against an IP flood
		// between ticks. Overflow packets go uncounted.
		if t.tracked.Load() >= 2*int64(t.opts.MaxIPs) {
			sh.mu.Unlock()
			return
		}
		s = &stat{}
		sh.m[k] = s
		t.tracked.Add(1)
	}
	if in {
		s.requests++
		s.bytesIn += int64(bytes)
	} else {
		s.responses++
		s.bytesOut += int64(bytes)
	}
	s.lastSeen = now
	sh.mu.Unlock()
}

func (t *Table) shardFor(k key) *shard {
	var h uint32 = 2166136261
	for _, c := range k.b {
		h ^= uint32(c)
		h *= 16777619
	}
	if k.is4 {
		h ^= 0xff
	}
	return &t.shards[h%shardCount]
}

// Top returns the current rows sorted by sortCol (requests | responses |
// bytes_in | bytes_out | pps_in | bps_in | bps_out | last_seen | ip; an
// unknown column falls back to requests), ascending when asc, limited to
// limit rows (<= 0 means all). Ties keep a stable IP order. IP strings are
// built only for the returned rows.
func (t *Table) Top(sortCol string, asc bool, limit int) ([]Row, int64, int64) {
	if t == nil || t.disabled.Load() {
		return nil, 0, 0
	}
	items := t.collect()
	less := lessFunc(sortCol, asc)
	sort.Slice(items, func(i, j int) bool { return less(items[i], items[j]) })
	if limit > 0 && limit < len(items) {
		items = items[:limit]
	}
	rows := make([]Row, len(items))
	for i, it := range items {
		rows[i] = Row{
			IP:        ipString(it.k),
			Requests:  it.s.requests,
			Responses: it.s.responses,
			BytesIn:   it.s.bytesIn,
			BytesOut:  it.s.bytesOut,
			PPSIn:     it.s.ppsIn,
			BPSIn:     it.s.bpsIn,
			BPSOut:    it.s.bpsOut,
			LastSeen:  it.s.lastSeen,
		}
	}
	return rows, t.tracked.Load(), t.evicted.Load()
}

// lessFunc builds the comparator for one sort column; ties always fall
// back to a stable IP order so equal rows do not shuffle between polls.
func lessFunc(col string, asc bool) func(a, b rowItem) bool {
	var prim func(a, b rowItem) bool
	switch col {
	case "responses":
		prim = func(a, b rowItem) bool { return a.s.responses < b.s.responses }
	case "bytes_in":
		prim = func(a, b rowItem) bool { return a.s.bytesIn < b.s.bytesIn }
	case "bytes_out":
		prim = func(a, b rowItem) bool { return a.s.bytesOut < b.s.bytesOut }
	case "pps_in":
		prim = func(a, b rowItem) bool { return a.s.ppsIn < b.s.ppsIn }
	case "bps_in":
		prim = func(a, b rowItem) bool { return a.s.bpsIn < b.s.bpsIn }
	case "bps_out":
		prim = func(a, b rowItem) bool { return a.s.bpsOut < b.s.bpsOut }
	case "last_seen":
		prim = func(a, b rowItem) bool { return a.s.lastSeen < b.s.lastSeen }
	case "ip":
		prim = func(a, b rowItem) bool { return ipLess(a.k, b.k) }
	default: // requests
		prim = func(a, b rowItem) bool { return a.s.requests < b.s.requests }
	}
	return func(a, b rowItem) bool {
		pa, pb := prim(a, b), prim(b, a)
		if !asc {
			pa, pb = pb, pa
		}
		if pa {
			return true
		}
		if pb {
			return false
		}
		return ipLess(a.k, b.k)
	}
}

func (t *Table) collect() []rowItem {
	items := make([]rowItem, 0, t.tracked.Load())
	for i := range t.shards {
		sh := &t.shards[i]
		sh.mu.Lock()
		for k, s := range sh.m {
			items = append(items, rowItem{k: k, s: *s})
		}
		sh.mu.Unlock()
	}
	return items
}

// Enabled reports whether the table is actually recording.
func (t *Table) Enabled() bool { return t != nil && !t.disabled.Load() }

// Tracked reports how many distinct IPs are currently held.
func (t *Table) Tracked() int64 {
	if t == nil {
		return 0
	}
	return t.tracked.Load()
}

// Evicted reports how many IPs idle-eviction has dropped.
func (t *Table) Evicted() int64 {
	if t == nil {
		return 0
	}
	return t.evicted.Load()
}

// Dir returns the configured snapshot directory.
func (t *Table) Dir() string {
	if t == nil {
		return ""
	}
	return t.opts.Dir
}

// Stop writes a final snapshot of today and terminates the background
// goroutine. It is safe to call more than once.
func (t *Table) Stop() {
	if t == nil || t.disabled.Load() {
		return
	}
	t.stopOnce.Do(func() {
		close(t.stop)
		<-t.done
	})
}

// run is the background goroutine: the only owner of day state, rates,
// eviction and the snapshot files.
func (t *Table) run() {
	defer close(t.done)
	ticker := time.NewTicker(t.opts.TickEvery)
	defer ticker.Stop()
	for {
		select {
		case <-t.stop:
			t.writeSnapshot(t.day)
			return
		case <-ticker.C:
			t.onTick(t.now())
		}
	}
}

// onTick advances rates, enforces the IP cap and handles day rollover and
// periodic snapshots. Split from run so tests can drive it deterministically.
func (t *Table) onTick(now time.Time) {
	t.updateRates(now)
	t.evictOverCap()
	day := now.Format(dayFormat)
	if day != t.day {
		t.finalizeDay(day)
		return
	}
	if now.Sub(t.prevSnap) >= t.opts.SnapshotInterval {
		t.writeSnapshot(day)
		t.prevSnap = now
	}
}

// updateRates refreshes each IP's EWMA rates from the counters' delta over
// the elapsed clock time between ticks.
func (t *Table) updateRates(now time.Time) {
	dt := now.Sub(t.prevTick).Seconds()
	t.prevTick = now
	if dt <= 0 {
		return
	}
	for i := range t.shards {
		sh := &t.shards[i]
		sh.mu.Lock()
		for _, s := range sh.m {
			if !s.havePrev {
				s.prevRequests, s.prevBytesIn, s.prevBytesOut = s.requests, s.bytesIn, s.bytesOut
				s.havePrev = true
				continue
			}
			dr := float64(s.requests-s.prevRequests) / dt
			dbi := float64(s.bytesIn-s.prevBytesIn) / dt
			dbo := float64(s.bytesOut-s.prevBytesOut) / dt
			s.ppsIn = rateAlpha*s.ppsIn + (1-rateAlpha)*dr
			s.bpsIn = rateAlpha*s.bpsIn + (1-rateAlpha)*dbi
			s.bpsOut = rateAlpha*s.bpsOut + (1-rateAlpha)*dbo
			s.prevRequests, s.prevBytesIn, s.prevBytesOut = s.requests, s.bytesIn, s.bytesOut
		}
		sh.mu.Unlock()
	}
}

// evictOverCap trims the table back to MaxIPs by dropping the
// least-recently-active IPs when the ticker finds it over cap.
func (t *Table) evictOverCap() {
	excess := t.tracked.Load() - int64(t.opts.MaxIPs)
	if excess <= 0 {
		return
	}
	type seen struct {
		k key
		t int64
	}
	all := make([]seen, 0, t.tracked.Load())
	for i := range t.shards {
		sh := &t.shards[i]
		sh.mu.Lock()
		for k, s := range sh.m {
			all = append(all, seen{k: k, t: s.lastSeen})
		}
		sh.mu.Unlock()
	}
	sort.Slice(all, func(i, j int) bool { return all[i].t < all[j].t })
	if excess > int64(len(all)) {
		excess = int64(len(all))
	}
	for _, it := range all[:excess] {
		sh := t.shardFor(it.k)
		sh.mu.Lock()
		if _, ok := sh.m[it.k]; ok {
			delete(sh.m, it.k)
			t.tracked.Add(-1)
			t.evicted.Add(1)
		}
		sh.mu.Unlock()
	}
}

// finalizeDay closes out the previous day: final snapshot, background gzip,
// retention, then the in-memory reset for the new day. Packets that land
// between the snapshot pass and the reset are dropped from the totals (a
// handful at the boundary; not worth cross-shard atomicity).
func (t *Table) finalizeDay(newDay string) {
	old := t.day
	t.writeSnapshot(old)
	go dailyfile.Compress(t.pathFor(old), t.logger)
	dailyfile.Prune(t.opts.Dir, filePrefix, fileSuffix, newDay, t.opts.RetentionDays, t.logger)
	for i := range t.shards {
		sh := &t.shards[i]
		sh.mu.Lock()
		clear(sh.m)
		sh.mu.Unlock()
	}
	t.tracked.Store(0)
	t.day = newDay
	t.prevSnap = t.now()
}

// writeSnapshot rewrites the day's file atomically: collect, write to a
// temp file, rename over the target. On POSIX the rename is atomic; on
// Windows it is a replace, so a crash mid-rename costs one snapshot.
func (t *Table) writeSnapshot(day string) {
	items := t.collect()
	// Deterministic order (by IP bytes) so consecutive snapshots diff cleanly.
	sort.Slice(items, func(i, j int) bool { return ipLess(items[i].k, items[j].k) })
	tmp, err := os.CreateTemp(t.opts.Dir, filePrefix+"*.tmp")
	if err != nil {
		t.warn("client stats snapshot failed", "dir", t.opts.Dir, "err", err)
		return
	}
	w := bufio.NewWriter(tmp)
	enc := json.NewEncoder(w)
	for _, it := range items {
		err := enc.Encode(&wireRow{
			IP:        ipString(it.k),
			Requests:  it.s.requests,
			Responses: it.s.responses,
			BytesIn:   it.s.bytesIn,
			BytesOut:  it.s.bytesOut,
			LastSeen:  it.s.lastSeen,
		})
		if err != nil {
			t.warn("client stats snapshot encode failed", "err", err)
			_ = tmp.Close()
			_ = os.Remove(tmp.Name())
			return
		}
	}
	if err := w.Flush(); err != nil {
		t.warn("client stats snapshot failed", "err", err)
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
		return
	}
	if err := tmp.Close(); err != nil {
		t.warn("client stats snapshot failed", "err", err)
		_ = os.Remove(tmp.Name())
		return
	}
	if err := os.Rename(tmp.Name(), t.pathFor(day)); err != nil {
		t.warn("client stats snapshot failed", "file", tmp.Name(), "err", err)
		_ = os.Remove(tmp.Name())
	}
}

// removeTempFiles deletes snapshot temp files a crash left behind: they are
// stale full copies, never referenced by recovery.
func (t *Table) removeTempFiles() {
	entries, err := os.ReadDir(t.opts.Dir)
	if err != nil {
		return
	}
	for _, de := range entries {
		name := de.Name()
		if strings.HasPrefix(name, filePrefix) && strings.HasSuffix(name, ".tmp") {
			_ = os.Remove(filepath.Join(t.opts.Dir, name))
		}
	}
}

// loadSnapshot recovers today's counters from the last snapshot, so a
// restart continues the same day's totals instead of starting from zero.
func (t *Table) loadSnapshot(day string) {
	f, err := os.Open(t.pathFor(day))
	if err != nil {
		return // no snapshot yet: start fresh
	}
	defer f.Close()
	now := t.now().UnixNano()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var wr wireRow
		if err := json.Unmarshal(line, &wr); err != nil {
			t.warn("client stats snapshot line skipped", "err", err)
			continue
		}
		ip := net.ParseIP(wr.IP)
		if ip == nil {
			continue
		}
		k, ok := keyOf(ip)
		if !ok {
			continue
		}
		sh := t.shardFor(k)
		sh.mu.Lock()
		if _, dup := sh.m[k]; !dup {
			ls := wr.LastSeen
			if ls <= 0 || ls > now {
				ls = now
			}
			sh.m[k] = &stat{
				requests:  wr.Requests,
				responses: wr.Responses,
				bytesIn:   wr.BytesIn,
				bytesOut:  wr.BytesOut,
				lastSeen:  ls,
			}
			t.tracked.Add(1)
		}
		sh.mu.Unlock()
	}
}
