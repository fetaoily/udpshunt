// Package requestlog writes one JSON line per client request to a daily
// log file, compressing each file (gzip) when its day rolls over and
// deleting files older than the retention window.
//
// The proxy hot path never blocks on disk: Record enqueues into a bounded
// channel and drops (counting) when it is full. A single writer goroutine
// owns all file state.
package requestlog

import (
	"bufio"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fetaoily/udpshunt/internal/dailyfile"
)

// Outcomes recorded per request (mirrors the listener forward path).
const (
	OutcomeForwarded     = "forwarded"
	OutcomeNoBackend     = "no_backend"
	OutcomeRejected      = "rejected"
	OutcomeUpstreamError = "upstream_error"
	OutcomeBlacklisted   = "blacklisted"
)

const (
	dayFormat    = "2006-01-02"
	timeFormat   = "2006-01-02T15:04:05.000Z07:00"
	filePrefix   = "udpshunt-requests-"
	fileSuffix   = ".log"
	gzipSuffix   = ".gz"
	writeBufSize = 256 << 10
)

// Entry is one logged request.
type Entry struct {
	Time     time.Time
	Listener string
	Client   string
	Backend  string // empty when no backend was selected
	Bytes    int
	Outcome  string
}

// wire is the JSON shape of one log line. Field names are a public
// contract (jq / log shippers); do not rename casually.
type wire struct {
	Time     string `json:"time"`
	Listener string `json:"listener"`
	Client   string `json:"client"`
	Backend  string `json:"backend,omitempty"`
	Bytes    int    `json:"bytes"`
	Outcome  string `json:"outcome"`
}

type Options struct {
	Dir           string
	RetentionDays int           // default 30
	QueueSize     int           // default 65536
	FlushEvery    time.Duration // default 1s
	Clock         func() time.Time
	Logger        *slog.Logger // nil silences the package's warnings
}

type Logger struct {
	ch       chan Entry
	opts     Options
	logger   *slog.Logger
	dropped  atomic.Int64
	disabled atomic.Bool
	stop     chan struct{}
	stopOnce sync.Once
	done     chan struct{}

	// File state below is owned by the writer goroutine only.
	day          string // day of the currently open file
	f            *os.File
	w            *bufio.Writer
	enc          *json.Encoder
	lastPruneDay string
}

// New creates the log directory and starts the writer goroutine. If the
// directory cannot be created the returned Logger is disabled: Record
// becomes a no-op and the proxy keeps running (a broken log sink must
// never take the data path down).
func New(opts Options) *Logger {
	if opts.QueueSize <= 0 {
		opts.QueueSize = 65536
	}
	if opts.FlushEvery <= 0 {
		opts.FlushEvery = time.Second
	}
	if opts.RetentionDays <= 0 {
		opts.RetentionDays = 30
	}
	if opts.Clock == nil {
		opts.Clock = time.Now
	}
	l := &Logger{
		ch:   make(chan Entry, opts.QueueSize),
		opts: opts,
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
	if opts.Logger != nil {
		l.logger = opts.Logger
	}
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		l.warn("request log disabled: cannot create directory", "dir", opts.Dir, "err", err)
		l.disabled.Store(true)
		close(l.done)
		return l
	}
	today := opts.Clock().Format(dayFormat)
	// Prune first: expired files must be deleted, not swept into fresh
	// gzip copies. Then compress what a crash left behind.
	dailyfile.Prune(opts.Dir, filePrefix, fileSuffix, today, opts.RetentionDays, l.logger)
	dailyfile.SweepStale(opts.Dir, filePrefix, fileSuffix, today, l.logger)
	go l.run()
	return l
}

// Record enqueues one entry without ever blocking the caller; when the
// queue is full the entry is dropped and counted (visible via Dropped).
func (l *Logger) Record(e Entry) {
	if l == nil || l.disabled.Load() {
		return
	}
	select {
	case l.ch <- e:
	default:
		l.dropped.Add(1)
	}
}

// Dropped reports how many entries were lost to a full queue or a failed
// open (never zero on a healthy disk for long).
func (l *Logger) Dropped() int64 { return l.dropped.Load() }

// Enabled reports whether the logger is actually writing.
func (l *Logger) Enabled() bool { return l != nil && !l.disabled.Load() }

// Dir returns the configured log directory.
func (l *Logger) Dir() string { return l.opts.Dir }

// Stop flushes and closes the current file (left uncompressed: today's
// file keeps growing on the next start) and terminates the writer. It is
// safe to call more than once.
func (l *Logger) Stop() {
	if l == nil || l.disabled.Load() {
		return
	}
	l.stopOnce.Do(func() {
		close(l.stop)
		<-l.done
	})
}

func (l *Logger) warn(msg string, args ...any) {
	if l.logger != nil {
		l.logger.Warn(msg, args...)
	}
}

// run is the writer goroutine: the only owner of the file state.
func (l *Logger) run() {
	defer close(l.done)
	t := time.NewTicker(l.opts.FlushEvery)
	defer t.Stop()
	for {
		select {
		case <-l.stop:
			// Flush what was recorded before Stop: drain the queue, then
			// close. Entries recorded concurrently with Stop may be left.
			for {
				select {
				case e := <-l.ch:
					l.write(e)
				default:
					l.closeFile()
					return
				}
			}
		case e := <-l.ch:
			l.write(e)
		case <-t.C:
			l.onTick()
		}
	}
}

func (l *Logger) onTick() {
	day := l.now().Format(dayFormat)
	if l.w == nil {
		// A previous open failed (permissions, disk full). Retry once per
		// tick so a fixed sink recovers without a restart.
		l.openDay(day)
		return
	}
	if day != l.day {
		l.rotateTo(day)
		return
	}
	if err := l.w.Flush(); err != nil {
		l.warn("flush request log failed", "err", err)
	}
	if day != l.lastPruneDay {
		// Prune once per day even with no traffic.
		dailyfile.Prune(l.opts.Dir, filePrefix, fileSuffix, day, l.opts.RetentionDays, l.logger)
		l.lastPruneDay = day
	}
}

func (l *Logger) write(e Entry) {
	if e.Time.IsZero() {
		e.Time = l.now()
	}
	// The entry's own time decides its day: a queued entry consumed after
	// a rollover still lands in the file matching the printed timestamp.
	day := e.Time.Local().Format(dayFormat)
	if l.w == nil || day != l.day {
		l.rotateTo(day)
	}
	if l.w == nil {
		l.dropped.Add(1)
		return
	}
	if err := l.enc.Encode(&wire{
		Time:     e.Time.Format(timeFormat),
		Listener: e.Listener,
		Client:   e.Client,
		Backend:  e.Backend,
		Bytes:    e.Bytes,
		Outcome:  e.Outcome,
	}); err != nil {
		l.dropped.Add(1)
	}
}

// rotateTo closes the current file (compressing it in the background when
// it belongs to a previous day), opens today's file and prunes expired
// ones.
func (l *Logger) rotateTo(day string) {
	old := ""
	if l.f != nil {
		old = l.pathFor(l.day)
		l.closeFile()
	}
	l.openDay(day)
	if old != "" {
		go dailyfile.Compress(old, l.logger)
	}
	dailyfile.Prune(l.opts.Dir, filePrefix, fileSuffix, day, l.opts.RetentionDays, l.logger)
	l.lastPruneDay = day
}

func (l *Logger) openDay(day string) error {
	f, err := os.OpenFile(l.pathFor(day), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		l.warn("open request log failed", "dir", l.opts.Dir, "err", err)
		l.f, l.w, l.enc = nil, nil, nil
		l.day = day
		return err
	}
	l.f = f
	l.w = bufio.NewWriterSize(f, writeBufSize)
	l.enc = json.NewEncoder(l.w)
	l.day = day
	return nil
}

func (l *Logger) closeFile() {
	if l.w != nil {
		if err := l.w.Flush(); err != nil {
			l.warn("flush request log failed", "err", err)
		}
	}
	if l.f != nil {
		_ = l.f.Close()
	}
	l.f, l.w, l.enc = nil, nil, nil
}

func (l *Logger) pathFor(day string) string {
	return filepath.Join(l.opts.Dir, filePrefix+day+fileSuffix)
}

func (l *Logger) now() time.Time { return l.opts.Clock() }
