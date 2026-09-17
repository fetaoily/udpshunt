// Command udpshunt is a UDP layer-4 load balancer (full proxy mode).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/fetaoily/udpshunt/internal/admin"
	"github.com/fetaoily/udpshunt/internal/config"
	"github.com/fetaoily/udpshunt/internal/tui"
	"github.com/fetaoily/udpshunt/internal/webui"
)

// version is stamped at release time via
// -ldflags "-X main.version=..." (goreleaser); source builds report "dev".
var version = "dev"

type cliOptions struct {
	cfgPath     string
	showVersion bool
}

func parseArgs(args []string) (cliOptions, error) {
	var o cliOptions
	fs := flag.NewFlagSet("udpshunt", flag.ContinueOnError)
	fs.StringVar(&o.cfgPath, "c", "/etc/udpshunt/udpshunt.yaml", "path to YAML config file")
	fs.BoolVar(&o.showVersion, "v", false, "print version and exit")
	if err := fs.Parse(args); err != nil {
		return o, err
	}
	return o, nil
}

func printVersion(w io.Writer) {
	fmt.Fprintf(w, "udpshunt %s\n", version)
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "udpshunt:", err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) > 1 && os.Args[1] == "tui" {
		return runTUI(os.Args[2:])
	}
	opts, err := parseArgs(os.Args[1:])
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil // usage already printed; --help is not an error
		}
		return err
	}
	if opts.showVersion {
		printVersion(os.Stdout)
		return nil
	}

	cfg, err := config.Load(opts.cfgPath)
	if err != nil {
		return err
	}
	logger := newLogger(cfg.Logging)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	app := NewApp(opts.cfgPath, logger)
	app.met.SetStartedAt(time.Now())
	app.met.SetSessionStats(app.mgr.Created, app.mgr.Expired, app.mgr.Rejected,
		func() int64 { return int64(app.mgr.Count()) })
	app.mgr.SetMax(int64(cfg.Sessions.Max))
	app.mgr.Start(ctx, time.Second) // spec §3: sweep 1/8 of shards per second
	go app.runSampler(ctx)          // spec §8: rate history, one sample per second

	adminSrv := admin.New(cfg.Admin.Bind, admin.Deps{
		Registry: app.met.Registry(),
		Status:   app.Status,
		Reload:   app.Reload,
		Logger:   logger,
		UI:       webui.Handler(),
	})
	go func() {
		if err := adminSrv.Run(ctx); err != nil {
			logger.Error("admin server stopped", "err", err)
		}
	}()

	sighup := make(chan os.Signal, 1)
	signal.Notify(sighup, syscall.SIGHUP) // never delivered on Windows; harmless
	go func() {
		for range sighup {
			_ = app.Reload()
		}
	}()

	if err := app.Apply(ctx, cfg); err != nil {
		return err
	}
	logger.Info("udpshunt started", "version", version, "listeners", len(cfg.Listeners), "admin", cfg.Admin.Bind)

	<-ctx.Done()
	logger.Info("shutting down")
	// Spec §10: stop receiving, give downstream relays a shared flush
	// window, close sessions, then close the frontend sockets.
	app.Shutdown(2 * time.Second)
	return nil
}

// runTUI runs the terminal dashboard against a running udpshunt's admin API.
func runTUI(args []string) error {
	fs := flag.NewFlagSet("tui", flag.ContinueOnError)
	addr := fs.String("addr", "http://127.0.0.1:9155", "admin API base URL")
	interval := fs.Duration("interval", time.Second, "poll interval")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil // usage already printed; --help is not an error
		}
		return err
	}
	return tui.Run(*addr, *interval)
}

func newLogger(lc config.Logging) *slog.Logger {
	var lvl slog.Level
	switch lc.Level {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	opts := &slog.HandlerOptions{Level: lvl}
	if lc.Format == "text" {
		return slog.New(slog.NewTextHandler(os.Stderr, opts))
	}
	return slog.New(slog.NewJSONHandler(os.Stderr, opts))
}
