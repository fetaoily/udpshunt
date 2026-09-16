// Command udpshunt is a UDP layer-4 load balancer (full proxy mode).
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/fetaoily/udpshunt/internal/admin"
	"github.com/fetaoily/udpshunt/internal/config"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "udpshunt:", err)
		os.Exit(1)
	}
}

func run() error {
	cfgPath := flag.String("c", "/etc/udpshunt.yaml", "path to YAML config file")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	logger := newLogger(cfg.Logging)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	app := NewApp(*cfgPath, logger)
	app.met.SetStartedAt(time.Now())
	app.met.SetSessionStats(app.mgr.Created, app.mgr.Expired, app.mgr.Rejected,
		func() int64 { return int64(app.mgr.Count()) })
	app.mgr.SetMax(int64(cfg.Sessions.Max))
	app.mgr.Start(ctx, time.Second) // spec §3: sweep 1/8 of shards per second

	adminSrv := admin.New(cfg.Admin.Bind, admin.Deps{
		Registry: app.met.Registry(),
		Status:   app.Status,
		Reload:   app.Reload,
		Logger:   logger,
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
	logger.Info("udpshunt started", "listeners", len(cfg.Listeners), "admin", cfg.Admin.Bind)

	<-ctx.Done()
	logger.Info("shutting down")
	// Spec §10: stop receiving, give downstream relays a shared flush
	// window, close sessions, then close the frontend sockets.
	app.Shutdown(2 * time.Second)
	return nil
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
