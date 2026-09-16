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

	"github.com/fetaoily/udpshunt/internal/balancer"
	"github.com/fetaoily/udpshunt/internal/config"
	"github.com/fetaoily/udpshunt/internal/listener"
	"github.com/fetaoily/udpshunt/internal/session"
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

	mgr := session.NewManager(int64(cfg.Sessions.Max))
	mgr.Start(ctx, time.Second) // spec §3: sweep 1/8 of shards per second

	ls := make([]*listener.Listener, 0, len(cfg.Listeners))
	for _, lc := range cfg.Listeners {
		l, err := buildListener(lc, mgr, logger)
		if err != nil {
			return fmt.Errorf("listener %s: %w", lc.Name, err)
		}
		ls = append(ls, l)
	}
	for _, l := range ls {
		go func(l *listener.Listener) {
			if err := l.Run(ctx); err != nil {
				logger.Error("listener stopped with error", "err", err)
			}
		}(l)
	}
	logger.Info("udpshunt started",
		"listeners", len(ls),
		"session_timeout", time.Duration(cfg.Sessions.Timeout).String(),
	)

	<-ctx.Done()
	logger.Info("shutting down")
	// Spec §10: stop receiving, give downstream relays a flush window, close
	// sessions, then close the frontend sockets.
	for _, l := range ls {
		l.WaitDownstream(2 * time.Second)
	}
	mgr.CloseAll()
	for _, l := range ls {
		if err := l.Close(); err != nil {
			logger.Warn("close listener socket failed", "err", err)
		}
	}
	return nil
}

// buildListener wires one configured listener with its own balancer and the
// shared session manager.
func buildListener(lc config.Listener, mgr *session.Manager, logger *slog.Logger) (*listener.Listener, error) {
	bal := balancer.New(lc.Backends, balancer.Options{})
	bal.SetOnStateChange(func(addr string, healthy bool) {
		if healthy {
			logger.Info("backend marked up", "listener", lc.Name, "backend", addr)
			return
		}
		n := mgr.CloseBackend(lc.Name, addr)
		logger.Info("backend marked down, sessions closed", "listener", lc.Name, "backend", addr, "sessions", n)
	})
	l, err := listener.New(lc.Name, lc, bal, mgr, logger)
	if err != nil {
		return nil, err
	}
	return l, nil
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
