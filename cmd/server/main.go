package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"jobserver/internal/config"
	"jobserver/internal/cron"
	"jobserver/internal/manager"
	"jobserver/internal/server"
	"jobserver/internal/storage"
)

func main() {
	if err := run(); err != nil {
		slog.Error("server stopped", "error", err)
		os.Exit(1)
	}
}

// applySettings loads all TOML files. It changes nothing when a file is invalid.
func applySettings(dir string, jobs *manager.Manager, scheduler *cron.Scheduler) error {
	settings, err := cron.LoadSettings(dir)
	if err != nil {
		return err
	}

	jobs.SetThrottles(settings.Throttles())
	scheduler.SetEntries(settings.Entries)

	return nil
}

func reloadOnHangup(ctx context.Context, dir string, jobs *manager.Manager, scheduler *cron.Scheduler, log *slog.Logger) {
	hangup := make(chan os.Signal, 1)
	signal.Notify(hangup, syscall.SIGHUP)

	for {
		select {
		case <-ctx.Done():
			return
		case <-hangup:
			if err := applySettings(dir, jobs, scheduler); err != nil {
				log.Error("reload failed, the old settings stay active", "error", err)

				continue
			}

			log.Info("settings reloaded")
		}
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: cfg.LogLevel}))

	if err := os.MkdirAll(cfg.DataDir, 0o700); err != nil {
		return err
	}

	store, err := storage.OpenBolt(cfg.DataDir)
	if err != nil {
		return err
	}
	defer store.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	listener, err := net.Listen("tcp", cfg.BindAddr)
	if err != nil {
		return err
	}

	jobs := manager.New(store)
	srv := server.New(jobs, cfg.Password, log)
	loopCtx, stopLoop := context.WithCancel(context.Background())

	defer stopLoop()

	scheduler := cron.New(jobs, log)

	if err := applySettings(cfg.ConfDir, jobs, scheduler); err != nil {
		return err
	}

	go scheduler.Run(loopCtx)
	go reloadOnHangup(loopCtx, cfg.ConfDir, jobs, scheduler, log)

	go jobs.Run(loopCtx, func(err error) {
		log.Error("background loop failed", "error", err)
	})

	serveErr := make(chan error, 1)

	go func() {
		serveErr <- srv.Serve(loopCtx, listener)
	}()

	log.Info("server started", "addr", listener.Addr().String(), "data_dir", cfg.DataDir)

	select {
	case <-ctx.Done():
	case err := <-serveErr:
		return err
	}

	log.Info("shutdown started")

	listener.Close()
	srv.Shutdown(cfg.ShutdownTimeout)

	return nil
}
