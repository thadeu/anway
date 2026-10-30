// Package config loads settings and holds the only reference to the product name.
package config

import (
	"fmt"
	"log/slog"
	"os"
	"time"
)

const (
	ProductName = "jobserver"
	Version     = "0.1.0"
)

type Config struct {
	BindAddr        string
	DataDir         string
	ConfDir         string
	Password        string
	ShutdownTimeout time.Duration
	LogLevel        slog.Level
}

func Load() (Config, error) {
	cfg := Config{
		BindAddr:        env("BIND_ADDR", "0.0.0.0:7419"),
		DataDir:         env("DATA_DIR", "./data"),
		ConfDir:         env("CONF_DIR", "./conf.d"),
		Password:        os.Getenv("SERVER_PASSWORD"),
		ShutdownTimeout: 30 * time.Second,
	}

	if raw := os.Getenv("SHUTDOWN_TIMEOUT"); raw != "" {
		timeout, err := time.ParseDuration(raw)
		if err != nil {
			return Config{}, fmt.Errorf("invalid SHUTDOWN_TIMEOUT: %w", err)
		}

		cfg.ShutdownTimeout = timeout
	}

	if err := cfg.LogLevel.UnmarshalText([]byte(env("LOG_LEVEL", "info"))); err != nil {
		return Config{}, fmt.Errorf("invalid LOG_LEVEL: %w", err)
	}

	return cfg, nil
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}

	return fallback
}
