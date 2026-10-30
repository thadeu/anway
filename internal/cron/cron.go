// Package cron loads TOML settings and pushes periodic jobs.
package cron

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	schedule "github.com/robfig/cron/v3"

	"github.com/thadeu/anway/internal/job"
	"github.com/thadeu/anway/internal/manager"
	"github.com/thadeu/anway/internal/storage"
)

const defaultThrottleTimeout = 60

type JobTemplate struct {
	Type       string         `toml:"type"`
	Queue      string         `toml:"queue"`
	Args       []any          `toml:"args"`
	Retry      *int           `toml:"retry"`
	ReserveFor int            `toml:"reserve_for"`
	Custom     map[string]any `toml:"custom"`
}

type Entry struct {
	Schedule string      `toml:"schedule"`
	Job      JobTemplate `toml:"job"`

	parsed schedule.Schedule
}

type ThrottleSetting struct {
	Concurrency int `toml:"concurrency"`
	Worker      int `toml:"worker"`
	Timeout     int `toml:"timeout"`
}

type Settings struct {
	Entries       []Entry                    `toml:"cron"`
	ThrottleTable map[string]ThrottleSetting `toml:"throttles"`
}

// LoadSettings merges all TOML files of a directory. A missing directory is empty.
func LoadSettings(dir string) (Settings, error) {
	merged := Settings{ThrottleTable: map[string]ThrottleSetting{}}

	files, err := filepath.Glob(filepath.Join(dir, "*.toml"))
	if err != nil {
		return merged, err
	}

	for _, file := range files {
		raw, err := os.ReadFile(file)
		if err != nil {
			return merged, err
		}

		var current Settings

		if err := toml.Unmarshal(raw, &current); err != nil {
			return merged, fmt.Errorf("%s: %w", file, err)
		}

		merged.Entries = append(merged.Entries, current.Entries...)

		for queue, throttle := range current.ThrottleTable {
			merged.ThrottleTable[queue] = throttle
		}
	}

	for i := range merged.Entries {
		entry := &merged.Entries[i]

		parsed, err := schedule.ParseStandard(entry.Schedule)
		if err != nil {
			return merged, fmt.Errorf("invalid schedule %q: %w", entry.Schedule, err)
		}

		if entry.Job.Type == "" {
			return merged, fmt.Errorf("cron entry %q has no job type", entry.Schedule)
		}

		entry.parsed = parsed
	}

	for queue, throttle := range merged.ThrottleTable {
		if !job.ValidQueue(queue) || (throttle.Concurrency <= 0 && throttle.Worker <= 0) {
			return merged, fmt.Errorf("invalid throttle for queue %q", queue)
		}
	}

	return merged, nil
}

func (s Settings) Throttles() map[string]storage.Throttle {
	throttles := map[string]storage.Throttle{}

	for queue, setting := range s.ThrottleTable {
		timeout := setting.Timeout
		if timeout <= 0 {
			timeout = defaultThrottleTimeout
		}

		throttles[queue] = storage.Throttle{
			Concurrency: setting.Concurrency,
			Worker:      setting.Worker,
			Timeout:     time.Duration(timeout) * time.Second,
		}
	}

	return throttles
}

// Scheduler pushes each entry at its next UTC time. Times missed during
// downtime do not run.
type Scheduler struct {
	manager *manager.Manager
	log     *slog.Logger

	mu      sync.Mutex
	entries []Entry
	next    []time.Time
}

func New(m *manager.Manager, log *slog.Logger) *Scheduler {
	return &Scheduler{manager: m, log: log}
}

func (s *Scheduler) SetEntries(entries []Entry) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UTC()

	s.entries = entries
	s.next = make([]time.Time, len(entries))

	for i, entry := range entries {
		s.next[i] = entry.parsed.Next(now)
	}
}

func (s *Scheduler) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			s.Tick(now.UTC())
		}
	}
}

// Tick pushes the entries that are due.
func (s *Scheduler) Tick(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i := range s.entries {
		if now.Before(s.next[i]) {
			continue
		}

		s.next[i] = s.entries[i].parsed.Next(now)

		if err := s.push(s.entries[i].Job); err != nil {
			s.log.Error("cron push failed", "jobtype", s.entries[i].Job.Type, "error", err)
		}
	}
}

func (s *Scheduler) push(template JobTemplate) error {
	pushed, err := template.build()
	if err != nil {
		return err
	}

	return s.manager.Push(pushed)
}

func (t JobTemplate) build() (*job.Job, error) {
	id := make([]byte, 12)

	if _, err := rand.Read(id); err != nil {
		return nil, err
	}

	args := t.Args
	if args == nil {
		args = []any{}
	}

	encodedArgs, err := json.Marshal(args)
	if err != nil {
		return nil, err
	}

	built := &job.Job{
		JID:        "cron-" + hex.EncodeToString(id),
		Type:       t.Type,
		Args:       encodedArgs,
		Queue:      t.Queue,
		ReserveFor: t.ReserveFor,
		Retry:      t.Retry,
	}

	if t.Custom != nil {
		if built.Custom, err = json.Marshal(t.Custom); err != nil {
			return nil, err
		}
	}

	return built, nil
}
