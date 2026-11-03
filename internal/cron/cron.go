// Package cron loads YAML settings and pushes periodic jobs.
package cron

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	schedule "github.com/robfig/cron/v3"
	"go.yaml.in/yaml/v3"

	"github.com/thadeu/anway/internal/job"
	"github.com/thadeu/anway/internal/manager"
	"github.com/thadeu/anway/internal/storage"
)

const defaultThrottleTimeout = 60

type JobTemplate struct {
	Type       string         `yaml:"type"`
	Queue      string         `yaml:"queue"`
	Args       []any          `yaml:"args"`
	Retry      *int           `yaml:"retry"`
	ReserveFor int            `yaml:"reserve_for"`
	Custom     map[string]any `yaml:"custom"`
}

type Entry struct {
	Schedule string      `yaml:"schedule"`
	Job      JobTemplate `yaml:"job"`

	parsed schedule.Schedule
}

type ThrottleSetting struct {
	Concurrency int `yaml:"concurrency"`
	Worker      int `yaml:"worker"`
	Timeout     int `yaml:"timeout"`
}

type Settings struct {
	Entries       []Entry                    `yaml:"cron"`
	ThrottleTable map[string]ThrottleSetting `yaml:"throttles"`
}

// LoadSettings merges all YAML files of a directory. A missing directory is empty.
func LoadSettings(dir string) (Settings, error) {
	merged := Settings{ThrottleTable: map[string]ThrottleSetting{}}

	files, err := settingFiles(dir)
	if err != nil {
		return merged, err
	}

	for _, file := range files {
		current, err := readSettings(file)
		if err != nil {
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

func settingFiles(dir string) ([]string, error) {
	var files []string

	for _, pattern := range []string{"*.yml", "*.yaml"} {
		matches, err := filepath.Glob(filepath.Join(dir, pattern))
		if err != nil {
			return nil, err
		}

		files = append(files, matches...)
	}

	sort.Strings(files)

	return files, nil
}

// readSettings rejects unknown fields, so that a typing error stops the load.
func readSettings(path string) (Settings, error) {
	var current Settings

	file, err := os.Open(path)
	if err != nil {
		return current, err
	}
	defer file.Close()

	decoder := yaml.NewDecoder(file)
	decoder.KnownFields(true)

	if err := decoder.Decode(&current); err != nil && !errors.Is(err, io.EOF) {
		return current, err
	}

	return current, nil
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

// Upcoming is one cron entry with its next run.
type Upcoming struct {
	Schedule string
	JobType  string
	Queue    string
	Next     time.Time
}

func (s *Scheduler) Upcoming() []Upcoming {
	s.mu.Lock()
	defer s.mu.Unlock()

	upcoming := make([]Upcoming, 0, len(s.entries))

	for i, entry := range s.entries {
		queue := entry.Job.Queue
		if queue == "" {
			queue = job.DefaultQueue
		}

		upcoming = append(upcoming, Upcoming{Schedule: entry.Schedule, JobType: entry.Job.Type, Queue: queue, Next: s.next[i]})
	}

	return upcoming
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
