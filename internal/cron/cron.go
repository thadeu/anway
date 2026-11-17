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

// Sources of a cron entry.
const (
	SourceConf   = "conf.d"
	SourceClient = "client"
)

// MaxClientEntries is the most entries that one SCHEDULE tag can have.
const MaxClientEntries = 256

// JobTemplate is the job of a cron entry. A conf.d file and SCHEDULE use the
// same keys.
type JobTemplate struct {
	Type       string         `yaml:"type" json:"type"`
	Queue      string         `yaml:"queue" json:"queue,omitempty"`
	Args       []any          `yaml:"args" json:"args,omitempty"`
	Retry      *int           `yaml:"retry" json:"retry,omitempty"`
	ReserveFor int            `yaml:"reserve_for" json:"reserve_for,omitempty"`
	Custom     map[string]any `yaml:"custom" json:"custom,omitempty"`
}

type Entry struct {
	Schedule string      `yaml:"schedule" json:"schedule"`
	Job      JobTemplate `yaml:"job" json:"job"`

	parsed schedule.Schedule
}

// prepare parses the schedule and checks that the job has a type.
func (e *Entry) prepare() error {
	parsed, err := schedule.ParseStandard(e.Schedule)
	if err != nil {
		return fmt.Errorf("invalid schedule %q: %w", e.Schedule, err)
	}

	if e.Job.Type == "" {
		return fmt.Errorf("cron entry %q has no job type", e.Schedule)
	}

	e.parsed = parsed

	return nil
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
		if err := merged.Entries[i].prepare(); err != nil {
			return merged, err
		}
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
// downtime do not run. It runs the conf.d entries and the client entries of
// all tags together.
type Scheduler struct {
	manager *manager.Manager
	log     *slog.Logger

	mu      sync.Mutex
	conf    []Entry
	client  map[string][]Entry
	entries []scheduled
	next    []time.Time
}

// scheduled is one entry with the place it came from.
type scheduled struct {
	Entry

	source string
	tag    string
}

func New(m *manager.Manager, log *slog.Logger) *Scheduler {
	return &Scheduler{manager: m, log: log, client: map[string][]Entry{}}
}

// SetEntries replaces the conf.d entries. The client entries stay.
func (s *Scheduler) SetEntries(entries []Entry) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.conf = entries
	s.rebuild()
}

// rebuild makes the list of all entries, and the next run of each one. The
// caller holds the lock.
func (s *Scheduler) rebuild() {
	now := time.Now().UTC()

	tags := make([]string, 0, len(s.client))

	for tag := range s.client {
		tags = append(tags, tag)
	}

	sort.Strings(tags)

	s.entries = make([]scheduled, 0, len(s.conf))

	for _, entry := range s.conf {
		s.entries = append(s.entries, scheduled{Entry: entry, source: SourceConf})
	}

	for _, tag := range tags {
		for _, entry := range s.client[tag] {
			s.entries = append(s.entries, scheduled{Entry: entry, source: SourceClient, tag: tag})
		}
	}

	s.next = make([]time.Time, len(s.entries))

	for i, entry := range s.entries {
		s.next[i] = entry.parsed.Next(now)
	}
}

// ScheduleRequest is the SCHEDULE argument: all cron entries of one client tag.
type ScheduleRequest struct {
	Tag     string  `json:"tag"`
	Entries []Entry `json:"entries"`
}

// Schedule replaces the client entries of a tag, and saves them. No entries
// removes the tag. An invalid entry changes nothing.
func (s *Scheduler) Schedule(request ScheduleRequest) error {
	if err := checkTag(request.Tag); err != nil {
		return err
	}

	if len(request.Entries) > MaxClientEntries {
		return fmt.Errorf("a tag has at most %d cron entries", MaxClientEntries)
	}

	for i := range request.Entries {
		if err := checkClientEntry(&request.Entries[i]); err != nil {
			return err
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if len(request.Entries) == 0 {
		return s.removeLocked(request.Tag)
	}

	encoded, err := json.Marshal(request.Entries)
	if err != nil {
		return err
	}

	if err := s.manager.SaveSchedule(request.Tag, encoded); err != nil {
		return err
	}

	s.client[request.Tag] = request.Entries
	s.rebuild()

	return nil
}

// Unschedule removes the client entries of a tag. A missing tag is not an
// error.
func (s *Scheduler) Unschedule(tag string) error {
	if err := checkTag(tag); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return s.removeLocked(tag)
}

func (s *Scheduler) removeLocked(tag string) error {
	if err := s.manager.DeleteSchedule(tag); err != nil {
		return err
	}

	delete(s.client, tag)
	s.rebuild()

	return nil
}

// LoadClient reads the saved client entries, for example at boot. A saved
// tag that this version cannot read is logged and skipped, so the server
// still starts.
func (s *Scheduler) LoadClient() error {
	saved, err := s.manager.Schedules()
	if err != nil {
		return err
	}

	client := map[string][]Entry{}

	for _, current := range saved {
		entries, err := decodeClient(current.Entries)
		if err != nil {
			s.log.Error("saved schedule skipped", "tag", current.Tag, "error", err)

			continue
		}

		client[current.Tag] = entries
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.client = client
	s.rebuild()

	return nil
}

func decodeClient(raw json.RawMessage) ([]Entry, error) {
	var entries []Entry

	if err := json.Unmarshal(raw, &entries); err != nil {
		return nil, err
	}

	for i := range entries {
		if err := checkClientEntry(&entries[i]); err != nil {
			return nil, err
		}
	}

	return entries, nil
}

func checkTag(tag string) error {
	if !job.ValidQueue(tag) {
		return fmt.Errorf("invalid tag: %q", tag)
	}

	return nil
}

// checkClientEntry prepares an entry and checks its job as a push would, so
// that a bad queue or reserve_for fails at SCHEDULE, not at each run.
func checkClientEntry(entry *Entry) error {
	if err := entry.prepare(); err != nil {
		return err
	}

	built, err := entry.Job.build()
	if err != nil {
		return err
	}

	if err := built.Normalize(time.Now()); err != nil {
		return fmt.Errorf("cron entry %q: %w", entry.Schedule, err)
	}

	return nil
}

// Upcoming is one cron entry with its next run. Source is SourceConf or
// SourceClient. Tag is the client tag, blank for conf.d.
type Upcoming struct {
	Schedule string
	JobType  string
	Queue    string
	Next     time.Time
	Source   string
	Tag      string
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

		upcoming = append(upcoming, Upcoming{
			Schedule: entry.Schedule,
			JobType:  entry.Job.Type,
			Queue:    queue,
			Next:     s.next[i],
			Source:   entry.source,
			Tag:      entry.tag,
		})
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
