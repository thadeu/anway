// Package cron loads YAML settings and pushes periodic jobs.
package cron

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
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

// NextRuns is the number of next run times that Upcoming gives.
const NextRuns = 5

// ErrUnknownEntry is the error for an id that no cron entry has now.
var ErrUnknownEntry = errors.New("no cron entry has this id")

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
// all tags together, and skips a paused entry.
type Scheduler struct {
	manager *manager.Manager
	log     *slog.Logger

	mu      sync.Mutex
	conf    []Entry
	client  map[string][]Entry
	entries []scheduled
	next    []time.Time
	paused  map[string]storage.CronPause
}

// scheduled is one entry with its id and the place it came from.
type scheduled struct {
	Entry

	id     string
	source string
	tag    string
}

func New(m *manager.Manager, log *slog.Logger) *Scheduler {
	return &Scheduler{manager: m, log: log, client: map[string][]Entry{}, paused: map[string]storage.CronPause{}}
}

// SetEntries replaces the conf.d entries. The client entries stay.
func (s *Scheduler) SetEntries(entries []Entry) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.conf = entries
	s.rebuild()
	s.keepRuns()
}

// EntryID is the id of an entry: a short hash of its source, tag, job type
// and schedule. It stays the same across restarts and deploys while these
// parts stay. repeat tells apart equal entries of one source and tag.
func EntryID(source, tag string, entry Entry, repeat int) string {
	key := strings.Join([]string{source, tag, entry.Job.Type, entry.Schedule}, "\x00")

	if repeat > 0 {
		key += "\x00" + strconv.Itoa(repeat)
	}

	sum := sha256.Sum256([]byte(key))

	return hex.EncodeToString(sum[:])[:job.CronIDLength]
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
	repeats := map[string]int{}

	add := func(entry Entry, source, tag string) {
		id := EntryID(source, tag, entry, 0)
		repeat := repeats[id]
		repeats[id]++

		if repeat > 0 {
			id = EntryID(source, tag, entry, repeat)
		}

		s.entries = append(s.entries, scheduled{Entry: entry, id: id, source: source, tag: tag})
	}

	for _, entry := range s.conf {
		add(entry, SourceConf, "")
	}

	for _, tag := range tags {
		for _, entry := range s.client[tag] {
			add(entry, SourceClient, tag)
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
	s.keepRuns()

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
	s.keepRuns()

	return nil
}

// keepRuns removes the runs of the entries that are gone. It is a separate
// write: when it fails, the old runs stay until the next change. The caller
// holds the lock.
func (s *Scheduler) keepRuns() {
	ids := make([]string, len(s.entries))

	for i, entry := range s.entries {
		ids[i] = entry.id
	}

	if err := s.manager.KeepCronRuns(ids); err != nil {
		s.log.Error("cron runs of removed entries stay", "error", err)
	}
}

// LoadClient reads the saved client entries and the pauses, for example at
// boot. A saved tag that this version cannot read is logged and skipped, so
// the server still starts.
func (s *Scheduler) LoadClient() error {
	saved, err := s.manager.Schedules()
	if err != nil {
		return err
	}

	pauses, err := s.manager.CronPauses()
	if err != nil {
		return err
	}

	paused := map[string]storage.CronPause{}

	for _, pause := range pauses {
		paused[pause.ID] = pause
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
	s.paused = paused
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

	built, err := entry.Job.build("cron-check")
	if err != nil {
		return err
	}

	if err := built.Normalize(time.Now()); err != nil {
		return fmt.Errorf("cron entry %q: %w", entry.Schedule, err)
	}

	return nil
}

// Upcoming is one cron entry with its next runs. Source is SourceConf or
// SourceClient. Tag is the client tag, blank for conf.d. Paused is nil for
// an entry that runs on its schedule.
type Upcoming struct {
	ID       string
	Schedule string
	JobType  string
	Queue    string
	Next     time.Time
	NextRuns []time.Time
	Source   string
	Tag      string
	Job      JobTemplate
	Paused   *storage.CronPause
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

		runs := make([]time.Time, NextRuns)
		runs[0] = s.next[i]

		for k := 1; k < NextRuns; k++ {
			runs[k] = entry.parsed.Next(runs[k-1])
		}

		current := Upcoming{
			ID:       entry.id,
			Schedule: entry.Schedule,
			JobType:  entry.Job.Type,
			Queue:    queue,
			Next:     s.next[i],
			NextRuns: runs,
			Source:   entry.source,
			Tag:      entry.tag,
			Job:      entry.Job,
		}

		if pause, paused := s.paused[entry.id]; paused {
			current.Paused = &pause
		}

		upcoming = append(upcoming, current)
	}

	return upcoming
}

// StalePauses returns the pauses whose entry is gone, for example because a
// deploy changed its schedule. A resume clears them.
func (s *Scheduler) StalePauses() []storage.CronPause {
	s.mu.Lock()
	defer s.mu.Unlock()

	stale := []storage.CronPause{}

	for id, pause := range s.paused {
		if _, found := s.findLocked(id); !found {
			stale = append(stale, pause)
		}
	}

	sort.Slice(stale, func(i, j int) bool { return stale[i].ID < stale[j].ID })

	return stale
}

// Pause stops the runs of an entry on its schedule, and saves the pause. It
// is kept apart from the entries, so a new SCHEDULE of the tag, as at a
// deploy, does not resume it. A pause of a paused entry keeps the first one.
func (s *Scheduler) Pause(id, by string) (storage.CronPause, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if current, paused := s.paused[id]; paused {
		return current, nil
	}

	entry, found := s.findLocked(id)
	if !found {
		return storage.CronPause{}, ErrUnknownEntry
	}

	pause := storage.CronPause{
		ID:       id,
		Source:   entry.source,
		Tag:      entry.tag,
		JobType:  entry.Job.Type,
		Schedule: entry.Schedule,
		PausedAt: time.Now().UTC().Format(time.RFC3339Nano),
		PausedBy: by,
	}

	if err := s.manager.SaveCronPause(pause); err != nil {
		return storage.CronPause{}, err
	}

	s.paused[id] = pause

	return pause, nil
}

// Resume removes the pause of an entry, also of an entry that is gone. The
// entry runs again at its next time, and missed times do not run. A missing
// pause is not an error.
func (s *Scheduler) Resume(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, paused := s.paused[id]; !paused {
		return nil
	}

	if err := s.manager.DeleteCronPause(id); err != nil {
		return err
	}

	delete(s.paused, id)

	return nil
}

// RunNow pushes one job of an entry now, also of a paused entry. The
// schedule does not change. The jid starts with cron-manual-, and the run
// shows in the history of the entry.
func (s *Scheduler) RunNow(id string) (string, error) {
	s.mu.Lock()
	entry, found := s.findLocked(id)
	s.mu.Unlock()

	if !found {
		return "", ErrUnknownEntry
	}

	return s.push(entry, true)
}

// findLocked returns the entry with an id. The caller holds the lock.
func (s *Scheduler) findLocked(id string) (scheduled, bool) {
	for _, entry := range s.entries {
		if entry.id == id {
			return entry, true
		}
	}

	return scheduled{}, false
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

		if _, paused := s.paused[s.entries[i].id]; paused {
			continue
		}

		if _, err := s.push(s.entries[i], false); err != nil {
			s.log.Error("cron push failed", "jobtype", s.entries[i].Job.Type, "error", err)
		}
	}
}

// push pushes one job of an entry. Its jid names the entry, so the store
// notes the run. See job.CronJID.
func (s *Scheduler) push(entry scheduled, manual bool) (string, error) {
	jid, err := job.CronJID(entry.id, manual)
	if err != nil {
		return "", err
	}

	pushed, err := entry.Job.build(jid)
	if err != nil {
		return "", err
	}

	return jid, s.manager.Push(pushed)
}

func (t JobTemplate) build(jid string) (*job.Job, error) {
	args := t.Args
	if args == nil {
		args = []any{}
	}

	encodedArgs, err := json.Marshal(args)
	if err != nil {
		return nil, err
	}

	built := &job.Job{
		JID:        jid,
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
