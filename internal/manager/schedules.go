package manager

import (
	"encoding/json"
	"time"

	"github.com/thadeu/anway/internal/storage"
)

// SaveSchedule replaces the client cron entries of a tag. The cron package
// checks the entries before it calls this.
func (m *Manager) SaveSchedule(tag string, entries json.RawMessage) error {
	return m.store.SaveSchedule(tag, entries, time.Now())
}

// DeleteSchedule removes the client cron entries of a tag. A missing tag is
// not an error.
func (m *Manager) DeleteSchedule(tag string) error {
	return m.store.DeleteSchedule(tag)
}

// Schedules returns the client cron entries of all tags.
func (m *Manager) Schedules() ([]storage.ClientSchedule, error) {
	return m.store.Schedules()
}

// CronRuns returns the runs of each cron entry by entry id, newest first.
func (m *Manager) CronRuns() (map[string][]storage.CronRun, error) {
	return m.store.CronRuns()
}

// KeepCronRuns removes the runs of the entries that are not in ids.
func (m *Manager) KeepCronRuns(ids []string) error {
	return m.store.KeepCronRuns(ids)
}

func (m *Manager) SaveCronPause(pause storage.CronPause) error {
	return m.store.SaveCronPause(pause)
}

func (m *Manager) DeleteCronPause(id string) error {
	return m.store.DeleteCronPause(id)
}

func (m *Manager) CronPauses() ([]storage.CronPause, error) {
	return m.store.CronPauses()
}

// TagWorkers is the worker processes of one tag: how many are connected now,
// and the last time that one was connected.
type TagWorkers struct {
	Online   int
	LastSeen time.Time
}

// Tags returns the workers of each tag that the server knows: the tags of
// the workers in the table, and the saved tags of workers that left.
func (m *Manager) Tags() (map[string]TagWorkers, error) {
	saved, err := m.store.TagsSeen()
	if err != nil {
		return nil, err
	}

	tags := map[string]TagWorkers{}

	for tag, at := range saved {
		tags[tag] = TagWorkers{LastSeen: at}
	}

	for _, worker := range m.Workers() {
		if worker.Tag == "" {
			continue
		}

		current := tags[worker.Tag]

		if worker.Connections > 0 {
			current.Online++
		}

		if worker.LastBeat.After(current.LastSeen) {
			current.LastSeen = worker.LastBeat
		}

		tags[worker.Tag] = current
	}

	return tags, nil
}
