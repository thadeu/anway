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
