package storage

import (
	"encoding/json"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"
)

var bucketSchedules = []byte("schedules")

// ClientSchedule holds the cron entries that a client sent with SCHEDULE for
// one tag. Entries is the JSON array of the request. The cron package reads
// and checks it, so this package does not know the entry format.
type ClientSchedule struct {
	Tag       string          `json:"tag"`
	Entries   json.RawMessage `json:"entries"`
	UpdatedAt string          `json:"updated_at"`
}

// SaveSchedule replaces the entries of a tag.
func (b *Bolt) SaveSchedule(tag string, entries json.RawMessage, now time.Time) error {
	raw, err := json.Marshal(ClientSchedule{Tag: tag, Entries: entries, UpdatedAt: now.UTC().Format(time.RFC3339Nano)})
	if err != nil {
		return err
	}

	return b.write(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketSchedules).Put([]byte(tag), raw)
	})
}

// DeleteSchedule removes the entries of a tag. A missing tag is not an error.
func (b *Bolt) DeleteSchedule(tag string) error {
	return b.write(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketSchedules).Delete([]byte(tag))
	})
}

// Schedules returns the client schedules of all tags, in tag order.
func (b *Bolt) Schedules() ([]ClientSchedule, error) {
	var schedules []ClientSchedule

	err := b.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketSchedules).ForEach(func(key, raw []byte) error {
			var saved ClientSchedule

			if err := json.Unmarshal(raw, &saved); err != nil {
				return fmt.Errorf("decode schedule %q: %w", key, err)
			}

			schedules = append(schedules, saved)

			return nil
		})
	})

	return schedules, err
}
