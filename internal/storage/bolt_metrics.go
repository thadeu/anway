package storage

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

var bucketMetrics = []byte("metrics")

// MetricMinute holds the counts of one minute. Minute is Unix seconds / 60.
type MetricMinute struct {
	Minute int64             `json:"-"`
	Done   uint64            `json:"d"`
	Failed uint64            `json:"f"`
	Queues map[string]uint64 `json:"q,omitempty"`
}

func minuteKey(minute int64) []byte {
	return binary.BigEndian.AppendUint64(nil, uint64(minute))
}

// SaveMetrics writes minutes in one transaction. A minute that exists is replaced.
func (b *Bolt) SaveMetrics(minutes []MetricMinute) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketMetrics)

		for _, minute := range minutes {
			raw, err := json.Marshal(minute)
			if err != nil {
				return err
			}

			if err := bucket.Put(minuteKey(minute.Minute), raw); err != nil {
				return err
			}
		}

		return nil
	})
}

// LoadMetrics returns the saved minutes from first to last, inclusive, in order.
func (b *Bolt) LoadMetrics(first, last int64) ([]MetricMinute, error) {
	var minutes []MetricMinute

	err := b.db.View(func(tx *bolt.Tx) error {
		cursor := tx.Bucket(bucketMetrics).Cursor()
		end := minuteKey(last)

		for key, raw := cursor.Seek(minuteKey(first)); key != nil && bytes.Compare(key, end) <= 0; key, raw = cursor.Next() {
			minute := MetricMinute{Minute: int64(binary.BigEndian.Uint64(key))}

			if err := json.Unmarshal(raw, &minute); err != nil {
				return fmt.Errorf("decode metrics: %w", err)
			}

			minutes = append(minutes, minute)
		}

		return nil
	})

	return minutes, err
}

// PruneMetrics deletes minutes before the given minute, up to a limit.
func (b *Bolt) PruneMetrics(before int64, limit int) (int, error) {
	deleted := 0

	err := b.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketMetrics)
		cursor := bucket.Cursor()
		end := minuteKey(before)

		var keys [][]byte

		for key, _ := cursor.First(); key != nil && bytes.Compare(key, end) < 0 && len(keys) < limit; key, _ = cursor.Next() {
			keys = append(keys, bytes.Clone(key))
		}

		for _, key := range keys {
			if err := bucket.Delete(key); err != nil {
				return err
			}
		}

		deleted = len(keys)

		return nil
	})

	return deleted, err
}
