package storage

import (
	"encoding/json"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

var bucketWorkerSettings = []byte("worker_settings")

// ScaleRange is an autoscale range of threads.
type ScaleRange struct {
	Min int `json:"min"`
	Max int `json:"max"`
}

// WorkerSetting is what an operator set for a worker name in the web UI or
// the HTTP API. It wins over the config of a worker with that name, also
// after a restart, until the operator resets it.
//
// Autoscale nil means autoscale off. Concurrency is the manual thread count,
// or 0 when the operator did not set one.
type WorkerSetting struct {
	Autoscale   *ScaleRange `json:"autoscale"`
	Concurrency int         `json:"concurrency,omitempty"`
}

// SaveWorkerSetting writes the setting of one worker name.
func (b *Bolt) SaveWorkerSetting(name string, setting WorkerSetting) error {
	raw, err := json.Marshal(setting)
	if err != nil {
		return err
	}

	return b.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketWorkerSettings).Put([]byte(name), raw)
	})
}

// LoadWorkerSetting returns the setting of one worker name, and false when
// there is none.
func (b *Bolt) LoadWorkerSetting(name string) (WorkerSetting, bool, error) {
	var (
		setting WorkerSetting
		found   bool
	)

	err := b.db.View(func(tx *bolt.Tx) error {
		raw := tx.Bucket(bucketWorkerSettings).Get([]byte(name))

		if raw == nil {
			return nil
		}

		found = true

		if err := json.Unmarshal(raw, &setting); err != nil {
			return fmt.Errorf("decode worker setting: %w", err)
		}

		return nil
	})

	return setting, found, err
}

// DeleteWorkerSetting removes the setting of one worker name. A missing one
// is not an error.
func (b *Bolt) DeleteWorkerSetting(name string) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketWorkerSettings).Delete([]byte(name))
	})
}
