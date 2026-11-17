package storage

import (
	"encoding/json"
	"fmt"
	"slices"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/thadeu/anway/internal/job"
)

var (
	bucketCronRuns   = []byte("cron_runs")
	bucketCronPauses = []byte("cron_pauses")
	bucketTagsSeen   = []byte("tags_seen")
)

// MaxCronRuns is the number of runs that the store keeps for each cron entry.
const MaxCronRuns = 8

// maxRunError is the most bytes of an error that a run keeps.
const maxRunError = 1000

// Statuses of a cron run.
const (
	RunPushed  = "pushed"
	RunRunning = "running"
	RunDone    = "done"
	RunFailed  = "failed"
	RunDead    = "dead"
)

// CronRun is one job that a cron entry pushed. Error is the error of its last
// failure. A run that ends done has no error.
type CronRun struct {
	JID        string `json:"jid"`
	Manual     bool   `json:"manual,omitempty"`
	Status     string `json:"status"`
	PushedAt   string `json:"pushed_at"`
	StartedAt  string `json:"started_at,omitempty"`
	FinishedAt string `json:"finished_at,omitempty"`
	DurationMS *int64 `json:"duration_ms,omitempty"`
	Error      string `json:"error,omitempty"`
}

// CronPause is a cron entry that an operator paused. It keeps the parts of the
// entry, so a pause whose entry is gone can still say what it was.
type CronPause struct {
	ID       string `json:"id"`
	Source   string `json:"source"`
	Tag      string `json:"tag,omitempty"`
	JobType  string `json:"jobtype"`
	Schedule string `json:"schedule"`
	PausedAt string `json:"paused_at"`
	PausedBy string `json:"paused_by,omitempty"`
}

var runStatus = map[string]string{
	trackEnqueued: RunPushed,
	trackWorking:  RunRunning,
	trackSuccess:  RunDone,
	trackFailed:   RunFailed,
	trackDead:     RunDead,
}

// noteCronRun changes the run of a cron job, in the transaction that changes
// the job. A job that is not a run of a cron entry costs one prefix check. A
// new run starts when the job goes into a queue the first time.
func noteCronRun(tx *bolt.Tx, j *job.Job, state string, now time.Time) error {
	entry, manual, ok := job.CronEntry(j.JID)
	if !ok {
		return nil
	}

	runs, err := loadCronRuns(tx, entry)
	if err != nil {
		return err
	}

	at := now.UTC().Format(time.RFC3339Nano)
	index := slices.IndexFunc(runs, func(run CronRun) bool { return run.JID == j.JID })

	// A run that left the history, because 8 newer runs came, stays out.
	if index < 0 && state != trackEnqueued {
		return nil
	}

	if index < 0 {
		runs = slices.Insert(runs, 0, CronRun{JID: j.JID, Manual: manual, Status: RunPushed, PushedAt: at})

		return saveCronRuns(tx, entry, runs[:min(len(runs), MaxCronRuns)])
	}

	run := &runs[index]
	run.Status = runStatus[state]

	switch state {
	case trackWorking:
		run.StartedAt = at
		run.FinishedAt = ""
		run.DurationMS = nil
	case trackSuccess:
		finishRun(run, now)
		run.Error = ""
	case trackFailed, trackDead:
		finishRun(run, now)
		run.Error = failureText(j.Failure)
	}

	return saveCronRuns(tx, entry, runs)
}

func finishRun(run *CronRun, now time.Time) {
	run.FinishedAt = now.UTC().Format(time.RFC3339Nano)

	started, err := time.Parse(time.RFC3339Nano, run.StartedAt)
	if err != nil {
		return
	}

	duration := max(now.Sub(started).Milliseconds(), 0)
	run.DurationMS = &duration
}

func failureText(failure *job.Failure) string {
	if failure == nil {
		return ""
	}

	text := failure.Message

	if failure.ErrType != "" {
		text = failure.ErrType + ": " + text
	}

	if len(text) > maxRunError {
		text = text[:maxRunError]
	}

	return text
}

func loadCronRuns(tx *bolt.Tx, entry string) ([]CronRun, error) {
	raw := tx.Bucket(bucketCronRuns).Get([]byte(entry))
	if raw == nil {
		return nil, nil
	}

	var runs []CronRun

	if err := json.Unmarshal(raw, &runs); err != nil {
		return nil, fmt.Errorf("decode cron runs %s: %w", entry, err)
	}

	return runs, nil
}

func saveCronRuns(tx *bolt.Tx, entry string, runs []CronRun) error {
	raw, err := json.Marshal(runs)
	if err != nil {
		return err
	}

	return tx.Bucket(bucketCronRuns).Put([]byte(entry), raw)
}

// CronRuns returns the runs of each cron entry by entry id, newest first.
func (b *Bolt) CronRuns() (map[string][]CronRun, error) {
	runs := map[string][]CronRun{}

	err := b.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketCronRuns).ForEach(func(key, _ []byte) error {
			current, err := loadCronRuns(tx, string(key))
			if err != nil {
				return err
			}

			runs[string(key)] = current

			return nil
		})
	})

	return runs, err
}

// KeepCronRuns removes the runs of every entry that is not in ids, such as an
// entry that a deploy removed. It reads first, so it writes only when an
// entry is gone: a boot or a SCHEDULE with the same entries costs no sync.
func (b *Bolt) KeepCronRuns(ids []string) error {
	var gone [][]byte

	err := b.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketCronRuns).ForEach(func(key, _ []byte) error {
			if !slices.Contains(ids, string(key)) {
				gone = append(gone, slices.Clone(key))
			}

			return nil
		})
	})
	if err != nil || len(gone) == 0 {
		return err
	}

	return b.write(func(tx *bolt.Tx) error {
		for _, key := range gone {
			if err := tx.Bucket(bucketCronRuns).Delete(key); err != nil {
				return err
			}
		}

		return nil
	})
}

// SaveCronPause pauses a cron entry, or replaces its pause.
func (b *Bolt) SaveCronPause(pause CronPause) error {
	raw, err := json.Marshal(pause)
	if err != nil {
		return err
	}

	return b.write(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketCronPauses).Put([]byte(pause.ID), raw)
	})
}

// DeleteCronPause removes the pause of an entry. A missing pause is not an
// error.
func (b *Bolt) DeleteCronPause(id string) error {
	return b.write(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketCronPauses).Delete([]byte(id))
	})
}

// CronPauses returns all pauses, in id order.
func (b *Bolt) CronPauses() ([]CronPause, error) {
	var pauses []CronPause

	err := b.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketCronPauses).ForEach(func(key, raw []byte) error {
			var pause CronPause

			if err := json.Unmarshal(raw, &pause); err != nil {
				return fmt.Errorf("decode cron pause %q: %w", key, err)
			}

			pauses = append(pauses, pause)

			return nil
		})
	})

	return pauses, err
}

// SaveTagSeen keeps the last time that a worker of a tag was connected.
func (b *Bolt) SaveTagSeen(tag string, at time.Time) error {
	return b.write(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketTagsSeen).Put([]byte(tag), []byte(at.UTC().Format(time.RFC3339Nano)))
	})
}

// TagsSeen returns the saved last seen time of each tag.
func (b *Bolt) TagsSeen() (map[string]time.Time, error) {
	seen := map[string]time.Time{}

	err := b.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketTagsSeen).ForEach(func(key, raw []byte) error {
			at, err := time.Parse(time.RFC3339Nano, string(raw))
			if err != nil {
				return fmt.Errorf("decode tag seen %q: %w", key, err)
			}

			seen[string(key)] = at

			return nil
		})
	})

	return seen, err
}
