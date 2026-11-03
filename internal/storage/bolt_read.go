package storage

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"regexp"
	"sort"
	"strings"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/thadeu/anway/internal/job"
)

const (
	clearPage   = 500
	maxJobsPage = 200
)

// QueueInfo is one queue for the web API.
type QueueInfo struct {
	Name     string
	Size     uint64
	Paused   bool
	OldestAt time.Time
}

// JobEntry is one job in a set or queue. At is the time in the set key.
type JobEntry struct {
	Job   job.Job
	State string
	At    time.Time
	WID   string
}

type JobPage struct {
	Jobs []JobEntry
	Next string
}

type JobGroup struct {
	Fingerprint string
	Count       int
	Latest      JobEntry
}

type JobGroups struct {
	Groups    []JobGroup
	Scanned   int
	Truncated bool
}

type BatchSummary struct {
	BatchStatus
	Committed bool `json:"committed"`
}

var volatileParts = regexp.MustCompile(`[0-9a-fA-F]{8,}|\d+`)

// Fingerprint groups failures with the same job type and cause. Numbers and
// long hex ids are removed from the message, so that one cause is one group.
func Fingerprint(j *job.Job) string {
	cause := "scheduled"

	if j.Failure != nil {
		cause = j.Failure.ErrType + "\x00" + volatileParts.ReplaceAllString(j.Failure.Message, "#")
	}

	digest := sha256.Sum256([]byte(j.Type + "\x00" + cause))

	return hex.EncodeToString(digest[:6])
}

func (b *Bolt) QueueDetails() ([]QueueInfo, error) {
	var queues []QueueInfo

	err := b.db.View(func(tx *bolt.Tx) error {
		names := map[string]bool{}

		tx.Bucket(bucketQueues).ForEachBucket(func(name []byte) error {
			names[string(name)] = true

			return nil
		})

		tx.Bucket(bucketPaused).ForEach(func(name, _ []byte) error {
			names[string(name)] = true

			return nil
		})

		for name := range names {
			info := QueueInfo{
				Name:   name,
				Size:   counter(tx, []byte(queuePrefix+name)),
				Paused: tx.Bucket(bucketPaused).Get([]byte(name)) != nil,
			}

			info.OldestAt = oldestInQueue(tx, name)
			queues = append(queues, info)
		}

		return nil
	})

	sort.Slice(queues, func(i, j int) bool { return queues[i].Name < queues[j].Name })

	return queues, err
}

func oldestInQueue(tx *bolt.Tx, name string) time.Time {
	queue := tx.Bucket(bucketQueues).Bucket([]byte(name))
	if queue == nil {
		return time.Time{}
	}

	key, jid := queue.Cursor().First()
	if key == nil {
		return time.Time{}
	}

	rec, err := load(tx, string(jid))
	if err != nil {
		return time.Time{}
	}

	at, _ := time.Parse(time.RFC3339Nano, rec.Job.EnqueuedAt)

	return at
}

// setBucket returns the bucket of a set name, and whether its keys hold a time.
// A queue set is "queue:<name>".
func setBucket(tx *bolt.Tx, set string) (*bolt.Bucket, bool, error) {
	if name, isQueue := strings.CutPrefix(set, "queue:"); isQueue {
		if !job.ValidQueue(name) {
			return nil, false, &RejectedError{Message: "invalid queue name"}
		}

		return tx.Bucket(bucketQueues).Bucket([]byte(name)), false, nil
	}

	switch set {
	case "retries", "scheduled", "dead", "working":
		return tx.Bucket([]byte(set)), true, nil
	default:
		return nil, false, &RejectedError{Message: "unknown set: " + set}
	}
}

// ListJobs reads one page in one short transaction. The cursor is opaque.
func (b *Bolt) ListJobs(set, cursor string, limit int) (JobPage, error) {
	page := JobPage{}

	if limit <= 0 || limit > maxJobsPage {
		limit = maxJobsPage
	}

	after, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return page, &RejectedError{Message: "invalid cursor"}
	}

	err = b.db.View(func(tx *bolt.Tx) error {
		bucket, timed, err := setBucket(tx, set)
		if err != nil || bucket == nil {
			return err
		}

		keys := pageKeysLimit(bucket.Cursor(), after, limit+1)

		if len(keys) > limit {
			keys = keys[:limit]
			page.Next = base64.RawURLEncoding.EncodeToString(keys[limit-1])
		}

		for _, key := range keys {
			entry, ok := readEntry(tx, bucket, key, timed)
			if ok {
				page.Jobs = append(page.Jobs, entry)
			}
		}

		return nil
	})

	return page, err
}

func pageKeysLimit(cursor *bolt.Cursor, after []byte, limit int) [][]byte {
	var key []byte

	if len(after) == 0 {
		key, _ = cursor.First()
	} else {
		key, _ = cursor.Seek(after)

		if bytes.Equal(key, after) {
			key, _ = cursor.Next()
		}
	}

	var keys [][]byte

	for ; key != nil && len(keys) < limit; key, _ = cursor.Next() {
		keys = append(keys, bytes.Clone(key))
	}

	return keys
}

func readEntry(tx *bolt.Tx, bucket *bolt.Bucket, key []byte, timed bool) (JobEntry, bool) {
	jid := string(bucket.Get(key))

	if timed {
		jid = jidFromKey(key)
	}

	rec, err := load(tx, jid)
	if err != nil {
		return JobEntry{}, false
	}

	entry := JobEntry{Job: rec.Job, State: rec.State, WID: rec.WID}

	if timed {
		entry.At = time.Unix(0, int64(binary.BigEndian.Uint64(key[:8]))).UTC()
	}

	return entry, true
}

// JobGroups scans a set page by page, so that no read transaction stays open.
func (b *Bolt) JobGroups(set string, maxScan int) (JobGroups, error) {
	result := JobGroups{}
	groups := map[string]*JobGroup{}
	cursor := ""

	for result.Scanned < maxScan {
		page, err := b.ListJobs(set, cursor, maxJobsPage)
		if err != nil {
			return result, err
		}

		for _, entry := range page.Jobs {
			fingerprint := Fingerprint(&entry.Job)
			group := groups[fingerprint]

			if group == nil {
				group = &JobGroup{Fingerprint: fingerprint, Latest: entry}
				groups[fingerprint] = group
			}

			group.Count++
			result.Scanned++
		}

		if page.Next == "" {
			break
		}

		cursor = page.Next

		if result.Scanned >= maxScan {
			result.Truncated = true
		}
	}

	for _, group := range groups {
		result.Groups = append(result.Groups, *group)
	}

	sort.Slice(result.Groups, func(i, j int) bool {
		if result.Groups[i].Count != result.Groups[j].Count {
			return result.Groups[i].Count > result.Groups[j].Count
		}

		return result.Groups[i].Fingerprint < result.Groups[j].Fingerprint
	})

	return result, nil
}

// BusyByWorker counts reserved jobs for each worker id.
func (b *Bolt) BusyByWorker() (map[string]int, error) {
	busy := map[string]int{}

	err := b.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketWorking).ForEach(func(key, _ []byte) error {
			rec, err := load(tx, jidFromKey(key))
			if err == nil && rec.WID != "" {
				busy[rec.WID]++
			}

			return nil
		})
	})

	return busy, err
}

// LeaseCounts counts live throttle leases for each queue.
func (b *Bolt) LeaseCounts(now time.Time) (map[string]int, error) {
	counts := map[string]int{}

	err := b.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketLeases).ForEach(func(key, value []byte) error {
			if int64(binary.BigEndian.Uint64(value)) <= now.UnixNano() {
				return nil
			}

			queue, _, _ := bytes.Cut(key, []byte{0})
			counts[string(queue)]++

			return nil
		})
	})

	return counts, err
}

func (b *Bolt) Batches(limit int) ([]BatchSummary, error) {
	var batches []BatchSummary

	err := b.db.View(func(tx *bolt.Tx) error {
		cursor := tx.Bucket(bucketBatches).Cursor()

		for key, raw := cursor.First(); key != nil && len(batches) < limit; key, raw = cursor.Next() {
			batch := &batchRecord{}

			if err := json.Unmarshal(raw, batch); err != nil {
				return err
			}

			batches = append(batches, BatchSummary{BatchStatus: batch.Status, Committed: batch.Committed})
		}

		return nil
	})

	return batches, err
}

// ClearQueue deletes the waiting jobs of a queue in short transactions. The
// queue keeps its paused state.
func (b *Bolt) ClearQueue(name string) (int, error) {
	if !job.ValidQueue(name) {
		return 0, &RejectedError{Message: "invalid queue name"}
	}

	total := 0

	for {
		deleted := 0

		err := b.db.Update(func(tx *bolt.Tx) error {
			queue := tx.Bucket(bucketQueues).Bucket([]byte(name))
			if queue == nil {
				return nil
			}

			keys := pageKeysLimit(queue.Cursor(), nil, clearPage)

			for _, key := range keys {
				jid := string(queue.Get(key))

				rec, err := load(tx, jid)
				if err != nil && !errors.Is(err, errJobNotFound) {
					return err
				}

				if rec != nil {
					if err := releaseUniqueLock(tx, &rec.Job); err != nil {
						return err
					}

					if err := tx.Bucket(bucketJobs).Delete([]byte(jid)); err != nil {
						return err
					}
				}

				if err := queue.Delete(key); err != nil {
					return err
				}

				if err := addCounter(tx, []byte(queuePrefix+name), -1); err != nil {
					return err
				}

				deleted++
			}

			return nil
		})

		total += deleted

		if err != nil || deleted < clearPage {
			return total, err
		}
	}
}

// fireDeadHook pushes the job that custom.on_dead names, in the same
// transaction as the death. Its only arg is a summary of the dead job.
func fireDeadHook(tx *bolt.Tx, rec *record, now time.Time) error {
	hook := rec.Job.DeadHook()
	if hook == nil {
		return nil
	}

	jid, err := randomID("dead-")
	if err != nil {
		return err
	}

	summary := map[string]any{
		"jid":     rec.Job.JID,
		"jobtype": rec.Job.Type,
		"queue":   rec.Job.Queue,
		"args":    rec.Job.Args,
		"failure": rec.Job.Failure,
		"custom":  rec.Job.Custom,
	}

	args, err := json.Marshal([]any{summary})
	if err != nil {
		return err
	}

	notice := job.Job{JID: jid, Type: hook.JobType, Args: args, Queue: hook.Queue}

	if err := notice.Normalize(now); err != nil {
		return err
	}

	return enqueue(tx, &record{Job: notice}, now)
}
