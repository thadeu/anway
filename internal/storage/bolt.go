package storage

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/thadeu/anway/internal/job"
)

const (
	initialMmapSize = 1 << 30
	maxGroupSize    = 1000
	mutationPage    = 500
	schemaVersion   = 1
	queuePrefix     = "queue/"
)

var (
	bucketJobs      = []byte("jobs")
	bucketQueues    = []byte("queues")
	bucketScheduled = []byte("scheduled")
	bucketRetries   = []byte("retries")
	bucketWorking   = []byte("working")
	bucketDead      = []byte("dead")
	bucketCounters  = []byte("counters")
	counterAcks     = []byte("total/processed")
	counterFailures = []byte("total/failures")
	counterExpired  = []byte("total/expired")
	bucketMeta      = []byte("meta")
	keySchema       = []byte("schema_version")
	bucketPaused    = []byte("paused")
	bucketUniques   = []byte("uniques")
	bucketUniqueTTL = []byte("unique_expiry")
	bucketLeases    = []byte("leases")
	bucketBatches   = []byte("batches")
	bucketBatchTTL  = []byte("batch_expiry")
	bucketTracks    = []byte("tracks")
	bucketTrackTTL  = []byte("track_expiry")
	topBuckets      = [][]byte{bucketJobs, bucketQueues, bucketScheduled, bucketRetries, bucketWorking, bucketDead, bucketCounters, bucketPaused, bucketUniques, bucketUniqueTTL, bucketLeases, bucketBatches, bucketBatchTTL, bucketTracks, bucketTrackTTL}
)

// record is the stored job with its current location.
type record struct {
	Job   job.Job `json:"j"`
	State string  `json:"s"`
	Key   []byte  `json:"k,omitempty"`
}

// writeOp is one change. It must validate before it writes, because an
// expected error does not roll back the group that holds the operation.
type writeOp struct {
	apply func(tx *bolt.Tx) error
	done  chan error
}

type Bolt struct {
	db      *bolt.DB
	writes  chan writeOp
	stopped chan struct{}

	closeMu sync.RWMutex
	closed  bool

	throttles atomic.Pointer[map[string]Throttle]
}

var _ Store = (*Bolt)(nil)

func OpenBolt(dir string) (*Bolt, error) {
	options := &bolt.Options{
		Timeout:         time.Second,
		FreelistType:    bolt.FreelistMapType,
		InitialMmapSize: initialMmapSize,
	}

	db, err := bolt.Open(filepath.Join(dir, "jobs.db"), 0o600, options)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}

	if err := db.Update(prepareSchema); err != nil {
		db.Close()

		return nil, err
	}

	store := &Bolt{db: db, writes: make(chan writeOp, maxGroupSize), stopped: make(chan struct{})}

	go store.commitLoop()

	return store, nil
}

// commitLoop puts all waiting operations in one transaction. Operations that
// arrive during a commit form the next group, so concurrent clients share a sync.
func (b *Bolt) commitLoop() {
	defer close(b.stopped)

	for first := range b.writes {
		group := []writeOp{first}

	collect:
		for len(group) < maxGroupSize {
			select {
			case op, open := <-b.writes:
				if !open {
					break collect
				}

				group = append(group, op)
			default:
				break collect
			}
		}

		b.commit(group)
	}
}

func (b *Bolt) commit(group []writeOp) {
	results := make([]error, len(group))

	err := b.db.Update(func(tx *bolt.Tx) error {
		for i, op := range group {
			results[i] = op.apply(tx)

			if results[i] != nil && !isExpected(results[i]) {
				return results[i]
			}
		}

		return nil
	})

	for i, op := range group {
		if err != nil {
			op.done <- err

			continue
		}

		op.done <- results[i]
	}
}

func isExpected(err error) bool {
	var rejected *RejectedError

	return errors.Is(err, ErrDuplicateJob) || errors.Is(err, ErrNotReserved) || errors.Is(err, ErrNotUnique) || errors.As(err, &rejected)
}

func (b *Bolt) write(apply func(tx *bolt.Tx) error) error {
	done := make(chan error, 1)

	b.closeMu.RLock()

	if b.closed {
		b.closeMu.RUnlock()

		return ErrClosed
	}

	b.writes <- writeOp{apply: apply, done: done}
	b.closeMu.RUnlock()

	return <-done
}

// prepareSchema refuses data from a newer version. Migrations run here.
func prepareSchema(tx *bolt.Tx) error {
	meta, err := tx.CreateBucketIfNotExists(bucketMeta)
	if err != nil {
		return err
	}

	stored := uint64(0)

	if raw := meta.Get(keySchema); raw != nil {
		stored = binary.BigEndian.Uint64(raw)
	}

	if stored > schemaVersion {
		return fmt.Errorf("data schema %d is newer than the supported schema %d", stored, schemaVersion)
	}

	if err := meta.Put(keySchema, binary.BigEndian.AppendUint64(nil, schemaVersion)); err != nil {
		return err
	}

	return createBuckets(tx)
}

func createBuckets(tx *bolt.Tx) error {
	for _, name := range topBuckets {
		if _, err := tx.CreateBucketIfNotExists(name); err != nil {
			return err
		}
	}

	return nil
}

func (b *Bolt) Close() error {
	b.closeMu.Lock()

	if b.closed {
		b.closeMu.Unlock()

		return nil
	}

	b.closed = true
	close(b.writes)
	b.closeMu.Unlock()

	<-b.stopped

	return b.db.Close()
}

// Snapshot writes a consistent copy. The file appears only when it is complete.
func (b *Bolt) Snapshot(path string) error {
	temporary := path + ".tmp"

	file, err := os.OpenFile(temporary, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}

	defer os.Remove(temporary)
	defer file.Close()

	err = b.db.View(func(tx *bolt.Tx) error {
		_, err := tx.WriteTo(file)

		return err
	})
	if err != nil {
		return err
	}

	if err := file.Sync(); err != nil {
		return err
	}

	if err := file.Close(); err != nil {
		return err
	}

	return os.Rename(temporary, path)
}

func (b *Bolt) Push(j *job.Job, now time.Time) error {
	return b.write(func(tx *bolt.Tx) error {
		if tx.Bucket(bucketJobs).Get([]byte(j.JID)) != nil {
			return ErrDuplicateJob
		}

		rec := &record{Job: *j}
		at, scheduled := j.ScheduledAt(now)

		if err := checkBatchOpen(tx, j); err != nil {
			return err
		}

		if err := takeUniqueLock(tx, j, at, scheduled, now); err != nil {
			return err
		}

		if err := batchJobPushed(tx, j); err != nil {
			return err
		}

		if scheduled {
			if err := setTrack(tx, j, trackEnqueued, now, now.Add(trackLifetime)); err != nil {
				return err
			}

			return moveToSet(tx, rec, bucketScheduled, at)
		}

		return enqueue(tx, rec, now)
	})
}

func (b *Bolt) Fetch(queues []string, wid string, now time.Time) (*job.Job, error) {
	ready, err := b.anyReady(queues)
	if err != nil || !ready {
		return nil, err
	}

	var fetched *job.Job

	err = b.write(func(tx *bolt.Tx) error {
		for _, queue := range b.fetchOrder(queues) {
			if tx.Bucket(bucketPaused).Get([]byte(queue)) != nil {
				continue
			}

			throttle, throttled := b.throttle(queue)

			if throttled {
				allowed, err := admit(tx, queue, wid, throttle, now)
				if err != nil {
					return err
				}

				if !allowed {
					continue
				}
			}

			rec, err := takeLive(tx, queue, now)
			if err != nil {
				return err
			}

			if rec == nil {
				continue
			}

			if throttled {
				if err := putLease(tx, rec, wid, now.Add(throttle.Timeout)); err != nil {
					return err
				}
			}

			if rec.Job.UniqueUntilStart() {
				if err := releaseUniqueLock(tx, &rec.Job); err != nil {
					return err
				}
			}

			expiry := now.Add(time.Duration(rec.Job.ReserveFor) * time.Second)

			if err := moveToSet(tx, rec, bucketWorking, expiry); err != nil {
				return err
			}

			if err := setTrack(tx, &rec.Job, trackWorking, now, expiry.Add(trackLifetime)); err != nil {
				return err
			}

			current := rec.Job
			fetched = &current

			return nil
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return fetched, nil
}

// anyReady avoids a write transaction when all queues are empty.
func (b *Bolt) anyReady(queues []string) (bool, error) {
	ready := false

	err := b.db.View(func(tx *bolt.Tx) error {
		for _, queue := range queues {
			if counter(tx, []byte(queuePrefix+queue)) > 0 {
				ready = true

				return nil
			}
		}

		return nil
	})

	return ready, err
}

func (b *Bolt) Ack(jid string) error {
	return b.write(func(tx *bolt.Tx) error {
		rec, err := reserved(tx, jid)
		if err != nil {
			return err
		}

		if err := removeFromSet(tx, rec); err != nil {
			return err
		}

		if err := addCounter(tx, counterAcks, 1); err != nil {
			return err
		}

		if err := releaseLease(tx, rec); err != nil {
			return err
		}

		if err := releaseUniqueLock(tx, &rec.Job); err != nil {
			return err
		}

		now := time.Now()

		if err := setTrack(tx, &rec.Job, trackSuccess, now, now.Add(trackLifetime)); err != nil {
			return err
		}

		if err := batchJobAcked(tx, &rec.Job, now); err != nil {
			return err
		}

		return tx.Bucket(bucketJobs).Delete([]byte(jid))
	})
}

func (b *Bolt) Fail(jid string, now time.Time, decide Decide) error {
	return b.write(func(tx *bolt.Tx) error {
		rec, err := reserved(tx, jid)
		if err != nil {
			return err
		}

		return settle(tx, rec, now, decide)
	})
}

func (b *Bolt) MoveDue(now time.Time, limit int) (int, error) {
	moved := 0

	err := b.db.Update(func(tx *bolt.Tx) error {
		moved = 0

		for _, name := range [][]byte{bucketScheduled, bucketRetries} {
			keys := dueKeys(tx.Bucket(name), now, limit-moved)

			for _, key := range keys {
				rec, err := load(tx, jidFromKey(key))
				if err != nil {
					return err
				}

				if err := removeFromSet(tx, rec); err != nil {
					return err
				}

				if err := enqueue(tx, rec, now); err != nil {
					return err
				}

				moved++
			}
		}

		return nil
	})

	return moved, err
}

func (b *Bolt) ExpireReservations(now time.Time, limit int, decide Decide) (int, error) {
	expired := 0

	err := b.db.Update(func(tx *bolt.Tx) error {
		expired = 0

		for _, key := range dueKeys(tx.Bucket(bucketWorking), now, limit) {
			rec, err := load(tx, jidFromKey(key))
			if err != nil {
				return err
			}

			if err := settle(tx, rec, now, decide); err != nil {
				return err
			}

			expired++
		}

		return nil
	})

	return expired, err
}

func (b *Bolt) Stats() (Stats, error) {
	stats := Stats{Queues: map[string]uint64{}}

	err := b.db.View(func(tx *bolt.Tx) error {
		stats.Scheduled = counter(tx, bucketScheduled)
		stats.Retries = counter(tx, bucketRetries)
		stats.Working = counter(tx, bucketWorking)
		stats.Dead = counter(tx, bucketDead)
		stats.Processed = counter(tx, counterAcks)
		stats.Failures = counter(tx, counterFailures)
		stats.Expired = counter(tx, counterExpired)

		tx.Bucket(bucketPaused).ForEach(func(name, _ []byte) error {
			stats.Paused = append(stats.Paused, string(name))

			return nil
		})

		cursor := tx.Bucket(bucketCounters).Cursor()
		prefix := []byte(queuePrefix)

		for key, value := cursor.Seek(prefix); bytes.HasPrefix(key, prefix); key, value = cursor.Next() {
			stats.Queues[strings.TrimPrefix(string(key), queuePrefix)] = binary.BigEndian.Uint64(value)
		}

		return nil
	})

	return stats, err
}

func (b *Bolt) Flush() error {
	return b.db.Update(func(tx *bolt.Tx) error {
		for _, name := range topBuckets {
			if err := tx.DeleteBucket(name); err != nil {
				return err
			}
		}

		return createBuckets(tx)
	})
}

func (b *Bolt) SetThrottles(throttles map[string]Throttle) {
	b.throttles.Store(&throttles)
}

func (b *Bolt) throttle(queue string) (Throttle, bool) {
	current := b.throttles.Load()
	if current == nil {
		return Throttle{}, false
	}

	throttle, ok := (*current)[queue]

	return throttle, ok
}

// fetchOrder puts throttled queues first and keeps the order in each group.
func (b *Bolt) fetchOrder(queues []string) []string {
	current := b.throttles.Load()
	if current == nil || len(*current) == 0 {
		return queues
	}

	ordered := slices.Clone(queues)

	slices.SortStableFunc(ordered, func(left, right string) int {
		_, leftThrottled := (*current)[left]
		_, rightThrottled := (*current)[right]

		switch {
		case leftThrottled == rightThrottled:
			return 0
		case leftThrottled:
			return -1
		default:
			return 1
		}
	})

	return ordered
}

func (b *Bolt) PauseQueues(names []string) error {
	return b.write(func(tx *bolt.Tx) error {
		for _, name := range b.expandQueues(tx, names) {
			if err := tx.Bucket(bucketPaused).Put([]byte(name), []byte{1}); err != nil {
				return err
			}
		}

		return nil
	})
}

func (b *Bolt) ResumeQueues(names []string) error {
	return b.write(func(tx *bolt.Tx) error {
		for _, name := range b.expandQueues(tx, names) {
			if err := tx.Bucket(bucketPaused).Delete([]byte(name)); err != nil {
				return err
			}
		}

		return nil
	})
}

// RemoveQueues deletes the queues and all their waiting jobs.
func (b *Bolt) RemoveQueues(names []string) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		for _, name := range b.expandQueues(tx, names) {
			queue := tx.Bucket(bucketQueues).Bucket([]byte(name))
			if queue == nil {
				continue
			}

			err := queue.ForEach(func(_, jid []byte) error {
				return tx.Bucket(bucketJobs).Delete(jid)
			})
			if err != nil {
				return err
			}

			if err := tx.Bucket(bucketQueues).DeleteBucket([]byte(name)); err != nil {
				return err
			}

			if err := tx.Bucket(bucketCounters).Delete([]byte(queuePrefix + name)); err != nil {
				return err
			}

			if err := tx.Bucket(bucketPaused).Delete([]byte(name)); err != nil {
				return err
			}
		}

		return nil
	})
}

// expandQueues replaces "*" with all known queue names.
func (b *Bolt) expandQueues(tx *bolt.Tx, names []string) []string {
	if !slices.Contains(names, "*") {
		return names
	}

	var all []string

	tx.Bucket(bucketQueues).ForEachBucket(func(name []byte) error {
		all = append(all, string(name))

		return nil
	})

	return all
}

// Mutate works in short transactions, so that a large set does not block reads.
func (b *Bolt) Mutate(mutation Mutation, now time.Time) (int, error) {
	set := []byte(mutation.Target)
	total := 0

	var after []byte

	for {
		var scanned int

		err := b.db.Update(func(tx *bolt.Tx) error {
			cursor := tx.Bucket(set).Cursor()
			keys := pageKeys(cursor, after)
			scanned = len(keys)

			for _, key := range keys {
				after = key

				rec, err := load(tx, jidFromKey(key))
				if err != nil {
					return err
				}

				encoded, err := json.Marshal(rec.Job)
				if err != nil {
					return err
				}

				if !mutation.Match(&rec.Job, encoded) {
					continue
				}

				if err := applyMutation(tx, rec, mutation.Command, now); err != nil {
					return err
				}

				total++
			}

			return nil
		})
		if err != nil {
			return total, err
		}

		if scanned < mutationPage {
			return total, nil
		}
	}
}

func pageKeys(cursor *bolt.Cursor, after []byte) [][]byte {
	var key []byte

	if after == nil {
		key, _ = cursor.First()
	} else {
		key, _ = cursor.Seek(after)

		if bytes.Equal(key, after) {
			key, _ = cursor.Next()
		}
	}

	var keys [][]byte

	for ; key != nil && len(keys) < mutationPage; key, _ = cursor.Next() {
		keys = append(keys, bytes.Clone(key))
	}

	return keys
}

func applyMutation(tx *bolt.Tx, rec *record, command string, now time.Time) error {
	if command == "kill" && rec.State == string(bucketDead) {
		return nil
	}

	if err := removeFromSet(tx, rec); err != nil {
		return err
	}

	switch command {
	case "kill":
		if err := setTrack(tx, &rec.Job, trackDead, now, now.Add(trackLifetime)); err != nil {
			return err
		}

		return moveToSet(tx, rec, bucketDead, now)
	case "requeue":
		return enqueue(tx, rec, now)
	default:
		return tx.Bucket(bucketJobs).Delete([]byte(rec.Job.JID))
	}
}

func takeUniqueLock(tx *bolt.Tx, j *job.Job, at time.Time, scheduled bool, now time.Time) error {
	window := j.UniqueFor()
	if window == 0 {
		return nil
	}

	key := j.UniqueKey()
	locks := tx.Bucket(bucketUniques)

	if raw := locks.Get(key); raw != nil && int64(binary.BigEndian.Uint64(raw)) > now.UnixNano() {
		return ErrNotUnique
	}

	start := now

	if scheduled {
		start = at
	}

	expiry := start.Add(window)
	value := binary.BigEndian.AppendUint64(nil, uint64(expiry.UnixNano()))

	if err := locks.Put(key, append(value, j.JID...)); err != nil {
		return err
	}

	return tx.Bucket(bucketUniqueTTL).Put(append(bytes.Clone(value[:8]), key...), nil)
}

// releaseUniqueLock keeps a lock that a newer job holds.
func releaseUniqueLock(tx *bolt.Tx, j *job.Job) error {
	if j.UniqueFor() == 0 {
		return nil
	}

	key := j.UniqueKey()
	locks := tx.Bucket(bucketUniques)
	raw := locks.Get(key)

	if raw == nil || string(raw[8:]) != j.JID {
		return nil
	}

	if err := tx.Bucket(bucketUniqueTTL).Delete(append(bytes.Clone(raw[:8]), key...)); err != nil {
		return err
	}

	return locks.Delete(key)
}

// SweepLocks deletes unique locks after their time ends.
func (b *Bolt) SweepLocks(now time.Time, limit int) (int, error) {
	swept := 0

	err := b.db.Update(func(tx *bolt.Tx) error {
		swept = 0
		index := tx.Bucket(bucketUniqueTTL)

		for _, key := range dueKeys(index, now, limit) {
			hash := key[8:]
			raw := tx.Bucket(bucketUniques).Get(hash)

			if raw != nil && bytes.Equal(raw[:8], key[:8]) {
				if err := tx.Bucket(bucketUniques).Delete(hash); err != nil {
					return err
				}
			}

			if err := index.Delete(key); err != nil {
				return err
			}

			swept++
		}

		return nil
	})

	return swept, err
}

func leaseKey(queue, jid string) []byte {
	return []byte(queue + "\x00" + jid)
}

// admit counts live leases of a queue and deletes expired ones.
func admit(tx *bolt.Tx, queue, wid string, throttle Throttle, now time.Time) (bool, error) {
	leases := tx.Bucket(bucketLeases)
	prefix := []byte(queue + "\x00")
	cursor := leases.Cursor()
	total, own := 0, 0

	var expired [][]byte

	for key, value := cursor.Seek(prefix); bytes.HasPrefix(key, prefix); key, value = cursor.Next() {
		if int64(binary.BigEndian.Uint64(value)) <= now.UnixNano() {
			expired = append(expired, bytes.Clone(key))

			continue
		}

		total++

		if string(value[8:]) == wid {
			own++
		}
	}

	for _, key := range expired {
		if err := leases.Delete(key); err != nil {
			return false, err
		}
	}

	if throttle.Concurrency > 0 && total >= throttle.Concurrency {
		return false, nil
	}

	return throttle.Worker == 0 || own < throttle.Worker, nil
}

func putLease(tx *bolt.Tx, rec *record, wid string, expiry time.Time) error {
	value := binary.BigEndian.AppendUint64(nil, uint64(expiry.UnixNano()))

	return tx.Bucket(bucketLeases).Put(leaseKey(rec.Job.Queue, rec.Job.JID), append(value, wid...))
}

func releaseLease(tx *bolt.Tx, rec *record) error {
	return tx.Bucket(bucketLeases).Delete(leaseKey(rec.Job.Queue, rec.Job.JID))
}

// takeLive returns the first job that did not expire, and discards expired jobs.
func takeLive(tx *bolt.Tx, queue string, now time.Time) (*record, error) {
	for {
		rec, err := takeFirst(tx, queue)
		if err != nil || rec == nil {
			return nil, err
		}

		if !rec.Job.Expired(now) {
			return rec, nil
		}

		if err := releaseUniqueLock(tx, &rec.Job); err != nil {
			return nil, err
		}

		if err := addCounter(tx, counterExpired, 1); err != nil {
			return nil, err
		}

		if err := tx.Bucket(bucketJobs).Delete([]byte(rec.Job.JID)); err != nil {
			return nil, err
		}
	}
}

func enqueue(tx *bolt.Tx, rec *record, now time.Time) error {
	queues := tx.Bucket(bucketQueues)

	queue, err := queues.CreateBucketIfNotExists([]byte(rec.Job.Queue))
	if err != nil {
		return err
	}

	seq, err := queue.NextSequence()
	if err != nil {
		return err
	}

	key := binary.BigEndian.AppendUint64(nil, seq)

	if err := queue.Put(key, []byte(rec.Job.JID)); err != nil {
		return err
	}

	rec.State = string(bucketQueues)
	rec.Key = key
	rec.Job.At = ""
	rec.Job.EnqueuedAt = now.UTC().Format(time.RFC3339Nano)

	if err := setTrack(tx, &rec.Job, trackEnqueued, now, now.Add(trackLifetime)); err != nil {
		return err
	}

	if err := addCounter(tx, []byte(queuePrefix+rec.Job.Queue), 1); err != nil {
		return err
	}

	return save(tx, rec)
}

// takeFirst removes the oldest job of a queue. It returns nil for an empty queue.
func takeFirst(tx *bolt.Tx, name string) (*record, error) {
	queue := tx.Bucket(bucketQueues).Bucket([]byte(name))
	if queue == nil {
		return nil, nil
	}

	key, jid := queue.Cursor().First()
	if key == nil {
		return nil, nil
	}

	rec, err := load(tx, string(jid))
	if err != nil {
		return nil, err
	}

	key = bytes.Clone(key)

	if err := queue.Delete(key); err != nil {
		return nil, err
	}

	if err := addCounter(tx, []byte(queuePrefix+name), -1); err != nil {
		return nil, err
	}

	return rec, nil
}

func moveToSet(tx *bolt.Tx, rec *record, set []byte, at time.Time) error {
	key := timeKey(at, rec.Job.JID)

	if err := tx.Bucket(set).Put(key, nil); err != nil {
		return err
	}

	if err := addCounter(tx, set, 1); err != nil {
		return err
	}

	rec.State = string(set)
	rec.Key = key

	return save(tx, rec)
}

func removeFromSet(tx *bolt.Tx, rec *record) error {
	set := []byte(rec.State)

	if err := tx.Bucket(set).Delete(rec.Key); err != nil {
		return err
	}

	return addCounter(tx, set, -1)
}

func settle(tx *bolt.Tx, rec *record, now time.Time, decide Decide) error {
	if err := removeFromSet(tx, rec); err != nil {
		return err
	}

	if err := addCounter(tx, counterFailures, 1); err != nil {
		return err
	}

	if err := releaseLease(tx, rec); err != nil {
		return err
	}

	firstFailure := rec.Job.Failure == nil
	disposition := decide(&rec.Job)
	state := trackFailed

	if disposition.Outcome == Dead {
		state = trackDead
	}

	if err := setTrack(tx, &rec.Job, state, now, now.Add(trackLifetime)); err != nil {
		return err
	}

	if err := batchJobFailed(tx, &rec.Job, firstFailure, now); err != nil {
		return err
	}

	switch disposition.Outcome {
	case Retry:
		return moveToSet(tx, rec, bucketRetries, disposition.At)
	case Dead:
		return moveToSet(tx, rec, bucketDead, now)
	default:
		return tx.Bucket(bucketJobs).Delete([]byte(rec.Job.JID))
	}
}

func reserved(tx *bolt.Tx, jid string) (*record, error) {
	rec, err := load(tx, jid)
	if errors.Is(err, errJobNotFound) {
		return nil, ErrNotReserved
	}

	if err != nil {
		return nil, err
	}

	if rec.State != string(bucketWorking) {
		return nil, ErrNotReserved
	}

	return rec, nil
}

var errJobNotFound = errors.New("job not found")

func load(tx *bolt.Tx, jid string) (*record, error) {
	raw := tx.Bucket(bucketJobs).Get([]byte(jid))
	if raw == nil {
		return nil, errJobNotFound
	}

	rec := &record{}

	if err := json.Unmarshal(raw, rec); err != nil {
		return nil, fmt.Errorf("decode job %s: %w", jid, err)
	}

	return rec, nil
}

func save(tx *bolt.Tx, rec *record) error {
	raw, err := json.Marshal(rec)
	if err != nil {
		return err
	}

	return tx.Bucket(bucketJobs).Put([]byte(rec.Job.JID), raw)
}

func counter(tx *bolt.Tx, name []byte) uint64 {
	raw := tx.Bucket(bucketCounters).Get(name)
	if raw == nil {
		return 0
	}

	return binary.BigEndian.Uint64(raw)
}

func addCounter(tx *bolt.Tx, name []byte, delta int64) error {
	value := uint64(int64(counter(tx, name)) + delta)

	return tx.Bucket(bucketCounters).Put(name, binary.BigEndian.AppendUint64(nil, value))
}

func timeKey(at time.Time, jid string) []byte {
	key := binary.BigEndian.AppendUint64(nil, uint64(at.UnixNano()))

	return append(key, jid...)
}

func jidFromKey(key []byte) string {
	return string(key[8:])
}

// dueKeys copies the keys, because a change during iteration can skip entries.
func dueKeys(bucket *bolt.Bucket, now time.Time, limit int) [][]byte {
	limitKey := binary.BigEndian.AppendUint64(nil, uint64(now.UnixNano()))
	cursor := bucket.Cursor()

	var keys [][]byte

	for key, _ := cursor.First(); key != nil && len(keys) < limit; key, _ = cursor.Next() {
		if bytes.Compare(key[:8], limitKey) > 0 {
			break
		}

		keys = append(keys, bytes.Clone(key))
	}

	return keys
}
