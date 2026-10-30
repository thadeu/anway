package storage

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/thadeu/anway/internal/job"
)

const (
	batchOpenLifetime     = 30 * time.Minute
	batchFinishedLifetime = 24 * time.Hour
	trackLifetime         = 30 * time.Minute

	callbackComplete = "complete"
	callbackSuccess  = "success"

	callbackQueued = "1"
	callbackDone   = "2"

	trackEnqueued = "enqueued"
	trackWorking  = "working"
	trackSuccess  = "success"
	trackFailed   = "failed"
	trackDead     = "dead"
	trackUnknown  = "unknown"
)

type batchRecord struct {
	Status               BatchStatus `json:"status"`
	Success              *job.Job    `json:"success,omitempty"`
	Complete             *job.Job    `json:"complete,omitempty"`
	Committed            bool        `json:"committed"`
	ChildrenIncomplete   int64       `json:"children_incomplete"`
	ChildrenUnsuccessful int64       `json:"children_unsuccessful"`
	ExpiresAt            int64       `json:"expires_at"`
}

type trackRecord struct {
	Track     Track `json:"track"`
	ExpiresAt int64 `json:"expires_at"`
}

func randomID(prefix string) (string, error) {
	raw := make([]byte, 12)

	if _, err := rand.Read(raw); err != nil {
		return "", err
	}

	return prefix + hex.EncodeToString(raw), nil
}

func (b *Bolt) BatchNew(definition BatchDefinition, now time.Time) (string, error) {
	if definition.Success == nil && definition.Complete == nil {
		return "", &RejectedError{Message: "a batch needs a success or complete callback"}
	}

	for _, callback := range []*job.Job{definition.Success, definition.Complete} {
		if err := validateCallback(callback, now); err != nil {
			return "", err
		}
	}

	bid, err := randomID("b-")
	if err != nil {
		return "", err
	}

	err = b.write(func(tx *bolt.Tx) error {
		if definition.ParentBID != "" {
			parent, err := loadBatch(tx, definition.ParentBID)
			if err != nil {
				return err
			}

			if parent.Committed {
				return &RejectedError{Message: "the parent batch is committed"}
			}

			parent.ChildrenIncomplete++
			parent.ChildrenUnsuccessful++

			if err := saveBatch(tx, parent); err != nil {
				return err
			}
		}

		batch := &batchRecord{
			Status: BatchStatus{
				BID:         bid,
				ParentBID:   definition.ParentBID,
				Description: definition.Description,
				CreatedAt:   now.UTC().Format(time.RFC3339),
			},
			Success:  definition.Success,
			Complete: definition.Complete,
		}

		return setBatchExpiry(tx, batch, now.Add(batchOpenLifetime))
	})
	if err != nil {
		return "", err
	}

	return bid, nil
}

func validateCallback(callback *job.Job, now time.Time) error {
	if callback == nil {
		return nil
	}

	if callback.JID == "" {
		callback.JID = "callback"
	}

	if len(callback.Args) == 0 || string(callback.Args) == "null" {
		callback.Args = json.RawMessage("[]")
	}

	if err := callback.Normalize(now); err != nil {
		return &RejectedError{Message: "invalid callback: " + err.Error()}
	}

	return nil
}

func (b *Bolt) BatchOpen(bid string) error {
	return b.write(func(tx *bolt.Tx) error {
		batch, err := loadBatch(tx, bid)
		if err != nil {
			return err
		}

		if batch.Status.SuccessState != "" {
			return &RejectedError{Message: "the batch already finished"}
		}

		batch.Committed = false
		batch.ExpiresAt = 0

		return saveBatch(tx, batch)
	})
}

func (b *Bolt) BatchCommit(bid string, now time.Time) error {
	return b.write(func(tx *bolt.Tx) error {
		batch, err := loadBatch(tx, bid)
		if err != nil {
			return err
		}

		if batch.Committed {
			return nil
		}

		batch.Committed = true
		batch.ExpiresAt = 0

		return advanceBatch(tx, batch, now)
	})
}

func (b *Bolt) BatchStatus(bid string) (BatchStatus, error) {
	var status BatchStatus

	err := b.db.View(func(tx *bolt.Tx) error {
		batch, err := loadBatch(tx, bid)
		if err != nil {
			return err
		}

		status = batch.Status

		return nil
	})

	return status, err
}

func (b *Bolt) TrackGet(jid string, now time.Time) (Track, error) {
	track := Track{JID: jid, State: trackUnknown}

	err := b.db.View(func(tx *bolt.Tx) error {
		record, err := loadTrack(tx, jid)
		if err != nil {
			return err
		}

		if record != nil && record.ExpiresAt > now.UnixNano() {
			track = record.Track
		}

		return nil
	})

	return track, err
}

// TrackSet updates progress and can extend the reservation of a running job.
func (b *Bolt) TrackSet(update TrackUpdate, now time.Time) error {
	var until time.Time

	if update.ReserveUntil != "" {
		parsed, err := time.Parse(time.RFC3339Nano, update.ReserveUntil)
		if err != nil {
			return &RejectedError{Message: "invalid reserve_until"}
		}

		until = parsed
	}

	return b.write(func(tx *bolt.Tx) error {
		record, err := loadTrack(tx, update.JID)
		if err != nil {
			return err
		}

		if record == nil {
			return &RejectedError{Message: "job is not tracked"}
		}

		if !until.IsZero() {
			if err := extendReservation(tx, update.JID, until); err != nil {
				return err
			}

			record.ExpiresAt = max(record.ExpiresAt, until.Add(trackLifetime).UnixNano())
		}

		if update.Percent != nil {
			record.Track.Percent = update.Percent
		}

		if update.Desc != "" {
			record.Track.Desc = update.Desc
		}

		record.Track.UpdatedAt = now.UTC().Format(time.RFC3339)

		return saveTrack(tx, record)
	})
}

func extendReservation(tx *bolt.Tx, jid string, until time.Time) error {
	rec, err := reserved(tx, jid)
	if err != nil {
		return err
	}

	key := timeKey(until, jid)

	if string(key) <= string(rec.Key) {
		return &RejectedError{Message: "reserve_until must be later than the reservation"}
	}

	working := tx.Bucket(bucketWorking)

	if err := working.Delete(rec.Key); err != nil {
		return err
	}

	if err := working.Put(key, nil); err != nil {
		return err
	}

	rec.Key = key

	return save(tx, rec)
}

// SweepExpired deletes abandoned batches, finished batches, and old tracks.
func (b *Bolt) SweepExpired(now time.Time, limit int) (int, error) {
	swept := 0

	err := b.db.Update(func(tx *bolt.Tx) error {
		swept = 0

		for _, key := range dueKeys(tx.Bucket(bucketBatchTTL), now, limit) {
			if err := sweepBatch(tx, key, now); err != nil {
				return err
			}

			swept++
		}

		for _, key := range dueKeys(tx.Bucket(bucketTrackTTL), now, limit-swept) {
			if err := sweepTrack(tx, key, now); err != nil {
				return err
			}

			swept++
		}

		return nil
	})

	return swept, err
}

func sweepBatch(tx *bolt.Tx, key []byte, now time.Time) error {
	bid := jidFromKey(key)
	raw := tx.Bucket(bucketBatches).Get([]byte(bid))

	if raw != nil {
		batch := &batchRecord{}

		if err := json.Unmarshal(raw, batch); err != nil {
			return err
		}

		if batch.ExpiresAt != 0 && batch.ExpiresAt <= now.UnixNano() {
			if err := tx.Bucket(bucketBatches).Delete([]byte(bid)); err != nil {
				return err
			}
		}
	}

	return tx.Bucket(bucketBatchTTL).Delete(key)
}

func sweepTrack(tx *bolt.Tx, key []byte, now time.Time) error {
	jid := jidFromKey(key)

	record, err := loadTrack(tx, jid)
	if err != nil {
		return err
	}

	if record != nil && record.ExpiresAt <= now.UnixNano() {
		if err := tx.Bucket(bucketTracks).Delete([]byte(jid)); err != nil {
			return err
		}
	}

	return tx.Bucket(bucketTrackTTL).Delete(key)
}

func loadBatch(tx *bolt.Tx, bid string) (*batchRecord, error) {
	raw := tx.Bucket(bucketBatches).Get([]byte(bid))
	if raw == nil {
		return nil, &RejectedError{Message: "no such batch: " + bid}
	}

	batch := &batchRecord{}

	if err := json.Unmarshal(raw, batch); err != nil {
		return nil, fmt.Errorf("decode batch %s: %w", bid, err)
	}

	return batch, nil
}

func saveBatch(tx *bolt.Tx, batch *batchRecord) error {
	raw, err := json.Marshal(batch)
	if err != nil {
		return err
	}

	return tx.Bucket(bucketBatches).Put([]byte(batch.Status.BID), raw)
}

func setBatchExpiry(tx *bolt.Tx, batch *batchRecord, at time.Time) error {
	batch.ExpiresAt = at.UnixNano()

	if err := tx.Bucket(bucketBatchTTL).Put(timeKey(at, batch.Status.BID), nil); err != nil {
		return err
	}

	return saveBatch(tx, batch)
}

func checkBatchOpen(tx *bolt.Tx, j *job.Job) error {
	bid := j.BatchID()
	if bid == "" {
		return nil
	}

	batch, err := loadBatch(tx, bid)
	if err != nil {
		return err
	}

	if batch.Committed {
		return &RejectedError{Message: "the batch is committed"}
	}

	return nil
}

func batchJobPushed(tx *bolt.Tx, j *job.Job) error {
	bid := j.BatchID()
	if bid == "" {
		return nil
	}

	batch, err := loadBatch(tx, bid)
	if err != nil {
		return err
	}

	batch.Status.Total++
	batch.Status.Pending++

	return saveBatch(tx, batch)
}

func batchJobAcked(tx *bolt.Tx, j *job.Job, now time.Time) error {
	if bid, kind := j.BatchCallback(); bid != "" {
		return callbackAcked(tx, bid, kind, now)
	}

	batch := currentBatch(tx, j)
	if batch == nil {
		return nil
	}

	batch.Status.Pending--

	if j.Failure != nil {
		batch.Status.Failed--
	}

	return advanceBatch(tx, batch, now)
}

// batchJobFailed counts a job one time, however many retries it uses.
func batchJobFailed(tx *bolt.Tx, j *job.Job, firstFailure bool, now time.Time) error {
	if !firstFailure {
		return nil
	}

	batch := currentBatch(tx, j)
	if batch == nil {
		return nil
	}

	batch.Status.Failed++

	return advanceBatch(tx, batch, now)
}

// currentBatch returns nil when the batch data expired.
func currentBatch(tx *bolt.Tx, j *job.Job) *batchRecord {
	bid := j.BatchID()
	if bid == "" {
		return nil
	}

	batch, err := loadBatch(tx, bid)
	if err != nil {
		return nil
	}

	return batch
}

func callbackAcked(tx *bolt.Tx, bid, kind string, now time.Time) error {
	batch, err := loadBatch(tx, bid)
	if err != nil {
		return nil
	}

	if kind == callbackComplete {
		batch.Status.CompleteState = callbackDone
	} else {
		batch.Status.SuccessState = callbackDone
	}

	if err := saveBatch(tx, batch); err != nil {
		return err
	}

	if err := notifyParent(tx, batch, kind, now); err != nil {
		return err
	}

	return advanceBatch(tx, batch, now)
}

// advanceBatch fires callbacks that are ready, and always saves the batch.
func advanceBatch(tx *bolt.Tx, batch *batchRecord, now time.Time) error {
	status := &batch.Status

	if batch.Committed && status.CompleteState == "" && status.Pending == status.Failed && batch.ChildrenIncomplete == 0 {
		if err := fireCallback(tx, batch, callbackComplete, batch.Complete, now); err != nil {
			return err
		}
	}

	if batch.Committed && status.CompleteState != "" && status.SuccessState == "" && status.Pending == 0 && batch.ChildrenUnsuccessful == 0 {
		if err := fireCallback(tx, batch, callbackSuccess, batch.Success, now); err != nil {
			return err
		}
	}

	if status.SuccessState == callbackDone && batch.ExpiresAt == 0 {
		return setBatchExpiry(tx, batch, now.Add(batchFinishedLifetime))
	}

	return saveBatch(tx, batch)
}

func fireCallback(tx *bolt.Tx, batch *batchRecord, kind string, callback *job.Job, now time.Time) error {
	state := &batch.Status.CompleteState

	if kind == callbackSuccess {
		state = &batch.Status.SuccessState
	}

	if callback == nil {
		*state = callbackDone

		return notifyParent(tx, batch, kind, now)
	}

	*state = callbackQueued

	jid, err := randomID("cb-")
	if err != nil {
		return err
	}

	copied := *callback
	copied.JID = jid

	if err := copied.SetCustom(map[string]string{"_bid": batch.Status.BID, "_cb": kind}); err != nil {
		return err
	}

	return enqueue(tx, &record{Job: copied}, now)
}

func notifyParent(tx *bolt.Tx, batch *batchRecord, kind string, now time.Time) error {
	if batch.Status.ParentBID == "" {
		return nil
	}

	parent, err := loadBatch(tx, batch.Status.ParentBID)
	if err != nil {
		return nil
	}

	if kind == callbackComplete {
		parent.ChildrenIncomplete--
	} else {
		parent.ChildrenUnsuccessful--
	}

	return advanceBatch(tx, parent, now)
}

func loadTrack(tx *bolt.Tx, jid string) (*trackRecord, error) {
	raw := tx.Bucket(bucketTracks).Get([]byte(jid))
	if raw == nil {
		return nil, nil
	}

	record := &trackRecord{}

	if err := json.Unmarshal(raw, record); err != nil {
		return nil, fmt.Errorf("decode track %s: %w", jid, err)
	}

	return record, nil
}

func saveTrack(tx *bolt.Tx, record *trackRecord) error {
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}

	at := time.Unix(0, record.ExpiresAt)

	if err := tx.Bucket(bucketTrackTTL).Put(timeKey(at, record.Track.JID), nil); err != nil {
		return err
	}

	return tx.Bucket(bucketTracks).Put([]byte(record.Track.JID), raw)
}

// setTrack changes the state of a tracked job, and keeps percent and desc.
func setTrack(tx *bolt.Tx, j *job.Job, state string, now, expiry time.Time) error {
	if !j.Tracked() {
		return nil
	}

	record, err := loadTrack(tx, j.JID)
	if err != nil {
		return err
	}

	if record == nil {
		record = &trackRecord{Track: Track{JID: j.JID}}
	}

	record.Track.State = state
	record.Track.UpdatedAt = now.UTC().Format(time.RFC3339)
	record.ExpiresAt = expiry.UnixNano()

	return saveTrack(tx, record)
}
