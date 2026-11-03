package storage

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/thadeu/anway/internal/job"
)

var bucketSubscriptions = []byte("subscriptions")

// MaxSubscriptions is the most subscriptions that one topic can have.
const MaxSubscriptions = 256

// UnroutedQueue is the queue of an event that no subscription received. It
// waits in the dead set, and a requeue publishes it again.
const UnroutedQueue = "topic"

// Subscription sends a copy of each event of Topic to Queue, as a job of
// JobType. Retry and ReserveFor are the options of that job.
type Subscription struct {
	Topic      string `json:"topic"`
	Queue      string `json:"queue"`
	JobType    string `json:"jobtype"`
	Retry      *int   `json:"retry,omitempty"`
	ReserveFor int    `json:"reserve_for,omitempty"`
	CreatedAt  string `json:"created_at"`
}

// Event is one PUBLISH. ID is the event id. With Dedup, a second event with
// the same topic and id before Dedup ends does nothing.
type Event struct {
	Topic  string
	ID     string
	Args   json.RawMessage
	Custom map[string]any
	Dedup  time.Duration
}

// Published is the result of a PUBLISH: the event id, the jobs that it made,
// and true when the event was a duplicate.
type Published struct {
	Event     string `json:"event"`
	Jobs      int    `json:"jobs"`
	Duplicate bool   `json:"duplicate"`
}

func subscriptionKey(topic, queue, jobtype string) []byte {
	return []byte(topic + "\x00" + queue + "\x00" + jobtype)
}

func eventLockKey(topic, id string) []byte {
	return []byte("topic\x00" + topic + "\x00" + id)
}

// Subscribe saves a subscription. The same topic, queue, and job type keep
// one subscription, with the new options.
func (b *Bolt) Subscribe(subscription Subscription, now time.Time) error {
	return b.write(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketSubscriptions)
		key := subscriptionKey(subscription.Topic, subscription.Queue, subscription.JobType)
		existing := bucket.Get(key)

		if existing == nil && len(topicSubscriptions(tx, subscription.Topic)) >= MaxSubscriptions {
			return fmt.Errorf("topic %q has %d subscriptions, the most it can have", subscription.Topic, MaxSubscriptions)
		}

		subscription.CreatedAt = now.UTC().Format(time.RFC3339Nano)

		if existing != nil {
			var saved Subscription

			if err := json.Unmarshal(existing, &saved); err == nil {
				subscription.CreatedAt = saved.CreatedAt
			}
		}

		raw, err := json.Marshal(subscription)
		if err != nil {
			return err
		}

		return bucket.Put(key, raw)
	})
}

// Unsubscribe removes a subscription. A missing one is not an error.
func (b *Bolt) Unsubscribe(topic, queue, jobtype string) error {
	return b.write(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketSubscriptions).Delete(subscriptionKey(topic, queue, jobtype))
	})
}

// Subscriptions returns the subscriptions of a topic, or of all topics for a
// blank topic, in key order.
func (b *Bolt) Subscriptions(topic string) ([]Subscription, error) {
	var subscriptions []Subscription

	err := b.db.View(func(tx *bolt.Tx) error {
		subscriptions = topicSubscriptions(tx, topic)

		return nil
	})

	return subscriptions, err
}

func topicSubscriptions(tx *bolt.Tx, topic string) []Subscription {
	var (
		prefix        []byte
		subscriptions []Subscription
	)

	if topic != "" {
		prefix = []byte(topic + "\x00")
	}

	cursor := tx.Bucket(bucketSubscriptions).Cursor()

	for key, raw := cursor.Seek(prefix); key != nil && bytes.HasPrefix(key, prefix); key, raw = cursor.Next() {
		var subscription Subscription

		if json.Unmarshal(raw, &subscription) == nil {
			subscriptions = append(subscriptions, subscription)
		}
	}

	return subscriptions
}

// Publish makes one job for each subscription of the event topic, in one
// write. An event with no subscription goes to the dead set. Every check runs
// before the first write, because a failed operation does not roll back the
// writes of the group that holds it.
func (b *Bolt) Publish(event Event, now time.Time) (Published, error) {
	result := Published{Event: event.ID}

	err := b.write(func(tx *bolt.Tx) error {
		result = Published{Event: event.ID}

		lock := eventLockKey(event.Topic, event.ID)

		if event.Dedup > 0 {
			if raw := tx.Bucket(bucketUniques).Get(lock); raw != nil && int64(binary.BigEndian.Uint64(raw)) > now.UnixNano() {
				result.Duplicate = true

				return nil
			}
		}

		records, err := fanOut(tx, event, now)
		if err != nil {
			return err
		}

		if event.Dedup > 0 {
			if err := lockEvent(tx, lock, now.Add(event.Dedup)); err != nil {
				return err
			}
		}

		result.Jobs, err = storeFanOut(tx, records, event, now)

		return err
	})

	return result, err
}

// fanOut builds one record for each subscription of the event topic. It only
// reads, so a failure leaves nothing behind.
func fanOut(tx *bolt.Tx, event Event, now time.Time) ([]*record, error) {
	subscriptions := topicSubscriptions(tx, event.Topic)
	records := make([]*record, 0, len(subscriptions))

	for _, subscription := range subscriptions {
		jid, err := randomID("")
		if err != nil {
			return nil, err
		}

		custom := map[string]any{}

		for key, value := range event.Custom {
			custom[key] = value
		}

		custom["topic"] = event.Topic
		custom["event"] = event.ID

		encoded, err := json.Marshal(custom)
		if err != nil {
			return nil, err
		}

		copied := job.Job{
			JID:        jid,
			Type:       subscription.JobType,
			Args:       event.Args,
			Queue:      subscription.Queue,
			Retry:      subscription.Retry,
			ReserveFor: subscription.ReserveFor,
			Custom:     encoded,
		}

		if err := copied.Normalize(now); err != nil {
			return nil, fmt.Errorf("subscription %s to %s: %w", subscription.JobType, subscription.Queue, err)
		}

		records = append(records, &record{Job: copied})
	}

	return records, nil
}

// storeFanOut enqueues the records, or puts the event in the dead set when
// there is no record. It returns the number of jobs.
func storeFanOut(tx *bolt.Tx, records []*record, event Event, now time.Time) (int, error) {
	if len(records) == 0 {
		return 0, deadEvent(tx, event, now)
	}

	for _, rec := range records {
		if err := enqueue(tx, rec, now); err != nil {
			return 0, err
		}
	}

	return len(records), nil
}

func lockEvent(tx *bolt.Tx, key []byte, until time.Time) error {
	value := binary.BigEndian.AppendUint64(nil, uint64(until.UnixNano()))

	if err := tx.Bucket(bucketUniques).Put(key, value); err != nil {
		return err
	}

	return tx.Bucket(bucketUniqueTTL).Put(append(bytes.Clone(value), key...), nil)
}

// deadEvent keeps an event that no subscription received, so an operator can
// see it, and publish it again with a requeue.
func deadEvent(tx *bolt.Tx, event Event, now time.Time) error {
	jid, err := randomID("")
	if err != nil {
		return err
	}

	custom := map[string]any{}

	for key, value := range event.Custom {
		custom[key] = value
	}

	custom["topic"] = event.Topic
	custom["event"] = event.ID
	custom["unrouted"] = true

	encoded, err := json.Marshal(custom)
	if err != nil {
		return err
	}

	stamp := now.UTC().Format(time.RFC3339Nano)

	rec := &record{Job: job.Job{
		JID:       jid,
		Type:      event.Topic,
		Args:      event.Args,
		Queue:     UnroutedQueue,
		CreatedAt: stamp,
		Custom:    encoded,
		Failure:   &job.Failure{FailedAt: stamp, ErrType: "NoSubscriber", Message: fmt.Sprintf("no subscription for topic %s", event.Topic)},
	}}

	return moveToSet(tx, rec, bucketDead, now)
}

// unroutedEvent reads the event of a dead job that no subscription received.
// It returns false for any other job.
func unroutedEvent(j *job.Job) (Event, bool) {
	var custom map[string]any

	if len(j.Custom) == 0 || json.Unmarshal(j.Custom, &custom) != nil || custom["unrouted"] != true {
		return Event{}, false
	}

	topic, _ := custom["topic"].(string)
	id, _ := custom["event"].(string)

	delete(custom, "topic")
	delete(custom, "event")
	delete(custom, "unrouted")

	return Event{Topic: topic, ID: id, Args: j.Args, Custom: custom}, topic != ""
}

// republish publishes an unrouted event again, and removes its dead job. With
// still no subscription, the event goes back to the dead set.
func republish(tx *bolt.Tx, rec *record, event Event, now time.Time) error {
	records, err := fanOut(tx, event, now)
	if err != nil {
		return err
	}

	if len(records) == 0 {
		rec.Job.Failure.FailedAt = now.UTC().Format(time.RFC3339Nano)

		return moveToSet(tx, rec, bucketDead, now)
	}

	if err := tx.Bucket(bucketJobs).Delete([]byte(rec.Job.JID)); err != nil {
		return err
	}

	_, err = storeFanOut(tx, records, event, now)

	return err
}
