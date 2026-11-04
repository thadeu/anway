package manager

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/thadeu/anway/internal/job"
	"github.com/thadeu/anway/internal/storage"
)

// A topic sends one event to many queues: PUBLISH makes one job for each
// subscription of the topic, in one write. See docs/dev/topics.md.

const (
	// DefaultEventDedup is how long a published event id blocks a second
	// PUBLISH with the same topic and id.
	DefaultEventDedup = 24 * time.Hour

	maxEventDedup = 30 * 24 * time.Hour
	maxNameLength = 256
)

// PublishRequest is the PUBLISH argument. UniqueFor is in seconds, and only
// applies with an ID.
type PublishRequest struct {
	Topic     string          `json:"topic"`
	Args      json.RawMessage `json:"args"`
	ID        string          `json:"id"`
	UniqueFor *float64        `json:"unique_for"`
	Custom    map[string]any  `json:"custom"`
}

// SubscribeRequest is the SUBSCRIBE and UNSUBSCRIBE argument.
type SubscribeRequest struct {
	Topic      string `json:"topic"`
	Queue      string `json:"queue"`
	JobType    string `json:"jobtype"`
	Retry      *int   `json:"retry"`
	ReserveFor int    `json:"reserve_for"`
}

func checkTopic(topic string) error {
	if !job.ValidQueue(topic) {
		return fmt.Errorf("invalid topic name: %q", topic)
	}

	return nil
}

func (request SubscribeRequest) check() error {
	if err := checkTopic(request.Topic); err != nil {
		return err
	}

	if !job.ValidQueue(request.Queue) {
		return fmt.Errorf("invalid queue name: %q", request.Queue)
	}

	if request.JobType == "" || len(request.JobType) > maxNameLength {
		return errors.New("jobtype must have 1 to 256 bytes")
	}

	if request.ReserveFor != 0 && request.ReserveFor < job.MinReserveFor {
		return fmt.Errorf("reserve_for must be at least %d", job.MinReserveFor)
	}

	return nil
}

// Subscribe saves a subscription. It is idempotent: the same topic, queue,
// and job type keep one subscription, with the new options.
func (m *Manager) Subscribe(request SubscribeRequest) error {
	if err := request.check(); err != nil {
		return err
	}

	return m.store.Subscribe(storage.Subscription{
		Topic:      request.Topic,
		Queue:      request.Queue,
		JobType:    request.JobType,
		Retry:      request.Retry,
		ReserveFor: request.ReserveFor,
	}, time.Now())
}

// Unsubscribe removes a subscription. A missing one is not an error.
func (m *Manager) Unsubscribe(request SubscribeRequest) error {
	if err := request.check(); err != nil {
		return err
	}

	return m.store.Unsubscribe(request.Topic, request.Queue, request.JobType)
}

// Subscriptions returns the subscriptions of a topic, or of all topics.
func (m *Manager) Subscriptions(topic string) ([]storage.Subscription, error) {
	return m.store.Subscriptions(topic)
}

// Publish sends an event to every subscription of its topic. Without an id,
// the server makes one, and the event has no dedup.
func (m *Manager) Publish(request PublishRequest) (storage.Published, error) {
	event, err := request.event()
	if err != nil {
		return storage.Published{}, err
	}

	published, err := m.store.Publish(event, time.Now())
	if err != nil {
		return published, err
	}

	if published.Jobs > 0 {
		m.wake()
	}

	return published, nil
}

// PublishResult is the result of one event of a bulk publish: the event id,
// the jobs that it made, duplicate, and the reason when the event is not in.
type PublishResult struct {
	storage.Published
	Error string `json:"error,omitempty"`
}

// PublishBulk publishes many events in one write. An event that fails its
// check, or that the store refuses, gets its reason, and the others go in.
// The results keep the order of the requests.
func (m *Manager) PublishBulk(requests []PublishRequest) ([]PublishResult, error) {
	if len(requests) > storage.MaxBulk {
		return nil, fmt.Errorf("a bulk publish has at most %d events, not %d", storage.MaxBulk, len(requests))
	}

	results := make([]PublishResult, len(requests))
	events := make([]storage.Event, 0, len(requests))
	positions := make([]int, 0, len(requests))

	for index, request := range requests {
		event, err := request.event()
		if err != nil {
			results[index] = PublishResult{Error: err.Error()}

			continue
		}

		events = append(events, event)
		positions = append(positions, index)
	}

	if len(events) == 0 {
		return results, nil
	}

	published, failures, err := m.store.PublishBulk(events, time.Now())
	if err != nil {
		return nil, err
	}

	jobs := 0

	for position, index := range positions {
		results[index] = PublishResult{Published: published[position]}

		if failures[position] != nil {
			results[index].Error = failures[position].Error()
		}

		jobs += published[position].Jobs
	}

	if jobs > 0 {
		m.wake()
	}

	return results, nil
}

func (request PublishRequest) event() (storage.Event, error) {
	if err := checkTopic(request.Topic); err != nil {
		return storage.Event{}, err
	}

	args := request.Args

	if len(args) == 0 || string(args) == "null" {
		args = json.RawMessage("[]")
	}

	if args[0] != '[' {
		return storage.Event{}, errors.New("args must be an array")
	}

	if len(request.ID) > maxNameLength {
		return storage.Event{}, errors.New("id must have at most 256 bytes")
	}

	event := storage.Event{Topic: request.Topic, ID: request.ID, Args: args, Custom: request.Custom}

	if event.ID == "" {
		id, err := eventID()
		if err != nil {
			return storage.Event{}, err
		}

		event.ID = id

		return event, nil
	}

	event.Dedup = DefaultEventDedup

	if request.UniqueFor != nil {
		event.Dedup = time.Duration(*request.UniqueFor * float64(time.Second))
	}

	if event.Dedup < 0 || event.Dedup > maxEventDedup {
		return storage.Event{}, fmt.Errorf("unique_for must be 0 to %d seconds", int(maxEventDedup/time.Second))
	}

	return event, nil
}

func eventID() (string, error) {
	raw := make([]byte, 12)

	if _, err := rand.Read(raw); err != nil {
		return "", err
	}

	return "evt-" + hex.EncodeToString(raw), nil
}
