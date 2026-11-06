// Package job defines the job payload and its validation rules.
package job

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"
)

const (
	DefaultQueue      = "default"
	DefaultReserveFor = 1800
	MinReserveFor     = 60
	DefaultRetry      = 25
	maxIDLength       = 256
)

var queueName = regexp.MustCompile(`^[A-Za-z0-9._-]{1,255}$`)

type Failure struct {
	RetryCount int      `json:"retry_count"`
	FailedAt   string   `json:"failed_at"`
	NextAt     string   `json:"next_at,omitempty"`
	Message    string   `json:"message,omitempty"`
	ErrType    string   `json:"errtype,omitempty"`
	Backtrace  []string `json:"backtrace,omitempty"`
}

// DefaultBacktrace is the number of backtrace lines a failure keeps when the
// job does not set backtrace. A negative backtrace keeps none.
const DefaultBacktrace = 30

type Job struct {
	JID        string          `json:"jid"`
	Type       string          `json:"jobtype"`
	Args       json.RawMessage `json:"args"`
	Queue      string          `json:"queue,omitempty"`
	ReserveFor int             `json:"reserve_for,omitempty"`
	At         string          `json:"at,omitempty"`
	Retry      *int            `json:"retry,omitempty"`
	Backtrace  int             `json:"backtrace,omitempty"`
	CreatedAt  string          `json:"created_at,omitempty"`
	EnqueuedAt string          `json:"enqueued_at,omitempty"`
	Failure    *Failure        `json:"failure,omitempty"`
	Custom     json.RawMessage `json:"custom,omitempty"`
}

// Normalize validates a pushed job and applies the default values.
func (j *Job) Normalize(now time.Time) error {
	if j.JID == "" || len(j.JID) > maxIDLength {
		return errors.New("jid must have 1 to 256 bytes")
	}

	if j.Type == "" {
		return errors.New("jobtype is required")
	}

	if string(bytes.TrimSpace(j.Args)) == "null" {
		j.Args = json.RawMessage("[]")
	}

	if !bytes.HasPrefix(bytes.TrimSpace(j.Args), []byte("[")) {
		return errors.New("args must be an array")
	}

	if j.Queue == "" {
		j.Queue = DefaultQueue
	}

	if !ValidQueue(j.Queue) {
		return fmt.Errorf("invalid queue name: %q", j.Queue)
	}

	if j.ReserveFor == 0 {
		j.ReserveFor = DefaultReserveFor
	}

	if j.ReserveFor < MinReserveFor {
		return fmt.Errorf("reserve_for must be at least %d", MinReserveFor)
	}

	if j.Retry == nil {
		retry := DefaultRetry
		j.Retry = &retry
	}

	if j.At != "" {
		if _, err := time.Parse(time.RFC3339Nano, j.At); err != nil {
			return fmt.Errorf("invalid at: %w", err)
		}
	}

	if err := j.normalizeCustom(now); err != nil {
		return err
	}

	if j.CreatedAt == "" {
		j.CreatedAt = now.UTC().Format(time.RFC3339Nano)
	}

	j.EnqueuedAt = ""
	j.Failure = nil

	return nil
}

// ScheduledAt returns the run time. The value is false for an immediate job.
func (j *Job) ScheduledAt(now time.Time) (time.Time, bool) {
	if j.At == "" {
		return time.Time{}, false
	}

	at, err := time.Parse(time.RFC3339Nano, j.At)
	if err != nil || !at.After(now) {
		return time.Time{}, false
	}

	return at, true
}

// options reads the custom fields that change server behavior.
type options struct {
	ExpiresAt   *string   `json:"expires_at"`
	ExpiresIn   *float64  `json:"expires_in"`
	UniqueFor   *float64  `json:"unique_for"`
	UniqueUntil string    `json:"unique_until"`
	BatchID     string    `json:"bid"`
	CallbackBID string    `json:"_bid"`
	Callback    string    `json:"_cb"`
	Track       float64   `json:"track"`
	OnDead      *DeadHook `json:"on_dead"`
	Wrapped     string    `json:"wrapped"`
}

// DeadHook names a job that the server pushes when this job dies.
type DeadHook struct {
	JobType string `json:"jobtype"`
	Queue   string `json:"queue,omitempty"`
}

func (j *Job) options() options {
	var parsed options

	if len(j.Custom) > 0 {
		json.Unmarshal(j.Custom, &parsed)
	}

	return parsed
}

// normalizeCustom validates custom fields and converts expires_in to expires_at.
func (j *Job) normalizeCustom(now time.Time) error {
	if len(j.Custom) == 0 {
		return nil
	}

	fields := map[string]json.RawMessage{}

	if err := json.Unmarshal(j.Custom, &fields); err != nil {
		return fmt.Errorf("custom must be a hash: %w", err)
	}

	var parsed options

	if err := json.Unmarshal(j.Custom, &parsed); err != nil {
		return fmt.Errorf("invalid custom field: %w", err)
	}

	if parsed.UniqueUntil != "" && parsed.UniqueUntil != "start" && parsed.UniqueUntil != "success" {
		return fmt.Errorf("invalid unique_until: %q", parsed.UniqueUntil)
	}

	if parsed.OnDead != nil {
		if parsed.OnDead.JobType == "" {
			return errors.New("on_dead needs a jobtype")
		}

		if parsed.OnDead.Queue != "" && !ValidQueue(parsed.OnDead.Queue) {
			return fmt.Errorf("invalid on_dead queue: %q", parsed.OnDead.Queue)
		}
	}

	if parsed.ExpiresAt != nil {
		if _, err := time.Parse(time.RFC3339Nano, *parsed.ExpiresAt); err != nil {
			return fmt.Errorf("invalid expires_at: %w", err)
		}
	}

	if parsed.ExpiresIn == nil {
		return nil
	}

	at := now.Add(time.Duration(*parsed.ExpiresIn * float64(time.Second))).UTC().Format(time.RFC3339Nano)

	encoded, err := json.Marshal(at)
	if err != nil {
		return err
	}

	fields["expires_at"] = encoded
	delete(fields, "expires_in")

	j.Custom, err = json.Marshal(fields)

	return err
}

func (j *Job) Expired(now time.Time) bool {
	parsed := j.options()
	if parsed.ExpiresAt == nil {
		return false
	}

	at, err := time.Parse(time.RFC3339Nano, *parsed.ExpiresAt)

	return err == nil && now.After(at)
}

// UniqueFor returns the lock time. Zero means that the job is not unique.
func (j *Job) UniqueFor() time.Duration {
	parsed := j.options()
	if parsed.UniqueFor == nil || *parsed.UniqueFor <= 0 {
		return 0
	}

	return time.Duration(*parsed.UniqueFor * float64(time.Second))
}

func (j *Job) UniqueUntilStart() bool {
	return j.options().UniqueUntil == "start"
}

// UniqueKey identifies a job by type, queue, and args.
func (j *Job) UniqueKey() []byte {
	var args bytes.Buffer

	if err := json.Compact(&args, j.Args); err != nil {
		args.Write(j.Args)
	}

	digest := sha256.New()
	digest.Write([]byte(j.Type + "\x00" + j.Queue + "\x00"))
	digest.Write(args.Bytes())

	return digest.Sum(nil)
}

func (j *Job) BatchID() string {
	return j.options().BatchID
}

// BatchCallback returns the batch and the kind of a callback job.
func (j *Job) BatchCallback() (string, string) {
	parsed := j.options()

	return parsed.CallbackBID, parsed.Callback
}

// DeadHook returns the job to push when this job dies, or nil.
func (j *Job) DeadHook() *DeadHook {
	return j.options().OnDead
}

// Wrapped is the class that a wrapper job runs, such as the Active Job class
// of a job with the type ActiveJob. It is blank for other jobs.
func (j *Job) Wrapped() string {
	return j.options().Wrapped
}

func (j *Job) Tracked() bool {
	return j.options().Track == 1
}

// SetCustom adds fields to the custom hash.
func (j *Job) SetCustom(values map[string]string) error {
	fields := map[string]any{}

	if len(j.Custom) > 0 {
		if err := json.Unmarshal(j.Custom, &fields); err != nil {
			return err
		}
	}

	for key, value := range values {
		fields[key] = value
	}

	encoded, err := json.Marshal(fields)
	if err != nil {
		return err
	}

	j.Custom = encoded

	return nil
}

func ValidQueue(name string) bool {
	return queueName.MatchString(name)
}
