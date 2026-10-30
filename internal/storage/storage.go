// Package storage persists jobs. Each exported operation is one atomic change.
package storage

import (
	"errors"
	"time"

	"jobserver/internal/job"
)

var (
	ErrDuplicateJob = errors.New("job id already exists")
	ErrNotReserved  = errors.New("job is not reserved")
	ErrNotUnique    = errors.New("job not unique")
)

// RejectedError is a client error. It does not roll back a commit group.
type RejectedError struct {
	Message string
}

func (e *RejectedError) Error() string {
	return e.Message
}

type BatchDefinition struct {
	ParentBID   string   `json:"parent_bid,omitempty"`
	Description string   `json:"description,omitempty"`
	Success     *job.Job `json:"success,omitempty"`
	Complete    *job.Job `json:"complete,omitempty"`
}

type BatchStatus struct {
	BID           string `json:"bid"`
	ParentBID     string `json:"parent_bid,omitempty"`
	Description   string `json:"description,omitempty"`
	CreatedAt     string `json:"created_at"`
	CompleteState string `json:"complete_st"`
	SuccessState  string `json:"success_st"`
	Total         int64  `json:"total"`
	Pending       int64  `json:"pending"`
	Failed        int64  `json:"failed"`
}

type Track struct {
	JID       string `json:"jid"`
	State     string `json:"state"`
	UpdatedAt string `json:"updated_at,omitempty"`
	Percent   *int   `json:"percent,omitempty"`
	Desc      string `json:"desc,omitempty"`
}

type TrackUpdate struct {
	JID          string `json:"jid"`
	Percent      *int   `json:"percent"`
	Desc         string `json:"desc"`
	ReserveUntil string `json:"reserve_until"`
}

type Outcome int

const (
	Retry Outcome = iota
	Dead
	Discard
)

// Disposition tells the store where a failed job goes.
type Disposition struct {
	Outcome Outcome
	At      time.Time
}

// Decide updates the failure data of a job and returns its next location.
type Decide func(j *job.Job) Disposition

type Stats struct {
	Queues    map[string]uint64 `json:"queues"`
	Scheduled uint64            `json:"scheduled"`
	Retries   uint64            `json:"retries"`
	Working   uint64            `json:"working"`
	Dead      uint64            `json:"dead"`
	Processed uint64            `json:"processed"`
	Failures  uint64            `json:"failures"`
	Expired   uint64            `json:"expired"`
	Paused    []string          `json:"paused"`
}

// Throttle limits how many jobs of a queue can run. Zero disables a limit.
type Throttle struct {
	Concurrency int
	Worker      int
	Timeout     time.Duration
}

// Mutation changes the jobs of one set that match the filter.
type Mutation struct {
	Command string
	Target  string
	Match   func(j *job.Job, encoded []byte) bool
}

type Store interface {
	Push(j *job.Job, now time.Time) error
	Fetch(queues []string, wid string, now time.Time) (*job.Job, error)
	Ack(jid string) error
	Fail(jid string, now time.Time, decide Decide) error
	MoveDue(now time.Time, limit int) (int, error)
	ExpireReservations(now time.Time, limit int, decide Decide) (int, error)
	SweepLocks(now time.Time, limit int) (int, error)
	SweepExpired(now time.Time, limit int) (int, error)
	BatchNew(definition BatchDefinition, now time.Time) (string, error)
	BatchOpen(bid string) error
	BatchCommit(bid string, now time.Time) error
	BatchStatus(bid string) (BatchStatus, error)
	TrackGet(jid string, now time.Time) (Track, error)
	TrackSet(update TrackUpdate, now time.Time) error
	Mutate(mutation Mutation, now time.Time) (int, error)
	PauseQueues(names []string) error
	ResumeQueues(names []string) error
	RemoveQueues(names []string) error
	SetThrottles(throttles map[string]Throttle)
	Stats() (Stats, error)
	Flush() error
	Close() error
}
