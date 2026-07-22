package retrier

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// TaskStatus task status
type TaskStatus string

const (
	// StatusPending task status
	StatusPending TaskStatus = "pending"
	// StatusSuccess task status
	StatusSuccess TaskStatus = "success"
	// StatusFailure task status
	StatusFailure TaskStatus = "failure"
)

func getID() uuid.UUID {
	ID, _ := uuid.NewV7()
	return ID
}

// Task represents a generic executable unit of work with retry tracking.
type Task struct {
	// ID uniquely identifies this task across the entire system.
	ID uuid.UUID `json:"id"`
	// Ctx context for tracing
	Ctx context.Context `json:"-"`
	// Payload holds the strongly-typed input arguments required for task execution.
	Payload []byte `json:"payload"`
	// ManagerWorker specifies the designated runner type or queue name for this task.
	Worker string `json:"worker"`
	// Status tracks the current lifecycle phase of the task (e.g., pending, running, failed).
	Status TaskStatus `json:"status"`

	// Retries count of execution times
	Retries int `json:"retries"`
	// MaxRetries max tries count
	MaxRetries int `json:"max_retries"`
	// BackOffCode code of back off strategy
	BackOffCode string `json:"backoff_code"`
	// BackOffParams params for back off strategy
	BackOffParams map[BackOffParam]interface{} `json:"backoff_params"`

	// Deadline is an optional time limit for task completion.
	// If the current time exceeds this deadline before the task starts executing,
	// the task will be marked as failed with a critical error and will not be retried.
	// Zero value (time.Time{}) indicates no deadline.
	Deadline time.Time `json:"deadline"`

	// CreatedAt records the exact timestamp when the task was initially created.
	CreatedAt time.Time `json:"created_at"`
	// LastRun records the timestamp of the most recent execution attempt, if any.
	LastRun time.Time `json:"last_run"`
	// NextRun records the scheduled timestamp when the task should be picked up next.
	NextRun time.Time `json:"next_run"`
}

// IsFinished is task finished and will not be retried
func (t *Task) IsFinished() bool {
	return t.Status == StatusSuccess || t.Status == StatusFailure || t.MaxRetries <= t.Retries
}

// GetBackOffCode returns the backoff strategy code.
func (t *Task) GetBackOffCode() string {
	return t.BackOffCode
}

// GetBackOffParams returns the parameters for the backoff strategy.
func (t *Task) GetBackOffParams() map[BackOffParam]interface{} {
	return t.BackOffParams
}

// GetRetries returns the current retry count.
func (t *Task) GetRetries() int {
	return t.Retries
}

// TaskExecutionResult records the outcome and metadata of a single execution attempt of a task.
type TaskExecutionResult struct {
	// ID uniquely identifies this specific execution outcome record.
	ID uuid.UUID
	// TaskID references the parent Task that generated this execution result.
	TaskID uuid.UUID
	// Status indicates whether this specific run succeeded or encountered an error.
	Status TaskStatus
	// RunAt records the exact timestamp when this execution attempt was performed.
	RunAt time.Time
	// Result stores the raw payload returned by the workerImpl, such as response data or error details.
	Result []byte
	// IsCritical if this is a validation error, we have no any tries
	IsCritical bool
	// ExecutionTime worker func duration
	ExecutionTime time.Duration
}
