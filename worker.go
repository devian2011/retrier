package retrier

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// WorkerStatus represents the current lifecycle state of a ManagerWorker pool.
type WorkerStatus string

const (
	// WorkerStatusCreated indicates the worker pool is initialized but not yet processing tasks.
	WorkerStatusCreated WorkerStatus = "created"
	// WorkerStatusRunning indicates the worker pool is actively accepting and executing tasks.
	WorkerStatusRunning WorkerStatus = "running"
	// WorkerStatusStopped indicates the worker pool is completely shut down and cannot be reused.
	WorkerStatusStopped WorkerStatus = "stopped"
	// WorkerStatusSuspended indicates the worker pool is temporarily paused; active tasks are drained.
	WorkerStatusSuspended WorkerStatus = "suspended"
	// WorkerStatusHalfRunning Half-Open in circuit breaker pattern
	WorkerStatusHalfRunning WorkerStatus = "half-running"
)

// ErrorState error state critical/usual
type ErrorState string

const (
	// CriticalState validation error and so on, after that we don't need to retry task
	CriticalState ErrorState = "critical"
	// UsualState service unreach or other problem
	UsualState ErrorState = "usual"
)

// ExecutionError Worker execution error
type ExecutionError struct {
	Err   error
	State ErrorState
}

// WorkerFn defines the execution contract for processing a task's raw payload.
// It returns a result string (or log) and an error if the execution fails.
type WorkerFn func(payload []byte) (string, *ExecutionError)

// WorkerExecutionResult pairs the original task with its final processing metrics and outcome.
type WorkerExecutionResult struct {
	task   *Task
	result *TaskExecutionResult
}

// WorkerState exposes the public exportable state snapshot of the worker pool.
type WorkerState struct {
	Status        WorkerStatus `json:"status"`
	ActiveTasks   int32        `json:"active_tasks"`
	ActiveWorkers int32        `json:"active_workers"`
}

// Worker manages a highly concurrent, thread-safe dynamic pool of goroutines
// that scale up and down automatically based on incoming load constraints.
type Worker struct {
	ctx context.Context
	wg  sync.WaitGroup // Tracks active worker loops for graceful shutdowns.

	mu        sync.RWMutex       // Protects the status field from concurrent access/modification.
	status    WorkerStatus       // Current operational state of the pool.
	cancelCtx context.Context    // Internal context used to signal individual worker goroutines.
	cancelFn  context.CancelFunc // Cancels the cancelCtx to trigger scaling down or stopping.

	inQueue  chan *Task                 // Unbuffered channel feeding tasks to available workers.
	outQueue chan WorkerExecutionResult // Channel broadcasting completed execution telemetry.

	minWorkers    int32         // Minimum floor limit of goroutines that must remain alive.
	maxWorkers    int32         // Maximum ceiling limit of concurrent goroutines allowed.
	activeWorkers atomic.Int32  // Total number of spawned goroutines currently running.
	activeTasks   atomic.Int32  // Number of goroutines currently processing a task.
	idleTimeout   time.Duration // Time duration a surplus worker waits before scaling down due to inactivity.

	fn WorkerFn // User-defined function mapping task workloads.
}

// NewWorker constructs and returns a new initialized ManagerWorker pool ready to scale.
func NewWorker(ctx context.Context, fn WorkerFn) *Worker {
	cancelCtx, cancelFn := context.WithCancel(ctx)

	return &Worker{
		ctx:       ctx,
		cancelCtx: cancelCtx,
		cancelFn:  cancelFn,

		inQueue:  make(chan *Task),
		outQueue: make(chan WorkerExecutionResult),

		minWorkers:    1,
		maxWorkers:    1,
		activeWorkers: atomic.Int32{},
		activeTasks:   atomic.Int32{},
		idleTimeout:   time.Second * 5,

		fn:     fn,
		status: WorkerStatusCreated,
	}
}

// SetMinAndMaxWorkers overrides the boundary thresholds for dynamic scaling operations.
func (w *Worker) SetMinAndMaxWorkers(minWorkers, maxWorkers int32) {
	w.minWorkers = minWorkers
	w.maxWorkers = maxWorkers
}

// SetIdleTimeout sets the duration a surplus worker can remain idle before it terminates itself.
func (w *Worker) SetIdleTimeout(idleTimeout time.Duration) {
	w.idleTimeout = idleTimeout
}

// GetStatus returns the current operational status of the worker pool in a thread-safe manner.
func (w *Worker) GetStatus() WorkerState {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return WorkerState{
		Status:        w.status,
		ActiveTasks:   w.activeTasks.Load(),
		ActiveWorkers: w.activeWorkers.Load(),
	}
}

// GetOutChan get result output channel
func (w *Worker) GetOutChan() chan WorkerExecutionResult {
	return w.outQueue
}

// Submit non-blockingly attempts to feed a task into the queue.
// If all current workers are busy, it scales up by spawning a new goroutine (up to maxWorkers).
// If the pool is saturated, it blocks until a worker is free or the context is cancelled.
func (w *Worker) Submit(task *Task) error {
	w.mu.RLock()
	currentStatus := w.status
	w.mu.RUnlock()

	if currentStatus != WorkerStatusRunning {
		return fmt.Errorf("worker is not running")
	}

	select {
	case w.inQueue <- task:
		// Task was immediately accepted by a waiting idle worker.
		return nil
	default:
		// No idle workers are listening on the channel. Attempt to scale up.
		currentActive := w.activeWorkers.Load()
		if currentActive < w.maxWorkers {
			// Atomically reserve a slot to prevent race conditions exceeding maxWorkers limit.
			if w.activeWorkers.Add(1) <= w.maxWorkers {
				w.wg.Add(1)
				go w.runWorker()
			} else {
				w.activeWorkers.Add(-1) // Revert allocation if beaten by another thread.
			}
		}

		// Fallback block: await queue availability or pool shutdown signaling.
		select {
		case w.inQueue <- task:
			return nil
		case <-w.cancelCtx.Done():
			return errors.New("worker pool was stopped/suspended while submitting task")
		}
	}
}

// Start transitions the pool into a running state and pre-spawns the minimum required workers.
func (w *Worker) Start() {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.status == WorkerStatusRunning {
		return
	}

	cancelCtx, cancelFn := context.WithCancel(w.ctx)
	w.cancelCtx = cancelCtx
	w.cancelFn = cancelFn

	for i := 0; i < int(w.minWorkers); i++ {
		w.wg.Add(1)
		w.activeWorkers.Add(1)
		go w.runWorker()
	}

	w.status = WorkerStatusRunning
}

// Suspend temporarily pauses task processing, cancels the active worker context,
// and blocks until all currently executing loops drain safely.
func (w *Worker) Suspend() {
	w.mu.Lock()
	w.status = WorkerStatusSuspended
	w.mu.Unlock()
}

// Stop permanently shuts down the pool, cancels contexts, closes the internal ingestion queue,
// waits for active processors to exit, and safely shuts down the outcome queue.
func (w *Worker) Stop() {
	w.mu.Lock()
	if w.status == WorkerStatusStopped {
		w.mu.Unlock()
		return
	}
	w.status = WorkerStatusStopped
	w.mu.Unlock()
	w.cancelFn()
	w.wg.Wait()
	close(w.inQueue)
	close(w.outQueue)
}

// runWorker implements the core lifecycle loop of a separate execution thread.
// It features an automated zero-allocation dynamic scaling cleanup mechanism.
func (w *Worker) runWorker() {
	timer := time.NewTimer(w.idleTimeout)

	defer w.wg.Done()
	defer w.activeWorkers.Add(-1)
	defer timer.Stop()

	for {
		// Clean the timer channel prior to resetting.
		// This protects against race conditions where an expired but unprocessed
		// timer tick triggers a false shutdown on the following loop iteration.
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(w.idleTimeout)

		select {
		case <-w.cancelCtx.Done():
			return // Context canceled via Stop or Suspend routines.
		case <-timer.C:
			// Dynamic scale-down: terminate surplus goroutines exceeding the min floor limit.
			if w.activeWorkers.Load() > w.minWorkers {
				return
			}
		case task, ok := <-w.inQueue:
			if !ok {
				return // Channel closed via explicit pool shutdown sequence.
			}

			w.activeTasks.Add(1)

			// Initialize base execution state records.
			tr := &TaskExecutionResult{
				ID:            getID(),
				TaskID:        task.ID,
				Status:        StatusPending,
				RunAt:         time.Now(),
				Result:        nil,
				ExecutionTime: 0,
			}

			timeStart := time.Now()
			execRes, execErr := w.fn(task.Payload)
			tr.ExecutionTime = time.Since(timeStart)

			if execErr != nil {
				tr.Result = []byte(fmt.Sprintf("Result: %s, Error: %s", execRes, execErr))
				tr.Status = StatusFailure
				tr.IsCritical = execErr.State == CriticalState
			} else {
				tr.Result = []byte(execRes)
				tr.Status = StatusSuccess
				tr.IsCritical = false
			}

			// Forward the telemetry outcomes to the output broadcasting channel.
			select {
			case w.outQueue <- WorkerExecutionResult{task: task, result: tr}:
			case <-w.cancelCtx.Done():
				w.activeTasks.Add(-1)
				return
			}

			w.activeTasks.Add(-1)
		}
	}
}
