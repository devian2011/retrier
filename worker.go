// Package retrier provides a dynamic, auto‑scaling worker pool for concurrent task processing.
// It supports graceful shutdown, suspension, and configurable limits with idle timeout.
package retrier

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

// WorkerStatus represents the current lifecycle state of the worker pool.
type WorkerStatus string

const (
	// WorkerStatusCreated indicates the pool is initialized but not yet started.
	WorkerStatusCreated WorkerStatus = "created"
	// WorkerStatusRunning indicates the pool is actively accepting and executing tasks.
	WorkerStatusRunning WorkerStatus = "running"
	// WorkerStatusStopped indicates the pool is completely shut down and cannot be reused.
	WorkerStatusStopped WorkerStatus = "stopped"
	// WorkerStatusSuspended indicates the pool is temporarily paused; active tasks are drained,
	// and new submissions are rejected.
	WorkerStatusSuspended WorkerStatus = "suspended"
)

// ErrorState classifies errors for retry or abort decisions.
type ErrorState string

const (
	// CriticalState marks errors that are unrecoverable (e.g., validation failures).
	// Tasks with critical errors should not be retried.
	CriticalState ErrorState = "critical"
	// UsualState marks transient errors (e.g., network timeouts) that may be retried.
	UsualState ErrorState = "usual"
)

// ExecutionError wraps an error with a state that indicates whether the error is critical.
type ExecutionError struct {
	Err   error
	State ErrorState
}

// WorkerFn is the user‑defined function that processes a task payload.
// It returns a result string and an optional ExecutionError.
// If the function panics, it is recovered and treated as a critical error.
type WorkerFn func(ctx context.Context, payload []byte) (string, *ExecutionError)

// WorkerExecutionResult pairs the original Task with its execution result.
type WorkerExecutionResult struct {
	Task   *Task
	Result *TaskExecutionResult
}

// WorkerState is a snapshot of the pool's current status.
type WorkerState struct {
	Status        WorkerStatus `json:"status"`
	ActiveTasks   int32        `json:"active_tasks"`   // number of tasks currently being processed
	ActiveWorkers int32        `json:"active_workers"` // number of running worker goroutines
}

// WorkerConfig holds the tunable parameters for the worker pool.
type WorkerConfig struct {
	minWorkers  int32         // minimum number of workers always kept alive
	maxWorkers  int32         // maximum number of workers that can be spawned
	idleTimeout time.Duration // duration after which an idle worker scales down (if above minWorkers)
}

// NewWorkerCfg creates a validated WorkerConfig.
// Returns an error if min > max, any value is negative, or idleTimeout <= 0.
func NewWorkerCfg(min, max int32, idleTimeout time.Duration) (*WorkerConfig, error) {
	if min > max {
		return nil, errors.New("min must be less than max")
	}
	if min < 0 || max < 0 {
		return nil, errors.New("min or max must be greater than 0")
	}
	if idleTimeout <= 0 {
		return nil, errors.New("idleTimeout must be greater than 0 seconds")
	}
	return &WorkerConfig{
		minWorkers:  min,
		maxWorkers:  max,
		idleTimeout: idleTimeout,
	}, nil
}

// Worker manages a dynamic pool of goroutines that process tasks from an input queue.
// It scales up when all workers are busy (up to maxWorkers) and scales down when workers
// are idle for idleTimeout (down to minWorkers). It provides thread‑safe operations for
// starting, stopping, suspending, and submitting tasks.
type Worker struct {
	ctx context.Context // parent context for cancellation propagation

	cfgMtx sync.RWMutex // protects cfg
	cfg    *WorkerConfig

	wg sync.WaitGroup // waits for all worker goroutines to finish

	mu        sync.RWMutex       // protects status, cancelCtx, cancelFn
	status    WorkerStatus       // current lifecycle state
	cancelCtx context.Context    // internal context for cancelling workers
	cancelFn  context.CancelFunc // cancels cancelCtx

	inQueue  chan *Task                 // unbuffered channel for task submissions
	outQueue chan WorkerExecutionResult // unbuffered channel for task results

	activeWorkers atomic.Int32 // current number of worker goroutines
	activeTasks   atomic.Int32 // current number of tasks being processed

	fn WorkerFn // user‑defined task processor

	sendMtx sync.Mutex // protects inQueue from concurrent close and send (prevents panic on closed channel)
}

// NewWorker constructs a new worker pool with the given configuration and processing function.
// The pool starts in the Created state; call Start() to begin processing.
func NewWorker(ctx context.Context, cfg *WorkerConfig, fn WorkerFn) (*Worker, error) {
	cancelCtx, cancelFn := context.WithCancel(ctx)
	return &Worker{
		ctx:       ctx,
		cancelCtx: cancelCtx,
		cancelFn:  cancelFn,
		inQueue:   make(chan *Task),
		outQueue:  make(chan WorkerExecutionResult),
		cfg:       cfg,
		fn:        fn,
		status:    WorkerStatusCreated,
	}, nil
}

// UpdateConfig replaces the current configuration with a new one.
// The change takes effect immediately for subsequent scaling decisions and idle timeouts.
// It is safe to call concurrently.
func (w *Worker) UpdateConfig(cfg *WorkerConfig) {
	w.cfgMtx.Lock()
	defer w.cfgMtx.Unlock()
	w.cfg = cfg
}

// GetStatus returns a snapshot of the pool's current state.
// It is safe to call concurrently.
func (w *Worker) GetStatus() WorkerState {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return WorkerState{
		Status:        w.status,
		ActiveTasks:   w.activeTasks.Load(),
		ActiveWorkers: w.activeWorkers.Load(),
	}
}

// GetOutChan returns the output channel where completed task results are delivered.
// The channel is closed when the pool is stopped.
// If the pool is restarted after Stop, a new channel is created; callers should obtain
// the new channel again.
func (w *Worker) GetOutChan() chan WorkerExecutionResult {
	return w.outQueue
}

// Submit enqueues a task for processing.
// It attempts a non‑blocking send first; if no worker is idle, it scales up (if possible)
// and then blocks until the task is accepted or the pool is cancelled/stopped.
// Returns an error if the pool is not Running.
func (w *Worker) Submit(task *Task) error {
	// One lock protects the entire operation, including scaling and sending.
	w.sendMtx.Lock()
	defer w.sendMtx.Unlock()

	// Pool must be in Running state to accept new tasks.
	w.mu.RLock()
	if w.status != WorkerStatusRunning {
		w.mu.RUnlock()
		return fmt.Errorf("worker is not running")
	}
	w.mu.RUnlock()

	// Try a non‑blocking send to an idle worker.
	select {
	case w.inQueue <- task:
		return nil
	default:
		// No idle worker – attempt to scale up if under maxWorkers.
		current := w.activeWorkers.Load()
		w.cfgMtx.RLock()
		if current < w.cfg.maxWorkers {
			if w.activeWorkers.Add(1) <= w.cfg.maxWorkers {
				w.wg.Add(1)
				go w.runWorker()
			} else {
				w.activeWorkers.Add(-1)
			}
		}
		w.cfgMtx.RUnlock()
	}

	// Block until the task is accepted or the pool is cancelled/stopped.
	select {
	case w.inQueue <- task:
		return nil
	case <-w.cancelCtx.Done():
		return errors.New("worker pool was stopped/suspended while submitting Task")
	}
}

// Start transitions the pool into the Running state and spawns the minimum number of workers.
// If the pool is already Running, it does nothing.
// If it was Suspended, it cancels the previous context to terminate old workers and starts fresh.
// If it was Stopped, it re‑creates the internal channels (since they were closed by Stop),
// resets all counters, and starts a new generation of workers.
// After Start returns, the pool is ready to accept tasks again.
func (w *Worker) Start() {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Already running – nothing to do.
	if w.status == WorkerStatusRunning {
		return
	}

	if w.status == WorkerStatusSuspended {
		w.status = WorkerStatusRunning
		return
	}

	// If the pool was stopped, we need to recreate the channels and reset the state.
	if w.status == WorkerStatusStopped {
		// Reassign new channels (old ones are already closed).
		w.inQueue = make(chan *Task)
		w.outQueue = make(chan WorkerExecutionResult)

		// Reset counters – they should already be zero, but we enforce it.
		w.activeWorkers.Store(0)
		w.activeTasks.Store(0)

		// The WaitGroup is already zero because Stop() waits for all workers.
	}

	// Cancel any previous internal context (for suspended or stopped cases).
	if w.cancelFn != nil {
		w.cancelFn()
	}

	// Create a fresh internal context for the new generation of workers.
	cancelCtx, cancelFn := context.WithCancel(w.ctx)
	w.cancelCtx = cancelCtx
	w.cancelFn = cancelFn

	// Spawn the minimum number of workers.
	w.cfgMtx.RLock()
	min := w.cfg.minWorkers
	w.cfgMtx.RUnlock()

	for i := 0; i < int(min); i++ {
		w.wg.Add(1)
		w.activeWorkers.Add(1)
		go w.runWorker()
	}

	w.status = WorkerStatusRunning
}

// Suspend puts the pool into the Suspended state. It does not cancel existing tasks;
// it only prevents new submissions. Active tasks continue to completion.
// The pool can be resumed by calling Start() again.
func (w *Worker) Suspend() {
	w.mu.Lock()
	w.status = WorkerStatusSuspended
	w.mu.Unlock()
}

// Stop permanently shuts down the pool. It cancels all workers, waits for them to finish,
// and closes the input and output channels. After Stop, the pool can be restarted by
// calling Start() (which will recreate the channels). It is safe to call multiple times.
func (w *Worker) Stop() {
	w.mu.Lock()
	if w.status == WorkerStatusStopped {
		w.mu.Unlock()
		return
	}
	w.status = WorkerStatusStopped
	w.mu.Unlock()

	// Signal all workers to exit.
	w.cancelFn()

	// Wait for all worker goroutines to finish.
	w.wg.Wait()

	// Close the input channel under the send mutex to prevent concurrent sends.
	w.sendMtx.Lock()
	close(w.inQueue)
	w.sendMtx.Unlock()

	// Close the output channel (no more workers are alive, so no one will write to it).
	close(w.outQueue)
}

// runWorker is the main loop for each worker goroutine.
// It processes tasks from the input queue, handles idle timeouts, and supports
// dynamic scaling down when the pool is over‑provisioned.
// On exit, it decrements the active worker count.
func (w *Worker) runWorker() {
	w.cfgMtx.RLock()
	timer := time.NewTimer(w.cfg.idleTimeout)
	w.cfgMtx.RUnlock()

	defer w.wg.Done()
	// Ensure that the worker counter is decremented when this goroutine exits.
	defer w.activeWorkers.Add(-1)
	defer timer.Stop()

	for {
		// Reset the idle timer for each loop iteration.
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		w.cfgMtx.RLock()
		timer.Reset(w.cfg.idleTimeout)
		w.cfgMtx.RUnlock()

		select {
		case <-w.cancelCtx.Done():
			// Pool is shutting down or suspending – exit gracefully.
			return

		case <-timer.C:
			// Idle timeout fired – check if we can scale down.
			w.cfgMtx.RLock()
			current := w.activeWorkers.Load()
			if current <= w.cfg.minWorkers {
				w.cfgMtx.RUnlock()
				continue // cannot go below minimum
			}
			w.cfgMtx.RUnlock()
			// We are above minimum – exit this worker.
			// The defer will decrease activeWorkers.
			return

		case task, ok := <-w.inQueue:
			if !ok {
				// Input queue closed – exit.
				return
			}

			w.activeTasks.Add(1)

			// Prepare the execution result record.
			tr := &TaskExecutionResult{
				ID:            getID(), // assume getID() returns a unique identifier
				TaskID:        task.ID,
				Status:        StatusPending,
				RunAt:         time.Now(),
				Result:        nil,
				ExecutionTime: 0,
			}

			ctx := task.Ctx
			if ctx == nil {
				ctx = w.ctx
			}

			timeStart := time.Now()

			// Execute the user function with panic recovery.
			execRes, execErr := func() (result string, err *ExecutionError) {
				defer func() {
					if pErr := recover(); pErr != nil {
						result = ""
						err = &ExecutionError{
							Err:   fmt.Errorf("panic: %v, stack: %s", pErr, string(debug.Stack())),
							State: CriticalState,
						}
					}
				}()
				return w.fn(ctx, task.Payload)
			}()

			tr.ExecutionTime = time.Since(timeStart)

			if execErr != nil {
				tr.Result = []byte(fmt.Sprintf("Result: %s, Error: %v", execRes, execErr.Err))
				tr.Status = StatusFailure
				tr.IsCritical = execErr.State == CriticalState
			} else {
				tr.Result = []byte(execRes)
				tr.Status = StatusSuccess
				tr.IsCritical = false
			}

			// Deliver the result. If the pool is cancelled while we try to send,
			// abandon the result and exit.
			select {
			case w.outQueue <- WorkerExecutionResult{Task: task, Result: tr}:
			case <-w.cancelCtx.Done():
				w.activeTasks.Add(-1)
				return
			}

			w.activeTasks.Add(-1)
		}
	}
}
