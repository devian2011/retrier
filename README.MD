Retrier – Task Processing with Retries, Circuit Breakers & Event Publishing body { max-width: 1000px; margin: 2rem auto; padding: 0 1.5rem; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; line-height: 1.7; color: #1e1e1e; background: #fafafa; } h1, h2, h3 { margin-top: 1.8em; margin-bottom: 0.6em; border-bottom: 1px solid #eaecef; padding-bottom: 0.3em; } h1 { border-bottom: 2px solid #2c3e50; } code { background: #f0f2f4; padding: 0.2em 0.4em; border-radius: 3px; font-size: 0.9em; color: #c7254e; } pre { background: #f6f8fa; padding: 1em 1.2em; overflow: auto; border-radius: 6px; border-left: 4px solid #2c3e50; font-size: 0.9em; } pre code { background: none; padding: 0; color: #1e1e1e; } table { border-collapse: collapse; width: 100%; margin: 1em 0; } th, td { border: 1px solid #d0d7de; padding: 0.5em 0.8em; text-align: left; } th { background: #e6eaef; } hr { border: 0; border-top: 1px solid #d0d7de; margin: 2em 0; } .badge { background: #2c3e50; color: white; padding: 0.2em 0.6em; border-radius: 12px; font-size: 0.8em; font-weight: 600; } .highlight { background: #fef9e7; padding: 0.1em 0.3em; border-radius: 3px; }

# Retrier

**Retrier** is a Go library for building resilient, scalable task processing systems. It provides:

-   A **dynamic worker pool** that scales up/down based on load.
-   **Pluggable backoff strategies** (linear, exponential, with/without jitter) for retries.
-   An **orchestration manager** that routes tasks to named workers, aggregates results, and buffers them when storage is unavailable.
-   **Graceful lifecycle** controls (start, stop, suspend) with context propagation.
-   **Circuit Breaker** with sliding window failure rate detection to protect workers from cascading failures.
-   **Event publishing** – receive real-time notifications for every completed task execution.
-   **Enhanced status** – query worker statuses including circuit breaker state.

---

## Features

-   **Automatic worker scaling** – configurable `min`/`max` goroutines, idle timeouts for scale‑down.
-   **Retry policies** – built‑in linear, exponential, and jitter‑based backoff; custom strategies can be registered.
-   **Multi‑worker manager** – each task type can have its own worker pool, all coordinated by a `Manager`.
-   **Result streaming** – consume execution outcomes via channels for real‑time monitoring.
-   **Storage buffering** – if the persistent store (`Store`) fails, results are kept in memory and flushed periodically.
-   **Circuit Breaker** – sliding window failure‑rate based protection. Automatically opens when failure ratio exceeds a threshold, and allows probing after a timeout.
-   **Event Publisher** – implement `EventPublisher` to receive every task result as soon as it is processed (e.g., for notifications, metrics, or logging).
-   **Comprehensive status** – `GetWorkerStatuses()` now returns `FullWorkerState` which includes the circuit breaker state for each worker.
-   **Thread‑safe** – all exported methods are safe for concurrent use.

---

## Installation

```
go get github.com/devian2011/retrier
```

---

## Core Concepts

### 1\. `Task`

Represents a unit of work. It includes:

-   `ID` (UUID) – unique identifier.
-   `Payload` (byte slice) – the actual data to process.
-   `Worker` – the named worker that should process this task.
-   `Status` – current lifecycle state (`pending`, `success`, `failure`, etc.).
-   `Retries` / `MaxRetries` – attempt counter and limit.
-   `BackOffCode` and `BackOffParams` – retry scheduling configuration.
-   Timestamps (`CreatedAt`, `LastRun`, `NextRun`) – for scheduling and monitoring.

### 2\. `TaskExecutionResult`

Records the outcome of a single execution attempt. Contains:

-   `ID` (UUID) – unique result record identifier.
-   `TaskID` – reference to the parent `Task`.
-   `Status` – success or failure of this specific attempt.
-   `RunAt` – timestamp of the execution.
-   `Result` (\[\]byte) – the output or error message from the worker.
-   `IsCritical` – if true, the task is permanently failed and won't be retried.
-   `ExecutionTime` – duration of the worker function call.

### 3\. `Worker`

A pool of goroutines that executes a user‑defined `WorkerFn`.  
It scales up when all workers are busy (up to `maxWorkers`) and scales down idle workers (down to `minWorkers`) after `idleTimeout`.  
Tasks are submitted via `Submit()`, results are emitted on a channel obtained via `GetOutChan()`.

### 4\. `Manager`

Orchestrates multiple workers registered by name. It:

-   Routes tasks to the correct worker (by `task.Worker`).
-   Aggregates results from all workers into a central channel.
-   Calls `Store.SaveTask()` to persist each result.
-   Buffers results in memory if storage fails, and periodically retries flushing.
-   Integrates with **Circuit Breakers** per worker to prevent overloading failing services.
-   Publishes every completed result via an optional `EventPublisher`.

### 5\. `BackOffStrategy`

A registry of functions that compute the next execution time (`time.Time`) based on a `BackOffParams` object (which `Task` implements).  
Built‑in strategies:

-   `linear` – fixed delay (`duration`)
-   `jitter-linear` – delay + random jitter (default 0.4)
-   `exponential` – `baseDelay * multiplier^(retries-1)`, capped at `maxDelay`
-   `jitter-exponential` – same as exponential with additional jitter (default 0.2)

### 6\. `Circuit Breaker`

A sliding‑window circuit breaker that protects a worker from repeated failures. It tracks success/failure counts over a configurable time window and opens the circuit when the failure rate exceeds a given threshold. While open, requests are rejected (skipped) for that worker. After a timeout, the breaker transitions to `HalfOpen` to allow a single probe request; if it succeeds, the circuit closes; otherwise it re‑opens.

The `Manager` uses a `Breaker` interface, so you can plug in any implementation. The default implementation uses a sliding window with configurable window size, failure threshold, minimum request count, and open‑state timeout.

### 7\. `EventPublisher`

An optional interface that allows you to receive real‑time notifications for every completed task execution.

```go
type EventPublisher interface {
    Publish(event WorkerExecutionResult)
}
```

If you pass a non‑nil publisher to `NewManager`, the `Manager` will call `Publish` after persisting the result (but before removing it from the “in‑flight” set). This can be used for:

-   Streaming results to a message queue (e.g., Kafka, RabbitMQ).
-   Sending webhooks or notifications.
-   Feeding metrics and monitoring systems.
-   Triggering downstream workflows.

---

## Persistence – Store Interface

The `Manager` relies on a `Store` interface to persist tasks and results. To implement your own storage backend, you need to understand what fields are used for retry scheduling.

```go
type Store interface {
    // GetTasks must return tasks that are eligible for execution:
    //   - Status is either "pending" or "suspended"
    //   - NextRun is in the past (or zero)
    GetTasks() ([]Task, error)

    // SaveTask persists a task and its execution result.
    // If result is nil, it means the task is being saved for the first time (submit).
    SaveTask(task *Task, result *TaskExecutionResult) error
}
```

**What to store for retry functionality?**

When you store a `Task`, you must preserve at least the following fields so that the `Manager` can correctly schedule retries:

-   `ID` – to identify the task.
-   `Worker` – which worker pool to route to.
-   `Status` – pending, suspended, success, or failure.
-   `Retries` – current attempt count.
-   `MaxRetries` – maximum allowed attempts.
-   `BackOffCode` and `BackOffParams` – used to compute `NextRun`.
-   `CreatedAt`, `LastRun`, `NextRun` – scheduling timestamps.
-   `Payload` – the actual data to be processed.

Additionally, each `TaskExecutionResult` should be stored to keep a history of attempts. The `SaveTask` method receives both the updated task and the result of the latest attempt. The manager updates the task’s `Retries`, `LastRun`, `NextRun`, and `Status` **before** calling `SaveTask`. Therefore, your store should simply persist the given `Task` and `TaskExecutionResult` as is.

**Important:** The manager expects `GetTasks()` to return only tasks that are ready to be executed (i.e., `Status` is `pending` or `suspended` and `NextRun` is in the past). This is crucial for correct retry timing.

---

## Quick Start

### Creating a standalone worker

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/devian2011/retrier"
)

func main() {
	// Define your processing function.
	workerFn := func(payload []byte) (string, *retrier.ExecutionError) {
		// Do some work...
		result := fmt.Sprintf("processed: %s", string(payload))
		return result, nil
	}

	ctx := context.Background()
	w := retrier.NewWorker(ctx, workerFn)
	w.SetMinAndMaxWorkers(2, 10)
	w.SetIdleTimeout(5 * time.Second)
	w.Start()
	defer w.Stop()

	task := retrier.Task{
		ID:      retrier.GetID(),
		Worker:  "default",
		Payload: []byte("hello world"),
	}

	if err := w.Submit(task); err != nil {
		log.Fatal(err)
	}

	// Read the result.
	select {
	case res := <-w.GetOutChan():
		fmt.Printf("Task %s finished with status %s\n", res.Task.ID, res.Result.Status)
	case <-time.After(2 * time.Second):
		log.Fatal("timeout")
	}
}
```

### Using the Manager with multiple workers, circuit breakers, and event publisher

```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/devian2011/retrier"
)

// Implement your own Store, Logger, and EventPublisher.
type myStore struct{}
func (s myStore) GetTasks() ([]retrier.Task, error) { /* ... */ }
func (s myStore) SaveTask(task *retrier.Task, result *retrier.TaskExecutionResult) error { /* ... */ }

type myLogger struct{}
func (l myLogger) Infof(format string, args ...interface{}) { /* ... */ }
func (l myLogger) Errorf(format string, args ...interface{}) { /* ... */ }

type myEventPublisher struct{}
func (p myEventPublisher) Publish(event retrier.WorkerExecutionResult) {
    // e.g., send to Kafka, emit metrics, etc.
    log.Printf("Task %s finished with status %s", event.Task.ID, event.Result.Status)
}

func main() {
	ctx := context.Background()
	store := myStore{}
	logger := myLogger{}
	publisher := myEventPublisher{}

	manager := retrier.NewManager(
        ctx,
        store,
        logger,
        retrier.NewBackOffStrategy(),
        1000,               // max buffer size
        5*time.Second,      // fetch interval
        publisher,          // can be nil if not needed
    )

	// Create a circuit breaker for workerA
	breakerA := retrier.NewCircuitBreaker(
		60*time.Second,  // window size
		0.5,             // failure threshold (50%)
		10,              // minimum requests in window
		30*time.Second,  // open-state timeout
	)

	workerA := retrier.NewWorker(ctx, processA)
	workerA.SetMinAndMaxWorkers(1, 3)
	manager.RegisterWorker("workerA", workerA, breakerA)

	// WorkerB without circuit breaker (pass nil)
	workerB := retrier.NewWorker(ctx, processB)
	workerB.SetMinAndMaxWorkers(2, 5)
	manager.RegisterWorker("workerB", workerB, nil)

	manager.Start()
	defer manager.Stop()

	task := retrier.Task{
		ID:      retrier.GetID(),
		Worker:  "workerA",
		Payload: []byte("data"),
	}
	if err := manager.Submit(task); err != nil {
		log.Fatal(err)
	}
}
```

---

## Circuit Breaker Configuration

The default circuit breaker implementation (`CircuitBreaker`) is configured with four parameters:

-   **windowSize** – duration of the sliding window over which success/failure counts are tracked.
-   **failureThreshold** – ratio (0.0 to 1.0) of failures to total requests that triggers the open state.
-   **minRequests** – minimum number of requests in the window before the threshold is evaluated.
-   **timeout** – duration the circuit stays open before transitioning to half‑open.

Example: `NewCircuitBreaker(60*time.Second, 0.5, 10, 30*time.Second)`

When a worker is registered with a breaker, the `Manager` will automatically check `Allow()` before submitting a task. If the breaker is open, the task is skipped (and will be retried later). After each submission attempt, `RecordSuccess()` or `RecordFailure()` is called to update the breaker’s statistics.

If you pass `nil` as the breaker, the manager will skip circuit‑breaker checks entirely.

---

## Enhanced Status – FullWorkerState

The `Manager` now provides a richer status snapshot via `GetWorkerStatuses()`, which returns a map of worker names to `FullWorkerState`:

```go
type FullWorkerState struct {
    Status        WorkerStatus        `json:"status"`
    ActiveTasks   int32               `json:"active_tasks"`
    ActiveWorkers int32               `json:"active_workers"`
    CBState       CircuitBreakerState `json:"cb_state"`
}
```

This includes both the worker’s own status (`running`, `stopped`, etc.) and the current state of its circuit breaker (`closed`, `open`, `half-open`). This helps you monitor system health and detect issues with specific workers.

---

## Configuring Retries with Backoff

Set the `BackOffCode` and `BackOffParams` on a `Task`. For example, an exponential backoff with jitter:

```go
task := retrier.Task{
	ID:           retrier.GetID(),
	Worker:       "myWorker",
	Payload:      []byte("important task"),
	Retries:      0,
	MaxRetries:   5,
	BackOffCode:  retrier.JitterExponentialBackoff,
	BackOffParams: map[retrier.BackOffParam]interface{}{
		retrier.BaseDelayKey:  1 * time.Second,
		retrier.MultiplierKey: 2.0,
		retrier.MaxDelayKey:   5 * time.Minute,
		retrier.JitterKey:     0.3,
	},
}
```

When a task fails with a `UsualState` error, the `Manager` (or your custom logic) can call `BackOffStrategy.Get(task)` to compute the next `NextRun` time and reschedule it.

---

## Error Handling

Your `WorkerFn` returns a `(string, *ExecutionError)`. The `ExecutionError` carries:

-   `Err` – the underlying error.
-   `State` – either `CriticalState` (permanent failure, no retry) or `UsualState` (transient, should retry).

Example:

```go
workerFn := func(payload []byte) (string, *retrier.ExecutionError) {
	if string(payload) == "invalid" {
		return "", &retrier.ExecutionError{
			Err:   errors.New("validation failed"),
			State: retrier.CriticalState,
		}
	}
	// ... simulate network error
	return "", &retrier.ExecutionError{
		Err:   errors.New("timeout"),
		State: retrier.UsualState,
	}
}
```

---

## Lifecycle Methods

| Method | Description |
| --- | --- |
| `Worker.Start()` | Spawns the minimum number of workers and sets status to `Running`. |
| `Worker.Suspend()` | Changes status to `Suspended`; new submissions are rejected, but active tasks run to completion. |
| `Worker.Stop()` | Cancels the internal context, waits for all goroutines to finish, closes channels, status becomes `Stopped`. |
| `Manager.Start()` | Starts all registered workers, launches collector and splitter pipelines. |
| `Manager.Stop()` | Stops all workers, cancels manager context, waits for pipelines. |

---

## Storage Buffering in Manager

The `Manager` uses a `Store` interface as described above. If `SaveTask` returns an error, the result is appended to an in‑memory buffer (up to `maxBufferSize`). A background goroutine (`diskBufferSwap`) attempts to flush the buffer every minute. You can customise the buffer size and flush interval (currently fixed at 1 minute; you can change it by modifying the source if needed).

---

## Custom Backoff Strategies

Register your own strategy with the global `BackOffStrategy`:

```go
strategy := retrier.NewBackOffStrategy()
strategy.Register("my-custom", func(params retrier.BackOffParams) (time.Time, error) {
	// Compute next time based on params.GetRetries(), params.GetBackOffParams()...
	return time.Now().Add(10 * time.Second), nil
})
```

Then set `task.BackOffCode = "my-custom"`.

---

## Full Example with Retry Loop

```go
ctx := context.Background()

workerFn := func(payload []byte) (string, *retrier.ExecutionError) {
	// Simulate transient failure.
	return "", &retrier.ExecutionError{
		Err:   errors.New("service unavailable"),
		State: retrier.UsualState,
	}
}

w := retrier.NewWorker(ctx, workerFn)
w.SetMinAndMaxWorkers(1, 2)
w.Start()
defer w.Stop()

task := retrier.Task{
	ID:           retrier.GetID(),
	Worker:       "default",
	Payload:      []byte("retry me"),
	Retries:      0,
	MaxRetries:   3,
	BackOffCode:  retrier.ExponentialBackOff,
	BackOffParams: map[retrier.BackOffParam]interface{}{
		retrier.BaseDelayKey:  500 * time.Millisecond,
		retrier.MultiplierKey: 2.0,
		retrier.MaxDelayKey:   10 * time.Second,
	},
}

w.Submit(task)

// In a real system, the manager would handle rescheduling.
// Here we simulate a simple retry loop.
for i := 0; i <= task.MaxRetries; i++ {
	res := <-w.GetOutChan()
	if res.Result.Status == retrier.StatusSuccess {
		fmt.Println("Task succeeded")
		break
	}
	// Compute next run using the backoff strategy.
	strategy := retrier.NewBackOffStrategy()
	nextTime, _ := strategy.Get(&task)
	fmt.Printf("Retry attempt %d scheduled at %s\n", i+1, nextTime)
	// In practice, you would update task.Retries and resubmit.
}
```

---

## Testing

The package includes a complete test suite. Run:

```
go test -v ./...
```

---

_This README describes the entire `retrier` package. For API details, refer to the Go doc comments in the source files._