# Retrier

**Retrier** is a Go library for building resilient, scalable task processing systems. It provides:

-   A **dynamic worker pool** that scales up/down based on load.
-   **Pluggable backoff strategies** (linear, exponential, with/without jitter) for retries.
-   An **orchestration manager** that routes tasks to named workers, aggregates results, and buffers them when storage is unavailable.
-   **Graceful lifecycle** controls (start, stop, suspend) with context propagation.

---

## Features

-   **Automatic worker scaling** – configurable `min`/`max` goroutines, idle timeouts for scale‑down.
-   **Retry policies** – built‑in linear, exponential, and jitter‑based backoff; custom strategies can be registered.
-   **Multi‑worker manager** – each task type can have its own worker pool, all coordinated by a `Manager`.
-   **Result streaming** – consume execution outcomes via channels for real‑time monitoring.
-   **Storage buffering** – if the persistent store (`Store`) fails, results are kept in memory and flushed periodically.
-   **Thread‑safe** – all exported methods are safe for concurrent use.

---

## Installation

```
go get github.com/yourusername/retrier
```

---

## Core Concepts

### 1\. `Task`

Represents a unit of work. It includes:

-   `ID` (UUID)
-   `Payload` (byte slice)
-   `Worker` – the named worker that should process it
-   `Status` (pending, success, failure, etc.)
-   `Retries` / `MaxRetries`
-   `BackOffCode` and `BackOffParams` – for retry scheduling
-   Timestamps (`CreatedAt`, `LastRun`, `NextRun`)

### 2\. `Worker`

A pool of goroutines that executes a user‑defined `WorkerFn`.  
It scales up when all workers are busy (up to `maxWorkers`) and scales down idle workers (down to `minWorkers`) after `idleTimeout`.  
Tasks are submitted via `Submit()`, results are emitted on a channel obtained via `GetOutChan()`.

### 3\. `Manager`

Orchestrates multiple workers registered by name. It:

-   Routes tasks to the correct worker (by `task.Worker`).
-   Aggregates results from all workers into a central channel.
-   Calls `Store.SaveTask()` to persist each result.
-   Buffers results in memory if storage fails, and periodically retries flushing.

### 4\. `BackOffStrategy`

A registry of functions that compute the next execution time (`time.Time`) based on a `BackOffParams` object (which `Task` implements).  
Built‑in strategies:

-   `linear` – fixed delay (`duration`)
-   `jitter-linear` – delay + random jitter (default 0.4)
-   `exponential` – `baseDelay * multiplier^(retries-1)`, capped at `maxDelay`
-   `jitter-exponential` – same as exponential with additional jitter (default 0.2)

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

	"github.com/yourusername/retrier"
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

### Using the Manager with multiple workers and storage

```go
package main

import (
	"context"
	"log"

	"github.com/yourusername/retrier"
)

// Implement your own Store and Logger.
type myStore struct{}
func (s myStore) GetTasks() ([]retrier.Task, error) { /* ... */ }
func (s myStore) SaveTask(task retrier.Task, result retrier.TaskExecutionResult) error { /* ... */ }

type myLogger struct{}
func (l myLogger) Infof(format string, args ...interface{}) { /* ... */ }
func (l myLogger) Errorf(format string, args ...interface{}) { /* ... */ }

func main() {
	ctx := context.Background()
	store := myStore{}
	logger := myLogger{}

	manager := retrier.NewManager(ctx, store, logger, 1000, 5) // buffer size, store workers

	// Create workers.
	workerA := retrier.NewWorker(ctx, processA)
	workerA.SetMinAndMaxWorkers(1, 3)
	manager.RegisterWorker("workerA", workerA)

	workerB := retrier.NewWorker(ctx, processB)
	workerB.SetMinAndMaxWorkers(2, 5)
	manager.RegisterWorker("workerB", workerB)

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

The `Manager` uses a `Store` interface:

```go
type Store interface {
	GetTasks() ([]Task, error)
	SaveTask(task Task, result TaskExecutionResult) error
}
```

If `SaveTask` returns an error, the result is appended to an in‑memory buffer (up to `maxBufferSize`). A background goroutine (`diskBufferSwap`) attempts to flush the buffer every minute. You can customise the buffer size and flush interval (currently fixed at 1 minute; you can change it by modifying the source if needed).

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
