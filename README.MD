# 🔄 Retrier

**Retrier** is a Go library for building resilient, scalable task processing systems. It provides a dynamic worker pool, flexible retry policies, orchestration, circuit breakers, and real‑time event streaming.

**✨ What’s new in this version?**

-   **Worker** now requires an explicit `WorkerConfig` upon creation.
-   The methods `SetMinAndMaxWorkers` and `SetIdleTimeout` have been **removed** — all parameters are set via `NewWorkerCfg`.
-   `NewManager` now accepts `fetchTaskTimeout` and `fetchTaskTimeoutMax` for adaptive polling of the store.
-   `Manager` now fully supports `EventPublisher`, `Breaker`, and extended status reporting.

---

## 📦 Installation

```
go get github.com/devian2011/retrier
```

---

## 🧠 Core Concepts

### 1\. `Task`

-   `ID` (UUID) – unique identifier.
-   `Payload` (\[\]byte) – data to process.
-   `Worker` – name of the worker that should handle this task.
-   `Status` – lifecycle state (`pending`, `success`, `failure`, etc.).
-   `Retries` / `MaxRetries` – attempt counter and limit.
-   `BackOffCode` and `BackOffParams` – retry scheduling configuration.
-   `Deadline` (optional) – absolute time by which the task must complete.
-   Timestamps (`CreatedAt`, `LastRun`, `NextRun`).

### 2\. `TaskExecutionResult`

-   `ID` – unique result record identifier.
-   `TaskID` – reference to the parent task.
-   `Status` – success or failure of this attempt.
-   `RunAt` – execution timestamp.
-   `Result` (\[\]byte) – output or error message.
-   `IsCritical` – if `true`, the task is permanently failed.
-   `ExecutionTime` – duration of the worker call.

### 3\. `Worker` – the dynamic pool

A pool of goroutines that executes a user‑defined `WorkerFn`. Scales up when all workers are busy (up to `maxWorkers`) and scales down idle workers (down to `minWorkers`) after `idleTimeout`. Tasks are submitted via `Submit()`, results are read from the channel returned by `GetOutChan()`.

Important **The worker function now receives a `context.Context`**: `func(ctx context.Context, payload []byte) (string, *ExecutionError)`. This enables distributed tracing, cancellation, and deadline propagation.

#### New Worker instantiation

Instead of `NewWorker(ctx, workerFn)`, you now pass a configuration:

```go
cfg, _ := NewWorkerCfg(minWorkers, maxWorkers, idleTimeout)
w, err := NewWorker(ctx, cfg, workerFn)
```

### 4\. `Manager` – orchestrator

Manages multiple workers registered by name. Routes tasks to the correct worker, aggregates results, persists them via `Store`, and buffers them when storage fails. Integrates with **Circuit Breakers**, checks task **deadlines**, and publishes events via `EventPublisher`.

### 5\. `BackOffStrategy`

Built‑in strategies: `linear`, `jitter-linear`, `exponential`, `jitter-exponential`.

### 6\. `Circuit Breaker`

Sliding‑window failure‑rate based protection. Opens when the failure ratio exceeds a threshold, and allows probing after a timeout.

### 7\. `EventPublisher`

Receive real‑time notifications for every completed task execution. Useful for streaming to Kafka, webhooks, metrics, or downstream workflows.

### 8\. Task Deadlines

If a task has a `Deadline` and it passes before the task is picked up, it is immediately marked as failed with a critical error and **will not be retried**.

### 9\. Context support

The `WorkerFn` receives a `context.Context`, enabling: distributed tracing, cancellation, deadline propagation, and passing request‑scoped values.

---

## 💾 `Store` interface – persistence

The manager relies on a `Store` to persist tasks and results. To ensure correct retry scheduling, you must store at least:

-   `ID`, `Worker`, `Status`, `Retries`, `MaxRetries`
-   `BackOffCode`, `BackOffParams`, `CreatedAt`, `LastRun`, `NextRun`
-   `Payload`, `Deadline`

`GetTasks()` must return only tasks that are ready to execute (`Status` = `pending` or `suspended`, `NextRun` in the past).

`MemStore` is an in‑memory implementation for testing.

---

## 🚀 Quick Start

### Standalone worker (new API)

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
    workerFn := func(ctx context.Context, payload []byte) (string, *retrier.ExecutionError) {
        // use ctx for tracing, cancellation, deadlines
        result := fmt.Sprintf("processed: %s", string(payload))
        return result, nil
    }

    cfg, err := retrier.NewWorkerCfg(2, 10, 5*time.Second)
    if err != nil {
        panic(err)
    }

    ctx := context.Background()
    w, err := retrier.NewWorker(ctx, cfg, workerFn)
    if err != nil {
        panic(err)
    }

    w.Start()
    defer w.Stop()

    task := retrier.Task{
        ID:      retrier.GetID(),
        Worker:  "default",
        Payload: []byte("hello world"),
    }

    if err := w.Submit(&task); err != nil {
        log.Fatal(err)
    }

    select {
    case res := <-w.GetOutChan():
        fmt.Printf("Task %s finished with status %s\n", res.Task.ID, res.Result.Status)
    case <-time.After(2 * time.Second):
        log.Fatal("timeout")
    }
}
```

### Manager with multiple workers, circuit breaker, and event publisher

```go
package main

import (
    "context"
    "log"
    "time"
    "github.com/devian2011/retrier"
)

type myStore struct{}
func (s myStore) GetTasks() ([]retrier.Task, error) { /* ... */ }
func (s myStore) SaveTask(task *retrier.Task, result *retrier.TaskExecutionResult) error { /* ... */ }

type myLogger struct{}
func (l myLogger) Infof(format string, args ...interface{}) { /* ... */ }
func (l myLogger) Errorf(format string, args ...interface{}) { /* ... */ }

type myEventPublisher struct{}
func (p myEventPublisher) Publish(event retrier.WorkerExecutionResult) {
    log.Printf("Task %s finished with status %s", event.Task.ID, event.Result.Status)
}

func main() {
    ctx := context.Background()
    store := myStore{}
    logger := myLogger{}
    publisher := myEventPublisher{}

    // New Manager call – now with fetchTaskTimeout and fetchTaskTimeoutMax
    manager := retrier.NewManager(
        ctx,
        store,
        logger,
        retrier.NewBackOffStrategy(),
        1000,               // maxBufferSize
        5*time.Second,      // fetchTaskTimeout (initial poll interval)
        60*time.Second,     // fetchTaskTimeoutMax (max poll interval)
        publisher,
    )

    // Circuit breaker for worker "workerA"
    breakerA := retrier.NewCircuitBreaker(
        60*time.Second,  // window size
        0.5,             // failure threshold (50%)
        10,              // min requests
        30*time.Second,  // open-state timeout
    )

    cfgA, _ := retrier.NewWorkerCfg(1, 3, 10*time.Second)
    workerA, _ := retrier.NewWorker(ctx, cfgA, processA)
    manager.RegisterWorker("workerA", workerA, breakerA)

    // WorkerB without circuit breaker
    cfgB, _ := retrier.NewWorkerCfg(2, 5, 10*time.Second)
    workerB, _ := retrier.NewWorker(ctx, cfgB, processB)
    manager.RegisterWorker("workerB", workerB, nil)

    manager.Start()
    defer manager.Stop()

    task := retrier.Task{
        ID:      retrier.GetID(),
        Worker:  "workerA",
        Payload: []byte("data"),
    }
    if err := manager.Submit(&task); err != nil {
        log.Fatal(err)
    }
}
```

---

## ⚙️ Circuit Breaker configuration

-   `windowSize` – duration of the sliding window (e.g., 60s).
-   `failureThreshold` – failure ratio (0.0 – 1.0).
-   `minRequests` – minimum requests in the window before evaluation.
-   `timeout` – duration the circuit stays open before transitioning to `HalfOpen`.

---

## 📊 Extended status – `FullWorkerState`

`GetWorkerStatuses()` returns a map of worker names to `FullWorkerState`, which includes circuit breaker state:

```go
type FullWorkerState struct {
    Status        WorkerStatus        `json:"status"`
    ActiveTasks   int32               `json:"active_tasks"`
    ActiveWorkers int32               `json:"active_workers"`
    CBState       CircuitBreakerState `json:"cb_state"`
}
```

---

## ⏱ Configuring retries with backoff

Example task with exponential backoff and jitter:

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

### Using deadlines

```go
task := retrier.Task{
    ID:         retrier.GetID(),
    Worker:     "myWorker",
    Payload:    []byte("time-sensitive job"),
    MaxRetries: 3,
    Deadline:   time.Now().Add(30 * time.Second), // must start within 30s
}
```

---

## 🧩 Lifecycle methods

| Method | Description |
| --- | --- |
| `Worker.Start()` | Starts the minimum number of workers, status → `Running`. |
| `Worker.Suspend()` | Suspends accepting new tasks; active tasks continue. |
| `Worker.Stop()` | Stops all workers, closes channels, status → `Stopped`. |
| `Manager.Start()` | Starts all registered workers and internal pipelines. |
| `Manager.Stop()` | Stops workers, flushes buffer, waits for all goroutines. |

---

## 🛠 Custom backoff strategies

```go
strategy := retrier.NewBackOffStrategy()
strategy.Register("my-custom", func(params retrier.BackOffParams) (time.Time, error) {
    // params.GetRetries(), params.GetBackOffParams()...
    return time.Now().Add(10 * time.Second), nil
})
```

---

## 🧪 Testing

```
go test -v ./...
```
