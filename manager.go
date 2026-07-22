package retrier

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// EventPublisher if we need to get task and task results immediately we can add published for send tasks
type EventPublisher interface {
	Publish(event WorkerExecutionResult)
}

// Breaker defines the public interface for a circuit breaker.
type Breaker interface {
	Allow() bool
	RecordSuccess()
	RecordFailure()
	State() CircuitBreakerState
	Reset()
}

// ManagerWorker defines the contract for a component capable of processing tasks
// and streaming execution results asynchronously.
type ManagerWorker interface {
	Start()
	Stop()
	Submit(t *Task) error
	GetOutChan() chan WorkerExecutionResult
	GetStatus() WorkerState
}

// Store abstracts persistence layer operations for logging and auditing task outcomes.
type Store interface {
	GetTasks() ([]Task, error)
	SaveTask(task *Task, result *TaskExecutionResult) error
}

// Logger specifies the logging capability required by the retry manager.
type Logger interface {
	Infof(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

// BackOff defines the interface for computing the next execution time based on backoff parameters.
type BackOff interface {
	Get(backOff BackOffParams) (time.Time, error)
}

// Manager orchestrates worker pools, routes execution results to storage,
// and buffers data locally in-memory during storage outages.
type Manager struct {
	ctx    context.Context
	cancel context.CancelFunc
	mtx    sync.RWMutex
	wg     sync.WaitGroup

	store   Store
	logger  Logger
	backOff BackOff

	breakers map[string]Breaker
	workers  map[string]ManagerWorker

	unionQueue chan WorkerExecutionResult

	isWorking bool

	bufferMtx     sync.Mutex
	buffer        []WorkerExecutionResult
	maxBufferSize int

	fetchTaskTimeout time.Duration

	pTasksMtx      sync.RWMutex
	processedTasks map[string]interface{}

	eventPublisher EventPublisher
}

// NewManager initializes a new Manager with the required dependencies and configurations.
func NewManager(
	ctx context.Context,
	store Store,
	logger Logger,
	backOff BackOff,
	maxBufferSize int,
	fetchTaskTimeout time.Duration,
	eventPublisher EventPublisher,
) *Manager {
	ctx, cancel := context.WithCancel(ctx)
	return &Manager{
		ctx:              ctx,
		cancel:           cancel,
		store:            store,
		logger:           logger,
		backOff:          backOff,
		breakers:         make(map[string]Breaker),
		workers:          make(map[string]ManagerWorker),
		buffer:           make([]WorkerExecutionResult, 0, maxBufferSize),
		unionQueue:       make(chan WorkerExecutionResult),
		maxBufferSize:    maxBufferSize,
		fetchTaskTimeout: fetchTaskTimeout,
		processedTasks:   make(map[string]interface{}),
		eventPublisher:   eventPublisher,
	}
}

// resultCollector monitors individual worker channels and aggregates results into a central channel.
func (m *Manager) resultCollector(results chan WorkerExecutionResult) {
	defer m.wg.Done()
	for {
		select {
		case <-m.ctx.Done():
			return
		case r, ok := <-results:
			if !ok {
				return
			}
			select {
			case m.unionQueue <- r:
			case <-m.ctx.Done():
				return
			}
		}
	}
}

// saveResult processes incoming task execution results, attempting to store them
// and routing them to an in-memory buffer if the store fails.
func (m *Manager) saveResult() {
	defer m.wg.Done()
	for {
		select {
		case r, ok := <-m.unionQueue:
			if !ok {
				return
			}

			r.task.Retries++
			r.task.LastRun = r.result.RunAt

			if r.result.IsCritical {
				r.task.Status = StatusFailure
			} else {
				switch r.result.Status {
				case StatusFailure:
					if r.task.Retries >= r.task.MaxRetries {
						r.task.Status = StatusFailure
					} else if r.task.Retries < r.task.MaxRetries {
						var err error
						r.task.Status = StatusPending
						r.task.NextRun, err = m.backOff.Get(r.task)
						if err != nil {
							m.logger.Errorf(
								"retrier: error: Getting next run for task id %v: %v", r.task.ID, err)
						}
					}
				case StatusSuccess:
					r.task.Status = StatusSuccess
				}
			}

			saveErr := m.store.SaveTask(r.task, r.result)

			if m.eventPublisher != nil {
				m.eventPublisher.Publish(r)
			}

			m.pTasksMtx.Lock()
			delete(m.processedTasks, r.task.ID.String())
			m.pTasksMtx.Unlock()

			if saveErr != nil {
				m.logger.Errorf("save task failed: %v", saveErr)
				m.bufferMtx.Lock()
				if len(m.buffer) >= m.maxBufferSize {
					m.logger.Errorf("retry manager: buffer size exceeds max buffer size, dropping task")
				} else {
					m.buffer = append(m.buffer, r)
				}
				m.bufferMtx.Unlock()
			}
		case <-m.ctx.Done():
			return
		}
	}
}

// Submit saves a task to the store without executing it immediately.
func (m *Manager) Submit(task *Task) error {
	if task.IsFinished() {
		return fmt.Errorf("task already finished")
	}

	return m.store.SaveTask(task, nil)
}

// Start boots the manager, launches all registered workers, and spins up pipeline routines.
func (m *Manager) Start() {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	if m.isWorking {
		return
	}
	m.isWorking = true

	for _, w := range m.workers {
		w.Start()
		m.wg.Add(1)
		go m.resultCollector(w.GetOutChan())
	}

	m.wg.Add(3)
	go m.diskBufferSwap()
	go m.saveResult()
	go m.getRetriableTasks()
}

// getRetriableTasks periodically fetches tasks that are due for execution
// and submits them to the appropriate worker, subject to circuit breaker checks.
func (m *Manager) getRetriableTasks() {
	timer := time.NewTimer(m.fetchTaskTimeout)
	defer timer.Stop()
	defer m.wg.Done()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-timer.C:
			m.bufferMtx.Lock()
			if len(m.buffer) > 0 {
				continue
			}
			m.bufferMtx.Unlock()

			tasks, getErr := m.store.GetTasks()
			if getErr != nil {
				m.logger.Errorf("retrier: get tasks failed: %v", getErr)
				timer.Reset(m.fetchTaskTimeout)
				continue
			}

			newTasks := make([]*Task, 0, len(tasks))
			now := time.Now()
			m.pTasksMtx.RLock()
			for _, t := range tasks {
				t := t
				// Skip finished tasks and tasks with NextRun in the future (not ready)
				if t.IsFinished() || (!t.NextRun.IsZero() && t.NextRun.After(now)) {
					continue
				}
				if _, exists := m.processedTasks[t.ID.String()]; !exists {
					newTasks = append(newTasks, &t)
				}
			}
			m.pTasksMtx.RUnlock()

			for _, task := range newTasks {
				if !task.Deadline.IsZero() && task.Deadline.Before(now) {
					m.saveBadWorkerTask(task, []byte("retrier: deadline exceeded"))
					continue
				}

				worker, exists := m.workers[task.Worker]
				if !exists {
					m.saveBadWorkerTask(task, []byte(fmt.Sprintf("retrier: unknown worker %s", task.Worker)))
					continue
				}

				// Retrieve circuit breaker for this worker (optional).
				cb, exists := m.breakers[task.Worker]
				if exists && cb != nil {
					if !cb.Allow() {
						m.logger.Infof("circuit breaker open for worker %s, skipping task %s",
							task.Worker, task.ID.String())
						continue
					}
				}

				submitErr := worker.Submit(task)
				if submitErr != nil {
					if exists && cb != nil {
						cb.RecordFailure()
					}
					m.logger.Errorf("retrier: submit failed taskID: %s error: %v",
						task.ID.String(), submitErr)
				} else {
					if exists && cb != nil {
						cb.RecordSuccess()
					}
					m.pTasksMtx.Lock()
					m.processedTasks[task.ID.String()] = true
					m.pTasksMtx.Unlock()
				}
			}
			timer.Reset(m.fetchTaskTimeout)
		}
	}
}

// saveBadWorkerTask handles tasks that reference a non-existent worker.
func (m *Manager) saveBadWorkerTask(task *Task, message []byte) {
	tr := WorkerExecutionResult{
		task: task,
		result: &TaskExecutionResult{
			ID:            getID(),
			TaskID:        task.ID,
			Status:        StatusFailure,
			RunAt:         time.Now(),
			Result:        message,
			IsCritical:    true,
			ExecutionTime: 0,
		},
	}
	saveErr := m.store.SaveTask(task, tr.result)
	if m.eventPublisher != nil {
		m.eventPublisher.Publish(tr)
	}
	if saveErr != nil {
		m.logger.Errorf("retrier: save task failed: %v", saveErr)
		m.bufferMtx.Lock()
		if len(m.buffer) >= m.maxBufferSize {
			m.logger.Errorf("retrier: buffer size exceeds max buffer size, dropping task")
		} else {
			m.buffer = append(m.buffer, tr)
		}
		m.bufferMtx.Unlock()
	}
}

// diskBufferSwap periodically flushes the in-memory buffer to the store.
func (m *Manager) diskBufferSwap() {
	defer m.wg.Done()
	timer := time.NewTimer(time.Minute)
	defer timer.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-timer.C:
			m.flushBuffer()
			timer.Reset(time.Minute)
		}
	}
}

// flushBuffer iterates over buffered items and saves them to the store.
func (m *Manager) flushBuffer() {
	m.bufferMtx.Lock()
	defer m.bufferMtx.Unlock()

	if len(m.buffer) == 0 {
		return
	}

	var failed []WorkerExecutionResult
	for _, t := range m.buffer {
		if err := m.store.SaveTask(t.task, t.result); err != nil {
			m.logger.Errorf("failed to save data from buffer: %v task: %v", err, t.task)
			failed = append(failed, t)
		}
	}
	m.buffer = failed
}

// Stop gracefully shuts down the manager, ensures all results are persisted,
// and waits for all goroutines to finish.
func (m *Manager) Stop() {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	if !m.isWorking {
		return
	}
	m.isWorking = false
	for _, w := range m.workers {
		w.Stop()
	}
	m.flushBuffer()
	m.cancel()
	close(m.unionQueue)

	m.wg.Wait()
}

// RegisterWorker adds a new worker with an optional circuit breaker.
func (m *Manager) RegisterWorker(name string, w ManagerWorker, b Breaker) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	if _, exists := m.workers[name]; exists {
		return fmt.Errorf("worker %s already exists", name)
	}

	m.workers[name] = w
	if b != nil {
		m.breakers[name] = b
	}

	if m.isWorking {
		m.workers[name].Start()
		m.wg.Add(1)
		go m.resultCollector(m.workers[name].GetOutChan())
	}
	return nil
}

// UnregisterWorker removes a worker and its associated circuit breaker.
func (m *Manager) UnregisterWorker(name string) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	if w, exists := m.workers[name]; exists {
		w.Stop()
		delete(m.workers, name)
		delete(m.breakers, name)
	}
}

// FullWorkerState worker state with data from Circuit Breaker
type FullWorkerState struct {
	Status        WorkerStatus        `json:"status"`
	ActiveTasks   int32               `json:"active_tasks"`
	ActiveWorkers int32               `json:"active_workers"`
	CBState       CircuitBreakerState `json:"cb_state"`
}

// GetWorkerStatuses returns a snapshot of the current state of all registered workers.
func (m *Manager) GetWorkerStatuses() map[string]FullWorkerState {
	m.mtx.RLock()
	defer m.mtx.RUnlock()
	result := make(map[string]FullWorkerState, len(m.workers))
	for n, w := range m.workers {
		ws := w.GetStatus()
		var cbState CircuitBreakerState
		if cb, exists := m.breakers[n]; exists {
			cbState = cb.State()
		}
		result[n] = FullWorkerState{
			Status:        ws.Status,
			ActiveTasks:   ws.ActiveTasks,
			ActiveWorkers: ws.ActiveWorkers,
			CBState:       cbState,
		}
	}
	return result
}
