package retrier

import (
	"context"
	"fmt"
	"sync"
	"time"
)

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
	// GetTasks Must return tasks with `NextRun` in the past and status is pending or suspended
	GetTasks() ([]Task, error)
	SaveTask(task *Task, result *TaskExecutionResult) error
}

// Logger specifies the logging capability required by the retry manager.
type Logger interface {
	Infof(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

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

	workers map[string]ManagerWorker
	tasks   map[string]chan Task

	unionQueue chan WorkerExecutionResult

	isWorking bool

	buffer        []WorkerExecutionResult
	maxBufferSize int

	fetchTaskTimeout time.Duration

	pTasksMtx      sync.RWMutex
	processedTasks map[string]interface{}
}

// NewManager initializes a new Manager with the required thread-safe structures and buffer configurations.
func NewManager(
	ctx context.Context,
	store Store,
	logger Logger,
	backOff BackOff,
	maxBufferSize int,
	fetchTaskTimeout time.Duration,
) *Manager {
	ctx, cancel := context.WithCancel(ctx)
	return &Manager{
		ctx:              ctx,
		cancel:           cancel,
		store:            store,
		logger:           logger,
		backOff:          backOff,
		workers:          make(map[string]ManagerWorker),
		tasks:            make(map[string]chan Task),
		buffer:           make([]WorkerExecutionResult, 0, maxBufferSize),
		unionQueue:       make(chan WorkerExecutionResult),
		maxBufferSize:    maxBufferSize,
		fetchTaskTimeout: fetchTaskTimeout,
		processedTasks:   make(map[string]interface{}),
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
		case <-m.ctx.Done():
			return
		case r := <-m.unionQueue:
			r.task.Retries++
			r.task.LastRun = r.result.RunAt
			if r.result.IsCritical {
				r.task.Status = StatusFailure
			} else {
				switch r.result.Status {
				case StatusFailure:
					if r.task.Retries == r.task.MaxRetries {
						r.task.Status = StatusFailure
					} else if r.task.Retries < r.task.MaxRetries {
						var errGetNextRun error
						r.task.Status = StatusPending
						r.task.NextRun, errGetNextRun = m.backOff.Get(r.task)
						if errGetNextRun != nil {
							m.logger.Errorf(
								"retrier: error: Getting next run for task id %v: %v", r.task.ID, errGetNextRun)
						}
					}
				case StatusSuccess:
					r.task.Status = StatusSuccess
				}
			}

			saveErr := m.store.SaveTask(r.task, r.result)

			// Unhold task
			m.pTasksMtx.Lock()
			delete(m.processedTasks, r.task.ID.String())
			m.pTasksMtx.Unlock()

			if saveErr != nil {
				m.logger.Errorf("save task failed: %v", saveErr)

				m.mtx.Lock()
				if len(m.buffer) >= m.maxBufferSize {
					m.logger.Errorf("retry manager: buffer size exceeds max buffer size, dropping task")
				} else {
					m.buffer = append(m.buffer, r)
				}
				m.mtx.Unlock()
			}
		}
	}
}

// Submit save task to store
func (m *Manager) Submit(task *Task) error {
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

	for name, w := range m.workers {
		w.Start()
		m.tasks[name] = make(chan Task)
		m.wg.Add(1)
		go m.resultCollector(w.GetOutChan())
	}

	m.wg.Add(3)
	go m.diskBufferSwap()
	go m.saveResult()
	go m.getRetriableTasks()
}

func (m *Manager) getRetriableTasks() {
	timer := time.NewTimer(m.fetchTaskTimeout)
	defer timer.Stop()
	defer m.wg.Done()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-timer.C:
			// If buffer not sync with store, we need to save all entities in buffer and get tasks after that
			if len(m.buffer) > 0 {
				continue
			}

			tasks, getErr := m.store.GetTasks()
			if getErr != nil {
				m.logger.Errorf("retrier: get tasks failed: %v", getErr)
			}

			// Skip processing tasks
			newTasks := make([]Task, 0, len(tasks))
			m.pTasksMtx.Lock()
			for _, t := range tasks {
				if _, exists := m.processedTasks[t.ID.String()]; exists {
					continue
				}
				newTasks = append(newTasks, t)
			}
			m.pTasksMtx.Unlock()

			for _, task := range newTasks {

				if worker, exists := m.workers[task.Worker]; exists {
					submitErr := worker.Submit(&task)
					if submitErr != nil {
						m.logger.Errorf("retrier: submit failed taskID: %s error: %v", task.ID.String(), submitErr)
					} else {
						// Hold task
						m.pTasksMtx.Lock()
						m.processedTasks[task.ID.String()] = true
						m.pTasksMtx.Unlock()
					}
				} else {
					tr := WorkerExecutionResult{
						task: &task,
						result: &TaskExecutionResult{
							ID:            getID(),
							TaskID:        task.ID,
							Status:        StatusFailure,
							RunAt:         time.Now(),
							Result:        []byte(fmt.Sprintf("retrier: unknown worker %s", task.Worker)),
							IsCritical:    true,
							ExecutionTime: 0,
						}}
					saveErr := m.store.SaveTask(&task, tr.result)
					if saveErr != nil {
						m.logger.Errorf("retrier: save task failed: %v", saveErr)
						if len(m.buffer) >= m.maxBufferSize {
							m.logger.Errorf("retrier: buffer size exceeds max buffer size, dropping task")
						} else {
							m.buffer = append(m.buffer, tr)
						}
					}
				}
			}
			timer.Reset(m.fetchTaskTimeout)
		}
	}
}

// diskBufferSwap runs on a periodic ticker to flush the temporary overflow buffer back into the store.
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

// flushBuffer iterates over buffered tasks and updates the storage, preserving failed attempts.
func (m *Manager) flushBuffer() {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	if len(m.buffer) == 0 {
		return
	}

	var failed []WorkerExecutionResult
	for _, t := range m.buffer {
		errSave := m.store.SaveTask(t.task, t.result)
		if errSave != nil {
			m.logger.Errorf("failed to save data from buffer: %v task: %v", errSave, t.task)
			failed = append(failed, t)
		}
	}
	m.buffer = failed
}

// Stop gracefully shuts down the manager, cascading signals down to components and blocking until finished.
func (m *Manager) Stop() {
	m.mtx.Lock()
	if !m.isWorking {
		m.mtx.Unlock()
		return
	}
	m.isWorking = false
	m.mtx.Unlock()

	m.cancel()

	m.mtx.Lock()
	for _, w := range m.workers {
		w.Stop()
	}
	m.mtx.Unlock()

	m.wg.Wait()
}

// RegisterWorker dynamically updates the manager routing pool with a new worker instance.
func (m *Manager) RegisterWorker(name string, w ManagerWorker) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	if _, exists := m.workers[name]; exists {
		return fmt.Errorf("worker %s already exists", name)
	}
	m.workers[name] = w
	if m.isWorking {
		m.workers[name].Start()
		m.tasks[name] = make(chan Task)
		m.wg.Add(1)
		go m.resultCollector(m.workers[name].GetOutChan())
	}
	return nil
}

// UnregisterWorker removes a worker from active routing tables and shuts it down.
func (m *Manager) UnregisterWorker(name string) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	if w, exists := m.workers[name]; exists {
		w.Stop()
		delete(m.workers, name)
	}
}

// GetWorkerStatuses extracts a thread-safe snapshot of the current state of all tracking workers.
func (m *Manager) GetWorkerStatuses() map[string]WorkerState {
	m.mtx.RLock()
	defer m.mtx.RUnlock()
	result := make(map[string]WorkerState, len(m.workers))
	for n, w := range m.workers {
		result[n] = w.GetStatus()
	}
	return result
}
