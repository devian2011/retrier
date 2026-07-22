package retrier

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

// --- Thread-safe mock breaker ---

type mockBreaker struct {
	mu            sync.Mutex
	allow         bool
	failures      int
	successes     int
	state         CircuitBreakerState
	recordFailure func()
	recordSuccess func()
}

func (m *mockBreaker) Allow() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.allow
}
func (m *mockBreaker) RecordSuccess() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.successes++
	if m.recordSuccess != nil {
		m.recordSuccess()
	}
}
func (m *mockBreaker) RecordFailure() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failures++
	if m.recordFailure != nil {
		m.recordFailure()
	}
}
func (m *mockBreaker) State() CircuitBreakerState {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state
}
func (m *mockBreaker) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state = StateClosed
	m.failures = 0
	m.successes = 0
	m.allow = true
}
func (m *mockBreaker) GetFailures() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.failures
}
func (m *mockBreaker) GetSuccesses() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.successes
}

// --- Mock EventPublisher ---

type mockEventPublisher struct {
	mu     sync.Mutex
	events []WorkerExecutionResult
}

func (m *mockEventPublisher) Publish(event WorkerExecutionResult) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, event)
}
func (m *mockEventPublisher) GetEvents() []WorkerExecutionResult {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.events
}
func (m *mockEventPublisher) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = nil
}

// --- Mocks ---

type mockManagerWorker struct {
	mu             sync.Mutex
	status         WorkerState
	outChan        chan WorkerExecutionResult
	subErr         error
	submittedTasks int
}

func (m *mockManagerWorker) Start() {}
func (m *mockManagerWorker) Stop()  {}
func (m *mockManagerWorker) Submit(*Task) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.submittedTasks++
	return m.subErr
}
func (m *mockManagerWorker) GetOutChan() chan WorkerExecutionResult { return m.outChan }
func (m *mockManagerWorker) GetStatus() WorkerState                 { return m.status }
func (m *mockManagerWorker) GetSubmittedTasks() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.submittedTasks
}

type mockStore struct {
	mu          sync.Mutex
	saveErr     error
	saved       int
	getTasksErr error
	tasks       []Task
	done        chan struct{}
}

func (m *mockStore) GetTasks() ([]Task, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.tasks, m.getTasksErr
}
func (m *mockStore) SaveTask(*Task, *TaskExecutionResult) error {
	m.mu.Lock()
	m.saved++
	m.mu.Unlock()
	if m.done != nil {
		select {
		case m.done <- struct{}{}:
		default:
		}
	}
	return m.saveErr
}

func (m *mockStore) SetTasks(tasks []Task) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tasks = tasks
}
func (m *mockStore) SetGetTasksErr(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.getTasksErr = err
}
func (m *mockStore) SetSaveErr(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.saveErr = err
}
func (m *mockStore) GetSaved() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.saved
}
func (m *mockStore) ResetDone() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.done != nil {
		close(m.done)
	}
	m.done = make(chan struct{}, 1)
}
func (m *mockStore) WaitSave(timeout time.Duration) error {
	if m.done == nil {
		return errors.New("done channel not initialized")
	}
	select {
	case <-m.done:
		return nil
	case <-time.After(timeout):
		return errors.New("timeout waiting for SaveTask")
	}
}

type mockLogger struct{}

func (m *mockLogger) Infof(string, ...interface{})  {}
func (m *mockLogger) Errorf(string, ...interface{}) {}

// --- Tests ---

func TestManager_Submit(t *testing.T) {
	tests := []struct {
		name      string
		task      *Task
		saveErr   error
		expectErr bool
	}{
		{
			name:      "Success",
			task:      &Task{Worker: "w1", Status: StatusPending, MaxRetries: 3},
			saveErr:   nil,
			expectErr: false,
		},
		{
			name:      "Store error",
			task:      &Task{Worker: "w1", Status: StatusPending, MaxRetries: 3},
			saveErr:   errors.New("db down"),
			expectErr: true,
		},
		{
			name:      "Task already finished",
			task:      &Task{Worker: "w1", Status: StatusSuccess, MaxRetries: 3},
			saveErr:   nil,
			expectErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &mockStore{}
			store.SetSaveErr(tt.saveErr)
			mgr := NewManager(context.Background(), store, &mockLogger{}, NewBackOffStrategy(), 2, time.Second, nil)
			mgr.isWorking = true
			err := mgr.Submit(tt.task)
			if (err != nil) != tt.expectErr {
				t.Errorf("expected error %v, got %v", tt.expectErr, err)
			}
		})
	}
}

func TestManager_Start(t *testing.T) {
	mgr := NewManager(context.Background(), &mockStore{}, &mockLogger{}, NewBackOffStrategy(), 2, time.Second, nil)
	w := &mockManagerWorker{status: WorkerState{Status: WorkerStatusRunning}}
	_ = mgr.RegisterWorker("w1", w, nil)
	mgr.Start()
	defer mgr.Stop()
	if !mgr.isWorking {
		t.Error("isWorking should be true after Start")
	}
	time.Sleep(10 * time.Millisecond)
}

func TestManager_Lifecycle(t *testing.T) {
	mgr := NewManager(context.Background(), &mockStore{}, &mockLogger{}, NewBackOffStrategy(), 2, time.Second, nil)
	w := &mockManagerWorker{status: WorkerState{Status: WorkerStatusRunning}}
	b := &mockBreaker{allow: true, state: StateClosed}
	if err := mgr.RegisterWorker("w1", w, b); err != nil {
		t.Fatalf("failed to register: %v", err)
	}
	statuses := mgr.GetWorkerStatuses()
	if statuses["w1"].Status != WorkerStatusRunning {
		t.Errorf("expected WorkerStatusRunning, got %v", statuses["w1"].Status)
	}
	if statuses["w1"].CBState != StateClosed {
		t.Errorf("expected CBState StateClosed, got %v", statuses["w1"].CBState)
	}

	mgr.UnregisterWorker("w1")
	if _, exists := mgr.workers["w1"]; exists {
		t.Fatal("worker should be removed")
	}
	if _, exists := mgr.breakers["w1"]; exists {
		t.Fatal("breaker should be removed")
	}
}

func TestManager_RegisterWorkerAfterStart(t *testing.T) {
	mgr := NewManager(context.Background(), &mockStore{}, &mockLogger{}, NewBackOffStrategy(), 2, time.Second, nil)
	mgr.Start()
	defer mgr.Stop()
	w := &mockManagerWorker{status: WorkerState{Status: WorkerStatusCreated}, outChan: make(chan WorkerExecutionResult)}
	b := &mockBreaker{allow: true, state: StateClosed}
	if err := mgr.RegisterWorker("w2", w, b); err != nil {
		t.Fatal(err)
	}
	mgr.mtx.RLock()
	_, ok := mgr.workers["w2"]
	mgr.mtx.RUnlock()
	if !ok {
		t.Fatal("worker not registered")
	}
}

func TestManager_GetRetriableTasks_WithBreaker(t *testing.T) {
	store := &mockStore{}
	store.SetTasks([]Task{
		{
			ID:            uuid.New(),
			Worker:        "w1",
			BackOffCode:   LinearBackOff,
			BackOffParams: map[BackOffParam]interface{}{DurationKey: time.Second},
			Retries:       0,
			MaxRetries:    3,
			Status:        StatusPending,
			NextRun:       time.Time{},
		},
		{
			ID:            uuid.New(),
			Worker:        "w2",
			BackOffCode:   LinearBackOff,
			BackOffParams: map[BackOffParam]interface{}{DurationKey: time.Second},
			Retries:       0,
			MaxRetries:    3,
			Status:        StatusPending,
			NextRun:       time.Time{},
		},
	})
	mgr := NewManager(context.Background(), store, &mockLogger{}, NewBackOffStrategy(), 2, 10*time.Millisecond, nil)
	w1Out := make(chan WorkerExecutionResult, 10)
	w1 := &mockManagerWorker{status: WorkerState{Status: WorkerStatusRunning}, outChan: w1Out}
	b1 := &mockBreaker{allow: true, state: StateClosed}
	_ = mgr.RegisterWorker("w1", w1, b1)
	w2Out := make(chan WorkerExecutionResult, 10)
	w2 := &mockManagerWorker{status: WorkerState{Status: WorkerStatusRunning}, outChan: w2Out}
	b2 := &mockBreaker{allow: false, state: StateOpen}
	_ = mgr.RegisterWorker("w2", w2, b2)
	mgr.Start()
	defer mgr.Stop()
	time.Sleep(50 * time.Millisecond)
	if got := w1.GetSubmittedTasks(); got != 1 {
		t.Errorf("w1 expected 1 Submit, got %d", got)
	}
	if got := w2.GetSubmittedTasks(); got != 0 {
		t.Errorf("w2 expected 0 Submit, got %d", got)
	}
}

func TestManager_GetRetriableTasks_SkipsFutureTasks(t *testing.T) {
	store := &mockStore{}
	future := time.Now().Add(time.Hour)
	store.SetTasks([]Task{
		{
			ID:            uuid.New(),
			Worker:        "w1",
			BackOffCode:   LinearBackOff,
			BackOffParams: map[BackOffParam]interface{}{DurationKey: time.Second},
			Retries:       0,
			MaxRetries:    3,
			Status:        StatusPending,
			NextRun:       future,
		},
	})
	mgr := NewManager(context.Background(), store, &mockLogger{}, NewBackOffStrategy(), 2, 10*time.Millisecond, nil)
	wOut := make(chan WorkerExecutionResult, 10)
	w := &mockManagerWorker{status: WorkerState{Status: WorkerStatusRunning}, outChan: wOut}
	_ = mgr.RegisterWorker("w1", w, nil)
	mgr.Start()
	defer mgr.Stop()
	time.Sleep(50 * time.Millisecond)
	if got := w.GetSubmittedTasks(); got != 0 {
		t.Errorf("expected 0 Submit (task skipped due to NextRun in future), got %d", got)
	}
}

func TestManager_GetRetriableTasks_WithoutBreaker(t *testing.T) {
	store := &mockStore{}
	taskID := uuid.New()
	store.SetTasks([]Task{
		{
			ID:            taskID,
			Worker:        "w1",
			BackOffCode:   LinearBackOff,
			BackOffParams: map[BackOffParam]interface{}{DurationKey: time.Second},
			Retries:       0,
			MaxRetries:    3,
			Status:        StatusPending,
			NextRun:       time.Time{},
		},
	})
	mgr := NewManager(context.Background(), store, &mockLogger{}, NewBackOffStrategy(), 2, 10*time.Millisecond, nil)
	wOut := make(chan WorkerExecutionResult, 10)
	w := &mockManagerWorker{status: WorkerState{Status: WorkerStatusRunning}, outChan: wOut}
	_ = mgr.RegisterWorker("w1", w, nil)
	mgr.Start()
	defer mgr.Stop()
	time.Sleep(50 * time.Millisecond)
	if got := w.GetSubmittedTasks(); got != 1 {
		t.Errorf("expected 1 Submit (breaker absent), got %d", got)
	}
}

func TestManager_GetRetriableTasks_WorkerNotFound(t *testing.T) {
	store := &mockStore{}
	store.SetTasks([]Task{
		{
			ID:            uuid.New(),
			Worker:        "unknown",
			BackOffCode:   LinearBackOff,
			BackOffParams: map[BackOffParam]interface{}{DurationKey: time.Second},
			Retries:       0,
			MaxRetries:    3,
			Status:        StatusPending,
			NextRun:       time.Time{},
		},
	})
	mgr := NewManager(context.Background(), store, &mockLogger{}, NewBackOffStrategy(), 2, 10*time.Millisecond, nil)
	mgr.Start()
	defer mgr.Stop()
	time.Sleep(50 * time.Millisecond)
	if saved := store.GetSaved(); saved < 1 {
		t.Errorf("expected at least 1 SaveTask call, got %d", saved)
	}
}

func TestManager_SaveResult_Logic(t *testing.T) {
	store := &mockStore{}
	mgr := NewManager(context.Background(), store, &mockLogger{}, NewBackOffStrategy(), 2, time.Second, nil)
	mgr.Start()
	defer mgr.Stop()

	store.ResetDone()
	sendAndWait := func(task *Task, result *TaskExecutionResult) {
		mgr.unionQueue <- WorkerExecutionResult{task: task, result: result}
		if err := store.WaitSave(200 * time.Millisecond); err != nil {
			t.Fatal(err)
		}
		store.mu.Lock()
		store.saved = 0
		store.mu.Unlock()
		store.ResetDone()
	}

	// Critical error
	taskCrit := &Task{ID: uuid.New(), Worker: "w1", Retries: 1, MaxRetries: 3}
	resultCrit := &TaskExecutionResult{Status: StatusFailure, IsCritical: true, RunAt: time.Now()}
	sendAndWait(taskCrit, resultCrit)
	if taskCrit.Status != StatusFailure {
		t.Errorf("critical error expected Failure, got %v", taskCrit.Status)
	}
	if taskCrit.Retries != 2 {
		t.Errorf("retries should be 2, got %d", taskCrit.Retries)
	}

	// Success
	taskSucc := &Task{ID: uuid.New(), Worker: "w1", Retries: 0, MaxRetries: 3}
	resultSucc := &TaskExecutionResult{Status: StatusSuccess, IsCritical: false, RunAt: time.Now()}
	sendAndWait(taskSucc, resultSucc)
	if taskSucc.Status != StatusSuccess {
		t.Errorf("success expected Success, got %v", taskSucc.Status)
	}

	// Failure with retries < max
	taskFail := &Task{ID: uuid.New(), Worker: "w1", Retries: 1, MaxRetries: 3, BackOffCode: LinearBackOff, BackOffParams: map[BackOffParam]interface{}{DurationKey: 2 * time.Second}}
	resultFail := &TaskExecutionResult{Status: StatusFailure, IsCritical: false, RunAt: time.Now()}
	sendAndWait(taskFail, resultFail)
	if taskFail.Status != StatusPending {
		t.Errorf("failure with retries<max expected Pending, got %v", taskFail.Status)
	}
	if taskFail.Retries != 2 {
		t.Errorf("retries should be 2, got %d", taskFail.Retries)
	}
	if taskFail.NextRun.IsZero() || taskFail.NextRun.Before(time.Now()) {
		t.Error("NextRun should be set in future")
	}

	// Failure with retries == max
	taskMax := &Task{ID: uuid.New(), Worker: "w1", Retries: 2, MaxRetries: 3}
	resultMax := &TaskExecutionResult{Status: StatusFailure, IsCritical: false, RunAt: time.Now()}
	sendAndWait(taskMax, resultMax)
	if taskMax.Status != StatusFailure {
		t.Errorf("failure with retries==max expected Failure, got %v", taskMax.Status)
	}
}

func TestManager_EventPublisher(t *testing.T) {
	publisher := &mockEventPublisher{}
	mgr := NewManager(context.Background(), &mockStore{}, &mockLogger{}, NewBackOffStrategy(), 2, time.Second, publisher)
	mgr.Start()
	defer mgr.Stop()

	task := &Task{ID: uuid.New(), Worker: "w1", Retries: 0, MaxRetries: 3}
	result := &TaskExecutionResult{Status: StatusSuccess, IsCritical: false, RunAt: time.Now()}
	mgr.unionQueue <- WorkerExecutionResult{task: task, result: result}

	time.Sleep(50 * time.Millisecond)

	events := publisher.GetEvents()
	if len(events) != 1 {
		t.Errorf("expected 1 event published, got %d", len(events))
	}
	if events[0].task.ID != task.ID {
		t.Errorf("event task ID mismatch: expected %v, got %v", task.ID, events[0].task.ID)
	}
}

func TestManager_PipelineAndBufferFallback(t *testing.T) {
	store := &mockStore{}
	store.SetSaveErr(errors.New("db_offline"))
	mgr := NewManager(context.Background(), store, &mockLogger{}, NewBackOffStrategy(), 1, time.Second, nil)
	wOut := make(chan WorkerExecutionResult, 5)
	w := &mockManagerWorker{status: WorkerState{Status: WorkerStatusRunning}, outChan: wOut}
	_ = mgr.RegisterWorker("w1", w, nil)
	mgr.Start()
	defer mgr.Stop()

	task := &Task{ID: uuid.New(), Worker: "w1", Retries: 0, MaxRetries: 0}
	store.ResetDone()
	for i := 0; i < 3; i++ {
		wOut <- WorkerExecutionResult{
			task: task,
			result: &TaskExecutionResult{
				ID:         uuid.New(),
				TaskID:     task.ID,
				Status:     StatusFailure,
				RunAt:      time.Now(),
				IsCritical: false,
			},
		}
	}
	if err := store.WaitSave(100 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	mgr.mtx.Lock()
	bufLen := len(mgr.buffer)
	mgr.mtx.Unlock()
	if bufLen != 1 {
		t.Errorf("buffer size expected 1, got %d", bufLen)
	}

	store.SetSaveErr(nil)
	mgr.flushBuffer()
	mgr.mtx.Lock()
	bufLen = len(mgr.buffer)
	mgr.mtx.Unlock()
	if bufLen != 0 {
		t.Errorf("buffer should be empty, got %d", bufLen)
	}
}

func TestManager_GracefulStop(t *testing.T) {
	mgr := NewManager(context.Background(), &mockStore{}, &mockLogger{}, NewBackOffStrategy(), 2, time.Second, nil)
	wOut := make(chan WorkerExecutionResult)
	w := &mockManagerWorker{status: WorkerState{Status: WorkerStatusRunning}, outChan: wOut}
	_ = mgr.RegisterWorker("w1", w, nil)
	mgr.Start()
	close(wOut)
	done := make(chan struct{})
	go func() {
		mgr.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("Stop() hung")
	}
}

func TestManager_StopTwice(t *testing.T) {
	mgr := NewManager(context.Background(), &mockStore{}, &mockLogger{}, NewBackOffStrategy(), 2, time.Second, nil)
	mgr.Start()
	mgr.Stop()
	mgr.Stop()
}

func TestManager_ProcessedTasksDeduplication(t *testing.T) {
	store := &mockStore{}
	taskID := uuid.New()
	store.SetTasks([]Task{
		{
			ID:            taskID,
			Worker:        "w1",
			BackOffCode:   LinearBackOff,
			BackOffParams: map[BackOffParam]interface{}{DurationKey: time.Second},
			Retries:       0,
			MaxRetries:    3,
			Status:        StatusPending,
			NextRun:       time.Time{},
		},
	})
	mgr := NewManager(context.Background(), store, &mockLogger{}, NewBackOffStrategy(), 2, 10*time.Millisecond, nil)
	wOut := make(chan WorkerExecutionResult, 10)
	w := &mockManagerWorker{status: WorkerState{Status: WorkerStatusRunning}, outChan: wOut}
	_ = mgr.RegisterWorker("w1", w, nil)
	mgr.Start()
	defer mgr.Stop()

	time.Sleep(50 * time.Millisecond)
	if got := w.GetSubmittedTasks(); got != 1 {
		t.Errorf("first cycle: expected 1 submit, got %d", got)
	}
	time.Sleep(50 * time.Millisecond)
	if got := w.GetSubmittedTasks(); got != 1 {
		t.Errorf("second cycle: expected still 1, got %d", got)
	}
	store.ResetDone()
	result := &TaskExecutionResult{ID: uuid.New(), TaskID: taskID, Status: StatusSuccess}
	mgr.unionQueue <- WorkerExecutionResult{task: &Task{ID: taskID}, result: result}
	if err := store.WaitSave(100 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	if got := w.GetSubmittedTasks(); got != 2 {
		t.Errorf("after completion: expected 2 submits, got %d", got)
	}
}

func TestManager_BreakerRecording(t *testing.T) {
	store := &mockStore{}
	taskID := uuid.New()
	store.SetTasks([]Task{
		{
			ID:            taskID,
			Worker:        "w1",
			BackOffCode:   LinearBackOff,
			BackOffParams: map[BackOffParam]interface{}{DurationKey: time.Second},
			Retries:       0,
			MaxRetries:    3,
			Status:        StatusPending,
			NextRun:       time.Time{},
		},
	})
	mgr := NewManager(context.Background(), store, &mockLogger{}, NewBackOffStrategy(), 2, 100*time.Millisecond, nil)

	wOut := make(chan WorkerExecutionResult, 10)
	w := &mockManagerWorker{status: WorkerState{Status: WorkerStatusRunning}, outChan: wOut, subErr: errors.New("submit error")}
	b := &mockBreaker{allow: true, state: StateClosed}
	_ = mgr.RegisterWorker("w1", w, b)

	mgr.Start()
	defer mgr.Stop()

	time.Sleep(120 * time.Millisecond)

	if b.GetFailures() != 1 {
		t.Errorf("expected 1 failure recorded, got %d", b.GetFailures())
	}
	if b.GetSuccesses() != 0 {
		t.Errorf("expected 0 successes, got %d", b.GetSuccesses())
	}
}
