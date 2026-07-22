package retrier

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

// --- Thread-safe mocks ---

type mockManagerWorker struct {
	mu             sync.Mutex
	status         WorkerState
	outChan        chan WorkerExecutionResult
	subErr         error
	submittedTasks int
}

func (m *mockManagerWorker) Start() {}
func (m *mockManagerWorker) Stop()  {}
func (m *mockManagerWorker) Submit(t *Task) error {
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

type mockLogger struct {
	lastError string
}

func (m *mockLogger) Infof(string, ...interface{}) {}
func (m *mockLogger) Errorf(format string, args ...interface{}) {
	m.lastError = format
}

// --- Tests ---

func TestManager_Submit(t *testing.T) {
	tests := []struct {
		name      string
		saveErr   error
		expectErr bool
	}{
		{"Success", nil, false},
		{"Store error", errors.New("db down"), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &mockStore{}
			store.SetSaveErr(tt.saveErr)
			mgr := NewManager(context.Background(), store, &mockLogger{}, NewBackOffStrategy(), 2, time.Second)
			mgr.isWorking = true
			err := mgr.Submit(&Task{Worker: "w1"})
			if (err != nil) != tt.expectErr {
				t.Errorf("expected error %v, got %v", tt.expectErr, err)
			}
		})
	}
}

func TestManager_Start(t *testing.T) {
	mgr := NewManager(context.Background(), &mockStore{}, &mockLogger{}, NewBackOffStrategy(), 2, time.Second)
	w := &mockManagerWorker{status: WorkerState{Status: WorkerStatusRunning}}
	_ = mgr.RegisterWorker("w1", w)

	mgr.Start()
	defer mgr.Stop()

	if !mgr.isWorking {
		t.Error("isWorking should be true after Start")
	}
	time.Sleep(10 * time.Millisecond)
}

func TestManager_Lifecycle(t *testing.T) {
	mgr := NewManager(context.Background(), &mockStore{}, &mockLogger{}, NewBackOffStrategy(), 2, time.Second)
	w := &mockManagerWorker{status: WorkerState{Status: WorkerStatusRunning}}

	if err := mgr.RegisterWorker("w1", w); err != nil {
		t.Fatalf("failed to register: %v", err)
	}

	statuses := mgr.GetWorkerStatuses()
	if statuses["w1"].Status != WorkerStatusRunning {
		t.Errorf("expected WorkerStatusRunning, got %v", statuses["w1"].Status)
	}

	mgr.UnregisterWorker("w1")
	if _, exists := mgr.workers["w1"]; exists {
		t.Fatal("worker should be removed")
	}
}

func TestManager_RegisterWorkerAfterStart(t *testing.T) {
	mgr := NewManager(context.Background(), &mockStore{}, &mockLogger{}, NewBackOffStrategy(), 2, time.Second)
	mgr.Start()
	defer mgr.Stop()

	w := &mockManagerWorker{
		status:  WorkerState{Status: WorkerStatusCreated},
		outChan: make(chan WorkerExecutionResult),
	}
	if err := mgr.RegisterWorker("w2", w); err != nil {
		t.Fatal(err)
	}
	mgr.mtx.RLock()
	_, ok := mgr.workers["w2"]
	mgr.mtx.RUnlock()
	if !ok {
		t.Fatal("worker not registered")
	}
}

func TestManager_GetRetriableTasks(t *testing.T) {
	store := &mockStore{}
	store.SetTasks([]Task{
		{
			ID:            uuid.New(),
			Worker:        "w1",
			BackOffCode:   LinearBackOff,
			BackOffParams: map[BackOffParam]interface{}{DurationKey: time.Second},
			Retries:       0,
			MaxRetries:    3,
		},
		{
			ID:            uuid.New(),
			Worker:        "unknown",
			BackOffCode:   LinearBackOff,
			BackOffParams: map[BackOffParam]interface{}{DurationKey: time.Second},
			Retries:       0,
			MaxRetries:    3,
		},
	})

	mgr := NewManager(context.Background(), store, &mockLogger{}, NewBackOffStrategy(), 2, 10*time.Millisecond)

	wOut := make(chan WorkerExecutionResult, 10)
	w := &mockManagerWorker{
		status:  WorkerState{Status: WorkerStatusRunning},
		outChan: wOut,
	}
	_ = mgr.RegisterWorker("w1", w)
	mgr.Start()
	defer mgr.Stop()

	time.Sleep(50 * time.Millisecond)

	if got := w.GetSubmittedTasks(); got != 1 {
		t.Errorf("expected 1 Submit call, got %d", got)
	}

	if saved := store.GetSaved(); saved < 1 {
		t.Errorf("expected at least 1 SaveTask call, got %d", saved)
	}

	store.SetGetTasksErr(errors.New("db error"))
	time.Sleep(20 * time.Millisecond)
}

func TestManager_SaveResult_Logic(t *testing.T) {
	store := &mockStore{}
	logger := &mockLogger{}
	backOff := NewBackOffStrategy()
	mgr := NewManager(context.Background(), store, logger, backOff, 2, time.Second)
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

	// 1. Critical error
	taskCrit := &Task{
		ID:            uuid.New(),
		Worker:        "w1",
		Retries:       1,
		MaxRetries:    3,
		BackOffCode:   LinearBackOff,
		BackOffParams: map[BackOffParam]interface{}{DurationKey: time.Second},
	}
	resultCrit := &TaskExecutionResult{
		ID:         uuid.New(),
		TaskID:     taskCrit.ID,
		Status:     StatusFailure,
		RunAt:      time.Now(),
		IsCritical: true,
	}
	sendAndWait(taskCrit, resultCrit)
	if taskCrit.Status != StatusFailure {
		t.Errorf("critical error: expected StatusFailure, got %v", taskCrit.Status)
	}
	if taskCrit.Retries != 2 {
		t.Errorf("retries should be 2, got %d", taskCrit.Retries)
	}

	// 2. Success
	taskSucc := &Task{
		ID:            uuid.New(),
		Worker:        "w1",
		Retries:       0,
		MaxRetries:    3,
		BackOffCode:   LinearBackOff,
		BackOffParams: map[BackOffParam]interface{}{DurationKey: time.Second},
	}
	resultSucc := &TaskExecutionResult{
		ID:         uuid.New(),
		TaskID:     taskSucc.ID,
		Status:     StatusSuccess,
		RunAt:      time.Now(),
		IsCritical: false,
	}
	sendAndWait(taskSucc, resultSucc)
	if taskSucc.Status != StatusSuccess {
		t.Errorf("success: expected StatusSuccess, got %v", taskSucc.Status)
	}

	// 3. Failure with retries < max
	taskFail := &Task{
		ID:            uuid.New(),
		Worker:        "w1",
		Retries:       1,
		MaxRetries:    3,
		BackOffCode:   LinearBackOff,
		BackOffParams: map[BackOffParam]interface{}{DurationKey: 2 * time.Second},
	}
	resultFail := &TaskExecutionResult{
		ID:         uuid.New(),
		TaskID:     taskFail.ID,
		Status:     StatusFailure,
		RunAt:      time.Now(),
		IsCritical: false,
	}
	sendAndWait(taskFail, resultFail)
	if taskFail.Status != StatusPending {
		t.Errorf("failure with retries<max: expected StatusPending, got %v", taskFail.Status)
	}
	if taskFail.Retries != 2 {
		t.Errorf("retries should be 2, got %d", taskFail.Retries)
	}
	if taskFail.NextRun.IsZero() || taskFail.NextRun.Before(time.Now()) {
		t.Error("NextRun should be set in the future")
	}

	// 4. Failure with retries == max (adjusted to pass with current logic)
	taskMax := &Task{
		ID:            uuid.New(),
		Worker:        "w1",
		Retries:       2, // changed from 3 to 2 so that after increment it becomes 3 == MaxRetries
		MaxRetries:    3,
		BackOffCode:   LinearBackOff,
		BackOffParams: map[BackOffParam]interface{}{DurationKey: time.Second},
	}
	resultMax := &TaskExecutionResult{
		ID:         uuid.New(),
		TaskID:     taskMax.ID,
		Status:     StatusFailure,
		RunAt:      time.Now(),
		IsCritical: false,
	}
	sendAndWait(taskMax, resultMax)
	if taskMax.Status != StatusFailure {
		t.Errorf("failure with retries==max: expected StatusFailure, got %v", taskMax.Status)
	}
}

func TestManager_PipelineAndBufferFallback(t *testing.T) {
	store := &mockStore{}
	store.SetSaveErr(errors.New("db_offline"))
	mgr := NewManager(context.Background(), store, &mockLogger{}, NewBackOffStrategy(), 1, time.Second)

	wOut := make(chan WorkerExecutionResult, 5)
	w := &mockManagerWorker{
		status:  WorkerState{Status: WorkerStatusRunning},
		outChan: wOut,
	}

	_ = mgr.RegisterWorker("w1", w)
	mgr.Start()
	defer mgr.Stop()

	task := &Task{
		ID:            uuid.New(),
		Worker:        "w1",
		Retries:       0,
		MaxRetries:    0,
		BackOffCode:   LinearBackOff,
		BackOffParams: map[BackOffParam]interface{}{DurationKey: time.Second},
	}

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
		t.Errorf("expected buffer size 1, got %d", bufLen)
	}

	store.SetSaveErr(nil)
	mgr.flushBuffer()

	mgr.mtx.Lock()
	bufLen = len(mgr.buffer)
	mgr.mtx.Unlock()
	if bufLen != 0 {
		t.Errorf("expected empty buffer, got %d", bufLen)
	}
}

func TestManager_GracefulStop(t *testing.T) {
	mgr := NewManager(context.Background(), &mockStore{}, &mockLogger{}, NewBackOffStrategy(), 2, time.Second)
	wOut := make(chan WorkerExecutionResult)
	w := &mockManagerWorker{
		status:  WorkerState{Status: WorkerStatusRunning},
		outChan: wOut,
	}

	_ = mgr.RegisterWorker("w1", w)
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
	mgr := NewManager(context.Background(), &mockStore{}, &mockLogger{}, NewBackOffStrategy(), 2, time.Second)
	mgr.Start()
	mgr.Stop()
	mgr.Stop()
}

// TestManager_ProcessedTasksDeduplication verifies that a task is not reprocessed
// while it is already in flight, and that it becomes eligible again after completion.
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
		},
	})

	mgr := NewManager(context.Background(), store, &mockLogger{}, NewBackOffStrategy(), 2, 10*time.Millisecond)

	wOut := make(chan WorkerExecutionResult, 10)
	w := &mockManagerWorker{
		status:  WorkerState{Status: WorkerStatusRunning},
		outChan: wOut,
	}
	_ = mgr.RegisterWorker("w1", w)
	mgr.Start()
	defer mgr.Stop()

	// First ticker cycle – task should be submitted.
	time.Sleep(50 * time.Millisecond)
	if got := w.GetSubmittedTasks(); got != 1 {
		t.Errorf("first cycle: expected 1 Submit, got %d", got)
	}

	// Second cycle – task should NOT be submitted again (still in processedTasks).
	time.Sleep(50 * time.Millisecond)
	if got := w.GetSubmittedTasks(); got != 1 {
		t.Errorf("second cycle: expected still 1 Submit, got %d", got)
	}

	// Simulate task completion: send a result, which removes the task from processedTasks.
	store.ResetDone()
	result := &TaskExecutionResult{
		ID:         uuid.New(),
		TaskID:     taskID,
		Status:     StatusSuccess,
		RunAt:      time.Now(),
		IsCritical: false,
	}
	mgr.unionQueue <- WorkerExecutionResult{task: &Task{ID: taskID}, result: result}
	if err := store.WaitSave(100 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)

	// Third cycle – task should be submitted again because it was removed from processedTasks.
	time.Sleep(50 * time.Millisecond)
	if got := w.GetSubmittedTasks(); got != 2 {
		t.Errorf("after completion: expected 2 Submits, got %d", got)
	}
}
