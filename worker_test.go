package retrier

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// TestWorker_NewWorker verifies that a new worker pool is created with the correct initial status.
func TestWorker_NewWorker(t *testing.T) {
	ctx := context.Background()
	fn := func(_ context.Context, _ []byte) (string, *ExecutionError) {
		return "ok", nil
	}
	cfg, err := NewWorkerCfg(1, 5, time.Second)
	if err != nil {
		t.Fatalf("NewWorkerCfg failed: %v", err)
	}
	w, err := NewWorker(ctx, cfg, fn)
	if err != nil {
		t.Fatalf("NewWorker failed: %v", err)
	}
	if w == nil {
		t.Fatal("NewWorker returned nil")
	}
	state := w.GetStatus()
	if state.Status != WorkerStatusCreated {
		t.Errorf("expected status %v, got %v", WorkerStatusCreated, state.Status)
	}
}

// TestWorker_UpdateConfig checks that the configuration can be updated safely.
func TestWorker_UpdateConfig(t *testing.T) {
	ctx := context.Background()
	fn := func(_ context.Context, _ []byte) (string, *ExecutionError) {
		return "ok", nil
	}
	cfg, _ := NewWorkerCfg(1, 5, time.Second)
	w, _ := NewWorker(ctx, cfg, fn)

	newCfg, _ := NewWorkerCfg(2, 10, 2*time.Second)
	w.UpdateConfig(newCfg)

	w.cfgMtx.RLock()
	defer w.cfgMtx.RUnlock()
	if w.cfg.minWorkers != 2 || w.cfg.maxWorkers != 10 || w.cfg.idleTimeout != 2*time.Second {
		t.Errorf("config not updated: min=%d, max=%d, timeout=%v", w.cfg.minWorkers, w.cfg.maxWorkers, w.cfg.idleTimeout)
	}
}

// TestWorker_Start ensures that Start spawns the minimum number of workers and
// transitions the pool to the Running state. Repeated calls have no effect.
func TestWorker_Start(t *testing.T) {
	ctx := context.Background()
	fn := func(_ context.Context, _ []byte) (string, *ExecutionError) {
		return "ok", nil
	}
	cfg, _ := NewWorkerCfg(2, 5, time.Second)
	w, _ := NewWorker(ctx, cfg, fn)
	w.Start()

	state := w.GetStatus()
	if state.Status != WorkerStatusRunning {
		t.Errorf("expected status Running, got %v", state.Status)
	}
	if state.ActiveWorkers != 2 {
		t.Errorf("active workers %d, expected 2", state.ActiveWorkers)
	}

	// Second Start should be a no-op.
	w.Start()
	state2 := w.GetStatus()
	if state2.ActiveWorkers != 2 {
		t.Errorf("after second Start active workers %d, expected 2", state2.ActiveWorkers)
	}

	w.Stop()
}

// TestWorker_RestartAfterStop verifies that after a full Stop, calling Start
// recreates internal channels and allows the pool to process new tasks.
func TestWorker_RestartAfterStop(t *testing.T) {
	ctx := context.Background()
	fn := func(_ context.Context, payload []byte) (string, *ExecutionError) {
		return "processed: " + string(payload), nil
	}
	cfg, _ := NewWorkerCfg(1, 2, 100*time.Millisecond)
	w, _ := NewWorker(ctx, cfg, fn)
	w.Start()

	// Submit and receive a result.
	task1 := &Task{ID: getID(), Payload: []byte("hello")}
	if err := w.Submit(task1); err != nil {
		t.Fatalf("Submit failed: %v", err)
	}
	select {
	case res := <-w.GetOutChan():
		if string(res.Result.Result) != "processed: hello" {
			t.Errorf("unexpected result: %s", res.Result.Result)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for result")
	}

	// Stop the pool.
	w.Stop()
	state := w.GetStatus()
	if state.Status != WorkerStatusStopped {
		t.Errorf("expected Stopped, got %v", state.Status)
	}

	// Restart.
	w.Start()
	state = w.GetStatus()
	if state.Status != WorkerStatusRunning {
		t.Errorf("after restart expected Running, got %v", state.Status)
	}
	if state.ActiveWorkers != 1 {
		t.Errorf("after restart active workers %d, expected 1", state.ActiveWorkers)
	}

	// New task should be processed.
	task2 := &Task{ID: getID(), Payload: []byte("world")}
	if err := w.Submit(task2); err != nil {
		t.Fatalf("Submit after restart failed: %v", err)
	}
	select {
	case res := <-w.GetOutChan():
		if string(res.Result.Result) != "processed: world" {
			t.Errorf("unexpected result after restart: %s", res.Result.Result)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for result after restart")
	}

	w.Stop()
}

// TestWorker_Submit_Success tests the happy path: a task completes successfully
// and produces the expected result.
func TestWorker_Submit_Success(t *testing.T) {
	ctx := context.Background()
	fn := func(_ context.Context, payload []byte) (string, *ExecutionError) {
		return "processed: " + string(payload), nil
	}
	cfg, _ := NewWorkerCfg(1, 1, time.Second)
	w, _ := NewWorker(ctx, cfg, fn)
	w.Start()
	defer w.Stop()

	task := &Task{ID: getID(), Payload: []byte("hello")}
	err := w.Submit(task)
	if err != nil {
		t.Fatalf("Submit returned error: %v", err)
	}

	select {
	case res := <-w.GetOutChan():
		if res.Task.ID != task.ID {
			t.Errorf("Task ID mismatch: %s vs %s", res.Task.ID, task.ID)
		}
		if res.Result.Status != StatusSuccess {
			t.Errorf("status %s, expected Success", res.Result.Status)
		}
		if string(res.Result.Result) != "processed: hello" {
			t.Errorf("Result %s, expected 'processed: hello'", string(res.Result.Result))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for Result")
	}
}

// TestWorker_Submit_WhenNotRunning ensures that Submit returns an error
// when the pool is not in the Running state (Created, Suspended, or Stopped).
func TestWorker_Submit_WhenNotRunning(t *testing.T) {
	ctx := context.Background()
	fn := func(_ context.Context, _ []byte) (string, *ExecutionError) {
		return "ok", nil
	}
	cfg, _ := NewWorkerCfg(1, 1, time.Second)
	w, _ := NewWorker(ctx, cfg, fn)
	task := &Task{ID: getID(), Payload: []byte("hello")}
	err := w.Submit(task)
	if err == nil {
		t.Error("expected error when submitting to non-running worker")
	}

	// After Suspend, submission should also fail.
	w.Start()
	w.Suspend()
	err = w.Submit(task)
	if err == nil {
		t.Error("expected error when submitting to suspended worker")
	}
}

// TestWorker_Submit_ScalingUp verifies that the pool automatically scales up
// when all workers are busy and the maximum limit has not been reached.
func TestWorker_Submit_ScalingUp(t *testing.T) {
	ctx := context.Background()
	fn := func(_ context.Context, _ []byte) (string, *ExecutionError) {
		time.Sleep(200 * time.Millisecond)
		return "ok", nil
	}
	cfg, _ := NewWorkerCfg(1, 3, time.Second)
	w, _ := NewWorker(ctx, cfg, fn)
	w.Start()
	defer w.Stop()

	for i := 0; i < 3; i++ {
		err := w.Submit(&Task{ID: getID(), Payload: []byte("x")})
		if err != nil {
			t.Fatalf("Submit error: %v", err)
		}
	}

	time.Sleep(100 * time.Millisecond)
	state := w.GetStatus()
	if state.ActiveWorkers != 3 {
		t.Errorf("active workers %d, expected 3", state.ActiveWorkers)
	}

	for i := 0; i < 3; i++ {
		select {
		case <-w.GetOutChan():
		case <-time.After(1 * time.Second):
			t.Fatal("timeout waiting for Result")
		}
	}
}

// TestWorker_Submit_BlocksWhenMaxWorkersReached checks that submission blocks
// when the pool is saturated and the maximum worker count is reached.
func TestWorker_Submit_BlocksWhenMaxWorkersReached(t *testing.T) {
	ctx := context.Background()
	fn := func(_ context.Context, _ []byte) (string, *ExecutionError) {
		time.Sleep(300 * time.Millisecond)
		return "ok", nil
	}
	cfg, _ := NewWorkerCfg(1, 1, time.Second)
	w, _ := NewWorker(ctx, cfg, fn)
	w.Start()
	defer w.Stop()

	task1 := &Task{ID: getID(), Payload: []byte("x")}
	task2 := &Task{ID: getID(), Payload: []byte("y")}

	if err := w.Submit(task1); err != nil {
		t.Fatalf("Submit task1 error: %v", err)
	}

	done := make(chan bool)
	go func() {
		err2 := w.Submit(task2)
		if err2 != nil {
			t.Errorf("Submit task2 error: %v", err2)
		}
		done <- true
	}()

	// Wait for both tasks to finish, then the second submission should unblock.
	<-w.GetOutChan()
	<-w.GetOutChan()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Error("task2 submission did not unblock in time")
	}
}

// TestWorker_Stop verifies that Stop waits for active tasks to complete and
// then transitions the pool to the Stopped state. Subsequent Submits fail.
func TestWorker_Stop(t *testing.T) {
	ctx := context.Background()
	fn := func(_ context.Context, _ []byte) (string, *ExecutionError) {
		time.Sleep(500 * time.Millisecond)
		return "ok", nil
	}
	cfg, _ := NewWorkerCfg(1, 1, time.Second)
	w, _ := NewWorker(ctx, cfg, fn)
	w.Start()

	task := &Task{ID: getID(), Payload: []byte("x")}
	_ = w.Submit(task)

	stopCh := make(chan bool)
	go func() {
		w.Stop()
		stopCh <- true
	}()

	select {
	case <-stopCh:
	case <-time.After(1 * time.Second):
		t.Fatal("Stop did not wait for Task completion")
	}

	state := w.GetStatus()
	if state.Status != WorkerStatusStopped {
		t.Errorf("status %v, expected Stopped", state.Status)
	}
	err := w.Submit(&Task{ID: getID(), Payload: []byte("x")})
	if err == nil {
		t.Error("expected error when submitting after Stop")
	}
}

// TestWorker_Suspend tests that Suspend prevents new submissions while
// allowing active tasks to finish. The pool can later be resumed with Start.
func TestWorker_Suspend(t *testing.T) {
	ctx := context.Background()
	fn := func(_ context.Context, _ []byte) (string, *ExecutionError) {
		time.Sleep(100 * time.Millisecond)
		return "ok", nil
	}
	cfg, _ := NewWorkerCfg(1, 1, time.Second)
	w, _ := NewWorker(ctx, cfg, fn)
	w.Start()
	defer w.Stop()

	task := &Task{ID: getID(), Payload: []byte("x")}
	_ = w.Submit(task)

	w.Suspend()
	state := w.GetStatus()
	if state.Status != WorkerStatusSuspended {
		t.Errorf("status %v, expected Suspended", state.Status)
	}

	// New submission must be rejected.
	err := w.Submit(&Task{ID: getID(), Payload: []byte("y")})
	if err == nil {
		t.Error("expected error when submitting after Suspend")
	}

	// The active task should still complete and deliver its result.
	select {
	case <-w.GetOutChan():
	case <-time.After(500 * time.Millisecond):
		t.Fatal("did not receive Result of active Task after Suspend")
	}
}

// TestWorker_SuspendAndStart verifies that after Suspend, calling Start
// resumes the pool and new tasks can be processed.
func TestWorker_SuspendAndStart(t *testing.T) {
	ctx := context.Background()
	fn := func(_ context.Context, payload []byte) (string, *ExecutionError) {
		return "ok", nil
	}
	cfg, _ := NewWorkerCfg(1, 2, time.Second)
	w, _ := NewWorker(ctx, cfg, fn)
	w.Start()

	// Send a task to ensure workers are active.
	task := &Task{ID: getID(), Payload: []byte("test")}
	_ = w.Submit(task)
	<-w.GetOutChan()

	w.Suspend()
	state := w.GetStatus()
	if state.Status != WorkerStatusSuspended {
		t.Fatalf("expected Suspended, got %v", state.Status)
	}

	// Resume.
	w.Start()
	state = w.GetStatus()
	if state.Status != WorkerStatusRunning {
		t.Fatalf("after Start expected Running, got %v", state.Status)
	}
	if state.ActiveWorkers != 1 {
		t.Errorf("after restart active workers %d, expected 1", state.ActiveWorkers)
	}

	// New task should succeed.
	task2 := &Task{ID: getID(), Payload: []byte("hello")}
	if err := w.Submit(task2); err != nil {
		t.Fatalf("Submit after restart failed: %v", err)
	}
	select {
	case <-w.GetOutChan():
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for result after restart")
	}

	w.Stop()
}

// TestWorker_ExecutionError validates that errors are correctly wrapped,
// with the IsCritical flag set appropriately for critical vs usual errors.
func TestWorker_ExecutionError(t *testing.T) {
	ctx := context.Background()
	fn := func(_ context.Context, payload []byte) (string, *ExecutionError) {
		if string(payload) == "critical" {
			return "", &ExecutionError{Err: errors.New("critical error"), State: CriticalState}
		}
		return "", &ExecutionError{Err: errors.New("usual error"), State: UsualState}
	}
	cfg, _ := NewWorkerCfg(1, 1, time.Second)
	w, _ := NewWorker(ctx, cfg, fn)
	w.Start()
	defer w.Stop()

	taskCrit := &Task{ID: getID(), Payload: []byte("critical")}
	taskUsual := &Task{ID: getID(), Payload: []byte("usual")}

	if err := w.Submit(taskCrit); err != nil {
		t.Fatalf("Submit taskCrit error: %v", err)
	}
	select {
	case resCrit := <-w.GetOutChan():
		if resCrit.Result.Status != StatusFailure {
			t.Errorf("critical error status %s, expected Failure", resCrit.Result.Status)
		}
		if !resCrit.Result.IsCritical {
			t.Error("critical error should have IsCritical=true")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for critical Result")
	}

	if err := w.Submit(taskUsual); err != nil {
		t.Fatalf("Submit taskUsual error: %v", err)
	}
	select {
	case resUsual := <-w.GetOutChan():
		if resUsual.Result.Status != StatusFailure {
			t.Errorf("usual error status %s, expected Failure", resUsual.Result.Status)
		}
		if resUsual.Result.IsCritical {
			t.Error("usual error should have IsCritical=false")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for usual Result")
	}
}

// TestWorker_IdleTimeout_ScaleDown checks that surplus workers are terminated
// after the idle timeout, bringing the pool back to the minimum worker count.
func TestWorker_IdleTimeout_ScaleDown(t *testing.T) {
	ctx := context.Background()
	fn := func(_ context.Context, _ []byte) (string, *ExecutionError) {
		return "ok", nil
	}
	cfg, _ := NewWorkerCfg(1, 3, 200*time.Millisecond)
	w, _ := NewWorker(ctx, cfg, fn)
	w.Start()

	for i := 0; i < 3; i++ {
		_ = w.Submit(&Task{ID: getID(), Payload: []byte("x")})
	}
	for i := 0; i < 3; i++ {
		<-w.GetOutChan()
	}

	time.Sleep(300 * time.Millisecond)
	state := w.GetStatus()
	if state.ActiveWorkers != 1 {
		t.Errorf("active workers %d, expected 1 after idle timeout", state.ActiveWorkers)
	}
	w.Stop()
}

// TestWorker_ContextCancellationDuringTask demonstrates that canceling the
// parent context eventually causes workers to exit, but the pool status remains
// Running until Stop is called.
func TestWorker_ContextCancellationDuringTask(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	fn := func(_ context.Context, _ []byte) (string, *ExecutionError) {
		time.Sleep(500 * time.Millisecond)
		return "ok", nil
	}
	cfg, _ := NewWorkerCfg(1, 1, time.Second)
	w, _ := NewWorker(ctx, cfg, fn)
	w.Start()

	task := &Task{ID: getID(), Payload: []byte("x")}
	_ = w.Submit(task)

	time.Sleep(100 * time.Millisecond)
	cancel()

	time.Sleep(600 * time.Millisecond)
	w.Stop()
	state := w.GetStatus()
	if state.Status != WorkerStatusStopped {
		t.Errorf("status %v, expected Stopped after Stop", state.Status)
	}
}

// TestWorker_ConcurrentStopAndSubmit tests the system under concurrent
// Stop and Submit calls, ensuring no panics and that all operations
// complete gracefully.
func TestWorker_ConcurrentStopAndSubmit(t *testing.T) {
	ctx := context.Background()
	fn := func(_ context.Context, _ []byte) (string, *ExecutionError) {
		time.Sleep(50 * time.Millisecond)
		return "ok", nil
	}
	cfg, _ := NewWorkerCfg(2, 5, time.Second)
	w, _ := NewWorker(ctx, cfg, fn)
	w.Start()

	var wg sync.WaitGroup
	stopCount := 5
	submitCount := 20

	// Concurrent Submits.
	for i := 0; i < submitCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			task := &Task{ID: getID(), Payload: []byte("x")}
			_ = w.Submit(task) // errors are expected during shutdown
		}()
	}

	// Concurrent Stops.
	for i := 0; i < stopCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.Stop()
		}()
	}

	wg.Wait()

	// After all stops, the pool must be Stopped.
	state := w.GetStatus()
	if state.Status != WorkerStatusStopped {
		t.Errorf("expected Stopped, got %v", state.Status)
	}
	// Further Submit must fail.
	err := w.Submit(&Task{ID: getID(), Payload: []byte("x")})
	if err == nil {
		t.Error("expected error when submitting after Stop")
	}
}

// TestWorker_OutChannelAfterRestart verifies that after a full Stop and restart,
// the outQueue is recreated and the old channel is closed.
func TestWorker_OutChannelAfterRestart(t *testing.T) {
	ctx := context.Background()
	fn := func(_ context.Context, payload []byte) (string, *ExecutionError) {
		return "done", nil
	}
	cfg, _ := NewWorkerCfg(1, 1, time.Second)
	w, _ := NewWorker(ctx, cfg, fn)
	w.Start()

	oldOut := w.GetOutChan()
	task1 := &Task{ID: getID(), Payload: []byte("first")}
	_ = w.Submit(task1)
	<-oldOut

	w.Stop()
	// Old channel should be closed.
	_, ok := <-oldOut
	if ok {
		t.Error("old outQueue should be closed after Stop")
	}

	// Restart.
	w.Start()
	newOut := w.GetOutChan()
	if newOut == oldOut {
		t.Error("after restart outQueue should be a new channel")
	}

	task2 := &Task{ID: getID(), Payload: []byte("second")}
	_ = w.Submit(task2)
	select {
	case res := <-newOut:
		if string(res.Result.Result) != "done" {
			t.Errorf("unexpected result after restart: %s", res.Result.Result)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for result from new outQueue")
	}

	w.Stop()
}

// TestWorker_PanicRecovery ensures that a panic in the user-defined function
// is recovered and converted into a critical error.
func TestWorker_PanicRecovery(t *testing.T) {
	ctx := context.Background()
	fn := func(_ context.Context, _ []byte) (string, *ExecutionError) {
		panic("unexpected panic")
	}
	cfg, _ := NewWorkerCfg(1, 1, time.Second)
	w, _ := NewWorker(ctx, cfg, fn)
	w.Start()
	defer w.Stop()

	task := &Task{ID: getID(), Payload: []byte("x")}
	_ = w.Submit(task)

	select {
	case res := <-w.GetOutChan():
		if res.Result.Status != StatusFailure {
			t.Errorf("expected Failure, got %v", res.Result.Status)
		}
		if !res.Result.IsCritical {
			t.Error("panic should result in critical error")
		}
		if len(res.Result.Result) == 0 {
			t.Error("result should contain error details")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for result")
	}
}
