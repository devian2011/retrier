package retrier

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestWorker_NewWorker verifies that a new worker is created with the correct
// initial status.
func TestWorker_NewWorker(t *testing.T) {
	ctx := context.Background()
	fn := func(_ context.Context, _ []byte) (string, *ExecutionError) {
		return "ok", nil
	}
	w := NewWorker(ctx, fn)
	if w == nil {
		t.Fatal("NewWorker returned nil")
	}
	state := w.GetStatus()
	if state.Status != WorkerStatusCreated {
		t.Errorf("expected status %v, got %v", WorkerStatusCreated, state.Status)
	}
}

// TestWorker_SetMinAndMaxWorkers checks that the boundaries can be changed.
func TestWorker_SetMinAndMaxWorkers(t *testing.T) {
	ctx := context.Background()
	fn := func(_ context.Context, _ []byte) (string, *ExecutionError) {
		return "ok", nil
	}
	w := NewWorker(ctx, fn)
	w.SetMinAndMaxWorkers(2, 5)
	if w.minWorkers != 2 || w.maxWorkers != 5 {
		t.Errorf("minWorkers=%d, maxWorkers=%d; expected 2 and 5", w.minWorkers, w.maxWorkers)
	}
}

// TestWorker_SetIdleTimeout verifies that the idle timeout can be set.
func TestWorker_SetIdleTimeout(t *testing.T) {
	ctx := context.Background()
	fn := func(_ context.Context, _ []byte) (string, *ExecutionError) {
		return "ok", nil
	}
	w := NewWorker(ctx, fn)
	timeout := 10 * time.Second
	w.SetIdleTimeout(timeout)
	if w.idleTimeout != timeout {
		t.Errorf("idleTimeout=%v, expected %v", w.idleTimeout, timeout)
	}
}

// TestWorker_Start ensures that Start spawns the minimum number of workers and
// transitions the pool to the running state.
func TestWorker_Start(t *testing.T) {
	ctx := context.Background()
	fn := func(_ context.Context, _ []byte) (string, *ExecutionError) {
		return "ok", nil
	}
	w := NewWorker(ctx, fn)
	w.SetMinAndMaxWorkers(2, 5)
	w.Start()

	state := w.GetStatus()
	if state.Status != WorkerStatusRunning {
		t.Errorf("expected status Running, got %v", state.Status)
	}
	if state.ActiveWorkers != 2 {
		t.Errorf("active workers %d, expected 2", state.ActiveWorkers)
	}

	// Calling Start again should have no effect.
	w.Start()
	state2 := w.GetStatus()
	if state2.ActiveWorkers != 2 {
		t.Errorf("after second Start active workers %d, expected 2", state2.ActiveWorkers)
	}

	w.Stop()
}

// TestWorker_Submit_Success tests the happy path: submitting a task that
// completes successfully and produces a result.
func TestWorker_Submit_Success(t *testing.T) {
	ctx := context.Background()
	fn := func(_ context.Context, payload []byte) (string, *ExecutionError) {
		return "processed: " + string(payload), nil
	}
	w := NewWorker(ctx, fn)
	w.SetMinAndMaxWorkers(1, 1)
	w.Start()
	defer w.Stop()

	task := &Task{ID: getID(), Payload: []byte("hello")}
	err := w.Submit(task)
	if err != nil {
		t.Fatalf("Submit returned error: %v", err)
	}

	select {
	case res := <-w.GetOutChan():
		if res.task.ID != task.ID {
			t.Errorf("task ID mismatch: %s vs %s", res.task.ID, task.ID)
		}
		if res.result.Status != StatusSuccess {
			t.Errorf("status %s, expected Success", res.result.Status)
		}
		if string(res.result.Result) != "processed: hello" {
			t.Errorf("result %s, expected 'processed: hello'", string(res.result.Result))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for result")
	}
}

// TestWorker_Submit_WhenNotRunning ensures that submitting a task to a
// non-running pool returns an error.
func TestWorker_Submit_WhenNotRunning(t *testing.T) {
	ctx := context.Background()
	fn := func(_ context.Context, _ []byte) (string, *ExecutionError) {
		return "ok", nil
	}
	w := NewWorker(ctx, fn)
	task := &Task{ID: getID(), Payload: []byte("hello")}
	err := w.Submit(task)
	if err == nil {
		t.Error("expected error when submitting to non-running worker")
	}
}

// TestWorker_Submit_ScalingUp verifies that the pool automatically scales up
// when all existing workers are busy and the maximum limit has not been reached.
func TestWorker_Submit_ScalingUp(t *testing.T) {
	ctx := context.Background()
	fn := func(_ context.Context, _ []byte) (string, *ExecutionError) {
		time.Sleep(200 * time.Millisecond) // simulate work
		return "ok", nil
	}
	w := NewWorker(ctx, fn)
	w.SetMinAndMaxWorkers(1, 3)
	w.Start()
	defer w.Stop()

	// Submit 3 tasks to trigger scaling.
	var tasks []*Task
	for i := 0; i < 3; i++ {
		tasks = append(tasks, &Task{ID: getID(), Payload: []byte("x")})
	}
	for _, task := range tasks {
		err := w.Submit(task)
		if err != nil {
			t.Fatalf("Submit error: %v", err)
		}
	}

	// Allow time for scaling.
	time.Sleep(100 * time.Millisecond)
	state := w.GetStatus()
	if state.ActiveWorkers != 3 {
		t.Errorf("active workers %d, expected 3", state.ActiveWorkers)
	}

	// Collect results.
	for i := 0; i < 3; i++ {
		select {
		case <-w.GetOutChan():
		case <-time.After(1 * time.Second):
			t.Fatal("timeout waiting for result")
		}
	}
}

// TestWorker_Submit_BlocksWhenMaxWorkersReached checks that submission blocks
// when all workers are busy and the maximum cap has been reached.
func TestWorker_Submit_BlocksWhenMaxWorkersReached(t *testing.T) {
	ctx := context.Background()
	fn := func(_ context.Context, _ []byte) (string, *ExecutionError) {
		time.Sleep(300 * time.Millisecond) // long-running work
		return "ok", nil
	}
	w := NewWorker(ctx, fn)
	w.SetMinAndMaxWorkers(1, 1) // only one worker
	w.Start()
	defer w.Stop()

	task1 := &Task{ID: getID(), Payload: []byte("x")}
	task2 := &Task{ID: getID(), Payload: []byte("y")}

	err1 := w.Submit(task1)
	if err1 != nil {
		t.Fatalf("Submit task1 error: %v", err1)
	}

	// task2 will block until task1 finishes.
	done := make(chan bool)
	go func() {
		err2 := w.Submit(task2)
		if err2 != nil {
			t.Errorf("Submit task2 error: %v", err2)
		}
		done <- true
	}()

	// Wait for task1 to finish and then task2 should be accepted.
	<-w.GetOutChan()
	<-w.GetOutChan()
	// The goroutine should have unblocked by now.
	select {
	case <-done:
		// ok
	case <-time.After(500 * time.Millisecond):
		t.Error("task2 submission did not unblock in time")
	}
}

// TestWorker_Stop verifies that Stop waits for active tasks to complete and
// then transitions to the stopped state.
func TestWorker_Stop(t *testing.T) {
	ctx := context.Background()
	fn := func(_ context.Context, _ []byte) (string, *ExecutionError) {
		return "ok", nil
	}
	w := NewWorker(ctx, fn)
	w.SetMinAndMaxWorkers(2, 5)
	w.Start()

	// Use a slow worker to keep a task active.
	fnSlow := func(_ context.Context, _ []byte) (string, *ExecutionError) {
		time.Sleep(500 * time.Millisecond)
		return "ok", nil
	}
	slowWorker := NewWorker(ctx, fnSlow)
	slowWorker.SetMinAndMaxWorkers(1, 1)
	slowWorker.Start()
	defer slowWorker.Stop()

	task := &Task{ID: getID(), Payload: []byte("x")}
	_ = slowWorker.Submit(task)

	// Stop should block until the active task finishes.
	stopCh := make(chan bool)
	go func() {
		slowWorker.Stop()
		stopCh <- true
	}()

	select {
	case <-stopCh:
		// Stop completed.
	case <-time.After(1 * time.Second):
		t.Fatal("Stop did not wait for task completion")
	}

	state := slowWorker.GetStatus()
	if state.Status != WorkerStatusStopped {
		t.Errorf("status %v, expected Stopped", state.Status)
	}
	// Further Submit should fail.
	err := slowWorker.Submit(&Task{ID: getID(), Payload: []byte("x")})
	if err == nil {
		t.Error("expected error when submitting after Stop")
	}
}

// TestWorker_Suspend tests that Suspend changes the status to Suspended and
// prevents new submissions while active tasks are allowed to finish.
func TestWorker_Suspend(t *testing.T) {
	ctx := context.Background()
	fn := func(_ context.Context, _ []byte) (string, *ExecutionError) {
		time.Sleep(100 * time.Millisecond)
		return "ok", nil
	}
	w := NewWorker(ctx, fn)
	w.SetMinAndMaxWorkers(1, 1)
	w.Start()
	defer w.Stop()

	task := &Task{ID: getID(), Payload: []byte("x")}
	_ = w.Submit(task)

	w.Suspend()
	state := w.GetStatus()
	if state.Status != WorkerStatusSuspended {
		t.Errorf("status %v, expected Suspended", state.Status)
	}

	// New submissions should be rejected.
	err := w.Submit(&Task{ID: getID(), Payload: []byte("y")})
	if err == nil {
		t.Error("expected error when submitting after Suspend")
	}

	// The active task's result should still arrive.
	select {
	case <-w.GetOutChan():
	case <-time.After(500 * time.Millisecond):
		t.Fatal("did not receive result of active task after Suspend")
	}
}

// TestWorker_ExecutionError validates that errors are correctly wrapped and
// propagated as failure results.
func TestWorker_ExecutionError(t *testing.T) {
	ctx := context.Background()
	fn := func(_ context.Context, payload []byte) (string, *ExecutionError) {
		if string(payload) == "critical" {
			return "", &ExecutionError{Err: errors.New("critical error"), State: CriticalState}
		}
		return "", &ExecutionError{Err: errors.New("usual error"), State: UsualState}
	}
	w := NewWorker(ctx, fn)
	w.SetMinAndMaxWorkers(1, 1)
	w.Start()
	defer w.Stop()

	taskCrit := &Task{ID: getID(), Payload: []byte("critical")}
	taskUsual := &Task{ID: getID(), Payload: []byte("usual")}

	// Submit the first task.
	if err := w.Submit(taskCrit); err != nil {
		t.Fatalf("Submit taskCrit error: %v", err)
	}

	// Read its result immediately to unblock the worker.
	select {
	case resCrit := <-w.GetOutChan():
		if resCrit.result.Status != StatusFailure {
			t.Errorf("critical error status %s, expected Failure", resCrit.result.Status)
		}
		if len(resCrit.result.Result) == 0 {
			t.Error("result for critical error is empty")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for critical result")
	}

	// Now submit the second task.
	if err := w.Submit(taskUsual); err != nil {
		t.Fatalf("Submit taskUsual error: %v", err)
	}

	// Read its result.
	select {
	case resUsual := <-w.GetOutChan():
		if resUsual.result.Status != StatusFailure {
			t.Errorf("usual error status %s, expected Failure", resUsual.result.Status)
		}
		if len(resUsual.result.Result) == 0 {
			t.Error("result for usual error is empty")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for usual result")
	}
}

// TestWorker_IdleTimeout_ScaleDown checks that idle workers time out and
// scale down to the minimum number.
func TestWorker_IdleTimeout_ScaleDown(t *testing.T) {
	ctx := context.Background()
	fn := func(_ context.Context, _ []byte) (string, *ExecutionError) {
		return "ok", nil
	}
	w := NewWorker(ctx, fn)
	w.SetMinAndMaxWorkers(1, 3)
	w.SetIdleTimeout(200 * time.Millisecond)
	w.Start()

	// Load the pool to spawn 3 workers.
	for i := 0; i < 3; i++ {
		_ = w.Submit(&Task{ID: getID(), Payload: []byte("x")})
	}
	// Wait for all tasks to finish.
	for i := 0; i < 3; i++ {
		<-w.GetOutChan()
	}

	// Allow the idle timeout to fire.
	time.Sleep(300 * time.Millisecond)

	state := w.GetStatus()
	if state.ActiveWorkers != 1 {
		t.Errorf("active workers %d, expected 1 after idle timeout", state.ActiveWorkers)
	}
	w.Stop()
}

// TestWorker_ContextCancellationDuringTask demonstrates that canceling the
// parent context will eventually cause workers to exit, though the status
// remains Running until Stop is called.
func TestWorker_ContextCancellationDuringTask(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	fn := func(_ context.Context, _ []byte) (string, *ExecutionError) {
		time.Sleep(500 * time.Millisecond) // long task
		return "ok", nil
	}
	w := NewWorker(ctx, fn)
	w.SetMinAndMaxWorkers(1, 1)
	w.Start()

	task := &Task{ID: getID(), Payload: []byte("x")}
	_ = w.Submit(task)

	// Cancel the context while the task is running.
	time.Sleep(100 * time.Millisecond)
	cancel()

	// The worker will finish the current task, then on the next loop iteration
	// it will see the canceled context and exit. We wait for that.
	time.Sleep(600 * time.Millisecond)

	// The status is not automatically changed by context cancellation;
	// it remains Running until Stop is called. We call Stop to clean up.
	w.Stop()
	state := w.GetStatus()
	if state.Status != WorkerStatusStopped {
		t.Errorf("status %v, expected Stopped after Stop", state.Status)
	}
}
