package retrier

import (
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestMemStore_NewMemStore(t *testing.T) {
	ms := NewMemStore()
	if ms == nil {
		t.Fatal("NewMemStore returned nil")
	}
	if len(ms.store) != 0 {
		t.Errorf("expected empty store, got %d items", len(ms.store))
	}
}

func TestMemStore_SaveTask_AddsPendingTask(t *testing.T) {
	ms := NewMemStore()
	task := &Task{
		ID:     uuid.New(),
		Status: StatusPending,
	}
	err := ms.SaveTask(task, nil)
	if err != nil {
		t.Fatalf("SaveTask error: %v", err)
	}
	tasks, _ := ms.GetTasks()
	if len(tasks) != 1 {
		t.Fatalf("expected 1 task, got %d", len(tasks))
	}
	if tasks[0].ID != task.ID {
		t.Errorf("task ID mismatch: got %v, want %v", tasks[0].ID, task.ID)
	}
}

func TestMemStore_SaveTask_IgnoresFinishedTasks(t *testing.T) {
	ms := NewMemStore()
	task := &Task{
		ID:     uuid.New(),
		Status: StatusSuccess,
	}
	err := ms.SaveTask(task, nil)
	if err != nil {
		t.Fatalf("SaveTask error: %v", err)
	}
	tasks, _ := ms.GetTasks()
	if len(tasks) != 0 {
		t.Errorf("expected 0 tasks (finished ignored), got %d", len(tasks))
	}
}

func TestMemStore_SaveTask_UpdatesExistingTask(t *testing.T) {
	ms := NewMemStore()
	id := uuid.New()
	task1 := &Task{
		ID:      id,
		Status:  StatusPending,
		Retries: 0,
		NextRun: time.Now(),
	}
	_ = ms.SaveTask(task1, nil)

	task2 := &Task{
		ID:      id,
		Status:  StatusPending,
		Retries: 1,
		NextRun: time.Now().Add(time.Minute),
	}
	_ = ms.SaveTask(task2, nil)

	tasks, _ := ms.GetTasks()
	if len(tasks) != 1 {
		t.Fatalf("expected 1 task, got %d", len(tasks))
	}
	if tasks[0].Retries != 1 {
		t.Errorf("expected Retries=1, got %d", tasks[0].Retries)
	}
	if tasks[0].NextRun != task2.NextRun {
		t.Errorf("NextRun not updated")
	}
}

func TestMemStore_GetTasks_ReturnsCopy(t *testing.T) {
	ms := NewMemStore()
	task := &Task{
		ID:     uuid.New(),
		Status: StatusPending,
	}
	_ = ms.SaveTask(task, nil)

	tasks, _ := ms.GetTasks()
	// Modify the returned slice.
	tasks[0].Retries = 99
	// Original store must remain unchanged.
	original, _ := ms.GetTasks()
	if original[0].Retries == 99 {
		t.Error("GetTasks returned a reference, not a copy")
	}
}

func TestMemStore_ConcurrentAccess(*testing.T) {
	ms := NewMemStore()
	var wg sync.WaitGroup
	iterations := 100

	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			task := &Task{
				ID:     uuid.New(),
				Status: StatusPending,
			}
			_ = ms.SaveTask(task, nil)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			_, _ = ms.GetTasks()
		}
	}()
	wg.Wait()
}
