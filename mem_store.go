package retrier

import (
	"sync"
)

// MemStore is an in-memory implementation of the Store interface.
// It retains only tasks that are not yet finished (pending or suspended),
// allowing them to be retried. Completed tasks (success or permanent failure)
// are skipped during SaveTask.
//
// This implementation is intended for testing and demonstration purposes only.
// For production use, consider a persistent store with proper indexing and
// transaction support.
type MemStore struct {
	mtx   sync.RWMutex
	store []Task
}

// NewMemStore creates a new empty in-memory store.
func NewMemStore() *MemStore {
	return &MemStore{
		store: make([]Task, 0),
	}
}

// GetTasks returns all tasks currently stored in memory.
// It returns a copy of the internal slice to avoid external modifications.
func (ms *MemStore) GetTasks() ([]Task, error) {
	ms.mtx.RLock()
	defer ms.mtx.RUnlock()
	tasks := make([]Task, len(ms.store))
	copy(tasks, ms.store)
	return tasks, nil
}

// SaveTask stores a task if it is not yet finished (status is not "success" or "failure").
// If a task with the same ID already exists, it is updated in place.
// This ensures that retry counts, status, and NextRun are kept current.
func (ms *MemStore) SaveTask(t *Task, _ *TaskExecutionResult) error {
	// Ignore finished tasks – they are no longer needed for retries.
	if t.Status == StatusSuccess || t.Status == StatusFailure {
		return nil
	}

	ms.mtx.Lock()
	defer ms.mtx.Unlock()

	// Look for an existing task with the same ID and update it.
	for i, existing := range ms.store {
		if existing.ID == t.ID {
			ms.store[i] = *t
			return nil
		}
	}

	// Otherwise, append the new task.
	ms.store = append(ms.store, *t)
	return nil
}
