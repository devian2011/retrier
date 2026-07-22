// circuit_breaker_test.go
package retrier

import (
	"testing"
	"time"
)

func TestNewSlidingWindowCircuitBreaker(t *testing.T) {
	cb := NewSlidingWindowCircuitBreaker(10*time.Second, 0.5, 5, 5*time.Second)
	if cb == nil {
		t.Fatal("expected non-nil circuit breaker")
	}
	if cb.State() != StateClosed {
		t.Errorf("expected initial state StateClosed, got %v", cb.State())
	}
}

func TestCircuitBreaker_Allow_Closed(t *testing.T) {
	cb := NewSlidingWindowCircuitBreaker(10*time.Second, 0.5, 3, 5*time.Second)
	for i := 0; i < 10; i++ {
		if !cb.Allow() {
			t.Errorf("Allow returned false in closed state (iteration %d)", i)
		}
	}
}

func TestCircuitBreaker_OpenOnThreshold(t *testing.T) {
	cb := NewSlidingWindowCircuitBreaker(10*time.Second, 0.5, 3, 5*time.Second)

	cb.RecordFailure()
	cb.RecordFailure()
	if cb.State() != StateClosed {
		t.Error("circuit should remain closed below minRequests threshold")
	}

	// Third failure triggers open (total=3, failures=3, ratio=1.0 > 0.5)
	cb.RecordFailure()
	if cb.State() != StateOpen {
		t.Error("circuit should be open after exceeding failure threshold")
	}
	if cb.Allow() {
		t.Error("Allow should return false in open state")
	}
}

func TestCircuitBreaker_StaysClosedWhenUnderThreshold(t *testing.T) {
	cb := NewSlidingWindowCircuitBreaker(10*time.Second, 0.5, 3, 5*time.Second)

	cb.RecordFailure()
	cb.RecordSuccess()
	cb.RecordSuccess()
	if cb.State() != StateClosed {
		t.Error("circuit should remain closed when failure ratio is below threshold")
	}
}

func TestCircuitBreaker_HalfOpenAfterTimeout(t *testing.T) {
	cb := NewSlidingWindowCircuitBreaker(10*time.Second, 0.5, 3, 100*time.Millisecond)

	// Force open: 3 failures
	cb.RecordFailure()
	cb.RecordFailure()
	cb.RecordFailure()
	if cb.State() != StateOpen {
		t.Fatal("failed to open circuit")
	}

	if cb.Allow() {
		t.Error("Allow should be false in open state")
	}

	time.Sleep(150 * time.Millisecond)

	if !cb.Allow() {
		t.Error("Allow should return true after timeout (half-open transition)")
	}
	if cb.State() != StateHalfOpen {
		t.Errorf("expected StateHalfOpen, got %v", cb.State())
	}
}

func TestCircuitBreaker_SuccessInHalfOpenCloses(t *testing.T) {
	cb := NewSlidingWindowCircuitBreaker(10*time.Second, 0.5, 3, 100*time.Millisecond)

	cb.RecordFailure()
	cb.RecordFailure()
	cb.RecordFailure()
	if cb.State() != StateOpen {
		t.Fatal("failed to open circuit")
	}

	time.Sleep(150 * time.Millisecond)

	if !cb.Allow() {
		t.Fatal("Allow should return true in half-open")
	}
	if cb.State() != StateHalfOpen {
		t.Fatalf("expected StateHalfOpen, got %v", cb.State())
	}

	cb.RecordSuccess()
	if cb.State() != StateClosed {
		t.Errorf("expected StateClosed after successful probe, got %v", cb.State())
	}
	if len(cb.successes) != 0 || len(cb.failures) != 0 {
		t.Errorf("windows should be cleared after closing, successes=%d, failures=%d",
			len(cb.successes), len(cb.failures))
	}
}

func TestCircuitBreaker_FailureInHalfOpenReopens(t *testing.T) {
	cb := NewSlidingWindowCircuitBreaker(10*time.Second, 0.5, 3, 100*time.Millisecond)

	cb.RecordFailure()
	cb.RecordFailure()
	cb.RecordFailure()
	if cb.State() != StateOpen {
		t.Fatal("failed to open circuit")
	}

	time.Sleep(150 * time.Millisecond)

	if !cb.Allow() {
		t.Fatal("Allow should return true in half-open")
	}
	if cb.State() != StateHalfOpen {
		t.Fatalf("expected StateHalfOpen, got %v", cb.State())
	}

	cb.RecordFailure()
	if cb.State() != StateOpen {
		t.Errorf("expected StateOpen after failed probe, got %v", cb.State())
	}
}

func TestCircuitBreaker_PurgeOldEntries(t *testing.T) {
	cb := NewSlidingWindowCircuitBreaker(100*time.Millisecond, 0.5, 3, 5*time.Second)

	cb.RecordFailure() // t0
	time.Sleep(50 * time.Millisecond)
	cb.RecordFailure() // t0+50ms
	time.Sleep(50 * time.Millisecond)
	cb.RecordSuccess() // t0+100ms

	now := time.Now()
	cb.purge(now.Add(200 * time.Millisecond)) // cutoff = now + 200ms - 100ms = now + 100ms

	if len(cb.successes) != 0 || len(cb.failures) != 0 {
		t.Errorf("after purging with future cutoff, successes=%d, failures=%d",
			len(cb.successes), len(cb.failures))
	}
}

func TestCircuitBreaker_Reset(t *testing.T) {
	cb := NewSlidingWindowCircuitBreaker(10*time.Second, 0.5, 3, 5*time.Second)

	cb.RecordFailure()
	cb.RecordFailure()
	cb.RecordFailure()
	if cb.State() != StateOpen {
		t.Fatal("failed to open circuit")
	}

	cb.Reset()
	if cb.State() != StateClosed {
		t.Errorf("expected StateClosed after Reset, got %v", cb.State())
	}
	if len(cb.successes) != 0 || len(cb.failures) != 0 {
		t.Errorf("windows not cleared after Reset, successes=%d, failures=%d",
			len(cb.successes), len(cb.failures))
	}
}

func TestCircuitBreaker_ConcurrentSafety(t *testing.T) {
	cb := NewSlidingWindowCircuitBreaker(1*time.Second, 0.5, 3, 100*time.Millisecond)

	done := make(chan bool, 2)

	go func() {
		for i := 0; i < 100; i++ {
			cb.Allow()
			cb.RecordSuccess()
			cb.RecordFailure()
			cb.State()
			time.Sleep(1 * time.Millisecond)
		}
		done <- true
	}()

	go func() {
		for i := 0; i < 100; i++ {
			cb.Allow()
			cb.RecordSuccess()
			cb.RecordFailure()
			cb.State()
			time.Sleep(1 * time.Millisecond)
		}
		done <- true
	}()

	<-done
	<-done
}
