// circuit_breaker.go
package retrier

import (
	"sync"
	"time"
)

// CircuitBreakerState represents the state of a circuit breaker.
type CircuitBreakerState string

const (
	StateClosed   CircuitBreakerState = "closed"
	StateOpen     CircuitBreakerState = "open"
	StateHalfOpen CircuitBreakerState = "half-open"
)

// CircuitBreaker implements a circuit breaker using a sliding time window.
// It tracks success/failure counts over a configurable window duration and
// opens the circuit when the failure rate exceeds a given threshold.
type CircuitBreaker struct {
	mu sync.RWMutex

	state            CircuitBreakerState
	windowSize       time.Duration // size of the sliding window
	failureThreshold float64       // failure ratio (0.0–1.0) that triggers open
	minRequests      int           // minimum requests in window to evaluate threshold

	successes []time.Time // timestamps of successful requests within window
	failures  []time.Time // timestamps of failed requests within window

	lastFailureTime time.Time     // used for half-open timeout
	timeout         time.Duration // time to wait before transitioning from open to half-open
}

// NewSlidingWindowCircuitBreaker creates a new circuit breaker with sliding window.
// Parameters:
//
//	windowSize: duration of the sliding window
//	failureThreshold: allowed failure ratio (e.g., 0.5 means 50% failures)
//	minRequests: minimum number of requests required before evaluating threshold
//	timeout: duration to wait in open state before attempting half-open
func NewSlidingWindowCircuitBreaker(
	windowSize time.Duration,
	failureThreshold float64,
	minRequests int,
	timeout time.Duration,
) *CircuitBreaker {
	return &CircuitBreaker{
		state:            StateClosed,
		windowSize:       windowSize,
		failureThreshold: failureThreshold,
		minRequests:      minRequests,
		timeout:          timeout,
		successes:        make([]time.Time, 0),
		failures:         make([]time.Time, 0),
	}
}

// Allow checks if a request is permitted.
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := time.Now()

	switch cb.state {
	case StateClosed:
		// allowed
		return true
	case StateOpen:
		// check if timeout expired
		if now.Sub(cb.lastFailureTime) > cb.timeout {
			cb.state = StateHalfOpen
			return true
		}
		return false
	case StateHalfOpen:
		// allow a single probe
		return true
	default:
		return false
	}
}

// RecordSuccess records a successful execution.
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := time.Now()
	cb.addSuccess(now)

	if cb.state == StateHalfOpen {
		cb.state = StateClosed
		cb.successes = cb.successes[:0]
		cb.failures = cb.failures[:0]
	}
}

func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := time.Now()
	cb.addFailure(now)
	cb.lastFailureTime = now

	switch cb.state {
	case StateClosed:
		if cb.shouldOpen(now) {
			cb.state = StateOpen
		}
	case StateHalfOpen:
		cb.state = StateOpen
	}
}

// shouldOpen evaluates whether the circuit should transition to open.
func (cb *CircuitBreaker) shouldOpen(now time.Time) bool {
	cb.purge(now)

	total := len(cb.successes) + len(cb.failures)
	if total < cb.minRequests {
		return false
	}

	if total == 0 {
		return false
	}

	failureRatio := float64(len(cb.failures)) / float64(total)
	return failureRatio >= cb.failureThreshold
}

// purge removes entries older than windowSize.
func (cb *CircuitBreaker) purge(now time.Time) {
	cutoff := now.Add(-cb.windowSize)
	successes := make([]time.Time, 0, len(cb.successes))
	for _, t := range cb.successes {
		if t.After(cutoff) {
			successes = append(successes, t)
		}
	}
	cb.successes = successes

	failures := make([]time.Time, 0, len(cb.failures))
	for _, t := range cb.failures {
		if t.After(cutoff) {
			failures = append(failures, t)
		}
	}
	cb.failures = failures
}

// addSuccess appends a success timestamp.
func (cb *CircuitBreaker) addSuccess(t time.Time) {
	cb.successes = append(cb.successes, t)
}

// addFailure appends a failure timestamp.
func (cb *CircuitBreaker) addFailure(t time.Time) {
	cb.failures = append(cb.failures, t)
}

// State returns the current state (thread-safe).
func (cb *CircuitBreaker) State() CircuitBreakerState {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}

// Reset manually resets the circuit to closed state and clears windows.
func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.state = StateClosed
	cb.successes = cb.successes[:0]
	cb.failures = cb.failures[:0]
}
