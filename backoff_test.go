package retrier

import (
	"sync"
	"testing"
	"time"
)

// mockBackOffParams implements the BackOffParams interface for testing purposes.
type mockBackOffParams struct {
	code   string
	params map[BackOffParam]interface{}
	tries  int
}

func (m mockBackOffParams) GetBackOffCode() string                         { return m.code }
func (m mockBackOffParams) GetBackOffParams() map[BackOffParam]interface{} { return m.params }
func (m mockBackOffParams) GetRetries() int                                { return m.tries }

// TestBackOffStrategy_Concurrency checks for race conditions during concurrent registration and reads.
func TestBackOffStrategy_Concurrency(_ *testing.T) {
	strategy := NewBackOffStrategy()
	dummyFn := func(BackOffParams) (time.Time, error) {
		return time.Now(), nil
	}

	// Register a common handler for the race test.
	const customCode = "custom"
	strategy.Register(customCode, dummyFn)

	var wg sync.WaitGroup
	const goroutineCount = 50
	const iterations = 100

	// Goroutines that read the strategy (call Get)
	for i := 0; i < goroutineCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				_, _ = strategy.Get(mockBackOffParams{code: customCode})
			}
		}()
	}

	// Goroutines that overwrite the same handler
	for i := 0; i < goroutineCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				strategy.Register(customCode, dummyFn)
			}
		}()
	}
	wg.Wait()
}

// TestBackOffStrategy_Get verifies routing within BackOffStrategy and error handling for missing strategies.
func TestBackOffStrategy_Get(t *testing.T) {
	strategy := NewBackOffStrategy()

	t.Run("Unsupported Strategy Code", func(t *testing.T) {
		mock := mockBackOffParams{code: "unsupported_code"}
		_, err := strategy.Get(mock)
		if err == nil {
			t.Fatal("expected an error for an unsupported backoff code, but got nil")
		}
	})

	t.Run("Successful Strategy Routing", func(t *testing.T) {
		mock := mockBackOffParams{
			code:   LinearBackOff,
			params: map[BackOffParam]interface{}{DurationKey: 2 * time.Second},
		}

		now := time.Now()
		res, err := strategy.Get(mock)
		if err != nil {
			t.Fatalf("unexpected routing error: %v", err)
		}

		expected := now.Add(2 * time.Second)
		if res.Before(expected) || res.After(expected.Add(50*time.Millisecond)) {
			t.Errorf("expected time near %v, got %v", expected, res)
		}
	})
}

// TestLinearBackOff ensures the linear strategy handles missing properties and type assertion mismatches properly.
func TestLinearBackOff(t *testing.T) {
	t.Run("Missing Duration Parameter", func(t *testing.T) {
		mock := mockBackOffParams{params: map[BackOffParam]interface{}{}}
		_, err := linearBackOff(mock)
		if err == nil {
			t.Error("expected an error due to missing 'duration' key, but got nil")
		}
	})

	t.Run("Invalid Duration Parameter Type", func(t *testing.T) {
		mock := mockBackOffParams{params: map[BackOffParam]interface{}{DurationKey: "10s"}}
		_, err := linearBackOff(mock)
		if err == nil {
			t.Error("expected an error due to an incorrect 'duration' parameter type, but got nil")
		}
	})
}

// TestJitterLinearBackOff verifies linear delay calculations both with custom and fallback default jitter values.
func TestJitterLinearBackOff(t *testing.T) {
	duration := 5 * time.Second
	jitter := 100.0 // ±100ms noise

	mock := mockBackOffParams{
		params: map[BackOffParam]interface{}{
			DurationKey: duration,
			JitterKey:   jitter,
		},
	}

	now := time.Now()
	res, err := jitterLinearBackOff(mock)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	minExpected := now.Add(duration).Add(-time.Duration(jitter) * time.Millisecond)
	maxExpected := now.Add(duration).Add(time.Duration(jitter) * time.Millisecond).Add(50 * time.Millisecond)

	if res.Before(minExpected) || res.After(maxExpected) {
		t.Errorf("result %v out of expected jitter execution bounds [%v, %v]", res, minExpected, maxExpected)
	}
}

// TestExponentialBackoff handles structural verification of the pure exponential growth algorithms.
func TestExponentialBackoff(t *testing.T) {
	tests := []struct {
		name        string
		tries       int
		params      map[BackOffParam]interface{}
		minExpected time.Duration
		maxExpected time.Duration
	}{
		{
			name:        "Immediate execution when tries <= 0",
			tries:       0,
			params:      map[BackOffParam]interface{}{},
			minExpected: 0,
			maxExpected: 10 * time.Millisecond,
		},
		{
			name:  "Initial attempt runs exactly on BaseDelay (multiplier^0 = 1)",
			tries: 1,
			params: map[BackOffParam]interface{}{
				BaseDelayKey:  2 * time.Second,
				MaxDelayKey:   30 * time.Second,
				MultiplierKey: 2.0,
			},
			minExpected: 2 * time.Second,
			maxExpected: 2 * time.Second,
		},
		{
			name:  "Subsequent attempts scale using the exponential multiplier (2s * 3^1 = 6s)",
			tries: 2,
			params: map[BackOffParam]interface{}{
				BaseDelayKey:  2 * time.Second,
				MaxDelayKey:   30 * time.Second,
				MultiplierKey: 3.0,
			},
			minExpected: 6 * time.Second,
			maxExpected: 6 * time.Second,
		},
		{
			name:        "Defaults safely trigger on empty maps (1s * 2^2 = 4s)",
			tries:       3,
			params:      map[BackOffParam]interface{}{},
			minExpected: 4 * time.Second,
			maxExpected: 4 * time.Second,
		},
		{
			name:  "Hard limits apply when values exceed MaxDelay bounds (1s * 2^9 = 512s -> capped at 10s)",
			tries: 10,
			params: map[BackOffParam]interface{}{
				BaseDelayKey:  1 * time.Second,
				MaxDelayKey:   10 * time.Second,
				MultiplierKey: 2.0,
			},
			minExpected: 10 * time.Second,
			maxExpected: 10 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := mockBackOffParams{
				tries:  tt.tries,
				params: tt.params,
			}

			now := time.Now()
			res, err := exponentialBackoff(mock)
			if err != nil {
				t.Fatalf("unexpected calculation error: %v", err)
			}

			minBound := now.Add(tt.minExpected)
			maxBound := now.Add(tt.maxExpected).Add(50 * time.Millisecond)

			if res.Before(minBound) || res.After(maxBound) {
				t.Errorf("calculated time %v is out of expected limits [%v, %v]", res, minBound, maxBound)
			}
		})
	}
}

// TestJitterExponentialBackoff validates exponential equations combined with randomization noise multipliers.
func TestJitterExponentialBackoff(t *testing.T) {
	jitterValue := 200.0 // ±200ms noise
	mock := mockBackOffParams{
		tries: 3, // Formula: 1s * 2^(3-1) = 4 seconds base delay.
		params: map[BackOffParam]interface{}{
			BaseDelayKey:  1 * time.Second,
			MaxDelayKey:   20 * time.Second,
			MultiplierKey: 2.0,
			JitterKey:     jitterValue,
		},
	}

	now := time.Now()
	res, err := jitterExponentialBackoff(mock)
	if err != nil {
		t.Fatalf("unexpected execution error: %v", err)
	}

	minBound := now.Add(4 * time.Second).Add(-time.Duration(jitterValue) * time.Millisecond)
	maxBound := now.Add(4 * time.Second).Add(time.Duration(jitterValue) * time.Millisecond).Add(50 * time.Millisecond)

	if res.Before(minBound) || res.After(maxBound) {
		t.Errorf("result %v out of expected randomized jitter-exponential bounds [%v, %v]", res, minBound, maxBound)
	}
}
