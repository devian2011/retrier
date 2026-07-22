package retrier

import (
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"sync"
	"time"
)

// BackOffParam defines the allowed parameter keys for backoff strategy configuration.
type BackOffParam string

const (
	// DurationKey specifies the fixed duration for linear backoff.
	DurationKey BackOffParam = "duration"
	// JitterKey specifies the jitter factor (as a fraction) to add randomness.
	JitterKey BackOffParam = "jitter"
	// MultiplierKey specifies the multiplier for exponential backoff.
	MultiplierKey BackOffParam = "multiplier"
	// MaxDelayKey specifies the upper bound for the calculated delay.
	MaxDelayKey BackOffParam = "maxDelay"
	// BaseDelayKey specifies the initial delay for exponential backoff.
	BaseDelayKey BackOffParam = "baseDelay"
)

// Predefined backoff strategy codes. Register these with the BackOffStrategy.
const (
	LinearBackOff            string = "linear"
	JitterLinearBackOff      string = "jitter-linear"
	ExponentialBackOff       string = "exponential"
	JitterExponentialBackoff string = "jitter-exponential"
)

// BackOffParams is an interface that any task or configuration must implement
// to be used with the backoff strategy. It provides the necessary metadata.
type BackOffParams interface {
	// GetBackOffCode returns the identifier of the desired backoff strategy.
	GetBackOffCode() string
	// GetBackOffParams returns the map of parameters for the strategy.
	GetBackOffParams() map[BackOffParam]interface{}
	// GetRetries returns the number of attempts already made.
	GetRetries() int
}

// BackOffFn is a function that computes the next execution time based on the
// provided BackOffParams. It returns the scheduled time or an error.
type BackOffFn func(params BackOffParams) (time.Time, error)

// BackOffStrategy manages a registry of named backoff strategies and provides
// a thread-safe way to compute the next execution time.
type BackOffStrategy struct {
	mtx        *sync.RWMutex
	strategies map[string]BackOffFn
}

// Register adds or overwrites a backoff strategy under the given code.
// This method is safe for concurrent use.
func (s *BackOffStrategy) Register(code string, fn BackOffFn) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.strategies[code] = fn
}

// Get computes the next execution time using the strategy identified by the
// BackOffParams. It returns an error if the strategy is not registered.
func (s *BackOffStrategy) Get(backOff BackOffParams) (time.Time, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	if strategy, exists := s.strategies[backOff.GetBackOffCode()]; exists {
		return strategy(backOff)
	}
	return time.Time{}, errors.New("backoff code not supported")
}

// NewBackOffStrategy creates a new BackOffStrategy pre‑registered with the four
// standard strategies: linear, jitter‑linear, exponential, and jitter‑exponential.
func NewBackOffStrategy() *BackOffStrategy {
	return &BackOffStrategy{
		mtx: &sync.RWMutex{},
		strategies: map[string]BackOffFn{
			LinearBackOff:            linearBackOff,
			JitterLinearBackOff:      jitterLinearBackOff,
			ExponentialBackOff:       exponentialBackoff,
			JitterExponentialBackoff: jitterExponentialBackoff,
		},
	}
}

// addJitter applies a random variation to the given duration based on the jitter factor.
// The jitter is uniformly distributed in the range [-jitter, +jitter] of the duration.
func addJitter(d time.Duration, jitter float64) time.Duration {
	//nolint:gosec // G404: weak RNG is acceptable for jitter in backoff strategies
	randomFactor := (rand.Float64() * 2 * jitter) - jitter
	return d + time.Duration(randomFactor*float64(time.Millisecond))
}

// getFromMap is a generic helper to safely extract a typed value from a parameter map.
// It returns an error if the key is missing or the value is of the wrong type.
func getFromMap[T any](m map[BackOffParam]interface{}, fnName string, key BackOffParam) (T, error) {
	var zero T

	if _, exists := m[key]; !exists {
		return zero, fmt.Errorf("%s: %s param not found", fnName, key)
	}

	result, converted := m[key].(T)
	if !converted {
		return zero, fmt.Errorf("%s: %s wrong param type", fnName, key)
	}

	return result, nil
}

// linearBackOff computes the next time as `now + duration`.
// It requires the `DurationKey` parameter.
func linearBackOff(backOff BackOffParams) (time.Time, error) {
	duration, err := getFromMap[time.Duration](backOff.GetBackOffParams(), LinearBackOff, DurationKey)
	if err != nil {
		return time.Time{}, err
	}

	return time.Now().Add(duration), nil
}

// jitterLinearBackOff computes `now + duration + jitter` where jitter is a random
// fraction of the duration. If JitterKey is not provided, it defaults to 0.4.
func jitterLinearBackOff(backOff BackOffParams) (time.Time, error) {
	const defaultJitter = 0.4

	duration, durationGetErr := getFromMap[time.Duration](backOff.GetBackOffParams(), LinearBackOff, DurationKey)
	if durationGetErr != nil {
		return time.Time{}, durationGetErr
	}

	jitter, jitterGetErr := getFromMap[float64](backOff.GetBackOffParams(), LinearBackOff, JitterKey)
	if jitterGetErr != nil {
		jitter = defaultJitter
	}

	duration = addJitter(duration, jitter)

	return time.Now().Add(duration), nil
}

// exponentialBackoff computes the delay as `baseDelay * multiplier^(retries-1)`,
// capped by MaxDelayKey. If parameters are omitted, sensible defaults are used.
func exponentialBackoff(backOff BackOffParams) (time.Time, error) {
	const (
		defaultMultiplier = 2
		defaultBaseDelay  = 1 * time.Second
		defaultMaxDelay   = 5 * time.Minute
	)

	if backOff.GetRetries() <= 0 {
		return time.Now(), nil
	}

	multiplier, mGetErr := getFromMap[float64](backOff.GetBackOffParams(), ExponentialBackOff, MultiplierKey)
	if mGetErr != nil {
		multiplier = defaultMultiplier
	}

	baseDelay, baseDelayErr := getFromMap[time.Duration](backOff.GetBackOffParams(), ExponentialBackOff, BaseDelayKey)
	if baseDelayErr != nil {
		baseDelay = defaultBaseDelay
	}

	maxDelay, maxDelayErr := getFromMap[time.Duration](backOff.GetBackOffParams(), ExponentialBackOff, MaxDelayKey)
	if maxDelayErr != nil {
		maxDelay = defaultMaxDelay
	}

	pow := math.Pow(multiplier, float64(backOff.GetRetries()-1))
	delay := time.Duration(float64(baseDelay) * pow)

	if delay > maxDelay || delay <= 0 {
		delay = maxDelay
	}

	return time.Now().Add(delay), nil
}

// jitterExponentialBackoff is like exponentialBackoff but adds jitter to the delay.
// JitterKey defaults to 0.2 if not provided.
func jitterExponentialBackoff(backOff BackOffParams) (time.Time, error) {
	const (
		defaultMultiplier = 2
		defaultBaseDelay  = 1 * time.Second
		defaultMaxDelay   = 5 * time.Minute
		defaultJitter     = 0.2
	)

	if backOff.GetRetries() <= 0 {
		return time.Now(), nil
	}

	multiplier, mGetErr := getFromMap[float64](backOff.GetBackOffParams(), JitterExponentialBackoff, MultiplierKey)
	if mGetErr != nil {
		multiplier = defaultMultiplier
	}

	baseDelay, baseDelayErr := getFromMap[time.Duration](backOff.GetBackOffParams(), JitterExponentialBackoff, BaseDelayKey)
	if baseDelayErr != nil {
		baseDelay = defaultBaseDelay
	}

	maxDelay, maxDelayErr := getFromMap[time.Duration](backOff.GetBackOffParams(), JitterExponentialBackoff, MaxDelayKey)
	if maxDelayErr != nil {
		maxDelay = defaultMaxDelay
	}

	pow := math.Pow(multiplier, float64(backOff.GetRetries()-1))
	delay := time.Duration(float64(baseDelay) * pow)

	if delay > maxDelay || delay <= 0 {
		delay = maxDelay
	}

	jitter, jitterGetErr := getFromMap[float64](backOff.GetBackOffParams(), JitterExponentialBackoff, JitterKey)
	if jitterGetErr != nil {
		jitter = defaultJitter
	}

	delay = addJitter(delay, jitter)

	return time.Now().Add(delay), nil
}
