package redislock

import (
	"context"
	"math/rand"
	"time"
)

type Options struct {
	Timeout         time.Duration
	Blocking        bool
	BlockTimeout    time.Duration
	RetryStrategy   RetryStrategy
	WatchdogEnabled bool
	WatchdogInterval time.Duration
	LockValue       string
}

type Option func(*Options)

func WithTimeout(d time.Duration) Option {
	return func(o *Options) { o.Timeout = d }
}

func WithBlocking(blocking bool) Option {
	return func(o *Options) { o.Blocking = blocking }
}

func WithBlockTimeout(d time.Duration) Option {
	return func(o *Options) { o.BlockTimeout = d }
}

func WithRetry(strategy RetryStrategy) Option {
	return func(o *Options) { o.RetryStrategy = strategy }
}

func WithWatchdog(enabled bool) Option {
	return func(o *Options) { o.WatchdogEnabled = enabled }
}

func WithWatchdogInterval(d time.Duration) Option {
	return func(o *Options) { o.WatchdogInterval = d }
}

func WithLockValue(value string) Option {
	return func(o *Options) { o.LockValue = value }
}

type RetryStrategy interface {
	Next() (delay time.Duration, ok bool)
}

type ExponentialBackoff struct {
	BaseDelay  time.Duration
	MaxDelay   time.Duration
	Multiplier float64
	Jitter     float64
	attempts   int
}

func NewExponentialBackoff(baseDelay, maxDelay time.Duration, multiplier float64) *ExponentialBackoff {
	return &ExponentialBackoff{
		BaseDelay:  baseDelay,
		MaxDelay:   maxDelay,
		Multiplier: multiplier,
		Jitter:     0.1,
	}
}

func (b *ExponentialBackoff) Next() (delay time.Duration, ok bool) {
	if b.attempts == 0 {
		b.attempts++
		return b.BaseDelay, true
	}

	delay = time.Duration(float64(b.BaseDelay) * pow(b.Multiplier, b.attempts-1))
	if delay > b.MaxDelay {
		delay = b.MaxDelay
	}

	// Add jitter
	jitter := time.Duration(rand.Float64() * b.Jitter * float64(delay))
	delay += jitter

	b.attempts++
	return delay, true
}

func (b *ExponentialBackoff) Reset() {
	b.attempts = 0
}

type NoRetry struct{}

func (NoRetry) Next() (delay time.Duration, ok bool) {
	return 0, false
}

type RetryUntilContext struct {
	ctx context.Context
}

func UntilContext(ctx context.Context) RetryStrategy {
	return &RetryUntilContext{ctx: ctx}
}

func (r *RetryUntilContext) Next() (delay time.Duration, ok bool) {
	select {
	case <-r.ctx.Done():
		return 0, false
	default:
		return 10 * time.Millisecond, true
	}
}

func pow(base float64, exp int) float64 {
	result := 1.0
	for i := 0; i < exp; i++ {
		result *= base
	}
	return result
}