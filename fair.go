package redislock

import (
	"context"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type FairLock struct {
	*baseLock
	client    *redis.Client
	queueKey  string
	requestID string
	mu        sync.Mutex
}

func (l *RedisLocker) newFairLock(ctx context.Context, key string, opts ...Option) (*FairLock, error) {
	options := l.opts
	for _, opt := range opts {
		opt(&options)
	}

	bl := newBaseLock(l.client, formatKey("fair", key), options)
	fairLock := &FairLock{
		baseLock: bl,
		client:   l.client,
		queueKey: formatKey("fair:queue", key),
	}

	return fairLock, nil
}

func (f *FairLock) Lock(ctx context.Context) error {
	f.mu.Lock()
	f.requestID = generateLockValue()
	f.mu.Unlock()

	return f.lock(ctx)
}

func (f *FairLock) tryAcquire(ctx context.Context) (bool, error) {
	timestamp := time.Now().UnixMilli()
	script := redis.NewScript(fairLockScript)
	result, err := script.Run(ctx, f.client,
		[]string{f.key, f.queueKey},
		f.value, timestamp, f.opts.Timeout.Milliseconds(), f.requestID,
	).Int()
	if err != nil {
		return false, err
	}
	return result == 1, nil
}

func (f *FairLock) lock(ctx context.Context) error {
	var retryStrategy RetryStrategy = f.opts.RetryStrategy
	if retryStrategy == nil {
		retryStrategy = NewExponentialBackoff(50*time.Millisecond, 30*time.Second, 2.0)
	}

	for {
		acquired, err := f.tryAcquire(ctx)
		if err != nil {
			return err
		}
		if acquired {
			f.setHeld(true)
			if f.opts.WatchdogEnabled {
				interval := f.opts.WatchdogInterval
				if interval == 0 {
					interval = f.opts.Timeout / 3
				}
				f.watchdog = newWatchdog(f.client, f.key, f.value, f.opts.Timeout, interval)
				f.watchdog.Start(ctx)
			}
			return nil
		}

		if !f.opts.Blocking {
			return ErrLockNotAcquired
		}

		delay, ok := retryStrategy.Next()
		if !ok {
			return ErrLockTimeout
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
			continue
		}
	}
}

func (f *FairLock) Unlock(ctx context.Context) error {
	return f.unlock(ctx)
}

func (f *FairLock) Extend(ctx context.Context, expiry time.Duration) error {
	if !f.IsHeld() {
		return ErrLockNotHeld
	}
	script := redis.NewScript(extendScript)
	_, err := script.Run(ctx, f.client, []string{f.key}, f.value, expiry.Milliseconds()).Result()
	return err
}

const fairLockScript = `
local current = redis.call('GET', KEYS[1])
if current == ARGV[1] then
    return 1
end

redis.call('ZADD', KEYS[2], ARGV[2], ARGV[4])

local front = redis.call('ZRANGE', KEYS[2], 0, 0)
if front[1] == ARGV[4] then
    redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[3])
    redis.call('ZREM', KEYS[2], ARGV[4])
    return 1
else
    redis.call('ZREM', KEYS[2], ARGV[4])
    return 0
end
`