package redislock

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisLocker struct {
	client *redis.Client
	opts   Options
}

func NewRedisLocker(client *redis.Client, opts ...Option) *RedisLocker {
	options := Options{
		Timeout:         30 * time.Second,
		Blocking:        true,
		WatchdogEnabled: true,
	}
	for _, opt := range opts {
		opt(&options)
	}
	return &RedisLocker{
		client: client,
		opts:   options,
	}
}

func generateLockValue() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func (l *RedisLocker) MutexLock(ctx context.Context, key string, opts ...Option) (Lock, error) {
	return l.newMutex(ctx, key, false, opts...)
}

func (l *RedisLocker) ReentrantLock(ctx context.Context, key string, opts ...Option) (Lock, error) {
	return l.newMutex(ctx, key, true, opts...)
}

func (l *RedisLocker) FairLock(ctx context.Context, key string, opts ...Option) (Lock, error) {
	return l.newFairLock(ctx, key, opts...)
}

func (l *RedisLocker) ReadLock(ctx context.Context, key string, opts ...Option) (Lock, error) {
	return l.newReadWriteLock(ctx, key, opts...)
}

func (l *RedisLocker) WriteLock(ctx context.Context, key string, opts ...Option) (Lock, error) {
	return l.newReadWriteLock(ctx, key, opts...)
}

type baseLock struct {
	client    *redis.Client
	key       string
	value     string
	opts      Options
	isHeld    bool
	mu        sync.RWMutex
	watchdog  *Watchdog
}

func newBaseLock(client *redis.Client, key string, opts Options) *baseLock {
	lockValue := opts.LockValue
	if lockValue == "" {
		lockValue = generateLockValue()
	}
	return &baseLock{
		client: client,
		key:    key,
		value:  lockValue,
		opts:   opts,
		isHeld: false,
	}
}

func (b *baseLock) Key() string {
	return b.key
}

func (b *baseLock) IsHeld() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.isHeld
}

func (b *baseLock) setHeld(held bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.isHeld = held
}

func (b *baseLock) extend(ctx context.Context) error {
	script := redis.NewScript(extendScript)
	_, err := script.Run(ctx, b.client, []string{b.key}, b.value, b.opts.Timeout.Milliseconds()).Result()
	return err
}

func (b *baseLock) tryLock(ctx context.Context) (bool, error) {
	script := redis.NewScript(lockScript)
	result, err := script.Run(ctx, b.client, []string{b.key}, b.value, b.opts.Timeout.Milliseconds()).Int()
	if err != nil {
		return false, err
	}
	return result == 1, nil
}

func (b *baseLock) lock(ctx context.Context) error {
	if b.opts.Blocking {
		return b.lockBlocking(ctx)
	}
	return b.lockNonBlocking(ctx)
}

func (b *baseLock) lockNonBlocking(ctx context.Context) error {
	acquired, err := b.tryLock(ctx)
	if err != nil {
		return err
	}
	if !acquired {
		return ErrLockNotAcquired
	}
	b.setHeld(true)
	if b.opts.WatchdogEnabled {
		b.watchdog = newWatchdog(b.client, b.key, b.value, b.opts.Timeout, b.opts.WatchdogInterval)
		b.watchdog.Start(ctx)
	}
	return nil
}

func (b *baseLock) lockBlocking(ctx context.Context) error {
	var retryStrategy RetryStrategy = b.opts.RetryStrategy
	if retryStrategy == nil {
		retryStrategy = NewExponentialBackoff(50*time.Millisecond, 30*time.Second, 2.0)
	}

	for {
		acquired, err := b.tryLock(ctx)
		if err != nil {
			return err
		}
		if acquired {
			b.setHeld(true)
			if b.opts.WatchdogEnabled {
				interval := b.opts.WatchdogInterval
				if interval == 0 {
					interval = b.opts.Timeout / 3
				}
				b.watchdog = newWatchdog(b.client, b.key, b.value, b.opts.Timeout, interval)
				b.watchdog.Start(ctx)
			}
			return nil
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

func (b *baseLock) unlock(ctx context.Context) error {
	if b.watchdog != nil {
		b.watchdog.Stop()
	}

	script := redis.NewScript(unlockScript)
	result, err := script.Run(ctx, b.client, []string{b.key}, b.value).Int()
	if err != nil {
		return err
	}
	if result == 0 {
		return ErrNotOwner
	}
	b.setHeld(false)
	return nil
}

const lockScript = `
local lockVal = redis.call('GET', KEYS[1])
if lockVal == false then
    redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
    return 1
elseif lockVal == ARGV[1] then
    redis.call('PEXPIRE', KEYS[1], ARGV[2])
    return 1
else
    return 0
end
`

const unlockScript = `
local lockVal = redis.call('GET', KEYS[1])
if lockVal == ARGV[1] then
    redis.call('DEL', KEYS[1])
    return 1
else
    return 0
end
`

const extendScript = `
local lockVal = redis.call('GET', KEYS[1])
if lockVal == ARGV[1] then
    redis.call('PEXPIRE', KEYS[1], ARGV[2])
    return 1
else
    return 0
end
`

func formatKey(prefix, key string) string {
	return fmt.Sprintf("%s:%s", prefix, key)
}