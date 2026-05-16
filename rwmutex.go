package redislock

import (
	"context"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type RWMutex struct {
	client      *redis.Client
	key         string
	value       string
	opts        Options
	readersKey  string
	isHeld      bool
	heldType    int // 0 = none, 1 = read, 2 = write
	mu          sync.RWMutex
	watchdog    *Watchdog
	readCounter int
	readMu      sync.Mutex
}

func (l *RedisLocker) newReadWriteLock(ctx context.Context, key string, opts ...Option) (*RWMutex, error) {
	options := l.opts
	for _, opt := range opts {
		opt(&options)
	}

	lockValue := options.LockValue
	if lockValue == "" {
		lockValue = generateLockValue()
	}

	return &RWMutex{
		client:     l.client,
		key:        formatKey("rw", key),
		value:      lockValue,
		opts:       options,
		readersKey: formatKey("rw:readers", key),
		isHeld:     false,
		heldType:   0,
	}, nil
}

func (r *RWMutex) RLock(ctx context.Context) error {
	if r.heldType == 2 {
		return ErrLockAlreadyExists
	}

	script := redis.NewScript(readLockScript)
	result, err := script.Run(ctx, r.client,
		[]string{r.readersKey, r.key},
		r.value, r.opts.Timeout.Milliseconds(),
	).Int()
	if err != nil {
		return err
	}
	if result == 1 {
		r.mu.Lock()
		r.heldType = 1
		r.readCounter++
		r.isHeld = true
		r.mu.Unlock()

		if r.opts.WatchdogEnabled {
			interval := r.opts.WatchdogInterval
			if interval == 0 {
				interval = r.opts.Timeout / 3
			}
			r.watchdog = newWatchdog(r.client, r.key, r.value, r.opts.Timeout, interval)
			r.watchdog.Start(ctx)
		}
	}
	return nil
}

func (r *RWMutex) RUnlock(ctx context.Context) error {
	r.mu.Lock()
	if r.heldType != 1 || r.readCounter <= 0 {
		r.mu.Unlock()
		return ErrReadLockNotHeld
	}
	r.readCounter--
	if r.readCounter == 0 {
		r.heldType = 0
		r.isHeld = false
		script := redis.NewScript(readUnlockScript)
		script.Run(ctx, r.client, []string{r.readersKey})
		if r.watchdog != nil {
			r.watchdog.Stop()
		}
	}
	r.mu.Unlock()
	return nil
}

func (r *RWMutex) WLock(ctx context.Context) error {
	if r.heldType == 1 {
		return ErrLockAlreadyExists
	}

	script := redis.NewScript(writeLockScript)
	result, err := script.Run(ctx, r.client,
		[]string{r.key, r.readersKey},
		r.value, r.opts.Timeout.Milliseconds(),
	).Int()
	if err != nil {
		return err
	}
	if result == 1 {
		r.mu.Lock()
		r.heldType = 2
		r.isHeld = true
		r.mu.Unlock()

		if r.opts.WatchdogEnabled {
			interval := r.opts.WatchdogInterval
			if interval == 0 {
				interval = r.opts.Timeout / 3
			}
			r.watchdog = newWatchdog(r.client, r.key, r.value, r.opts.Timeout, interval)
			r.watchdog.Start(ctx)
		}
		return nil
	}
	return ErrLockNotAcquired
}

func (r *RWMutex) WUnlock(ctx context.Context) error {
	r.mu.Lock()
	if r.heldType != 2 {
		r.mu.Unlock()
		return ErrWriteLockNotHeld
	}

	script := redis.NewScript(unlockScript)
	result, err := script.Run(ctx, r.client, []string{r.key}, r.value).Int()
	if err != nil {
		r.mu.Unlock()
		return err
	}
	if result == 0 {
		r.mu.Unlock()
		return ErrNotOwner
	}

	r.heldType = 0
	r.isHeld = false
	r.mu.Unlock()

	if r.watchdog != nil {
		r.watchdog.Stop()
	}
	return nil
}

func (r *RWMutex) Lock(ctx context.Context) error {
	return r.WLock(ctx)
}

func (r *RWMutex) Unlock(ctx context.Context) error {
	return r.WUnlock(ctx)
}

func (r *RWMutex) Extend(ctx context.Context, expiry time.Duration) error {
	if !r.IsHeld() {
		return ErrLockNotHeld
	}
	script := redis.NewScript(extendScript)
	_, err := script.Run(ctx, r.client, []string{r.key}, r.value, expiry.Milliseconds()).Result()
	return err
}

func (r *RWMutex) IsHeld() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.isHeld
}

func (r *RWMutex) Key() string {
	return r.key
}

const readLockScript = `
local writeLocked = redis.call('GET', KEYS[2])
if writeLocked ~= false then
    return 0
end

local count = redis.call('INCR', KEYS[1])
if count == 1 then
    redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
end

return 1
`

const readUnlockScript = `
local count = redis.call('DECR', KEYS[1])
if count <= 0 then
    redis.call('DEL', KEYS[1])
end
`

const writeLockScript = `
local readCount = redis.call('GET', KEYS[2])
if readCount ~= false and tonumber(readCount) > 0 then
    return 0
end

local result = redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', ARGV[2])
if result then
    return 1
end
return 0
`