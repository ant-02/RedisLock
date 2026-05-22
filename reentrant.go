package redislock

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

type ReentrantMutex struct {
	*baseLock
	counters sync.Map
}

func (l *RedisLocker) newReentrantMutex(ctx context.Context, key string, opts ...Option) (*ReentrantMutex, error) {
	options := l.opts
	for _, opt := range opts {
		opt(&options)
	}

	bl := newBaseLock(l.client, formatKey("reentrant", key), options)
	return &ReentrantMutex{
		baseLock: bl,
	}, nil
}

type reentrantCounter struct {
	count int
	mu    sync.Mutex
}

func (m *ReentrantMutex) Lock(ctx context.Context) error {
	gid := getGoroutineID()

	if counter, ok := m.counters.Load(gid); ok {
		c := counter.(*reentrantCounter)
		c.mu.Lock()
		c.count++
		c.mu.Unlock()
		return m.extend(ctx)
	}

	if err := m.lock(ctx); err != nil {
		return err
	}

	c := &reentrantCounter{count: 1}
	m.counters.Store(gid, c)
	return nil
}

func (m *ReentrantMutex) Unlock(ctx context.Context) error {
	gid := getGoroutineID()

	counter, ok := m.counters.Load(gid)
	if !ok {
		return ErrLockNotHeld
	}

	c := counter.(*reentrantCounter)
	c.mu.Lock()
	c.count--
	if c.count > 0 {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	m.counters.Delete(gid)
	return m.unlock(ctx)
}

func (m *ReentrantMutex) Extend(ctx context.Context, expiry time.Duration) error {
	if !m.IsHeld() {
		return ErrLockNotHeld
	}
	script := redis.NewScript(extendScript)
	_, err := script.Run(ctx, m.baseLock.client, []string{m.key}, m.value, expiry.Milliseconds()).Result()
	return err
}

var goroutineID uint64

func getGoroutineID() uint64 {
	return atomic.AddUint64(&goroutineID, 1) - 1
}