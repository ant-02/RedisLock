package redislock

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type Mutex struct {
	*baseLock
}

func (l *RedisLocker) newMutex(ctx context.Context, key string, opts ...Option) (*Mutex, error) {
	options := l.opts
	for _, opt := range opts {
		opt(&options)
	}

	bl := newBaseLock(l.client, formatKey("mutex", key), options)
	return &Mutex{
		baseLock: bl,
	}, nil
}

func (m *Mutex) Lock(ctx context.Context) error {
	return m.lock(ctx)
}

func (m *Mutex) Unlock(ctx context.Context) error {
	return m.unlock(ctx)
}

func (m *Mutex) Extend(ctx context.Context, expiry time.Duration) error {
	if !m.IsHeld() {
		return ErrLockNotHeld
	}
	script := redis.NewScript(extendScript)
	_, err := script.Run(ctx, m.baseLock.client, []string{m.key}, m.value, expiry.Milliseconds()).Result()
	return err
}