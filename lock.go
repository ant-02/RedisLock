package redislock

import (
	"context"
	"time"
)

type Locker interface {
	MutexLock(ctx context.Context, key string, opts ...Option) (Lock, error)
	ReentrantLock(ctx context.Context, key string, opts ...Option) (Lock, error)
	FairLock(ctx context.Context, key string, opts ...Option) (Lock, error)
	ReadLock(ctx context.Context, key string, opts ...Option) (Lock, error)
	WriteLock(ctx context.Context, key string, opts ...Option) (Lock, error)
}

type Lock interface {
	Lock(ctx context.Context) error
	Unlock(ctx context.Context) error
	Extend(ctx context.Context, expiry time.Duration) error
	IsHeld() bool
	Key() string
}

type ReadLock interface {
	Lock
	RLock(ctx context.Context) error
	RUnlock(ctx context.Context) error
}

type WriteLock interface {
	Lock
	WLock(ctx context.Context) error
	WUnlock(ctx context.Context) error
}

type RedLock interface {
	Lock(ctx context.Context) error
	Unlock(ctx context.Context) error
	Extend(ctx context.Context, expiry time.Duration) error
}