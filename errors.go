package redislock

import "errors"

var (
	ErrLockNotHeld              = errors.New("redis-lock: lock not held by this instance")
	ErrLockTimeout              = errors.New("redis-lock: lock acquisition timeout")
	ErrLockNotAcquired          = errors.New("redis-lock: lock could not be acquired")
	ErrRedLockAcquisitionFailed = errors.New("redis-lock: RedLock acquisition failed")
	ErrLockAlreadyExists        = errors.New("redis-lock: lock already exists")
	ErrInvalidOption            = errors.New("redis-lock: invalid option")
	ErrWatchdogNotRunning       = errors.New("redis-lock: watchdog not running")
	ErrContextCancelled         = errors.New("redis-lock: context cancelled")
	ErrReadLockNotHeld          = errors.New("redis-lock: read lock not held by this instance")
	ErrWriteLockNotHeld         = errors.New("redis-lock: write lock not held by this instance")
	ErrNotOwner                 = errors.New("redis-lock: not the owner of this lock")
)