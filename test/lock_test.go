package test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"redislock"
)

func setupTestClient(t *testing.T) *redis.Client {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	ctx := context.Background()
	client.FlushDB(ctx)
	return client
}

func TestMutexLock(t *testing.T) {
	client := setupTestClient(t)
	defer client.Close()

	locker := redislock.NewRedisLocker(client)
	ctx := context.Background()

	lock, err := locker.MutexLock(ctx, "test-mutex")
	if err != nil {
		t.Fatalf("Failed to acquire lock: %v", err)
	}

	if !lock.IsHeld() {
		t.Fatal("Lock should be held")
	}

	if lock.Key() != "mutex:test-mutex" {
		t.Fatalf("Unexpected key: %s", lock.Key())
	}

	err = lock.Unlock(ctx)
	if err != nil {
		t.Fatalf("Failed to unlock: %v", err)
	}

	if lock.IsHeld() {
		t.Fatal("Lock should not be held after unlock")
	}
}

func TestReentrantLock(t *testing.T) {
	client := setupTestClient(t)
	defer client.Close()

	locker := redislock.NewRedisLocker(client)
	ctx := context.Background()

	lock1, err := locker.ReentrantLock(ctx, "test-reentrant")
	if err != nil {
		t.Fatalf("First lock failed: %v", err)
	}

	lock2, err := locker.ReentrantLock(ctx, "test-reentrant")
	if err != nil {
		t.Fatalf("Second lock failed: %v", err)
	}

	if !lock1.IsHeld() || !lock2.IsHeld() {
		t.Fatal("Both locks should be held")
	}

	// Unlock once - should still hold
	err = lock1.Unlock(ctx)
	if err != nil {
		t.Fatalf("First unlock failed: %v", err)
	}

	if !lock2.IsHeld() {
		t.Fatal("Lock should still be held after first unlock")
	}

	// Unlock second time - should fully release
	err = lock2.Unlock(ctx)
	if err != nil {
		t.Fatalf("Second unlock failed: %v", err)
	}
}

func TestFairLock(t *testing.T) {
	client := setupTestClient(t)
	defer client.Close()

	locker := redislock.NewRedisLocker(client)
	ctx := context.Background()

	lock, err := locker.FairLock(ctx, "test-fair")
	if err != nil {
		t.Fatalf("Failed to acquire fair lock: %v", err)
	}

	if !lock.IsHeld() {
		t.Fatal("Fair lock should be held")
	}

	err = lock.Unlock(ctx)
	if err != nil {
		t.Fatalf("Failed to unlock fair lock: %v", err)
	}
}

func TestReadWriteLock(t *testing.T) {
	client := setupTestClient(t)
	defer client.Close()

	locker := redislock.NewRedisLocker(client)
	ctx := context.Background()

	// Write lock
	wlock, err := locker.WriteLock(ctx, "test-rw")
	if err != nil {
		t.Fatalf("Failed to acquire write lock: %v", err)
	}

	if !wlock.IsHeld() {
		t.Fatal("Write lock should be held")
	}

	err = wlock.Unlock(ctx)
	if err != nil {
		t.Fatalf("Failed to unlock write lock: %v", err)
	}

	// Read lock
	rlock, err := locker.ReadLock(ctx, "test-rw")
	if err != nil {
		t.Fatalf("Failed to acquire read lock: %v", err)
	}

	if !rlock.IsHeld() {
		t.Fatal("Read lock should be held")
	}

	err = rlock.RUnlock(ctx)
	if err != nil {
		t.Fatalf("Failed to unlock read lock: %v", err)
	}
}

func TestConcurrentLocks(t *testing.T) {
	client := setupTestClient(t)
	defer client.Close()

	locker := redislock.NewRedisLocker(client)
	ctx := context.Background()

	var wg sync.WaitGroup
	acquired := make(chan string, 2)

	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			lock, err := locker.MutexLock(ctx, "concurrent-test",
				redislock.WithBlocking(false),
			)
			if err == nil {
				acquired <- "locker"
				time.Sleep(50 * time.Millisecond)
				lock.Unlock(ctx)
			} else {
				acquired <- "failed"
			}
		}(i)
	}

	wg.Wait()
	close(acquired)

	count := 0
	for r := range acquired {
		if r == "locker" {
			count++
		}
	}

	// Only one should have acquired the lock
	if count != 1 {
		t.Fatalf("Expected 1 lock acquired, got %d", count)
	}
}

func TestWatchdogExtension(t *testing.T) {
	client := setupTestClient(t)
	defer client.Close()

	locker := redislock.NewRedisLocker(client)
	ctx := context.Background()

	lock, err := locker.MutexLock(ctx, "test-watchdog",
		redislock.WithTimeout(5*time.Second),
		redislock.WithWatchdog(true),
	)
	if err != nil {
		t.Fatalf("Failed to acquire lock: %v", err)
	}

	// Let watchdog run
	time.Sleep(2 * time.Second)

	if !lock.IsHeld() {
		t.Fatal("Lock should still be held with watchdog")
	}

	lock.Unlock(ctx)
}

func TestNonBlockingLock(t *testing.T) {
	client := setupTestClient(t)
	defer client.Close()

	locker := redislock.NewRedisLocker(client)
	ctx := context.Background()

	// Acquire first lock
	lock1, _ := locker.MutexLock(ctx, "nonblock-test")

	// Try non-blocking on second
	lock2, err := locker.MutexLock(ctx, "nonblock-test",
		redislock.WithBlocking(false),
	)

	if err != redislock.ErrLockNotAcquired {
		t.Fatalf("Expected ErrLockNotAcquired, got: %v", err)
	}

	if lock2 != nil {
		t.Fatal("Lock2 should be nil")
	}

	lock1.Unlock(ctx)
}

func TestExtendLock(t *testing.T) {
	client := setupTestClient(t)
	defer client.Close()

	locker := redislock.NewRedisLocker(client)
	ctx := context.Background()

	lock, _ := locker.MutexLock(ctx, "test-extend")

	err := lock.Extend(ctx, 10*time.Second)
	if err != nil {
		t.Fatalf("Failed to extend lock: %v", err)
	}

	lock.Unlock(ctx)
}