package benchmark

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"redislock"
)

func setupBenchClient() *redis.Client {
	return redis.NewClient(&redis.Options{Addr: "localhost:6379"})
}

func BenchmarkMutexLock(b *testing.B) {
	client := setupBenchClient()
	defer client.Close()

	locker := redislock.NewRedisLocker(client)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lock, _ := locker.MutexLock(ctx, "bench-mutex")
		lock.Unlock(ctx)
	}
}

func BenchmarkMutexLockParallel(b *testing.B) {
	client := setupBenchClient()
	defer client.Close()

	locker := redislock.NewRedisLocker(client)
	ctx := context.Background()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			lock, _ := locker.MutexLock(ctx, "bench-mutex-parallel")
			lock.Unlock(ctx)
		}
	})
}

func BenchmarkReentrantLock(b *testing.B) {
	client := setupBenchClient()
	defer client.Close()

	locker := redislock.NewRedisLocker(client)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lock, _ := locker.ReentrantLock(ctx, "bench-reentrant")
		lock2, _ := locker.ReentrantLock(ctx, "bench-reentrant")
		lock2.Unlock(ctx)
		lock.Unlock(ctx)
	}
}

func BenchmarkReadWriteLock(b *testing.B) {
	client := setupBenchClient()
	defer client.Close()

	locker := redislock.NewRedisLocker(client)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wlock, _ := locker.WriteLock(ctx, "bench-rw")
		wlock.Unlock(ctx)

		rlock, _ := locker.ReadLock(ctx, "bench-rw")
		redislock.ReadLock(rlock).RUnlock(ctx)
	}
}

func BenchmarkFairLock(b *testing.B) {
	client := setupBenchClient()
	defer client.Close()

	locker := redislock.NewRedisLocker(client)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lock, _ := locker.FairLock(ctx, "bench-fair")
		lock.Unlock(ctx)
	}
}

func BenchmarkWatchdogOverhead(b *testing.B) {
	client := setupBenchClient()
	defer client.Close()

	locker := redislock.NewRedisLocker(client)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lock, _ := locker.MutexLock(ctx, "bench-watchdog",
			redislock.WithWatchdog(true),
			redislock.WithTimeout(10*time.Second),
		)
		time.Sleep(10 * time.Millisecond)
		lock.Unlock(ctx)
	}
}

func BenchmarkConcurrentAccess(b *testing.B) {
	client := setupBenchClient()
	defer client.Close()

	locker := redislock.NewRedisLocker(client)
	ctx := context.Background()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			lock, _ := locker.MutexLock(ctx, "bench-concurrent",
				redislock.WithBlocking(false),
			)
			if lock != nil {
				lock.Unlock(ctx)
			}
		}
	})
}

func BenchmarkMultipleLockTypes(b *testing.B) {
	client := setupBenchClient()
	defer client.Close()

	locker := redislock.NewRedisLocker(client)
	ctx := context.Background()

	var wg sync.WaitGroup
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			wg.Add(1)
			go func() {
				defer wg.Done()
				lock, _ := locker.MutexLock(ctx, "bench-multi")
				lock.Unlock(ctx)
			}()
			wg.Add(1)
			go func() {
				defer wg.Done()
				lock, _ := locker.ReentrantLock(ctx, "bench-multi2")
				lock.Unlock(ctx)
			}()
		}
	})
}