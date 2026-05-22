package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
	"redislock"
)

func main() {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer client.Close()

	locker := redislock.NewRedisLocker(client)
	ctx := context.Background()

	// Non-blocking lock with immediate failure
	fmt.Println("=== Non-blocking Lock ===")
	lock, err := locker.MutexLock(ctx, "nonblock-lock",
		redislock.WithBlocking(false),
		redislock.WithTimeout(1*time.Second),
	)
	if err != nil {
		fmt.Printf("Non-blocking lock failed (expected): %v\n", err)
	} else {
		lock.Unlock(ctx)
	}

	// Blocking lock with exponential backoff
	fmt.Println("\n=== Blocking Lock with Retry ===")
	locker2 := redislock.NewRedisLocker(client,
		redislock.WithRetry(redislock.NewExponentialBackoff(
			100*time.Millisecond,
			5*time.Second,
			2.0,
		)),
	)

	lock2, err := locker2.MutexLock(ctx, "retry-lock",
		redislock.WithTimeout(30*time.Second),
	)
	if err != nil {
		log.Fatalf("Failed to acquire lock with retry: %v", err)
	}

	fmt.Println("Lock acquired with exponential backoff retry!")
	time.Sleep(500 * time.Millisecond)
	lock2.Unlock(ctx)

	// Retry until context cancelled
	fmt.Println("\n=== Retry Until Context ===")
	bgCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	lock3, err := locker.MutexLock(bgCtx, "context-retry",
		redislock.WithTimeout(10*time.Second),
	)
	if err != nil {
		fmt.Printf("Context timeout reached: %v\n", err)
	} else {
		fmt.Println("Lock acquired!")
		lock3.Unlock(ctx)
	}

	// Custom retry with NoRetry (single attempt)
	fmt.Println("\n=== No Retry (Single Attempt) ===")
	locker3 := redislock.NewRedisLocker(client,
		redislock.WithRetry(redislock.NoRetry{}),
	)

	lock4, err := locker3.MutexLock(ctx, "no-retry-lock")
	if err != nil {
		fmt.Printf("Single attempt failed (expected if lock held): %v\n", err)
	} else {
		lock4.Unlock(ctx)
	}

	fmt.Println("\nDone!")
}