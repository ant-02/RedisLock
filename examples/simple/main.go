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

	// Basic mutex lock with timeout
	fmt.Println("=== Mutex Lock ===")
	lock, err := locker.MutexLock(ctx, "my-lock",
		redislock.WithTimeout(10*time.Second),
	)
	if err != nil {
		log.Fatalf("Failed to acquire lock: %v", err)
	}

	fmt.Printf("Lock acquired: key=%s, held=%v\n", lock.Key(), lock.IsHeld())

	// Do work
	time.Sleep(500 * time.Millisecond)

	// Release lock
	if err := lock.Unlock(ctx); err != nil {
		log.Fatalf("Failed to release lock: %v", err)
	}
	fmt.Printf("Lock released: held=%v\n", lock.IsHeld())

	// Non-blocking lock
	fmt.Println("\n=== Non-blocking Lock ===")
	lock2, err := locker.MutexLock(ctx, "my-lock",
		redislock.WithBlocking(false),
		redislock.WithTimeout(1*time.Second),
	)
	if err != nil {
		fmt.Printf("Non-blocking lock failed (expected): %v\n", err)
	} else {
		lock2.Unlock(ctx)
	}

	// Lock with extend
	fmt.Println("\n=== Lock Extend ===")
	lock3, _ := locker.MutexLock(ctx, "extend-lock",
		redislock.WithTimeout(5*time.Second),
		redislock.WithWatchdog(false),
	)
	fmt.Printf("Lock acquired: held=%v\n", lock3.IsHeld())

	time.Sleep(1 * time.Second)
	lock3.Extend(ctx, 10*time.Second)
	fmt.Println("Lock extended by 10 seconds")

	lock3.Unlock(ctx)
	fmt.Println("Done!")
}