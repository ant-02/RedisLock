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

	// Create locker with retry options
	locker := redislock.NewRedisLocker(client,
		redislock.WithBlocking(true),
		redislock.WithRetry(redislock.NewExponentialBackoff(
			100*time.Millisecond,
			5*time.Second,
			2.0,
		)),
	)

	ctx := context.Background()

	// Non-blocking lock with retry
	lock, err := locker.MutexLock(ctx, "retry-lock",
		redislock.WithBlocking(false),
	)
	if err != nil {
		fmt.Printf("Non-blocking lock failed: %v\n", err)
	}

	// Blocking lock with exponential backoff retry
	lock, err = locker.MutexLock(ctx, "blocking-lock",
		redislock.WithTimeout(30*time.Second),
	)
	if err != nil {
		log.Fatalf("Failed to acquire lock with retry: %v", err)
	}

	fmt.Println("Lock acquired with retry!")

	time.Sleep(1 * time.Second)

	if err := lock.Unlock(ctx); err != nil {
		log.Fatalf("Failed to release lock: %v", err)
	}

	fmt.Println("Lock released!")
}