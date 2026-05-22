package main

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"redislock"
)

func main() {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer client.Close()

	locker := redislock.NewRedisLocker(client)
	ctx := context.Background()

	// Lock with watchdog enabled (default)
	fmt.Println("=== Lock with Watchdog ===")
	fmt.Println("Watchdog automatically extends lock before expiry")

	lock, err := locker.MutexLock(ctx, "watchdog-lock",
		redislock.WithTimeout(5*time.Second),
		redislock.WithWatchdog(true),
	)
	if err != nil {
		fmt.Printf("Failed to acquire lock: %v\n", err)
		return
	}

	fmt.Printf("Lock acquired with 5s timeout: held=%v\n", lock.IsHeld())
	fmt.Println("Watchdog will extend lock every ~1.6s")

	// Simulate long-running task
	fmt.Println("Simulating long task (10 seconds)...")
	for i := 0; i < 10; i++ {
		time.Sleep(1 * time.Second)
		fmt.Printf("  [%d/%d] lock still held: %v\n", i+1, 10, lock.IsHeld())
	}

	lock.Unlock(ctx)
	fmt.Println("Lock released")

	// Lock without watchdog - will expire
	fmt.Println("\n=== Lock without Watchdog ===")
	lock2, _ := locker.MutexLock(ctx, "no-watchdog-lock",
		redislock.WithTimeout(2*time.Second),
		redislock.WithWatchdog(false),
	)

	fmt.Printf("Lock acquired with 2s timeout: held=%v\n", lock2.IsHeld())
	fmt.Println("Waiting for lock to expire...")
	time.Sleep(3 * time.Second)
	fmt.Printf("Lock expired, held=%v\n", lock2.IsHeld())

	fmt.Println("\nDone!")
}