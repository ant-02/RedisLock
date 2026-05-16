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

	// Basic mutex lock
	lock, err := locker.MutexLock(ctx, "my-lock")
	if err != nil {
		log.Fatalf("Failed to acquire lock: %v", err)
	}

	fmt.Println("Lock acquired!")

	// Do work
	time.Sleep(2 * time.Second)

	// Release lock
	if err := lock.Unlock(ctx); err != nil {
		log.Fatalf("Failed to release lock: %v", err)
	}

	fmt.Println("Lock released!")
}