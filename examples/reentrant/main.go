package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"redislock"
)

func main() {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer client.Close()

	locker := redislock.NewRedisLocker(client)
	ctx := context.Background()

	// Reentrant lock - same goroutine can acquire multiple times
	fmt.Println("=== Reentrant Lock ===")
	reentrant, err := locker.ReentrantLock(ctx, "reentrant-key",
		redislock.WithTimeout(10*time.Second),
	)
	if err != nil {
		log.Fatalf("Failed to acquire reentrant lock: %v", err)
	}

	// Same goroutine can lock again - it just increments counter
	reentrant2, err := locker.ReentrantLock(ctx, "reentrant-key",
		redislock.WithTimeout(10*time.Second),
	)
	if err != nil {
		log.Fatalf("Failed to acquire second reentrant lock: %v", err)
	}

	fmt.Printf("First lock held: %v\n", reentrant.IsHeld())
	fmt.Printf("Second lock held: %v\n", reentrant2.IsHeld())

	// Release once - still holds because count=2
	reentrant.Unlock(ctx)
	fmt.Println("First unlock done, should still hold")

	reentrant2.Unlock(ctx)
	fmt.Println("Second unlock done, fully released")

	// Concurrent reentrant locks from different goroutines
	fmt.Println("\n=== Concurrent Reentrant Locks ===")
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			lock, _ := locker.ReentrantLock(ctx, "concurrent-reentrant")
			fmt.Printf("Goroutine %d acquired lock: %v\n", id, lock.IsHeld())
			time.Sleep(100 * time.Millisecond)
			lock.Unlock(ctx)
			fmt.Printf("Goroutine %d released lock\n", id)
		}(i)
	}
	wg.Wait()

	fmt.Println("\nDone!")
}