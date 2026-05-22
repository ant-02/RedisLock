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

	// Fair lock ensures FIFO ordering
	fmt.Println("=== Fair Lock ===")
	fairLock, err := locker.FairLock(ctx, "fair-key",
		redislock.WithTimeout(10*time.Second),
	)
	if err != nil {
		log.Fatalf("Failed to acquire fair lock: %v", err)
	}

	fmt.Printf("Fair lock acquired: key=%s, held=%v\n", fairLock.Key(), fairLock.IsHeld())

	// Simulate work
	time.Sleep(500 * time.Millisecond)

	fairLock.Unlock(ctx)
	fmt.Println("Fair lock released")

	// Concurrent fair locks - all waiting, first to acquire is first in queue
	fmt.Println("\n=== Concurrent Fair Locks (FIFO ordering) ===")
	var wg sync.WaitGroup
	acquired := make(chan int, 3)

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			lock, err := locker.FairLock(ctx, "concurrent-fair",
				redislock.WithTimeout(5*time.Second),
			)
			if err != nil {
				fmt.Printf("Goroutine %d failed to acquire: %v\n", id, err)
				return
			}
			acquired <- id
			fmt.Printf("Goroutine %d acquired fair lock (queue position: %d)\n", id, id)
			time.Sleep(200 * time.Millisecond)
			lock.Unlock(ctx)
			fmt.Printf("Goroutine %d released fair lock\n", id)
		}(i)
	}

	wg.Wait()
	close(acquired)

	// Show acquisition order
	fmt.Println("\nAcquisition order:")
	for id := range acquired {
		fmt.Printf("  - Goroutine %d\n", id)
	}

	fmt.Println("\nDone!")
}