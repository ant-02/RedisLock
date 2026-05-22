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

	// Write lock - exclusive access
	fmt.Println("=== Write Lock (Exclusive) ===")
	wlock, err := locker.WriteLock(ctx, "shared-resource",
		redislock.WithTimeout(10*time.Second),
	)
	if err != nil {
		log.Fatalf("Failed to acquire write lock: %v", err)
	}

	fmt.Printf("Write lock acquired: key=%s, held=%v\n", wlock.Key(), wlock.IsHeld())

	// Simulate write operation
	time.Sleep(500 * time.Millisecond)

	wlock.Unlock(ctx)
	fmt.Println("Write lock released")

	// Read lock - shared access
	fmt.Println("\n=== Read Lock (Shared) ===")
	rlock, err := locker.ReadLock(ctx, "shared-resource",
		redislock.WithTimeout(10*time.Second),
	)
	if err != nil {
		log.Fatalf("Failed to acquire read lock: %v", err)
	}

	fmt.Printf("Read lock acquired: held=%v\n", rlock.IsHeld())

	// Multiple concurrent readers
	fmt.Println("\n=== Multiple Concurrent Readers ===")
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rl, err := locker.ReadLock(ctx, "shared-resource")
			if err != nil {
				fmt.Printf("Reader %d failed: %v\n", id, err)
				return
			}
			fmt.Printf("Reader %d acquired read lock (held=%v)\n", id, rl.IsHeld())
			time.Sleep(100 * time.Millisecond)
			rl.RUnlock(ctx)
			fmt.Printf("Reader %d released read lock\n", id)
		}(i)
	}
	wg.Wait()

	rlock.RUnlock(ctx)
	fmt.Println("Main read lock released")

	// Demonstrate write blocks readers
	fmt.Println("\n=== Write blocks Readers ===")
	wlock2, _ := locker.WriteLock(ctx, "exclusive-resource")
	fmt.Println("Write lock acquired - no readers can proceed")

	time.Sleep(500 * time.Millisecond)
	wlock2.Unlock(ctx)
	fmt.Println("Write lock released")

	fmt.Println("\nDone!")
}