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

	// Read-Write lock example
	lock, err := locker.WriteLock(ctx, "shared-resource")
	if err != nil {
		log.Fatalf("Failed to acquire write lock: %v", err)
	}

	fmt.Println("Write lock acquired!")

	// Simulate write operation
	time.Sleep(1 * time.Second)

	// Release write lock
	if err := lock.Unlock(ctx); err != nil {
		log.Fatalf("Failed to release write lock: %v", err)
	}

	// Now acquire read lock
	readLock, err := locker.ReadLock(ctx, "shared-resource")
	if err != nil {
		log.Fatalf("Failed to acquire read lock: %v", err)
	}

	fmt.Println("Read lock acquired!")

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rLock, _ := locker.ReadLock(ctx, "shared-resource")
			fmt.Printf("Reader %d acquired read lock\n", id)
			time.Sleep(100 * time.Millisecond)
			rLock.Unlock(ctx)
			fmt.Printf("Reader %d released read lock\n", id)
		}(i)
	}

	wg.Wait()
	readLock.Unlock(ctx)

	fmt.Println("All done!")
}