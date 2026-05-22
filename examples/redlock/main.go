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
	// Connect to multiple Redis instances for RedLock
	clients := []*redis.Client{
		redis.NewClient(&redis.Options{Addr: "localhost:6379"}),
		redis.NewClient(&redis.Options{Addr: "localhost:6380"}),
		redis.NewClient(&redis.Options{Addr: "localhost:6381"}),
	}
	defer func() {
		for _, c := range clients {
			c.Close()
		}
	}()

	// Create RedLock client with 3 nodes
	nodes := []string{"localhost:6379", "localhost:6380", "localhost:6381"}
	redlock, err := redislock.NewRedLock(nodes, 10*time.Second)
	if err != nil {
		log.Fatalf("Failed to create RedLock: %v", err)
	}

	ctx := context.Background()

	// Acquire distributed lock across multiple Redis instances
	fmt.Println("=== RedLock (Distributed Lock) ===")
	fmt.Println("Acquiring lock on majority of 3 Redis nodes...")

	err = redlock.Lock(ctx)
	if err != nil {
		log.Fatalf("Failed to acquire RedLock: %v", err)
	}
	fmt.Println("RedLock acquired! (2+ nodes agreed)")

	// Do critical section work
	time.Sleep(2 * time.Second)

	// Extend lock if needed
	fmt.Println("Extending lock...")
	redlock.Extend(ctx, 30*time.Second)
	fmt.Println("Lock extended by 30 seconds")

	// Release lock
	fmt.Println("Releasing lock...")
	err = redlock.Unlock(ctx)
	if err != nil {
		log.Fatalf("Failed to unlock RedLock: %v", err)
	}
	fmt.Println("RedLock released!")

	// Demonstrate quorum requirement
	fmt.Println("\n=== Quorum Requirement ===")
	fmt.Println("RedLock requires N/2+1 nodes to agree (2 out of 3 for 3 nodes)")
	fmt.Println("If 2+ nodes fail, the lock is automatically released")
}