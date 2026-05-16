package redislock

import (
	"context"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedLockNode struct {
	client *redis.Client
	url    string
}

type RedLock struct {
	nodes    []RedLockNode
	quorum   int
	timeout  time.Duration
	key      string
	value    string
	mu       sync.Mutex
	acquired int
}

func NewRedLock(nodes []string, timeout time.Duration) (*RedLock, error) {
	clients := make([]RedLockNode, len(nodes))
	for i, url := range nodes {
		client := redis.NewClient(&redis.Options{Addr: url})
		clients[i] = RedLockNode{client: client, url: url}
	}

	quorum := len(nodes)/2 + 1
	return &RedLock{
		nodes:   clients,
		quorum:  quorum,
		timeout: timeout,
	}, nil
}

func (r *RedLock) Lock(ctx context.Context) error {
	r.mu.Lock()
	r.value = generateLockValue()
	r.acquired = 0
	r.mu.Unlock()

	var successfulNodes []RedLockNode

	for _, node := range r.nodes {
		if r.acquireOnNode(ctx, node) {
			r.mu.Lock()
			r.acquired++
			successfulNodes = append(successfulNodes, node)
			r.mu.Unlock()
		}

		if r.acquired >= r.quorum {
			break
		}
	}

	if r.acquired < r.quorum {
		for _, node := range successfulNodes {
			r.releaseOnNode(ctx, node)
		}
		return ErrRedLockAcquisitionFailed
	}

	return nil
}

func (r *RedLock) Unlock(ctx context.Context) error {
	var wg sync.WaitGroup
	var errMu sync.Mutex
	var firstErr error

	for _, node := range r.nodes {
		wg.Add(1)
		go func(n RedLockNode) {
			defer wg.Done()
			if err := r.releaseOnNode(ctx, n); err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
			}
		}(node)
	}
	wg.Wait()

	return firstErr
}

func (r *RedLock) Extend(ctx context.Context, expiry time.Duration) error {
	var wg sync.WaitGroup
	var errMu sync.Mutex
	var firstErr error

	for _, node := range r.nodes {
		wg.Add(1)
		go func(n RedLockNode) {
			defer wg.Done()
			script := redis.NewScript(extendScript)
			_, err := script.Run(ctx, n.client, []string{r.key}, r.value, expiry.Milliseconds()).Result()
			if err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
			}
		}(node)
	}
	wg.Wait()

	return firstErr
}

func (r *RedLock) acquireOnNode(ctx context.Context, node RedLockNode) bool {
	script := redis.NewScript(lockScript)
	ctx, cancel := context.WithTimeout(ctx, r.timeout/2)
	defer cancel()

	err := script.Run(ctx, node.client, []string{"redlock:" + r.key}, r.value, r.timeout.Milliseconds()).Err()
	return err == nil
}

func (r *RedLock) releaseOnNode(ctx context.Context, node RedLockNode) error {
	script := redis.NewScript(unlockScript)
	return script.Run(ctx, node.client, []string{"redlock:" + r.key}, r.value).Err()
}