package redislock

import (
	"context"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type Watchdog struct {
	client    *redis.Client
	key       string
	value     string
	timeout   time.Duration
	interval  time.Duration
	stopCh    chan struct{}
	stoppedCh chan struct{}
	mu        sync.Mutex
	isRunning bool
}

func newWatchdog(client *redis.Client, key, value string, timeout, interval time.Duration) *Watchdog {
	if interval == 0 {
		interval = timeout / 3
	}
	if interval < 100*time.Millisecond {
		interval = 100 * time.Millisecond
	}
	return &Watchdog{
		client:    client,
		key:       key,
		value:     value,
		timeout:   timeout,
		interval:  interval,
		stopCh:    make(chan struct{}),
		stoppedCh: make(chan struct{}),
	}
}

func (w *Watchdog) Start(ctx context.Context) {
	w.mu.Lock()
	if w.isRunning {
		w.mu.Unlock()
		return
	}
	w.isRunning = true
	w.mu.Unlock()

	go w.run(ctx)
}

func (w *Watchdog) Stop() {
	w.mu.Lock()
	if !w.isRunning {
		w.mu.Unlock()
		return
	}
	w.mu.Unlock()

	close(w.stopCh)
	<-w.stoppedCh
}

func (w *Watchdog) run(ctx context.Context) {
	defer close(w.stoppedCh)

	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-w.stopCh:
			return
		case <-ticker.C:
			w.extend(ctx)
		}
	}
}

func (w *Watchdog) extend(ctx context.Context) {
	script := redis.NewScript(extendScript)
	_, err := script.Run(ctx, w.client, []string{w.key}, w.value, w.timeout.Milliseconds()).Result()
	if err != nil {
		// Lock may have expired, stop watchdog
		w.mu.Lock()
		w.isRunning = false
		w.mu.Unlock()
	}
}