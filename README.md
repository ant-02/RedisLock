# RedisLock

高性能 Redis 分布式锁 SDK，支持互斥锁、读写锁、可重入锁、看门狗自动续期、RedLock 红锁算法、公平锁等多种锁模式。

## 特性

- **多种锁模式**: 互斥锁、可重入锁、公平锁、读写锁
- **原子操作**: 使用 Lua 脚本保证加解锁原子性
- **看门狗自动续期**: 后台协程智能延期，避免锁提前释放
- **RedLock 多节点**: 多 Redis 场景下保证分布式一致性
- **可重入锁**: 同一线程可多次获取锁
- **公平锁 (FIFO)**: 使用 Redis Sorted Set 实现
- **丰富的配置选项**: 锁超时、重试策略（指数退避）、阻塞/非阻塞模式

## 安装

```bash
go get github.com/redis/go-redis/v9
```

## 快速开始

```go
package main

import (
    "context"
    "fmt"
    "github.com/redis/go-redis/v9"
    "redislock"
)

func main() {
    client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
    defer client.Close()

    locker := redislock.NewRedisLocker(client)
    ctx := context.Background()

    // 获取互斥锁
    lock, err := locker.MutexLock(ctx, "my-lock")
    if err != nil {
        panic(err)
    }
    defer lock.Unlock(ctx)

    fmt.Println("Lock acquired!")
}
```

## 锁类型

### 互斥锁 (Mutex)

```go
lock, _ := locker.MutexLock(ctx, "key")
lock.Unlock(ctx)
```

### 可重入锁 (Reentrant)

```go
// 同一 goroutine 可多次获取锁
lock1, _ := locker.ReentrantLock(ctx, "key")
lock2, _ := locker.ReentrantLock(ctx, "key")
lock2.Unlock(ctx)  // 仍持有锁
lock1.Unlock(ctx)  // 完全释放
```

### 公平锁 (Fair)

```go
// FIFO 顺序获取锁
lock, _ := locker.FairLock(ctx, "key")
lock.Unlock(ctx)
```

### 读写锁 (Read-Write)

```go
// 写锁（独占）
wlock, _ := locker.WriteLock(ctx, "key")
wlock.Unlock(ctx)

// 读锁（共享）
rlock, _ := locker.ReadLock(ctx, "key")
rlock.RUnlock(ctx)
```

## 配置选项

```go
locker := redislock.NewRedisLocker(client,
    redislock.WithTimeout(30 * time.Second),
    redislock.WithBlocking(true),
    redislock.WithWatchdog(true),
    redislock.WithRetry(redislock.NewExponentialBackoff(
        100 * time.Millisecond,
        5 * time.Second,
        2.0,
    )),
)
```

### 选项说明

- `WithTimeout`: 锁超时时间，默认 30 秒
- `WithBlocking`: 阻塞/非阻塞模式，默认 true
- `WithWatchdog`: 启用看门狗自动续期，默认 true
- `WithRetry`: 重试策略

### 重试策略

```go
// 指数退避
NewExponentialBackoff(baseDelay, maxDelay, multiplier)

// 非阻塞（无重试）
NoRetry{}

// 直到上下文取消
UntilContext(ctx)
```

## RedLock 多节点

```go
nodes := []string{
    "localhost:6379",
    "localhost:6380",
    "localhost:6381",
}
redLock, _ := redislock.NewRedLock(nodes, 10*time.Second)

redLock.Lock(ctx)
redLock.Unlock(ctx)
```

## 示例

示例代码位于 `examples/` 目录：

- `examples/simple/` - 基础使用
- `examples/rwlock/` - 读写锁示例
- `examples/retry/` - 重试机制示例

## 运行测试

```bash
go test ./test/... -v
```

## 运行基准测试

```bash
go test ./benchmark/... -bench=.
```

## 核心文件

| 文件 | 说明 |
|------|------|
| lock.go | 核心接口定义 |
| redis_lock.go | Redis 锁基础实现 |
| watchdog.go | 看门狗自动续期 |
| mutex.go | 互斥锁 |
| reentrant.go | 可重入锁 |
| fair.go | 公平锁 |
| rwmutex.go | 读写锁 |
| redlock.go | RedLock 多节点算法 |
| lua/*.lua | 原子操作脚本 |