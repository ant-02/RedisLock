# RedisLock

高性能 Redis 分布式锁 SDK，支持互斥锁、读写锁、可重入锁、看门狗自动续期、RedLock 红锁算法、公平锁等多种锁模式。

## 特性

- **多种锁模式**: 互斥锁、可重入锁、公平锁、读写锁、RedLock 多节点
- **原子操作**: 使用 Lua 脚本保证加解锁原子性
- **看门狗自动续期**: 后台协程智能延期，避免锁提前释放
- **可重入锁**: 同一 goroutine 可多次获取锁
- **公平锁 (FIFO)**: 使用 Redis Sorted Set 实现
- **丰富的配置选项**: 锁超时、重试策略（指数退避）、阻塞/非阻塞模式

---

## 目录

1. [快速开始](#快速开始)
2. [锁类型详解](#锁类型详解)
   - [互斥锁 (Mutex)](#互斥锁-mutex)
   - [可重入锁 (Reentrant)](#可重入锁-reentrant)
   - [公平锁 (Fair)](#公平锁-fair)
   - [读写锁 (RWMutex)](#读写锁-rwmutex)
   - [RedLock 多节点锁](#redlock-多节点锁)
3. [核心组件](#核心组件)
   - [baseLock 基础实现](#baselock-基础实现)
   - [Watchdog 看门狗](#watchdog-看门狗)
4. [配置选项](#配置选项)
5. [接口定义](#接口定义)

---

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

---

## 锁类型详解

### 互斥锁 (Mutex)

**原理**：最基础的分布式锁，只允许一个持有者，获取锁采用 SET NX PX 原子操作。

**解决的问题**：
- 保证同一时刻只有一个客户端能执行临界区代码
- 防止并发冲突和数据不一致

**流程**：

```
Client A                    Redis                     Client B
   │                          │                          │
   │──SET lock UUID NX PX 30s→│                          │
   │←──1 (成功)──────────────│                          │
   │                          │                          │
   │  业务处理...             │                          │
   │                          │──SET lock UUID NX PX 30s→│
   │                          │←──0 (失败)──────────────│
   │                          │                          │
   │──DEL lock──────────────→│                          │
   │←──1 (成功)──────────────│                          │
   │                          │──SET lock UUID NX PX 30s→│
   │                          │←──1 (成功)──────────────│
```

**代码实现**：

```go
// mutex.go
type Mutex struct {
    *baseLock  // 嵌入基础锁实现
}

func (m *Mutex) Lock(ctx context.Context) error {
    return m.lock(ctx)  // 委托给 baseLock.lock()
}

func (m *Mutex) Unlock(ctx context.Context) error {
    return m.unlock(ctx)
}
```

**Lua 脚本**（原子加锁）：

```lua
-- lockScript: SET NX PX
local lockVal = redis.call('GET', KEYS[1])
if lockVal == false then
    -- 锁不存在，直接 SET
    redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
    return 1
elseif lockVal == ARGV[1] then
    -- 可重入：同一持有者续期
    redis.call('PEXPIRE', KEYS[1], ARGV[2])
    return 1
else
    -- 锁被他人持有
    return 0
end
```

```lua
-- unlockScript: 防误删
local lockVal = redis.call('GET', KEYS[1])
if lockVal == ARGV[1] then
    redis.call('DEL', KEYS[1])
    return 1
else
    return 0  -- 不是持有者，不能删除
end
```

---

### 可重入锁 (Reentrant)

**原理**：同一 goroutine 可以多次获取同一把锁，通过本地计数器追踪重入次数。

**解决的问题**：
- 递归调用场景
- 同一逻辑在持有锁的情况下调用子方法

**流程**：

```
goroutine 1                    Redis                     goroutine 2
   │                              │                          │
   │──SET lock UUID PX 30s──────→│←──1──────────────         │
   │←──计数器[gid1]=1────────────│                          │
   │                              │                          │
   │──SET lock UUID PX 30s──────→│←──1 (续期)──────────     │
   │←──计数器[gid1]=2────────────│                          │
   │                              │                          │
   │                              │──SET lock UUID2 PX 30s──→│←──0 (失败)──
   │                              │                          │
   │──DEL lock (count=1)────────→│←──不删除──────────       │
   │←──计数器[gid1]=0────────────│                          │
   │                              │                          │
   │──DEL lock (count=0)────────→│←──删除成功──────────     │
```

**代码实现**：

```go
// reentrant.go
type ReentrantMutex struct {
    *baseLock
    counters sync.Map  // goroutineID → reentrantCounter
}

type reentrantCounter struct {
    count int
    mu    sync.Mutex
}

func (m *ReentrantMutex) Lock(ctx context.Context) error {
    gid := getGoroutineID()

    // 检查是否已持有
    if counter, ok := m.counters.Load(gid); ok {
        c := counter.(*reentrantCounter)
        c.mu.Lock()
        c.count++           // 重入次数 +1
        c.mu.Unlock()
        return m.extend(ctx) // 续期
    }

    // 首次获取
    if err := m.lock(ctx); err != nil {
        return err
    }
    m.counters.Store(gid, &reentrantCounter{count: 1})
    return nil
}

func (m *ReentrantMutex) Unlock(ctx context.Context) error {
    gid := getGoroutineID()
    counter, ok := m.counters.Load(gid)
    if !ok {
        return ErrLockNotHeld
    }

    c := counter.(*reentrantCounter)
    c.mu.Lock()
    c.count--
    if c.count > 0 {
        c.mu.Unlock()
        return nil  // 还有重入，不释放
    }
    c.mu.Unlock()

    m.counters.Delete(gid)
    return m.unlock(ctx)  // 完全释放
}
```

---

### 公平锁 (Fair)

**原理**：通过 Redis Sorted Set 实现 FIFO 队列，确保按请求顺序获取锁。

**解决的问题**：
- 锁饥饿：非公平锁可能导致某些请求永远获取不到
- 需要严格按顺序执行的场景

**流程**：

```
Client A                    Redis                     Client B
   │                          │                          │
   │──ZADD queue ts UUIDA────→│                          │
   │←──1─────────────────────│                          │
   │                          │──ZADD queue ts UUIDB────→│
   │                          │←──1─────────────────────│
   │                          │                          │
   │──ZRANK queue UUIDA = 0──→│←──0 (队首)──────────────│
   │←──是队首? Yes────────────│                          │
   │──SET lock NX PX────────→│←──1 (获取成功)──────────│
   │──ZREM queue UUIDA──────→│                          │
   │                          │                          │
   │                          │──ZRANK queue UUIDB─────→│
   │                          │←──1 (非队首)────────────│
   │                          │←──获取失败，等待────────│
```

**代码实现**：

```go
// fair.go
type FairLock struct {
    *baseLock
    queueKey  string   // "fair:queue:key"
    requestID string   // 本次请求 ID
}

func (f *FairLock) Lock(ctx context.Context) error {
    f.requestID = generateLockValue()  // 生成唯一请求 ID
    return f.lock(ctx)
}

// 公平锁 Lua 脚本
const fairLockScript = `
local current = redis.call('GET', KEYS[1])
if current == ARGV[1] then
    return 1  -- 已持有（可重入）
end

-- 入队：score=timestamp, member=requestID
redis.call('ZADD', KEYS[2], ARGV[2], ARGV[4])

-- 检查是否队首
local front = redis.call('ZRANGE', KEYS[2], 0, 0)
if front[1] == ARGV[4] then
    -- 队首，尝试获取锁
    redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[3])
    redis.call('ZREM', KEYS[2], ARGV[4])  -- 从队列移除
    return 1
else
    -- 非队首，移除并失败
    redis.call('ZREM', KEYS[2], ARGV[4])
    return 0
end
`
```

---

### 读写锁 (RWMutex)

**原理**：写锁独占，读锁共享。通过两个 Redis key 实现读写互斥。

**解决的问题**：
- 读多写少场景的性能优化
- 写锁等待时读操作可以并发执行

**流程**：

```
写锁获取：
Client W                   Redis                     Client R1, R2
   │                        │                          │
   │──GET readers:key──────→│←──nil (无读者)──────────│
   │←──可写────────────────│                          │
   │                        │                          │
   │──SET rw:key UUID NX PX→│←──1─────────────────────│
   │←──写锁成功────────────│                          │
   │                        │                          │
   │  业务处理...            │──RLOCK─────────────────→│←──0 (拒绝)
   │                        │←──写锁存在──────────────│

读锁获取：
Client R1                  Redis                     Client R2
   │                        │                          │
   │──GET rw:key───────────→│←──nil (无写锁)──────────│
   │←──可读────────────────│                          │
   │                        │                          │
   │──INCR readers:key────→│←──1─────────────────────│
   │←──读锁成功────────────│                          │
   │                        │──INCR readers:key──────→│
   │                        │←──2─────────────────────│
   │                        │←──读锁成功──────────────│
```

**代码实现**：

```go
// rwmutex.go
type RWMutex struct {
    client      *redis.Client
    key         string        // 写锁 key: "rw:key"
    readersKey  string        // 读锁计数器 key: "rw:readers:key"
    heldType    int           // 0=none, 1=read, 2=write
    readCounter int           // 本地读计数器（支持同一 goroutine 重入）
}

func (r *RWMutex) RLock(ctx context.Context) error {
    // 检查无写锁
    script := redis.NewScript(readLockScript)
    result, err := script.Run(ctx, r.client,
        []string{r.readersKey, r.key},  // 两个 key
        r.value, r.opts.Timeout.Milliseconds(),
    ).Int()
    if result == 1 {
        // 读锁成功
    }
    return nil
}

// 读锁 Lua 脚本
const readLockScript = `
local writeLocked = redis.call('GET', KEYS[2])  -- 检查写锁
if writeLocked ~= false then
    return 0  -- 有写锁，拒绝读
end

local count = redis.call('INCR', KEYS[1])  -- 读者计数 +1
if count == 1 then
    -- 第一个读者设置过期时间
    redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
end
return 1
`

// 写锁 Lua 脚本
const writeLockScript = `
local readCount = redis.call('GET', KEYS[2])  -- 检查读者数
if readCount ~= false and tonumber(readCount) > 0 then
    return 0  -- 还有读者，拒绝写
end

local result = redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', ARGV[2])
if result then
    return 1
end
return 0
`
```

---

### RedLock 多节点锁

**原理**：在 N 个独立 Redis 节点上获取锁，N/2+1 个成功才算成功。

**解决的问题**：
- 单点故障：任意节点挂了不影响锁的可用性
- 跨机房部署：机房级别故障不影响其他机房

**架构**：

```
                    RedLock (5 节点)
                           │
        ┌──────────────────┼──────────────────┐
        │                  │                  │
    ┌───┴───┐          ┌───┴───┐          ┌───┴───┐
    │Redis A│          │Redis B│          │Redis C│
    │(独立) │          │(独立) │          │(独立) │
    └───────┘          └───────┘          └───────┘
        │                  │                  │
        └──────────────────┼──────────────────┘
                           │
              N=5, quorum=3 (3 个成功即成功)
                           │
              任意 2 个节点挂了不影响
```

**流程**：

```go
// redlock.go
func (r *RedLockClient) Lock(ctx context.Context) error {
    r.value = generateLockValue()  // 生成唯一锁值
    r.acquired = 0

    // 1. 依次在每个节点尝试获取
    for _, node := range r.nodes {
        if r.acquireOnNode(ctx, node) {
            r.acquired++
        }
        if r.acquired >= r.quorum {  // 达到多数派
            break
        }
    }

    // 2. 未达多数派，释放已获取的锁
    if r.acquired < r.quorum {
        for _, node := range successfulNodes {
            r.releaseOnNode(ctx, node)
        }
        return ErrRedLockAcquisitionFailed
    }
    return nil
}
```

**注意**：RedLock 节点必须是**独立 Redis 实例**，不能是同一集群的节点。

| 场景 | 方案 |
|------|------|
| 同机房高可用 | Redis Sentinel 或 Cluster |
| 跨机房/金融级 | RedLock |
| 简单互斥 | 单节点锁 |

---

## 核心组件

### baseLock 基础实现

所有单节点锁（Mutex、Reentrant、Fair）都嵌入 `baseLock`，提供通用逻辑。

```go
// redis_lock.go
type baseLock struct {
    client    *redis.Client   // Redis 客户端
    key       string          // Redis key (如 "mutex:mykey")
    value     string          // 锁值（owner 凭证，UUID）
    opts      Options          // 配置项
    isHeld    bool             // 是否持有（本地状态）
    mu        sync.RWMutex     // 保护 isHeld
    watchdog  *Watchdog        // 看门狗
}
```

**核心方法**：

```go
func (b *baseLock) tryLock(ctx context.Context) (bool, error) {
    // 执行 Lua: SET NX PX
    script := redis.NewScript(lockScript)
    result, err := script.Run(ctx, b.client, []string{b.key}, b.value, b.opts.Timeout.Milliseconds()).Int()
    return result == 1, nil
}

func (b *baseLock) lock(ctx context.Context) error {
    if b.opts.Blocking {
        return b.lockBlocking(ctx)   // 阻塞模式：循环重试
    }
    return b.lockNonBlocking(ctx)    // 非阻塞模式：一次
}

func (b *baseLock) unlock(ctx context.Context) error {
    if b.watchdog != nil {
        b.watchdog.Stop()
    }
    script := redis.NewScript(unlockScript)
    result, err := script.Run(ctx, b.client, []string{b.key}, b.value).Int()
    if result == 0 {
        return ErrNotOwner
    }
    return nil
}
```

---

### Watchdog 看门狗

**原理**：后台协程定时续期，防止锁因超时自动释放。

**解决的问题**：
```
无看门狗：
    获取锁 (超时 30s)
        ↓
    业务处理耗时 60s
        ↓
    Redis 自动释放锁 (30s 到期)
        ↓
    其他客户端获取锁 → 数据不一致

有看门狗：
    获取锁 + 启动看门狗
        ↓
    业务处理耗时 60s
        ↓
    看门狗每 10s 续期一次
        ↓
    锁始终保持有效
```

**代码实现**：

```go
// watchdog.go
type Watchdog struct {
    client    *redis.Client
    key       string
    value     string
    timeout   time.Duration
    interval  time.Duration    // 默认 timeout/3
    stopCh    chan struct{}
    stoppedCh chan struct{}
    mu        sync.Mutex
    isRunning bool
}

func (w *Watchdog) Start(ctx context.Context) {
    w.mu.Lock()
    if w.isRunning {
        w.mu.Unlock()
        return
    }
    w.isRunning = true
    w.mu.Unlock()
    go w.run(ctx)  // 启动后台协程
}

func (w *Watchdog) run(ctx context.Context) {
    ticker := time.NewTicker(w.interval)
    defer ticker.Stop()
    defer close(w.stoppedCh)

    for {
        select {
        case <-ctx.Done():
            return
        case <-w.stopCh:
            return
        case <-ticker.C:
            w.extend(ctx)  // 定时续期
        }
    }
}

func (w *Watchdog) extend(ctx context.Context) {
    script := redis.NewScript(extendScript)
    _, err := script.Run(ctx, w.client, []string{w.key}, w.value, w.timeout.Milliseconds()).Result()
    if err != nil {
        w.mu.Lock()
        w.isRunning = false  // 锁可能已过期
        w.mu.Unlock()
    }
}
```

---

## 配置选项

```go
// options.go
type Options struct {
    Timeout          time.Duration  // 锁超时，默认 30s
    Blocking         bool            // 阻塞模式，默认 true
    BlockTimeout     time.Duration  // 阻塞超时
    RetryStrategy    RetryStrategy   // 重试策略
    WatchdogEnabled  bool            // 看门狗，默认 true
    WatchdogInterval time.Duration   // 续期间隔
    LockValue        string          // 自定义锁值
}

type Option func(*Options)

// 使用示例
locker := redislock.NewRedisLocker(client,
    redislock.WithTimeout(30*time.Second),
    redislock.WithBlocking(true),
    redislock.WithWatchdog(true),
    redislock.WithRetry(redislock.NewExponentialBackoff(
        100*time.Millisecond,
        5*time.Second,
        2.0,
    )),
)
```

### 重试策略

```go
// 指数退避
NewExponentialBackoff(baseDelay, maxDelay, multiplier)

// 非阻塞
NoRetry{}

// 直到上下文取消
UntilContext(ctx)
```

---

## 接口定义

```go
// lock.go

// 锁工厂
type Locker interface {
    MutexLock(ctx context.Context, key string, opts ...Option) (Lock, error)
    ReentrantLock(ctx context.Context, key string, opts ...Option) (Lock, error)
    FairLock(ctx context.Context, key string, opts ...Option) (Lock, error)
    ReadLock(ctx context.Context, key string, opts ...Option) (ReadLock, error)
    WriteLock(ctx context.Context, key string, opts ...Option) (WriteLock, error)
}

// 基础锁
type Lock interface {
    Lock(ctx context.Context) error
    Unlock(ctx context.Context) error
    Extend(ctx context.Context, expiry time.Duration) error
    IsHeld() bool
    Key() string
}

// 读锁
type ReadLock interface {
    Lock
    RLock(ctx context.Context) error
    RUnlock(ctx context.Context) error
}

// 写锁
type WriteLock interface {
    Lock
    WLock(ctx context.Context) error
    WUnlock(ctx context.Context) error
}

// RedLock 多节点
type RedLock interface {
    Lock(ctx context.Context) error
    Unlock(ctx context.Context) error
    Extend(ctx context.Context, expiry time.Duration) error
}
```

---

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
| [lock.go](lock.go) | 核心接口定义 |
| [redis_lock.go](redis_lock.go) | Redis 锁基础实现、baseLock |
| [watchdog.go](watchdog.go) | 看门狗自动续期 |
| [mutex.go](mutex.go) | 互斥锁 |
| [reentrant.go](reentrant.go) | 可重入锁 |
| [fair.go](fair.go) | 公平锁 |
| [rwmutex.go](rwmutex.go) | 读写锁 |
| [redlock.go](redlock.go) | RedLock 多节点算法 |
| [options.go](options.go) | 配置选项 |
| [errors.go](errors.go) | 错误定义 |