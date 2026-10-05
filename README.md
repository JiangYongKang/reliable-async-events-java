# Reliable Async Events（进程内可靠异步事件）

基于 Spring Boot 3 / Java 21 实现的一套**本地可复现**的可靠异步事件处理能力。事件的提交、
状态流转、重试、死信与重放全部由运行在**当前进程内**的组件承载，使用本地预写日志（WAL）持久化，
**不依赖任何外部中间件（无 Kafka/数据库/Redis）、真实凭据或网络服务**。

## 1. 要解决的问题

业务写入与异步事件处理在以下场景下需要确定、可解释的结果：

- 重复提交（同一业务幂等键被多次提交，含高并发）；
- 瞬时故障（远程超时、连接重置等）与永久故障（资源不存在、输入非法）混杂；
- 并发消费、消费者进程崩溃与重启。

## 2. 总体架构

```
业务服务
  │ publish(idempotencyKey, type, payload, businessSideEffects)
  ▼
DefaultEventPublisher ──提交期校验（非法 JSON/空键直接 400，不落事件）
  │
  ▼
DurableWALEventStore  ── 业务写入 + 事件在同一磁盘批次（一次 fsync）原子落盘
  │                          内部状态全部由 WAL 重放得到，内存仅作索引
  ▼
DurableFileLog (data/reliable-events/events.wal)
  ▲
  │ 领取（租约 + 条件更新）
EventDispatcher（后台 poller + worker 线程，随 Spring 容器启停）
  │ 按异常类型分类：成功 / 瞬时重试 / 永久死信 / 校验死信
  ▼
EventHandler（业务实现，返回的副作用 KV 与 SUCCEEDED 同批落盘）
```

关键一致性手段是**事务性发件箱（Transactional Outbox）的进程内变体**：业务写入与事件投递
被编码为同一个 WAL 提交批次，要么整批可见、要么整批不可见（崩溃时尾部不完整帧按 CRC/长度
校验丢弃并截断），因此：

- 不可能“业务已提交但事件丢失”；
- 不可能“事件已存在而业务回滚”；
- 处理器返回的副作用与 SUCCEEDED 标记同批落盘，失败的尝试不产生任何副作用。

## 3. 事件状态机

```
                 领取(claim, 获得租约 token)
   PENDING ───────────────────────────────┬──────────────► SUCCEEDED（终态）
     ▲      退避未到/租约未释放：不可再领取   │ 处理器成功：副作用 + SUCCEEDED 同批落盘
     │                                     │
     │ 死信 replay                         │ 处理器抛“瞬时”异常（其它任意 Throwable）
     │                                     ▼
     └─────────────────────────────── RETRYING（记录 nextAttemptAt、lastError）
     ▲                                     │ 退避到期后可被再次领取
     │                                     ├─ 达到 maxAttempts 仍失败
     │                                     ▼
     └──────────────────────────────── DEAD_LETTER（终态，可查询 / 可 replay）
                处理器抛 NonRetryableEventException   ──► NON_RETRYABLE
                处理器抛 EventValidationException      ──► VALIDATION_FAILED
                瞬时重试次数耗尽                        ──► RETRIES_EXHAUSTED
```

- `PENDING`：已落盘，等待处理；死信重放后也回到此状态（尝试次数清零）。
- `RETRYING`：处理过至少一次且遇到瞬时故障，等待指数退避窗口结束。
- `SUCCEEDED`：终态。业务副作用与成功标记在同一批次落盘。
- `DEAD_LETTER`：终态，携带互相区分的 `reason` 与 `lastError`，可列出、可重放。

所有状态流转都通过 **WAL 追加**完成，不做原地更新；重启时重放整个日志重建状态机、
幂等索引与业务 KV。

### 3.1 失败分类（互相区分，不静默丢弃、不无限重试）

| 情形 | 处理器表现 | 判定结果 | reason |
| --- | --- | --- | --- |
| 瞬时故障 | 抛任意未被识别为下面两类的异常（如 `RuntimeException`、IO 异常） | 指数退避自动重试，耗尽后死信 | `RETRIES_EXHAUSTED` |
| 永久故障 | 抛 `NonRetryableEventException` | **不重试**，立即死信 | `NON_RETRYABLE` |
| 消费期输入校验失败 | 抛 `EventValidationException` | **不重试**，立即死信 | `VALIDATION_FAILED` |
| 未注册的事件类型 | 分发器查不到处理器 | **不重试**，立即死信 | `NON_RETRYABLE` |
| 提交期输入校验失败 | 空幂等键 / 空类型 / 非法 JSON | HTTP 400 拒绝，**不产生事件** | —（接口层） |

## 4. 幂等语义

### 4.1 提交幂等

以业务幂等键 `idempotencyKey` 为唯一约束：

- 第一次提交：业务写入 + 事件整批落盘，返回 `duplicate=false`；
- 此后对**同一键**的任意重复提交（不同 payload/副作用也一样）：直接返回先前那条事件，
  `duplicate=true`，**不写第二条事件、不覆盖业务副作用**；
- 高并发下，临界区在同一把锁内“先查幂等索引再落盘”，保证恰好一个提交者胜出。
  测试 `ConcurrentSubmissionTests` 用 32 线程 × 25 次 = 800 次重复提交验证仅 1 条事件。

### 4.2 消费幂等 / 恰好一次

- 领取事件会获得单调递增的**租约令牌（lease token）**；后续成功、重试、死信都必须携带
  匹配的令牌做**条件更新（CAS）**，令牌不符（事件已被租约超时回收并由他人重新领取）时，
  迟到的处理结果被直接丢弃并记录，避免重复副作用。
- 成功的副作用以业务 KV 形式与 SUCCEEDED 同批落盘；崩溃恢复后旧令牌失效。
- 消费者在处理中途崩溃：事件停留在“已领取但非终态”，租约到期后可被重新领取；
  旧消费者迟到的终态写入因令牌不符而失败。`RestartRecoveryTests` 验证已成功事件不重复处理、
  在途事件恢复后最终成功且副作用唯一。

## 5. 重试与退避规则

- 最大尝试次数 `reliable-events.max-attempts`（**含首次**，默认 5）。第 N 次失败后：

  ```
  delay(N) = min(baseBackoff * 2^(N-1), maxBackoff) + uniform(0, jitter)
  ```

- 退避窗口未到、租约未释放的事件不会被领取。
- 达到最大尝试次数的瞬时故障进入死信（`RETRIES_EXHAUSTED`），保留最后一次错误信息。
- 退避参数全部可配置，见下表。

### 配置项（前缀 `reliable-events`）

| 属性 | 默认值 | 含义 |
| --- | --- | --- |
| `store-dir` | `data/reliable-events` | 本地 WAL 目录（仅本地文件，无外部服务） |
| `max-attempts` | `5` | 最大尝试次数（含首次） |
| `base-backoff-millis` | `200` | 首次重试基础退避 |
| `max-backoff-millis` | `5000` | 指数退避上限 |
| `jitter-millis` | `50` | 随机抖动，避免并发同时唤醒 |
| `lease-millis` | `30000` | 消费租约时长，超时后事件可被重新领取 |
| `poll-interval-millis` | `50` | 分发器轮询间隔 |
| `worker-count` | `2` | 消费线程数 |

## 6. 死信与恢复

- 死信可通过 `DeadLetterService` / REST 列出，包含事件 ID、幂等键、`reason`、
  `lastError`、尝试次数、进入死信时间，**原因可查询、可解释**。
- 修复根因后调用 `replay(eventId)`：事件原子地从 `DEAD_LETTER` 回到 `PENDING`，
  尝试次数清零，由正常分发链路重新领取；重放本身不执行任何副作用，因此不会产生重复副作用。
- 可以多次重放；若问题未修复，事件会按相同规则再次进入死信。
- 大量死信的批量恢复见第 8 节。

## 7. 流转历史（可查询的审计时间线）

每一次状态推进都会留下一条不可变的 `TransitionRecord`：

```
seq, eventId, eventType, action, fromStatus, toStatus, attemptCount, atMillis, reason, trigger
```

- `action` 覆盖完整生命周期：`SUBMITTED`（提交）、`CLAIMED`（被领取，含租约超时回收）、
  `RETRY_SCHEDULED`（排定重试，携带失败原因）、`SUCCEEDED`、`DEAD_LETTER`（携带区分原因）、
  `REPLAYED`（携带触发来源：`manual-replay` 或 `batch-recovery:<jobId>`）。
- **一致性保证**：历史条目与对应的状态流转写在**同一个 WAL 批次**（同一次 fsync），
  因此不可能出现“状态变了但历史缺一笔”或“历史有记录但状态没跟上”；进程在任意写入点
  被强杀，重启重放后历史与状态依然一一对应（`EventHistoryTests` 逐步校验
  “历史末笔 toStatus == 事件当前状态”）。
- 历史随 WAL 持久化，进程重启、事件终态完成后仍可查询。
- 查询方式：
  - 单事件时间线：`EventStore.historyOf(eventId)` / `GET /api/events/{eventId}/history`；
  - 跨事件检索：`EventStore.searchTransitions(TransitionQuery)` /
    `GET /api/events/history?type=&action=&toStatus=&fromMillis=&toMillis=&offset=&limit=`，
    支持按事件类型、流转动作（结果）、目标状态、时间窗口过滤，`offset/limit` 分页；
    `limit` 有硬上限（500），超限或非法分页参数直接 400 拒绝，避免一次倒出全量。

**兼容范围**：流转历史是纯增量能力——旧版本写入的 WAL 文件不含 `HISTORY` 条目，
升级后照常恢复，老事件从升级后的第一次流转开始积累历史；状态机、幂等、租约、
重试/死信语义完全不变。批量恢复复用既有的原子 `replay` 路径，不改变单条重放的语义。

## 8. 批量死信恢复

根因修复后，可通过 `DeadLetterRecoveryService` / REST 一次恢复一批死信：

- **选事件**：显式 `eventIds` 列表，或条件筛选（`eventType` / `reason` /
  `deadLetteredBeforeMillis`），两者取并集去重；任务启动时对目标集合做快照。
- **受控恢复**：`ratePerSecond` 全局限速（每秒最多重放多少条）+ `concurrency`
  并发上限，避免恢复流量打满处理链路与下游。
- **进度可查 / 可停止**：任务异步执行，`progress(jobId)` 返回
  `total/replayed/skipped/failed` 与状态（RUNNING/STOPPING/STOPPED/COMPLETED/FAILED）；
  `stop(jobId)` 幂等请求中途停止，在途重放收尾后任务进入 STOPPED。
- **幂等与最终一致**：
  - 单条重放仍是原子的 WAL 批次（REPLAY + HISTORY 同批 fsync），进程在任意时刻被强杀，
    已重放的保持 PENDING、未重放的保持死信，**不留半截状态**；
  - 目标事件若已不在死信态（重复提交同一批、被其它路径重放）计为 `skipped`，
    不重复写历史、不产生重复副作用；
  - 停止后或重启后，用相同条件再发起一个任务即可续跑，最终所有死信一致地跑完；
    每条事件的重放历史恰好一条（`BatchRecoveryTests` 逐条断言）。
- **不饿死新事件**：重放仅把事件置回 PENDING 并以其重放时刻参与
  `nextAttemptAt` 公平排队，正常新事件按同一规则竞争领取；限速参数进一步约束
  恢复流量的注入速率。

## 9. REST 接口（演示）

| 方法与路径 | 说明 |
| --- | --- |
| `POST /api/events` | 提交事件。body：`{"idempotencyKey","type","payload"(任意JSON),"businessState":{k:v}}`，返回事件快照与 `duplicate` |
| `GET /api/events/{eventId}` | 按事件 ID 查询完整状态 |
| `GET /api/events/by-key/{idempotencyKey}` | 按幂等键查询 |
| `GET /api/events/dead-letters` | 列出全部死信 |
| `POST /api/events/{eventId}/replay` | 重放死信 |
| `GET /api/events/{eventId}/history` | 单个事件的完整流转时间线 |
| `GET /api/events/history?type=&action=&toStatus=&fromMillis=&toMillis=&offset=&limit=` | 跨事件检索流转历史（分页，limit≤500） |
| `POST /api/events/dead-letters/recover` | 发起批量恢复。body：`{"eventIds":[...],"eventType","reason","deadLetteredBeforeMillis","ratePerSecond","concurrency"}`（均可选，速率默认 10/s、并发默认 2），返回 `jobId` |
| `GET /api/events/recoveries/{jobId}` | 查询批量恢复进度 |
| `POST /api/events/recoveries/{jobId}/stop` | 中途停止批量恢复（幂等） |

## 10. 本地验证（一条命令，无需任何外部依赖）

前置：JDK 21、Maven（使用工程自带的 Spring Boot 3.4.1 BOM，首次会拉取 Maven 依赖）。

```bash
mvn test
```

该命令运行全部 31 个测试，覆盖需求列出的全部场景：

| 测试类 | 覆盖场景 |
| --- | --- |
| `DurableFileLogTests` | WAL 原子批次、重启重放、崩溃撕裂写自动截断 |
| `EventStoreStateMachineTests` | 幂等提交、租约条件更新、退避重试、三种死信原因区分、死信重放、重启状态恢复、租约超时回收 |
| `DispatcherFailureClassificationTests` | 瞬时故障自动重试恢复、永久故障直入死信、消费期校验失败死信、重试耗尽死信、修复后死信重放成功且副作用唯一 |
| `ConcurrentSubmissionTests` | 800 次并发重复提交仅 1 条事件；100 个不同键并发全部落盘不丢 |
| `ConcurrentConsumptionTests` | 多分发器/多 worker 并发消费每事件恰好一次；瞬时失败在并发下恢复 |
| `RestartRecoveryTests` | 处理中崩溃后重启：已完成不重复、在途事件租约超时回收后完成 |
| `EventHistoryTests` | 全生命周期时间线、每步“历史末笔==当前状态”一致性、死信/重放的原因与触发来源、重启后历史完整且 seq 连续、跨事件过滤与分页、limit 硬上限校验 |
| `BatchRecoveryTests` | 条件筛选批量恢复、限速生效、进度可查、显式 ID 恢复、同一批重复提交幂等跳过、中途停止+续跑无重复、新事件不被饿死、崩溃重启后恢复结果一致 |
| `SpringBootIntegrationTests` | REST 端到端：提交、自动消费/死信、400 拒绝非法输入、幂等重复、死信查询与重放 |
| `HistoryAndRecoveryApiTests` | REST 端到端：单事件时间线、跨事件历史检索分页、批量恢复发起/进度/停止、恢复后历史含 batch-recovery 来源 |
| `ApplicationTests` | Spring 上下文装配冒烟 |

测试日志按要求打印**输入**（幂等键、类型、payload、并发规模）、**事件状态变化**
（PENDING/RETRYING/DEAD_LETTER/SUCCEEDED 及尝试次数）和**判定依据**
（异常分类、退避窗口、租约匹配、CRC 校验、幂等索引命中等）。日志中以
`【输入】` / `【状态变化】` / `【判定依据】` 前缀便于检索。

手动体验（可选）：

```bash
mvn spring-boot:run
# 提交
curl -s -X POST localhost:8080/api/events -H 'Content-Type: application/json' \
  -d '{"idempotencyKey":"o-1","type":"DEMO","payload":{"x":1},"businessState":{"order:o-1":"CREATED"}}'
# 查询 / 死信 / 重放
curl -s localhost:8080/api/events/by-key/o-1
curl -s localhost:8080/api/events/dead-letters
curl -s -X POST localhost:8080/api/events/<eventId>/replay
# 流转历史：单事件时间线 / 跨事件检索
curl -s localhost:8080/api/events/<eventId>/history
curl -s 'localhost:8080/api/events/history?action=DEAD_LETTER&offset=0&limit=50'
# 批量恢复死信：发起 -> 查进度 -> （可选）停止
curl -s -X POST localhost:8080/api/events/dead-letters/recover -H 'Content-Type: application/json' \
  -d '{"eventType":"DEMO","ratePerSecond":10,"concurrency":2}'
curl -s localhost:8080/api/events/recoveries/<jobId>
curl -s -X POST localhost:8080/api/events/recoveries/<jobId>/stop
```

> 注：默认无内置 `DEMO` 处理器，未注册类型会进入死信（`NON_RETRYABLE`），
> 可用于直接观察死信与重放流程；业务事件类型请实现 `EventHandler` 并注册为 Spring Bean。

## 11. 主要代码位置

```
src/main/java/com/github/highcumontoa/reliableasynceventsjava/events/
├─ EventStatus / DeadLetterReason / EventRecord   # 状态机与事件快照
├─ TransitionAction / TransitionRecord            # 流转历史记录
├─ TransitionQuery / TransitionPage               # 历史检索条件与分页结果
├─ RecoveryRequest / RecoveryProgress             # 批量恢复请求与进度
├─ EventHandler / EventPublisher / DeadLetterService / DeadLetterRecoveryService
├─ EventStore / BusinessStateStore
├─ EventValidationException / NonRetryableEventException / OptimisticLockingFailureException
├─ RetryPolicy / ExponentialBackoffRetryPolicy    # 指数退避
├─ log/DurableFileLog                             # 帧格式 + fsync + CRC + 撕裂写截断
├─ DurableWALEventStore                           # WAL 重放状态机、租约条件更新、历史同批落盘与检索
├─ HandlerRegistry / DefaultEventPublisher / DefaultDeadLetterService
├─ DefaultDeadLetterRecoveryService               # 批量恢复：限速/并发/进度/停止（SmartLifecycle）
├─ EventDispatcher                                # 后台轮询消费与失败分类（SmartLifecycle）
├─ config/ReliableEvents*                         # Spring 装配与配置属性
└─ api/ReliableEventsController                   # REST 接口
```
