# 可靠异步事件处理（进程内 / 本地可复现）

基于 Spring Boot + 嵌入式 H2 的 **transactional outbox（事务性发件箱）** 实现，全部组件运行在进程内，
不依赖消息队列、外部数据库、真实凭据或网络服务。业务写入与事件投递在重复提交、瞬时故障、并发压力
和进程重启下具备确定性的、可解释的结果。

## 1. 本地验证（一条命令）

```bash
mvn clean test
```

- JDK 21+（在 JDK 25 上验证通过）；首次运行会从 Maven Central 拉取 H2/JDBC 依赖，之后可离线运行。
- 28 个测试覆盖：正常链路、退避算法、瞬时故障恢复、三类死信区分、并发重复提交、并发消费、
  崩溃租约回收、跨重启持久化、死信重放（含并发重放）、Spring/Web 端到端、调度器生命周期。
- 测试日志通过 SLF4J 打印 **输入 → 事件状态变化 → 判定依据**（关键前缀：
  `[submit]`、`[transition]`、`[dispatch]`、`[dispatch-result]`、`[fault]`、`[handler]`）。

手动运行应用（HTTP 演示）：

```bash
mvn spring-boot:run
# 另开终端：
curl -s -X POST localhost:8080/api/orders -H 'Content-Type: application/json' \
  -d '{"requestId":"r-1001","product":"book","quantity":2}'
curl -s localhost:8080/api/events            # 死信列表
curl -s localhost:8080/api/events/1          # 事件详情（含状态/尝试次数/退避时间）
curl -s localhost:8080/api/events/1/transitions   # 状态流转流水（原因可解释）
curl -s -X POST localhost:8080/api/events/1/replay -H 'Content-Type: application/json' -d '{"note":"已修复"}'
```

应用使用本地文件库 `target/event-data/eventdb.mv.db`（`target/` 已被 gitignore；`mvn clean` 即重置）。

## 2. 事件状态机

```text
                         ┌────────────── 校验失败 / 永久故障 / 重试耗尽 ──────────────┐
                         ▼                                                          │
 (提交) ──> PENDING ──claim──> PROCESSING ──成功──> COMPLETED（终态）                 │
              ▲                  │                                                  │
              │                  │瞬时故障                                            │
              │                  ▼                                                  │
              └────────退避到期重新领取──── PENDING                                   │
                                 │                                                   │
                                 └──── 三类失败 ────────────────────────────────> DEAD_LETTER
                                                                                    │
                                                          replay（修复原因后）───────┘
```

合法流转及 `reason`（写入 `outbox_event_transition`，逐条可查）：

| 流转 | reason | 判定依据 |
|---|---|---|
| ∅→PENDING | `ENQUEUED` | 与业务写入同一事务落库 |
| ∅→PENDING→DEAD_LETTER | `VALIDATION_REJECTED` | 提交时输入校验失败 |
| PENDING→PROCESSING | `CLAIMED` | 调度器乐观领取成功（记录 claimToken） |
| PROCESSING→PENDING | `RETRY_BACKOFF` | 瞬时故障，按指数退避安排下一次领取 |
| PROCESSING→DEAD_LETTER | `NON_RETRYABLE` / `HANDLER_VALIDATION` / `RETRY_EXHAUSTED` | 见第 4 节 |
| PROCESSING→COMPLETED | `COMPLETED` | 处理器成功（或命中消费端去重） |
| PROCESSING→PENDING | `LEASE_STALE_RECLAIMED` | 消费者崩溃，租约 TTL 到期被回收 |
| DEAD_LETTER→PENDING | `REPLAYED` | 运维重放（计数清零） |

## 3. 一致性与幂等语义

### 3.1 业务与事件的原子性

`OrderService.placeOrder` 在 **同一个数据库事务** 中写入 `orders` 与 `outbox_event`：

- 业务提交 ⇒ 事件一定在 outbox 中（随后由调度器投递，绝不丢）；
- 事件不存在 ⇒ 业务必然已回滚（不会出现"业务已提交但事件丢失"）；
- 处理器副作用与消费端去重标记在同一事务中：失败整体回滚，重试时副作用不残留半成品。

### 3.2 提交端幂等（重复提交不产生可独立处理的两条事件）

- 业务键：`orders.request_id` 唯一；事件键：`outbox_event.idempotency_key` 唯一（数据库约束兜底）。
- 同键并发/重复提交：一个事务成功，其余触发唯一冲突后改查已有记录，返回 `duplicated=true`，
  事件 ID 与订单 ID 完全一致。
- 独立提交（`ReliableEventService.submitStandalone`）语义相同。

### 3.3 消费端幂等（不重复副作用）

双层保证：

1. **乐观领取**：`UPDATE ... WHERE status='PENDING' AND next_attempt_at<=now`，
   每次领取生成唯一 `claimToken`，后续所有状态推进都带 token 校验，并发消费者只有一个成功；
2. **去重表 + 幂等副作用**：处理事务先写 `processed_event`（主键冲突即跳过处理器），
   演示处理器的投影写入使用 `MERGE ... KEY(order_id)`。因此 at-least-once 投递下副作用 **恰好一次**。

### 3.4 重启安全

- 事件、尝试次数、退避时间、死信原因全部持久化；`RestartPersistenceTest` 用全新 DataSource/引擎
  实例重连同一文件库，模拟进程重启，验证 PENDING/DEAD_LETTER 均存活且不重复处理。
- 消费者崩溃时事件停在 `PROCESSING` 且持有旧 claimToken；租约超过
  `reliable.events.lease-ttl` 后由 `LEASE_STALE_RECLAIMED` 回收为 PENDING 重新投递。
  若崩溃发生在副作用提交之后，重投命中去重表，直接完成而不重复副作用。

## 4. 失败分类、重试与死信规则

| 失败类型 | 判定 | 处理 | 死信原因（互相区分） |
|---|---|---|---|
| 瞬时故障 | `RetryableEventException`，或处理器抛出的其它未分类异常（保守重试） | 指数退避自动重试 | 达到 `max-attempts` 后 `RETRY_EXHAUSTED` |
| 永久故障 | `NonRetryableEventException`，或事件类型无已注册处理器 | **不重试**，立即死信 | `NON_RETRYABLE` |
| 输入校验失败 | 提交时非法 JSON/空字段；或处理器抛 `InvalidEventException` | **不重试**，立即死信 | `VALIDATION_FAILED` |

- 退避：`delay(n) = min(initial × multiplier^(n-1), max) × (1 + rand(0..jitter))`，参数均可配；
  抖动避免惊群。默认 5 次尝试、200ms 起、2 倍增长、30s 封顶、20% 抖动。
- **绝不静默丢弃，也绝不无限重试**：任何终态失败都带 `dead_reason + error_detail` 进入死信列表，
  并伴随流转流水；`attempts` 从 1 开始计数，成功后事件不再被拾取。
- 死信恢复：修复根因后 `POST /api/events/{id}/replay`（或
  `ReliableEventService.replayDeadLetter`），事件回到 PENDING、计数清零；条件更新保证
  并发重放只有一次生效，消费端去重保证重放不产生重复副作用。

## 5. 进程内组件结构

```text
core/      EventHandler 接口；EventStatus/DeadReason/TransitionReason；
           Retryable/NonRetryable/Invalid 三类异常；事件与流水记录类型
storage/   SchemaInitializer（幂等建表）、EventRepository（outbox / 去重表 / 流水 / 乐观领取）
engine/    RetryPolicy、EventHandlerRegistry、
           ReliableEventService（提交门面：事务内登记、校验死信、查询、重放）、
           EventDispatcher（轮询→领取→执行→分类→重试/死信；租约回收；后台线程生命周期）
demo/      OrderService（业务+事件同事务）、OrderCreatedHandler（幂等投影副作用）、
           FaultSimulator（进程内瞬时/永久故障注入，供演示与测试）
web/       /api/orders（演示下单）、/api/events（查询、流水、死信列表、重放）
```

处理器异常约定见 `EventHandler` Javadoc。

## 6. 配置项（application.properties）

| 键 | 默认 | 说明 |
|---|---|---|
| `reliable.events.dispatcher-enabled` | true | 后台调度器开关 |
| `reliable.events.worker-count` | 2 | 消费工作线程数 |
| `reliable.events.poll-interval` | 200ms | 轮询间隔（上一轮未结束自动跳过） |
| `reliable.events.batch-size` | 32 | 每轮最多领取条数 |
| `reliable.events.max-attempts` | 5 | 最大尝试次数（含首次） |
| `reliable.events.initial-backoff` | 200ms | 首次退避 |
| `reliable.events.backoff-multiplier` | 2.0 | 退避倍数 |
| `reliable.events.max-backoff` | 30s | 退避上限 |
| `reliable.events.jitter-factor` | 0.2 | 抖动比例（[0,1)） |
| `reliable.events.lease-ttl` | 30s | 处理租约；崩溃事件超过该时长可被回收 |

## 7. 测试场景对照

| 测试类 | 场景 |
|---|---|
| `RetryPolicyTest` | 指数退避、上限截断、抖动边界 |
| `HappyPathTest` | 提交→领取→完成、流水完整、同键去重 |
| `TransientFailureRecoveryTest` | 前 N 次瞬时故障后退避重试并恢复；未知类型判永久 |
| `DeadLetterClassificationTest` | RETRY_EXHAUSTED / NON_RETRYABLE / VALIDATION_FAILED 三类区分 |
| `ConcurrencyTest` | 32 线程同键并发提交只生成 1 条；6 消费者并发处理 60 条无重复副作用 |
| `LeaseRecoveryTest` | 崩溃于副作用前/后两种租约回收路径 |
| `RestartPersistenceTest` | 文件库跨"进程重启"存活与恢复 |
| `DeadLetterReplayTest` | 永久故障/重试耗尽后重放完成；非死信重放空操作；16 路并发重放仅 1 次生效 |
| `OrderServiceFlowTest` | 业务端到端：幂等并发下单、故障恢复、非法输入死信、修复后重放 |
| `DispatcherLifecycleTest` | 后台轮询自动投递、优雅停止、重复启停 |
| `SpringWebIntegrationTest` | 真实 Spring 装配 + HTTP 全链路 |
