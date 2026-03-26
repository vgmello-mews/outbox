# Parallel Publish Threads Design

## Problem

Today each publisher instance runs a single publish loop (`PublishLoopAsync`) that processes all its owned partitions sequentially. With 64 partitions and, say, 2 publishers, each publisher owns ~32 partitions but processes them through a single poll-and-publish cycle. This limits throughput — the transport round-trip for one partition key blocks processing of all others.

## Goal

Allow configurable parallelism **within** a publisher instance so that multiple threads can concurrently process different partitions from the same leased batch. Ordering within a partition key must be preserved.

## Design

### Configuration

New option in `OutboxPublisherOptions`:

```csharp
public int PublishThreadCount { get; set; } = 4;
```

- Default: 4
- Validation: must be >= 1
- When set to 1, behavior is identical to the current single-loop design

### Architecture: Coordinator + N Workers

The current `PublishLoopAsync` is refactored into a coordinator and N worker tasks.

**Coordinator (single loop, replaces current `PublishLoopAsync`):**

1. Snapshots `opts = GetOptions()` once at the top of each iteration — all workers use this same snapshot (prevents hot-reload mid-cycle inconsistency)
2. Snapshots `totalPartitions` once from the cached partition count — used for all partition-to-worker assignments in this cycle
3. Polls `LeaseBatchAsync` — unchanged, fetches up to `BatchSize` messages across all owned partitions
4. If batch is empty — adaptive backoff (same as today)
5. Separates poison messages (`RetryCount >= MaxRetryCount`) and dead-letters them immediately, removing their sequence numbers from `unprocessedSequences` before dispatching to workers (same as today)
6. Groups healthy messages by `(TopicName, PartitionKey)` — same as today
7. Assigns each group to a worker using `partition_id % PublishThreadCount`, where `partition_id = hash(partition_key) % totalPartitions` (using the snapshotted value)
8. Launches N workers concurrently via `Task.WhenAll`
9. After `Task.WhenAll` completes, aggregates `publishedAny` from all workers. If no worker published anything (all circuits open or all errors), applies adaptive backoff to prevent tight polling loops
10. If any worker faulted, logs all exceptions from `Task.Exception.InnerExceptions` before proceeding to cleanup
11. Runs the finally block to release any unprocessed sequences (same safety net as today)

**Each worker (runs concurrently, returns bool `publishedAny`):**

- Receives its assigned message groups and the snapshotted `opts`
- Iterates each group sequentially: circuit breaker check, `SendAsync`, delete/release
- Uses the same logic as today's inner loop — full success, full failure, partial failure paths are unchanged
- Shares the circuit breaker instance and instrumentation counters with other workers
- Returns whether it successfully published at least one group

**Housekeeping loops unchanged:**

The 4 non-publish loops (heartbeat, rebalance, orphan sweep, dead-letter sweep) remain single-instance per publisher. Only the publish loop is parallelized.

### Partition-to-Worker Assignment

Assignment is deterministic, using values snapshotted once per poll cycle:

```
total_partitions = snapshotted at cycle start
partition_id     = hash(partition_key) % total_partitions
worker_index     = partition_id % PublishThreadCount
```

Properties:
- Same partition key always goes to the same worker within a poll cycle
- Ordering within a partition key is preserved (single worker, sequential processing)
- Different partition keys can be processed concurrently across workers
- No coordination needed between workers — assignment is purely derived from the messages in hand
- `total_partitions` is snapshotted once before assignment to prevent inconsistency if the cached value refreshes mid-cycle

### Ordering Guarantee

Messages with the same `(TopicName, PartitionKey)` are always assigned to the same worker. Within that worker, they are processed in `EventDateTimeUtc + EventOrdinal` order (the existing sort). This preserves the causal ordering invariant.

### Thread Safety

**Already thread-safe (no changes needed):**

- `TopicCircuitBreaker` — uses `ConcurrentDictionary` with per-entry locks internally
- `OutboxInstrumentation` — uses `System.Diagnostics.Metrics` counters (atomic by design)
- `IOutboxStore` — each call is an independent DB operation

**Changed for concurrency:**

- `unprocessedSequences` — changes from `HashSet<long>` to `ConcurrentDictionary<long, byte>`. Each worker removes its sequences as it processes them. Workers operate on disjoint sets of sequence numbers (poison sequences are removed by the coordinator before worker dispatch), so no contention occurs. The finally block releases whatever remains.
- `OutboxHealthState` — verify that `RecordSuccessfulPublish()` and other write methods are safe for concurrent calls. If not, add synchronization.

**No shared mutable state between workers:**

- Each worker gets its own subset of message groups (no overlap by partition)
- Transport `SendAsync` calls are independent per group
- Delete/release calls operate on disjoint sequence numbers

### Error Handling

**Worker-level failures:**

- Each worker handles its own transport errors exactly as today (full failure, partial failure, circuit-open release). No change to retry/release logic.
- If a worker throws an unexpected exception, the coordinator inspects all tasks after `Task.WhenAll` and logs every faulted task's exception (not just the first). Unfinalized sequences remain in `unprocessedSequences` and are released without retry in the finally block. This preserves the "no catch-all without logging" invariant.

**Backoff after no progress (`publishedAny` aggregation):**

- Each worker returns a `bool` indicating whether it successfully published at least one group
- The coordinator aggregates these results: `publishedAny = workers.Any(w => w.Result == true)`
- If no worker published anything (all circuits open, all transport errors), the coordinator applies adaptive backoff before the next poll — same as today's tight-loop prevention

**Cancellation:**

- The coordinator passes its `CancellationToken` to all workers
- Workers check cancellation between groups (not mid-send)
- On cancellation, workers exit their loops, and the finally block cleans up with `CancellationToken.None`

**Circuit breaker interaction:**

- One worker's transport failure records against the shared circuit breaker
- If the circuit opens, other workers see it on their next group iteration and release remaining messages without retry
- This is desired behavior — a broker outage affects all workers immediately

**HalfOpen probe behavior:**

The circuit breaker's `IsOpen()` method uses per-entry locking for the Open-to-HalfOpen transition. With N concurrent workers, only one will observe the transition, but once HalfOpen, `IsOpen()` returns `false` for all callers — meaning multiple workers could probe the same topic simultaneously in the same cycle. This relaxes the current single-probe guarantee from "exactly one probe batch" to "up to N probe batches." This is acceptable: the circuit either closes (all probes succeed) or re-opens (at least one fails and records failure). No invariant is violated — the single-probe guarantee was a performance optimization, not a correctness requirement.

No new error paths are introduced. The structural change is `Task.WhenAll` over N workers instead of a sequential `foreach` over groups.

### Batch Size Semantics

`BatchSize` (default 100) remains the total per poll, not per thread. Each thread gets roughly `BatchSize / PublishThreadCount` messages worth of work. Database load is unchanged.

Note: the parallelism benefit is proportional to partition key diversity in each batch. If all messages in a batch share the same `(TopicName, PartitionKey)`, they will all be assigned to a single worker — no parallelism gain. This is expected: ordering requires same-key messages to be processed sequentially. The feature benefits workloads with many distinct partition keys.

## Changes

### Files to Modify

| File | Change |
|------|--------|
| `src/Outbox.Core/Options/OutboxPublisherOptions.cs` | Add `PublishThreadCount` property (default 4), validation >= 1 |
| `src/Outbox.Core/Engine/OutboxPublisherService.cs` | Refactor `PublishLoopAsync` into coordinator + N workers, `ConcurrentDictionary` for unprocessed sequences, `publishedAny` aggregation, exception logging for all faulted workers |

### Documentation Updates

| File | Change |
|------|--------|
| `docs/architecture.md` | Document parallel publish threads concept |
| `docs/getting-started.md` | Mention `PublishThreadCount` in scaling section |
| `docs/known-limitations.md` | Add note that Kafka `Flush()` thread usage scales with `PublishThreadCount` — users with Kafka transport should tune conservatively if thread pool pressure is observed |

### Tests

- Unit test for `PublishThreadCount >= 1` validation in `OutboxPublisherOptionsValidatorTests`
- Unit tests verifying partition-to-worker assignment distributes correctly
- Unit tests verifying `PublishThreadCount=1` preserves sequential behavior
- Existing integration tests run with default of 4 threads (validates no regressions)

### No Changes To

- `IOutboxStore` or any store implementations
- SQL queries
- Transport interface (`IOutboxTransport`)
- Circuit breaker (`TopicCircuitBreaker`)
- Rebalance/heartbeat/orphan sweep/dead-letter sweep loops
- Message model or partition table schema
