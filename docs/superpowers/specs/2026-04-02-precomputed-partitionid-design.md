# Precomputed PartitionId for SQL Server

## Problem

The SQL Server FetchBatch query performs a full clustered index scan of the entire Outbox table. The execution plan shows:

```
Sort(TOP 500)
  └── Hash Match (Inner Join on PartitionId)
       ├── Clustered Index Seek on OutboxPartitions (good)
       └── Compute Scalar (CHECKSUM per row)
            └── Clustered Index Scan on PK_Outbox ← FULL TABLE SCAN
```

The `IX_Outbox_Pending` index (keyed on `EventDateTimeUtc, EventOrdinal`) is never used for FetchBatch because the WHERE clause filters on `RetryCount` and `RowVersion`, and the JOIN condition computes `CHECKSUM(PartitionKey) % @TotalPartitions` per row. SQL Server cannot seek into an index when the filter columns aren't leading keys and the join requires a per-row computation.

This results in ~130ms poll latency (p50) compared to PostgreSQL's 3-5ms. The bottleneck is confirmed to be identical on both Azure SQL Edge ARM and SQL Server 2025 x86 — it's the query plan, not the hardware.

## Solution

Add a persisted computed column `PartitionId` to `dbo.Outbox` that precomputes the partition hash at INSERT time. Create an index keyed on `PartitionId` so FetchBatch can seek directly to owned partitions. Rewrite the FetchBatch query to use `WHERE PartitionId IN (owned partitions)` instead of the JOIN with per-row CHECKSUM.

Validated on SQL Server 2025: execution plan changes from Clustered Index Scan to Index Seek, query time drops from ~130ms to ~7ms.

## Scope

- **SQL Server only.** PostgreSQL keeps dynamic partitioning (3-5ms poll, no improvement needed).
- **Partition count increases from 64 to 128** (power of 2, supports up to 128 concurrent publishers).
- **Partition count is baked into the schema** for SQL Server. Changing it requires ALTER TABLE + downtime. A runbook documents the procedure.

## Schema Change

### Computed column

```sql
ALTER TABLE dbo.Outbox ADD PartitionId AS 
    (ABS(CAST(CHECKSUM(PartitionKey) AS BIGINT)) % 128) PERSISTED;
```

In the install script, the column is defined inline with the CREATE TABLE:

```sql
PartitionId AS (ABS(CAST(CHECKSUM(PartitionKey) AS BIGINT)) % 128) PERSISTED
```

### Index

Replace the current index:

```sql
-- Old (never used for FetchBatch — full table scan)
CREATE NONCLUSTERED INDEX IX_Outbox_Pending
ON dbo.Outbox (EventDateTimeUtc, EventOrdinal)
INCLUDE (SequenceNumber, TopicName, PartitionKey, EventType, Headers, Payload, PayloadContentType, RetryCount, CreatedAtUtc);

-- New (Index Seek by PartitionId, then ordered by EventDateTimeUtc, EventOrdinal)
CREATE NONCLUSTERED INDEX IX_Outbox_Pending 
ON dbo.Outbox (PartitionId, EventDateTimeUtc, EventOrdinal)
INCLUDE (SequenceNumber, TopicName, PartitionKey, EventType, Headers, Payload, 
         PayloadContentType, RetryCount, CreatedAtUtc, RowVersion);
```

Note: `RowVersion` is added to INCLUDE to make the index fully covering (the WHERE clause filters on it).

### Partition seed

Change from 64 to 128 partitions:

```sql
-- Old
INSERT INTO dbo.OutboxPartitions (...) SELECT ..., value, ... FROM GENERATE_SERIES(0, 63);

-- New
INSERT INTO dbo.OutboxPartitions (...) SELECT ..., value, ... FROM GENERATE_SERIES(0, 127);
```

## Query Change

### FetchBatch (SQL Server only)

Old query (JOIN with per-row CHECKSUM):

```sql
SELECT TOP (@BatchSize)
    o.SequenceNumber, o.TopicName, o.PartitionKey, o.EventType,
    o.Headers, o.Payload, o.PayloadContentType,
    o.EventDateTimeUtc, o.EventOrdinal,
    o.RetryCount, o.CreatedAtUtc
FROM dbo.Outbox o
INNER JOIN dbo.OutboxPartitions op
    ON  op.OutboxTableName = @OutboxTableName
    AND op.OwnerPublisherId = @PublisherId
    AND (op.GraceExpiresUtc IS NULL OR op.GraceExpiresUtc < SYSUTCDATETIME())
    AND (ABS(CAST(CHECKSUM(o.PartitionKey) AS BIGINT)) % @TotalPartitions) = op.PartitionId
WHERE o.RetryCount < @MaxRetryCount
  AND o.RowVersion < MIN_ACTIVE_ROWVERSION()
ORDER BY o.EventDateTimeUtc, o.EventOrdinal;
```

New query (IN subquery on precomputed PartitionId):

```sql
SELECT TOP (@BatchSize)
    o.SequenceNumber, o.TopicName, o.PartitionKey, o.EventType,
    o.Headers, o.Payload, o.PayloadContentType,
    o.EventDateTimeUtc, o.EventOrdinal,
    o.RetryCount, o.CreatedAtUtc
FROM dbo.Outbox o
WHERE o.PartitionId IN (
    SELECT op.PartitionId 
    FROM dbo.OutboxPartitions op
    WHERE op.OutboxTableName = @OutboxTableName
      AND op.OwnerPublisherId = @PublisherId
      AND (op.GraceExpiresUtc IS NULL OR op.GraceExpiresUtc < SYSUTCDATETIME())
)
  AND o.RetryCount < @MaxRetryCount
  AND o.RowVersion < MIN_ACTIVE_ROWVERSION()
ORDER BY o.EventDateTimeUtc, o.EventOrdinal;
```

The `@TotalPartitions` parameter is no longer needed in FetchBatch. It's still used by rebalance and orphan sweep queries for fair-share calculation.

### Other queries — no changes

DeletePublished, IncrementRetryCount, DeadLetter, Heartbeat, Rebalance, OrphanSweep, SweepDeadLetters — all remain unchanged. They either operate by SequenceNumber (PK seek) or on the OutboxPartitions table directly.

## PostgreSQL — No Changes

PostgreSQL keeps its current design:
- Hash computed at query time: `(hashtext(partition_key) & 2147483647) % @total_partitions`
- Partition count is dynamic — determined by rows in `outbox_partitions`
- Changing partition count is a data-only operation (INSERT/DELETE rows, no schema change, no downtime)
- Poll latency is already 3-5ms

## Partition Count Asymmetry

| Aspect | SQL Server | PostgreSQL |
|--------|-----------|------------|
| Default partition count | 128 | 64 |
| Partition hash | Precomputed at INSERT, persisted column | Computed at query time |
| Index strategy | Seek by PartitionId (leading key) | Scan with runtime hash |
| Change partition count | ALTER TABLE + index rebuild + reseed (downtime required) | INSERT/DELETE rows in outbox_partitions (no downtime) |
| Max concurrent publishers | 128 (1 per partition) | Configurable at runtime |

## Application Code

The application uses partition count in two places:

1. **`IOutboxStore.GetTotalPartitionsAsync`** — Counts rows in `outbox_partitions`. Cached 60s. Used by rebalance/orphan sweep fair-share calculation. Both stores. **No change needed** — still counts rows from the table.

2. **`OutboxPublisherService.ComputeWorkerIndex`** — Uses `totalPartitions` for in-process worker distribution (`hash % totalPartitions % workerCount`). Uses `string.GetHashCode()`, NOT the SQL hash. **No change needed** — this is process-local, doesn't need to match the DB hash.

3. **`SqlServerOutboxStore.FetchBatchAsync`** — Currently passes `TotalPartitions` as a query parameter. **Remove this parameter** — the new query doesn't use it.

## Runbook: Changing SQL Server Partition Count

### When to change

- Scaling beyond 128 concurrent publishers (one publisher per partition)
- Rebalancing hotspots (some partitions get more traffic than others)

### Prerequisites

- All publishers must be stopped
- Outbox should ideally be drained (empty) to avoid re-hashing existing messages

### Procedure

```sql
-- 1. Stop all publishers first!

-- 2. Drop the index (depends on the computed column)
DROP INDEX IX_Outbox_Pending ON dbo.Outbox;

-- 3. Drop and recreate the computed column with new modulus
ALTER TABLE dbo.Outbox DROP COLUMN PartitionId;
ALTER TABLE dbo.Outbox ADD PartitionId AS 
    (ABS(CAST(CHECKSUM(PartitionKey) AS BIGINT)) % <NEW_COUNT>) PERSISTED;

-- 4. Recreate the index
CREATE NONCLUSTERED INDEX IX_Outbox_Pending 
ON dbo.Outbox (PartitionId, EventDateTimeUtc, EventOrdinal)
INCLUDE (SequenceNumber, TopicName, PartitionKey, EventType, Headers, Payload, 
         PayloadContentType, RetryCount, CreatedAtUtc, RowVersion);

-- 5. Reseed partitions
DELETE FROM dbo.OutboxPartitions WHERE OutboxTableName = 'Outbox';
INSERT INTO dbo.OutboxPartitions (OutboxTableName, PartitionId)
SELECT 'Outbox', value FROM GENERATE_SERIES(0, <NEW_COUNT> - 1);

-- 6. Start publishers — rebalance will distribute the new partition set
```

### Ordering safety

If the outbox is not empty when changing partition count, existing messages will be re-hashed to potentially different partitions. This is safe because:

- Partition ownership prevents two publishers from processing the same partition simultaneously
- `ORDER BY EventDateTimeUtc, EventOrdinal` preserves ordering within each partition
- Messages may move between partitions, but within any given partition, ordering is preserved
- The version ceiling filter (`RowVersion < MIN_ACTIVE_ROWVERSION()`) still prevents out-of-order publishing from concurrent transactions

The only risk: if a message was partially processed (fetched but not deleted) before the change, it could be re-assigned to a different partition and processed by a different publisher after restart. At-least-once semantics handle this — consumers must be idempotent.

## Files Changed

- `src/Outbox.SqlServer/db_scripts/install.sql` — computed column, new index, 128 partitions
- `src/Outbox.SqlServer/SqlServerQueries.cs` — FetchBatch query rewrite
- `src/Outbox.SqlServer/SqlServerOutboxStore.cs` — remove TotalPartitions param from FetchBatch
- `docs/known-limitations.md` — partition count asymmetry between stores
- `docs/production-runbook.md` — partition count change procedure for SQL Server
- `docs/sqlserver-eventhub-publisher-reference.md` — updated query and filter table
- `docs/architecture.md` — note precomputed column
- `docs/publisher-flow.md` — updated partition hashing section

## Performance Impact

Validated on SQL Server 2025 x86 with 10K messages:

| Metric | Before | After |
|--------|--------|-------|
| FetchBatch execution plan | Clustered Index Scan (full table) | Index Seek (by PartitionId) |
| FetchBatch avg latency | ~130ms | ~7ms |
| Missing index suggestions | 1 (71.68% impact) | 0 |
| IX_Outbox_Pending seeks | 0 | Used on every fetch |
