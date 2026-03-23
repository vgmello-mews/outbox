using System.Data;
using System.Data.Common;
using System.Text.Json;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;
using Outbox.Core.Abstractions;
using Outbox.Core.Models;
using Outbox.Core.Options;

namespace Outbox.SqlServer;

/// <summary>
/// SQL Server implementation of <see cref="IOutboxStore"/>.
/// </summary>
public sealed class SqlServerOutboxStore : IOutboxStore
{
    private const string ParamProducerId = "@ProducerId";
    private const string ParamPublisherId = "@PublisherId";

    private readonly SqlServerDbHelper _db;
    private readonly SqlServerStoreOptions _options;
    private readonly IOptionsMonitor<OutboxPublisherOptions> _publisherOptions;

    private readonly string _outboxTable;
    private readonly string _deadLetterTable;
    private readonly string _producersTable;
    private readonly string _partitionsTable;
    private readonly string _tvpType;

    private volatile int _cachedPartitionCount;
    private long _partitionCountRefreshedAtTicks;

    public SqlServerOutboxStore(
        Func<IServiceProvider, CancellationToken, Task<DbConnection>> connectionFactory,
        IServiceProvider serviceProvider,
        IOptions<SqlServerStoreOptions> options,
        IOptionsMonitor<OutboxPublisherOptions> publisherOptions)
    {
        _options = options.Value;
        _publisherOptions = publisherOptions;
        _db = new SqlServerDbHelper(connectionFactory, serviceProvider, _options);
        var s = _options.SchemaName;
        var p = _options.TablePrefix;
        _outboxTable = $"{s}.{p}Outbox";
        _deadLetterTable = $"{s}.{p}OutboxDeadLetter";
        _producersTable = $"{s}.{p}OutboxProducers";
        _partitionsTable = $"{s}.{p}OutboxPartitions";
        _tvpType = $"{s}.{p}SequenceNumberList";
    }

    // -------------------------------------------------------------------------
    // Producer registration
    // -------------------------------------------------------------------------

    public async Task<string> RegisterProducerAsync(CancellationToken ct)
    {
        var producerId = $"{Environment.MachineName}:{Environment.ProcessId}:{Guid.NewGuid():N}";
        var hostName = Environment.MachineName;

        var sql = $"""
            MERGE {_producersTable} WITH (HOLDLOCK) AS target
            USING (SELECT @ProducerId AS ProducerId, @HostName AS HostName) AS source
                ON target.ProducerId = source.ProducerId
            WHEN MATCHED THEN
                UPDATE SET LastHeartbeatUtc = SYSUTCDATETIME(),
                           HostName         = source.HostName
            WHEN NOT MATCHED THEN
                INSERT (ProducerId, RegisteredAtUtc, LastHeartbeatUtc, HostName)
                VALUES (source.ProducerId, SYSUTCDATETIME(), SYSUTCDATETIME(), source.HostName);
            """;

        await _db.ExecuteWithRetryAsync(async (conn, cancel) =>
        {
            await using var cmd = (SqlCommand)conn.CreateCommand();
            cmd.CommandText = sql;
            cmd.CommandTimeout = _options.CommandTimeoutSeconds;
            cmd.Parameters.AddWithValue(ParamProducerId, producerId);
            cmd.Parameters.AddWithValue("@HostName", hostName);
            await cmd.ExecuteNonQueryAsync(cancel).ConfigureAwait(false);
        }, ct).ConfigureAwait(false);

        return producerId;
    }

    public async Task UnregisterProducerAsync(string producerId, CancellationToken ct)
    {
        var sql = $"""
            UPDATE {_partitionsTable}
            SET    OwnerProducerId = NULL,
                   OwnedSinceUtc  = NULL,
                   GraceExpiresUtc = NULL
            WHERE  OwnerProducerId = @ProducerId;

            DELETE FROM {_producersTable}
            WHERE  ProducerId = @ProducerId;
            """;

        await _db.ExecuteWithRetryAsync(async (conn, cancel) =>
        {
            await using var tx = (SqlTransaction)await conn.BeginTransactionAsync(cancel).ConfigureAwait(false);
            await using var cmd = (SqlCommand)conn.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = sql;
            cmd.CommandTimeout = _options.CommandTimeoutSeconds;
            cmd.Parameters.AddWithValue(ParamProducerId, producerId);
            await cmd.ExecuteNonQueryAsync(cancel).ConfigureAwait(false);
            await tx.CommitAsync(cancel).ConfigureAwait(false);
        }, ct).ConfigureAwait(false);
    }

    // -------------------------------------------------------------------------
    // Lease batch (unified poll)
    // -------------------------------------------------------------------------

    public async Task<IReadOnlyList<OutboxMessage>> LeaseBatchAsync(
        string producerId, int batchSize, int leaseDurationSeconds,
        int maxRetryCount, CancellationToken ct)
    {
        int totalPartitions = await GetCachedPartitionCountAsync(ct).ConfigureAwait(false);
        if (totalPartitions == 0)
            return Array.Empty<OutboxMessage>();

        var sql = $"""
            WITH Batch AS
            (
                SELECT TOP (@BatchSize)
                    o.SequenceNumber,
                    o.TopicName,
                    o.PartitionKey,
                    o.EventType,
                    o.Headers,
                    o.Payload,
                    o.PayloadContentType,
                    o.EventDateTimeUtc,
                    o.EventOrdinal,
                    o.LeasedUntilUtc,
                    o.LeaseOwner,
                    o.RetryCount,
                    o.CreatedAtUtc
                FROM {_outboxTable} o WITH (ROWLOCK, READPAST)
                INNER JOIN {_partitionsTable} op
                    ON  op.OwnerProducerId = @PublisherId
                    AND (op.GraceExpiresUtc IS NULL OR op.GraceExpiresUtc < SYSUTCDATETIME())
                    AND (ABS(CAST(CHECKSUM(o.PartitionKey) AS BIGINT)) % @TotalPartitions) = op.PartitionId
                WHERE (o.LeasedUntilUtc IS NULL OR o.LeasedUntilUtc < SYSUTCDATETIME())
                  AND o.RetryCount < @MaxRetryCount
                ORDER BY o.EventDateTimeUtc, o.EventOrdinal
            )
            UPDATE Batch
            SET    LeasedUntilUtc = DATEADD(SECOND, @LeaseDurationSeconds, SYSUTCDATETIME()),
                   LeaseOwner     = @PublisherId,
                   RetryCount     = CASE WHEN LeasedUntilUtc IS NOT NULL
                                         THEN RetryCount + 1
                                         ELSE RetryCount END
            OUTPUT inserted.SequenceNumber,
                   inserted.TopicName,
                   inserted.PartitionKey,
                   inserted.EventType,
                   inserted.Headers,
                   inserted.Payload,
                   inserted.PayloadContentType,
                   inserted.EventDateTimeUtc,
                   inserted.EventOrdinal,
                   inserted.RetryCount,
                   inserted.CreatedAtUtc;
            """;

        var rows = new List<OutboxMessage>();

        await _db.ExecuteWithRetryAsync(async (conn, cancel) =>
        {
            rows.Clear();
            await using var cmd = (SqlCommand)conn.CreateCommand();
            cmd.CommandText = sql;
            cmd.CommandTimeout = _options.CommandTimeoutSeconds;
            cmd.Parameters.AddWithValue("@BatchSize", batchSize);
            cmd.Parameters.AddWithValue("@LeaseDurationSeconds", leaseDurationSeconds);
            cmd.Parameters.AddWithValue(ParamPublisherId, producerId);
            cmd.Parameters.AddWithValue("@TotalPartitions", totalPartitions);
            cmd.Parameters.AddWithValue("@MaxRetryCount", maxRetryCount);

            await using var reader = await cmd.ExecuteReaderAsync(cancel).ConfigureAwait(false);
            while (await reader.ReadAsync(cancel).ConfigureAwait(false))
            {
                rows.Add(new OutboxMessage(
                    SequenceNumber: reader.GetInt64(0),
                    TopicName: reader.GetString(1),
                    PartitionKey: reader.GetString(2),
                    EventType: reader.GetString(3),
                    Headers: await reader.IsDBNullAsync(4, cancel).ConfigureAwait(false)
                        ? null
                        : JsonSerializer.Deserialize<Dictionary<string, string>>(reader.GetString(4)),
                    Payload: await reader.GetFieldValueAsync<byte[]>(5, cancel).ConfigureAwait(false),
                    PayloadContentType: reader.GetString(6),
                    EventDateTimeUtc: new DateTimeOffset(DateTime.SpecifyKind(reader.GetDateTime(7), DateTimeKind.Utc)),
                    EventOrdinal: reader.GetInt16(8),
                    RetryCount: reader.GetInt32(9),
                    CreatedAtUtc: new DateTimeOffset(DateTime.SpecifyKind(reader.GetDateTime(10), DateTimeKind.Utc))));
            }
        }, ct).ConfigureAwait(false);

        return rows;
    }

    // -------------------------------------------------------------------------
    // Delete / Release / Dead-letter
    // -------------------------------------------------------------------------

    public async Task DeletePublishedAsync(
        string producerId, IReadOnlyList<long> sequenceNumbers, CancellationToken ct)
    {
        if (sequenceNumbers.Count == 0) return;

        var sql = $"""
            DELETE o
            FROM   {_outboxTable} o
            INNER JOIN @PublishedIds p ON o.SequenceNumber = p.SequenceNumber
            WHERE  o.LeaseOwner = @PublisherId;
            """;

        await _db.ExecuteWithRetryAsync(async (conn, cancel) =>
        {
            await using var cmd = (SqlCommand)conn.CreateCommand();
            cmd.CommandText = sql;
            cmd.CommandTimeout = _options.CommandTimeoutSeconds;
            cmd.Parameters.AddWithValue(ParamPublisherId, producerId);
            SqlServerDbHelper.AddSequenceNumberTvp(cmd, "@PublishedIds", sequenceNumbers, _tvpType);
            await cmd.ExecuteNonQueryAsync(cancel).ConfigureAwait(false);
        }, ct).ConfigureAwait(false);
    }

    public async Task ReleaseLeaseAsync(
        string producerId, IReadOnlyList<long> sequenceNumbers,
        bool incrementRetry, CancellationToken ct)
    {
        if (sequenceNumbers.Count == 0) return;

        var sql = incrementRetry
            ? $"""
            UPDATE o
            SET    o.LeasedUntilUtc = NULL,
                   o.LeaseOwner     = NULL,
                   o.RetryCount     = o.RetryCount + 1
            FROM   {_outboxTable} o
            INNER JOIN @Ids p ON o.SequenceNumber = p.SequenceNumber
            WHERE  o.LeaseOwner = @PublisherId;
            """
            : $"""
            UPDATE o
            SET    o.LeasedUntilUtc = NULL,
                   o.LeaseOwner     = NULL
            FROM   {_outboxTable} o
            INNER JOIN @Ids p ON o.SequenceNumber = p.SequenceNumber
            WHERE  o.LeaseOwner = @PublisherId;
            """;

        await _db.ExecuteWithRetryAsync(async (conn, cancel) =>
        {
            await using var cmd = (SqlCommand)conn.CreateCommand();
            cmd.CommandText = sql;
            cmd.CommandTimeout = _options.CommandTimeoutSeconds;
            cmd.Parameters.AddWithValue(ParamPublisherId, producerId);
            SqlServerDbHelper.AddSequenceNumberTvp(cmd, "@Ids", sequenceNumbers, _tvpType);
            await cmd.ExecuteNonQueryAsync(cancel).ConfigureAwait(false);
        }, ct).ConfigureAwait(false);
    }

    public async Task DeadLetterAsync(
        string producerId, IReadOnlyList<long> sequenceNumbers, string? lastError, CancellationToken ct)
    {
        if (sequenceNumbers.Count == 0) return;

        // Dead-letter specific sequence numbers (by ID list), not by RetryCount threshold.
        // Use DELETE...OUTPUT INTO for atomicity. LeaseOwner guard prevents zombie publisher race.
        var sql = $"""
            DELETE o
            OUTPUT deleted.SequenceNumber, deleted.TopicName, deleted.PartitionKey,
                   deleted.EventType, deleted.Headers, deleted.Payload,
                   deleted.PayloadContentType,
                   deleted.CreatedAtUtc, deleted.RetryCount,
                   deleted.EventDateTimeUtc, deleted.EventOrdinal,
                   SYSUTCDATETIME(), @LastError
            INTO {_deadLetterTable}(SequenceNumber, TopicName, PartitionKey, EventType,
                 Headers, Payload, PayloadContentType,
                 CreatedAtUtc, RetryCount,
                 EventDateTimeUtc, EventOrdinal,
                 DeadLetteredAtUtc, LastError)
            FROM {_outboxTable} o
            INNER JOIN @Ids p ON o.SequenceNumber = p.SequenceNumber
            WHERE o.LeaseOwner = @PublisherId;
            """;

        await _db.ExecuteWithRetryAsync(async (conn, cancel) =>
        {
            await using var cmd = (SqlCommand)conn.CreateCommand();
            cmd.CommandText = sql;
            cmd.CommandTimeout = _options.CommandTimeoutSeconds;
            cmd.Parameters.AddWithValue(ParamPublisherId, producerId);
            cmd.Parameters.Add("@LastError", SqlDbType.NVarChar, 2000).Value =
                (object?)lastError ?? DBNull.Value;
            SqlServerDbHelper.AddSequenceNumberTvp(cmd, "@Ids", sequenceNumbers, _tvpType);
            await cmd.ExecuteNonQueryAsync(cancel).ConfigureAwait(false);
        }, ct).ConfigureAwait(false);
    }

    // -------------------------------------------------------------------------
    // Heartbeat
    // -------------------------------------------------------------------------

    public async Task HeartbeatAsync(string producerId, CancellationToken ct)
    {
        var sql = $"""
            UPDATE {_producersTable}
            SET    LastHeartbeatUtc = SYSUTCDATETIME()
            WHERE  ProducerId = @ProducerId;

            UPDATE {_partitionsTable}
            SET    GraceExpiresUtc = NULL
            WHERE  OwnerProducerId = @ProducerId
              AND  GraceExpiresUtc IS NOT NULL;
            """;

        await _db.ExecuteWithRetryAsync(async (conn, cancel) =>
        {
            await using var tx = (SqlTransaction)await conn.BeginTransactionAsync(cancel).ConfigureAwait(false);
            await using var cmd = (SqlCommand)conn.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = sql;
            cmd.CommandTimeout = _options.CommandTimeoutSeconds;
            cmd.Parameters.AddWithValue(ParamProducerId, producerId);
            await cmd.ExecuteNonQueryAsync(cancel).ConfigureAwait(false);
            await tx.CommitAsync(cancel).ConfigureAwait(false);
        }, ct).ConfigureAwait(false);
    }

    // -------------------------------------------------------------------------
    // Partition management
    // -------------------------------------------------------------------------

    private async Task<int> GetCachedPartitionCountAsync(CancellationToken ct)
    {
        const long refreshIntervalMs = 60_000; // 60s
        long now = Environment.TickCount64;
        int cached = _cachedPartitionCount;

        if (cached > 0 && (now - Volatile.Read(ref _partitionCountRefreshedAtTicks)) < refreshIntervalMs)
            return cached;

        int fresh = await GetTotalPartitionsAsync(ct).ConfigureAwait(false);
        _cachedPartitionCount = fresh;
        Volatile.Write(ref _partitionCountRefreshedAtTicks, now);
        return fresh;
    }

    public async Task<int> GetTotalPartitionsAsync(CancellationToken ct)
    {
        var sql = $"SELECT COUNT(*) FROM {_partitionsTable};";

        int result = 0;
        await _db.ExecuteWithRetryAsync(async (conn, cancel) =>
        {
            await using var cmd = (SqlCommand)conn.CreateCommand();
            cmd.CommandText = sql;
            cmd.CommandTimeout = _options.CommandTimeoutSeconds;
            var scalar = await cmd.ExecuteScalarAsync(cancel).ConfigureAwait(false);
            result = Convert.ToInt32(scalar);
        }, ct).ConfigureAwait(false);

        return result;
    }

    public async Task<IReadOnlyList<int>> GetOwnedPartitionsAsync(string producerId, CancellationToken ct)
    {
        var sql = $"""
            SELECT PartitionId
            FROM   {_partitionsTable}
            WHERE  OwnerProducerId = @ProducerId;
            """;

        var partitions = new List<int>();
        await _db.ExecuteWithRetryAsync(async (conn, cancel) =>
        {
            partitions.Clear();
            await using var cmd = (SqlCommand)conn.CreateCommand();
            cmd.CommandText = sql;
            cmd.CommandTimeout = _options.CommandTimeoutSeconds;
            cmd.Parameters.AddWithValue(ParamProducerId, producerId);
            await using var reader = await cmd.ExecuteReaderAsync(cancel).ConfigureAwait(false);
            while (await reader.ReadAsync(cancel).ConfigureAwait(false))
                partitions.Add(reader.GetInt32(0));
        }, ct).ConfigureAwait(false);

        return partitions;
    }

    public async Task RebalanceAsync(string producerId, CancellationToken ct)
    {
        // Snapshot options once to ensure consistent values across all steps within one transaction.
        var opts = _publisherOptions.CurrentValue;
        var sql = $"""
            DECLARE @TotalPartitions   INT;
            DECLARE @ActiveProducers   INT;
            DECLARE @FairShare         INT;
            DECLARE @CurrentlyOwned    INT;
            DECLARE @ToAcquire         INT;

            SELECT @TotalPartitions = COUNT(*) FROM {_partitionsTable};

            SELECT @ActiveProducers = COUNT(*)
            FROM {_producersTable}
            WHERE LastHeartbeatUtc >= DATEADD(SECOND, -@HeartbeatTimeoutSeconds, SYSUTCDATETIME());

            SET @FairShare = CEILING(CAST(@TotalPartitions AS FLOAT) / NULLIF(@ActiveProducers, 0));

            SELECT @CurrentlyOwned = COUNT(*)
            FROM {_partitionsTable}
            WHERE OwnerProducerId = @ProducerId;

            SET @ToAcquire = @FairShare - @CurrentlyOwned;

            IF @ToAcquire > 0
            BEGIN
                UPDATE {_partitionsTable}
                SET    GraceExpiresUtc = DATEADD(SECOND, @PartitionGracePeriodSeconds, SYSUTCDATETIME())
                WHERE  OwnerProducerId <> @ProducerId
                  AND  OwnerProducerId IS NOT NULL
                  AND  GraceExpiresUtc IS NULL
                  AND  OwnerProducerId NOT IN
                       (
                           SELECT ProducerId
                           FROM   {_producersTable}
                           WHERE  LastHeartbeatUtc >= DATEADD(SECOND, -@HeartbeatTimeoutSeconds, SYSUTCDATETIME())
                       );

                UPDATE op
                SET    OwnerProducerId = @ProducerId,
                       OwnedSinceUtc   = SYSUTCDATETIME(),
                       GraceExpiresUtc = NULL
                FROM   {_partitionsTable} op WITH (UPDLOCK, READPAST)
                WHERE  op.PartitionId IN (
                           SELECT TOP (@ToAcquire) PartitionId
                           FROM   {_partitionsTable} WITH (UPDLOCK, READPAST)
                           WHERE  (OwnerProducerId IS NULL
                                   OR GraceExpiresUtc < SYSUTCDATETIME())
                           ORDER BY PartitionId
                       );
            END;

            SELECT @CurrentlyOwned = COUNT(*)
            FROM {_partitionsTable}
            WHERE OwnerProducerId = @ProducerId;

            IF @CurrentlyOwned > @FairShare
            BEGIN
                DECLARE @ToRelease INT = @CurrentlyOwned - @FairShare;

                UPDATE op
                SET    OwnerProducerId = NULL,
                       OwnedSinceUtc  = NULL,
                       GraceExpiresUtc = NULL
                FROM   {_partitionsTable} op
                WHERE  op.PartitionId IN (
                           SELECT TOP (@ToRelease) PartitionId
                           FROM   {_partitionsTable}
                           WHERE  OwnerProducerId = @ProducerId
                           ORDER BY PartitionId DESC
                       );
            END;
            """;

        await _db.ExecuteWithRetryAsync(async (conn, cancel) =>
        {
            await using var tx = (SqlTransaction)await conn.BeginTransactionAsync(cancel).ConfigureAwait(false);
            await using var cmd = (SqlCommand)conn.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = sql;
            cmd.CommandTimeout = _options.CommandTimeoutSeconds;
            cmd.Parameters.AddWithValue(ParamProducerId, producerId);
            cmd.Parameters.AddWithValue("@HeartbeatTimeoutSeconds", opts.HeartbeatTimeoutSeconds);
            cmd.Parameters.AddWithValue("@PartitionGracePeriodSeconds", opts.PartitionGracePeriodSeconds);
            await cmd.ExecuteNonQueryAsync(cancel).ConfigureAwait(false);
            await tx.CommitAsync(cancel).ConfigureAwait(false);
        }, ct).ConfigureAwait(false);
    }

    public async Task ClaimOrphanPartitionsAsync(string producerId, CancellationToken ct)
    {
        var opts = _publisherOptions.CurrentValue;
        var sql = $"""
            DECLARE @TotalPartitions   INT;
            DECLARE @ActiveProducers   INT;
            DECLARE @FairShare         INT;
            DECLARE @CurrentlyOwned    INT;
            DECLARE @ToAcquire         INT;

            SELECT @TotalPartitions = COUNT(*) FROM {_partitionsTable};

            SELECT @ActiveProducers = COUNT(*)
            FROM {_producersTable}
            WHERE LastHeartbeatUtc >= DATEADD(SECOND, -@HeartbeatTimeoutSeconds, SYSUTCDATETIME());

            SET @FairShare = CEILING(CAST(@TotalPartitions AS FLOAT) / NULLIF(@ActiveProducers, 0));

            SELECT @CurrentlyOwned = COUNT(*)
            FROM {_partitionsTable}
            WHERE OwnerProducerId = @ProducerId;

            SET @ToAcquire = @FairShare - @CurrentlyOwned;

            IF @ToAcquire > 0
            BEGIN
                UPDATE op
                SET    OwnerProducerId = @ProducerId,
                       OwnedSinceUtc   = SYSUTCDATETIME(),
                       GraceExpiresUtc = NULL
                FROM   {_partitionsTable} op WITH (UPDLOCK, READPAST)
                WHERE  op.PartitionId IN (
                           SELECT TOP (@ToAcquire) PartitionId
                           FROM   {_partitionsTable} WITH (UPDLOCK, READPAST)
                           WHERE  OwnerProducerId IS NULL
                           ORDER BY PartitionId
                       );
            END;
            """;

        await _db.ExecuteWithRetryAsync(async (conn, cancel) =>
        {
            await using var tx = (SqlTransaction)await conn.BeginTransactionAsync(cancel).ConfigureAwait(false);
            await using var cmd = (SqlCommand)conn.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = sql;
            cmd.CommandTimeout = _options.CommandTimeoutSeconds;
            cmd.Parameters.AddWithValue(ParamProducerId, producerId);
            cmd.Parameters.AddWithValue("@HeartbeatTimeoutSeconds", opts.HeartbeatTimeoutSeconds);
            await cmd.ExecuteNonQueryAsync(cancel).ConfigureAwait(false);
            await tx.CommitAsync(cancel).ConfigureAwait(false);
        }, ct).ConfigureAwait(false);
    }

    // -------------------------------------------------------------------------
    // Dead-letter sweep
    // -------------------------------------------------------------------------

    public async Task SweepDeadLettersAsync(int maxRetryCount, CancellationToken ct)
    {
        var opts = _publisherOptions.CurrentValue;
        // Only sweep messages whose LeaseOwner is NULL (explicitly released),
        // whose owner is a dead producer (stale heartbeat), or whose lease has been
        // expired for longer than LeaseDurationSeconds (publisher had ample time to
        // delete but didn't — covers the case where DeadLetterAsync itself failed).
        var sql = $"""
            DELETE o
            OUTPUT deleted.SequenceNumber, deleted.TopicName, deleted.PartitionKey,
                   deleted.EventType, deleted.Headers, deleted.Payload,
                   deleted.PayloadContentType,
                   deleted.CreatedAtUtc, deleted.RetryCount,
                   deleted.EventDateTimeUtc, deleted.EventOrdinal,
                   SYSUTCDATETIME(), @LastError
            INTO {_deadLetterTable}(SequenceNumber, TopicName, PartitionKey, EventType,
                 Headers, Payload, PayloadContentType,
                 CreatedAtUtc, RetryCount,
                 EventDateTimeUtc, EventOrdinal,
                 DeadLetteredAtUtc, LastError)
            FROM {_outboxTable} o WITH (ROWLOCK, READPAST)
            WHERE o.RetryCount >= @MaxRetryCount
              AND (o.LeasedUntilUtc IS NULL OR o.LeasedUntilUtc < SYSUTCDATETIME())
              AND (o.LeaseOwner IS NULL
                   OR o.LeasedUntilUtc < DATEADD(SECOND, -@LeaseDurationSeconds, SYSUTCDATETIME())
                   OR o.LeaseOwner NOT IN (
                       SELECT ProducerId
                       FROM {_producersTable}
                       WHERE LastHeartbeatUtc >= DATEADD(SECOND, -@HeartbeatTimeoutSeconds, SYSUTCDATETIME())
                   ));
            """;

        await _db.ExecuteWithRetryAsync(async (conn, cancel) =>
        {
            await using var cmd = (SqlCommand)conn.CreateCommand();
            cmd.CommandText = sql;
            cmd.CommandTimeout = _options.CommandTimeoutSeconds;
            cmd.Parameters.AddWithValue("@MaxRetryCount", maxRetryCount);
            cmd.Parameters.AddWithValue("@HeartbeatTimeoutSeconds", opts.HeartbeatTimeoutSeconds);
            cmd.Parameters.AddWithValue("@LeaseDurationSeconds", opts.LeaseDurationSeconds);
            cmd.Parameters.Add("@LastError", SqlDbType.NVarChar, 2000).Value = "Max retry count exceeded (background sweep)";
            await cmd.ExecuteNonQueryAsync(cancel).ConfigureAwait(false);
        }, ct).ConfigureAwait(false);
    }

    public async Task<long> GetPendingCountAsync(CancellationToken ct)
    {
        var sql = $@"
            SELECT COUNT_BIG(*) FROM {_outboxTable}
            WHERE  LeasedUntilUtc IS NULL OR LeasedUntilUtc < SYSUTCDATETIME();";

        long result = 0;
        await _db.ExecuteWithRetryAsync(async (conn, cancel) =>
        {
            await using var cmd = (SqlCommand)conn.CreateCommand();
            cmd.CommandText = sql;
            cmd.CommandTimeout = _options.CommandTimeoutSeconds;
            var scalar = await cmd.ExecuteScalarAsync(cancel).ConfigureAwait(false);
            result = Convert.ToInt64(scalar);
        }, ct).ConfigureAwait(false);

        return result;
    }

}
