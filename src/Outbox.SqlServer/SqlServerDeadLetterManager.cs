using System.Data.Common;
using System.Text.Json;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;
using Outbox.Core.Abstractions;
using Outbox.Core.Models;

namespace Outbox.SqlServer;

/// <summary>
/// SQL Server implementation of <see cref="IDeadLetterManager"/>.
/// </summary>
public sealed class SqlServerDeadLetterManager : IDeadLetterManager
{
    private readonly SqlServerDbHelper _db;
    private readonly SqlServerStoreOptions _options;

    private readonly string _outboxTable;
    private readonly string _deadLetterTable;
    private readonly string _tvpType;

    public SqlServerDeadLetterManager(
        Func<IServiceProvider, CancellationToken, Task<DbConnection>> connectionFactory,
        IServiceProvider serviceProvider,
        IOptions<SqlServerStoreOptions> options)
    {
        _options = options.Value;
        _db = new SqlServerDbHelper(connectionFactory, serviceProvider, _options);
        var s = _options.SchemaName;
        var p = _options.TablePrefix;
        _outboxTable = $"{s}.{p}Outbox";
        _deadLetterTable = $"{s}.{p}OutboxDeadLetter";
        _tvpType = $"{s}.{p}SequenceNumberList";
    }

    // -------------------------------------------------------------------------
    // Get
    // -------------------------------------------------------------------------

    public async Task<IReadOnlyList<DeadLetteredMessage>> GetAsync(
        int limit, int offset, CancellationToken ct)
    {
        var sql = $"""
            SELECT
                DeadLetterSeq,
                SequenceNumber,
                TopicName,
                PartitionKey,
                EventType,
                Headers,
                Payload,
                PayloadContentType,
                EventDateTimeUtc,
                EventOrdinal,
                RetryCount,
                CreatedAtUtc,
                DeadLetteredAtUtc,
                LastError
            FROM {_deadLetterTable}
            ORDER BY DeadLetterSeq
            OFFSET @Offset ROWS
            FETCH NEXT @Limit ROWS ONLY;
            """;

        var messages = new List<DeadLetteredMessage>();

        await _db.ExecuteWithRetryAsync(async (conn, cancel) =>
        {
            messages.Clear();
            await using var cmd = (SqlCommand)conn.CreateCommand();
            cmd.CommandText = sql;
            cmd.CommandTimeout = _options.CommandTimeoutSeconds;
            cmd.Parameters.AddWithValue("@Offset", offset);
            cmd.Parameters.AddWithValue("@Limit", limit);

            await using var reader = await cmd.ExecuteReaderAsync(cancel).ConfigureAwait(false);
            while (await reader.ReadAsync(cancel).ConfigureAwait(false))
            {
                messages.Add(new DeadLetteredMessage(
                    DeadLetterSeq: reader.GetInt64(0),
                    SequenceNumber: reader.GetInt64(1),
                    TopicName: reader.GetString(2),
                    PartitionKey: reader.GetString(3),
                    EventType: reader.GetString(4),
                    Headers: await reader.IsDBNullAsync(5, cancel).ConfigureAwait(false)
                        ? null
                        : JsonSerializer.Deserialize<Dictionary<string, string>>(reader.GetString(5)),
                    Payload: await reader.GetFieldValueAsync<byte[]>(6, cancel).ConfigureAwait(false),
                    PayloadContentType: reader.GetString(7),
                    EventDateTimeUtc: new DateTimeOffset(DateTime.SpecifyKind(reader.GetDateTime(8), DateTimeKind.Utc)),
                    EventOrdinal: reader.GetInt16(9),
                    RetryCount: reader.GetInt32(10),
                    CreatedAtUtc: new DateTimeOffset(DateTime.SpecifyKind(reader.GetDateTime(11), DateTimeKind.Utc)),
                    DeadLetteredAtUtc: new DateTimeOffset(DateTime.SpecifyKind(reader.GetDateTime(12), DateTimeKind.Utc)),
                    LastError: await reader.IsDBNullAsync(13, cancel).ConfigureAwait(false) ? null : reader.GetString(13)));
            }
        }, ct).ConfigureAwait(false);

        return messages;
    }

    // -------------------------------------------------------------------------
    // Replay — move rows back to Outbox for reprocessing
    // -------------------------------------------------------------------------

    public async Task ReplayAsync(IReadOnlyList<long> deadLetterSeqs, CancellationToken ct)
    {
        if (deadLetterSeqs.Count == 0) return;

        // Atomic DELETE...OUTPUT INTO using DeadLetterSeq (PK) for precise targeting.
        var sql = $"""
            DELETE dl
            OUTPUT deleted.TopicName, deleted.PartitionKey, deleted.EventType,
                   deleted.Headers, deleted.Payload,
                   deleted.PayloadContentType,
                   deleted.CreatedAtUtc,
                   deleted.EventDateTimeUtc, deleted.EventOrdinal,
                   0, NULL, NULL
            INTO {_outboxTable}(TopicName, PartitionKey, EventType,
                 Headers, Payload, PayloadContentType,
                 CreatedAtUtc,
                 EventDateTimeUtc, EventOrdinal,
                 RetryCount, LeasedUntilUtc, LeaseOwner)
            FROM {_deadLetterTable} dl
            INNER JOIN @Ids p ON dl.DeadLetterSeq = p.SequenceNumber;
            """;

        await _db.ExecuteWithRetryAsync(async (conn, cancel) =>
        {
            await using var cmd = (SqlCommand)conn.CreateCommand();
            cmd.CommandText = sql;
            cmd.CommandTimeout = _options.CommandTimeoutSeconds;
            SqlServerDbHelper.AddSequenceNumberTvp(cmd, "@Ids", deadLetterSeqs, _tvpType);
            await cmd.ExecuteNonQueryAsync(cancel).ConfigureAwait(false);
        }, ct).ConfigureAwait(false);
    }

    // -------------------------------------------------------------------------
    // Purge
    // -------------------------------------------------------------------------

    public async Task PurgeAsync(IReadOnlyList<long> deadLetterSeqs, CancellationToken ct)
    {
        if (deadLetterSeqs.Count == 0) return;

        var sql = $"""
            DELETE dl
            FROM {_deadLetterTable} dl
            INNER JOIN @Ids p ON dl.DeadLetterSeq = p.SequenceNumber;
            """;

        await _db.ExecuteWithRetryAsync(async (conn, cancel) =>
        {
            await using var cmd = (SqlCommand)conn.CreateCommand();
            cmd.CommandText = sql;
            cmd.CommandTimeout = _options.CommandTimeoutSeconds;
            SqlServerDbHelper.AddSequenceNumberTvp(cmd, "@Ids", deadLetterSeqs, _tvpType);
            await cmd.ExecuteNonQueryAsync(cancel).ConfigureAwait(false);
        }, ct).ConfigureAwait(false);
    }

    public async Task PurgeAllAsync(CancellationToken ct)
    {
        var sql = $"DELETE FROM {_deadLetterTable};";

        await _db.ExecuteWithRetryAsync(async (conn, cancel) =>
        {
            await using var cmd = (SqlCommand)conn.CreateCommand();
            cmd.CommandText = sql;
            cmd.CommandTimeout = _options.CommandTimeoutSeconds;
            await cmd.ExecuteNonQueryAsync(cancel).ConfigureAwait(false);
        }, ct).ConfigureAwait(false);
    }

}
