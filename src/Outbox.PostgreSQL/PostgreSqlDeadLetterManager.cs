using System.Data.Common;
using System.Text.Json;
using Microsoft.Extensions.Options;
using Npgsql;
using NpgsqlTypes;
using Outbox.Core.Abstractions;
using Outbox.Core.Models;

namespace Outbox.PostgreSQL;

public sealed class PostgreSqlDeadLetterManager : IDeadLetterManager
{
    private readonly PostgreSqlDbHelper _db;
    private readonly PostgreSqlStoreOptions _options;
    private readonly string _outboxTable;
    private readonly string _deadLetterTable;

    public PostgreSqlDeadLetterManager(
        Func<IServiceProvider, CancellationToken, Task<DbConnection>> connectionFactory,
        IServiceProvider serviceProvider,
        IOptions<PostgreSqlStoreOptions> options)
    {
        _options = options.Value;
        var s = _options.SchemaName;
        var p = _options.TablePrefix;
        _outboxTable = $"{s}.{p}outbox";
        _deadLetterTable = $"{s}.{p}outbox_dead_letter";
        _db = new PostgreSqlDbHelper(connectionFactory, serviceProvider, _options);
    }

    public async Task<IReadOnlyList<DeadLetteredMessage>> GetAsync(
        int limit, int offset, CancellationToken ct)
    {
        var sql = $@"
SELECT dead_letter_seq, sequence_number, topic_name, partition_key, event_type, headers, payload,
       payload_content_type,
       event_datetime_utc, event_ordinal, retry_count, created_at_utc,
       dead_lettered_at_utc, last_error
FROM   {_deadLetterTable}
ORDER  BY dead_letter_seq
LIMIT  @limit OFFSET @offset;";

        var messages = new List<DeadLetteredMessage>();

        await _db.ExecuteWithRetryAsync(async (conn, token) =>
        {
            messages.Clear();
            await using var cmd = _db.CreateCommand(sql, conn);
            cmd.Parameters.AddWithValue("@limit", limit);
            cmd.Parameters.AddWithValue("@offset", offset);

            await using var reader = await cmd.ExecuteReaderAsync(token).ConfigureAwait(false);
            while (await reader.ReadAsync(token).ConfigureAwait(false))
            {
                messages.Add(new DeadLetteredMessage(
                    DeadLetterSeq: reader.GetInt64(0),
                    SequenceNumber: reader.GetInt64(1),
                    TopicName: reader.GetString(2),
                    PartitionKey: reader.GetString(3),
                    EventType: reader.GetString(4),
                    Headers: await reader.IsDBNullAsync(5, token).ConfigureAwait(false)
                        ? null
                        : JsonSerializer.Deserialize<Dictionary<string, string>>(reader.GetString(5)),
                    Payload: await reader.GetFieldValueAsync<byte[]>(6, token).ConfigureAwait(false),
                    PayloadContentType: reader.GetString(7),
                    EventDateTimeUtc: await reader.GetFieldValueAsync<DateTimeOffset>(8, token).ConfigureAwait(false),
                    EventOrdinal: reader.GetInt16(9),
                    RetryCount: reader.GetInt32(10),
                    CreatedAtUtc: await reader.GetFieldValueAsync<DateTimeOffset>(11, token).ConfigureAwait(false),
                    DeadLetteredAtUtc: await reader.GetFieldValueAsync<DateTimeOffset>(12, token).ConfigureAwait(false),
                    LastError: await reader.IsDBNullAsync(13, token).ConfigureAwait(false) ? null : reader.GetString(13)));
            }
        }, ct).ConfigureAwait(false);

        return messages;
    }

    public async Task ReplayAsync(IReadOnlyList<long> deadLetterSeqs, CancellationToken ct)
    {
        var sql = $@"
WITH replayed AS (
    DELETE FROM {_deadLetterTable}
    WHERE  dead_letter_seq = ANY(@ids)
    RETURNING sequence_number, topic_name, partition_key, event_type, headers, payload,
              payload_content_type,
              created_at_utc, event_datetime_utc, event_ordinal
)
INSERT INTO {_outboxTable}
    (topic_name, partition_key, event_type, headers, payload,
     payload_content_type,
     created_at_utc, event_datetime_utc, event_ordinal,
     leased_until_utc, lease_owner, retry_count)
SELECT topic_name, partition_key, event_type, headers, payload,
       payload_content_type,
       created_at_utc, event_datetime_utc, event_ordinal,
       NULL, NULL, 0
FROM replayed;";

        await _db.ExecuteWithRetryAsync(async (conn, token) =>
        {
            await using var cmd = _db.CreateCommand(sql, conn);
#pragma warning disable S3265 // NpgsqlDbType.Array requires bitwise OR by design
            cmd.Parameters.Add(new NpgsqlParameter("@ids", NpgsqlDbType.Array | NpgsqlDbType.Bigint)
#pragma warning restore S3265
            {
                Value = deadLetterSeqs.ToArray()
            });
            await cmd.ExecuteNonQueryAsync(token).ConfigureAwait(false);
        }, ct).ConfigureAwait(false);
    }

    public async Task PurgeAsync(IReadOnlyList<long> deadLetterSeqs, CancellationToken ct)
    {
        var sql = $@"
DELETE FROM {_deadLetterTable}
WHERE  dead_letter_seq = ANY(@ids);";

        await _db.ExecuteWithRetryAsync(async (conn, token) =>
        {
            await using var cmd = _db.CreateCommand(sql, conn);
#pragma warning disable S3265 // NpgsqlDbType.Array requires bitwise OR by design
            cmd.Parameters.Add(new NpgsqlParameter("@ids", NpgsqlDbType.Array | NpgsqlDbType.Bigint)
#pragma warning restore S3265
            {
                Value = deadLetterSeqs.ToArray()
            });
            await cmd.ExecuteNonQueryAsync(token).ConfigureAwait(false);
        }, ct).ConfigureAwait(false);
    }

    public async Task PurgeAllAsync(CancellationToken ct)
    {
        var sql = $"DELETE FROM {_deadLetterTable};";

        await _db.ExecuteWithRetryAsync(async (conn, token) =>
        {
            await using var cmd = _db.CreateCommand(sql, conn);
            await cmd.ExecuteNonQueryAsync(token).ConfigureAwait(false);
        }, ct).ConfigureAwait(false);
    }

}
