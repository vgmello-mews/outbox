using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Data.SqlClient;
using Npgsql;

var sqlServerConn = Environment.GetEnvironmentVariable("SQLSERVER_CONN");
var postgresConn = Environment.GetEnvironmentVariable("POSTGRES_CONN");
var redpandaBrokers = Environment.GetEnvironmentVariable("REDPANDA_BROKERS");

Console.WriteLine("DbInit starting...");

if (sqlServerConn is not null)
    await InitSqlServerAsync(sqlServerConn);

if (postgresConn is not null)
    await InitPostgresAsync(postgresConn);

if (redpandaBrokers is not null)
    await InitRedpandaAsync(redpandaBrokers);

Console.WriteLine("DbInit completed successfully.");

// ── SQL Server ────────────────────────────────────────────────────────────

static async Task InitSqlServerAsync(string connStr)
{
    Console.WriteLine("Initializing SQL Server...");

    var builder = new SqlConnectionStringBuilder(connStr);
    var dbName = string.IsNullOrWhiteSpace(builder.InitialCatalog) ? "outbox" : builder.InitialCatalog;

    var masterConn = new SqlConnectionStringBuilder(connStr)
    {
        InitialCatalog = "master"
    }.ConnectionString;

    await RetryAsync(async () =>
    {
        await using var conn = new SqlConnection(masterConn);
        await conn.OpenAsync();

        var dbExists = (int?)await new SqlCommand(
            $"SELECT COUNT(1) FROM sys.databases WHERE name = '{dbName}'", conn)
            .ExecuteScalarAsync();

        if (dbExists == 0)
        {
            Console.WriteLine($"Creating {dbName} database...");
            await new SqlCommand($"CREATE DATABASE [{dbName}]", conn).ExecuteNonQueryAsync();
        }
    }, "SQL Server connection");

    var dbConn = new SqlConnectionStringBuilder(connStr)
    {
        InitialCatalog = dbName
    }.ConnectionString;

    await RetryAsync(async () =>
    {
        await using var conn = new SqlConnection(dbConn);
        await conn.OpenAsync();

        var installSql = await File.ReadAllTextAsync("/db_scripts/sqlserver/install.sql");
        foreach (var batch in installSql.Split("\nGO", StringSplitOptions.RemoveEmptyEntries))
        {
            var trimmed = batch.Trim();
            if (!string.IsNullOrWhiteSpace(trimmed))
                await new SqlCommand(trimmed, conn).ExecuteNonQueryAsync();
        }

        Console.WriteLine("SQL Server: install.sql executed.");

        await new SqlCommand("DELETE FROM dbo.OutboxPartitions", conn).ExecuteNonQueryAsync();
        for (var i = 0; i < 8; i++)
        {
            await new SqlCommand(
                $"INSERT INTO dbo.OutboxPartitions (PartitionId) VALUES ({i})", conn)
                .ExecuteNonQueryAsync();
        }

        Console.WriteLine("SQL Server: partitions overridden to 8.");
    }, $"SQL Server {dbName}");
}

// ── PostgreSQL ────────────────────────────────────────────────────────────

static async Task InitPostgresAsync(string connStr)
{
    Console.WriteLine("Initializing PostgreSQL...");

    await RetryAsync(async () =>
    {
        await using var conn = new NpgsqlConnection(connStr);
        await conn.OpenAsync();

        var installSql = await File.ReadAllTextAsync("/db_scripts/pgsql/install.sql");
        await new NpgsqlCommand(installSql, conn).ExecuteNonQueryAsync();

        Console.WriteLine("PostgreSQL: install.sql executed.");

        await new NpgsqlCommand("DELETE FROM outbox_partitions", conn).ExecuteNonQueryAsync();
        for (var i = 0; i < 8; i++)
        {
            await new NpgsqlCommand(
                $"INSERT INTO outbox_partitions (partition_id) VALUES ({i})", conn)
                .ExecuteNonQueryAsync();
        }

        Console.WriteLine("PostgreSQL: partitions overridden to 8.");
    }, "PostgreSQL");
}

// ── Redpanda ──────────────────────────────────────────────────────────────

static async Task InitRedpandaAsync(string brokers)
{
    Console.WriteLine("Initializing Redpanda topic...");

    await RetryAsync(async () =>
    {
        using var adminClient = new AdminClientBuilder(
            new AdminClientConfig { BootstrapServers = brokers }).Build();

        try
        {
            await adminClient.CreateTopicsAsync(new[]
            {
                new TopicSpecification
                {
                    Name = "orders",
                    NumPartitions = 8,
                    ReplicationFactor = 1
                }
            });
            Console.WriteLine("Redpanda: 'orders' topic created with 8 partitions.");
        }
        catch (CreateTopicsException ex) when (ex.Results.Any(r =>
            r.Error.Code == ErrorCode.TopicAlreadyExists))
        {
            Console.WriteLine("Redpanda: 'orders' topic already exists.");
        }
    }, "Redpanda topic creation");
}

// ── Retry helper ──────────────────────────────────────────────────────────

static async Task RetryAsync(Func<Task> action, string context, int maxRetries = 30, int delayMs = 2000)
{
    for (var attempt = 1; attempt <= maxRetries; attempt++)
    {
        try
        {
            await action();
            return;
        }
        catch (Exception ex) when (attempt < maxRetries)
        {
            Console.WriteLine($"[{context}] Attempt {attempt}/{maxRetries} failed: {ex.Message}. Retrying in {delayMs}ms...");
            await Task.Delay(delayMs);
        }
    }

    await action();
}
