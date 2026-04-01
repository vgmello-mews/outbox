# Performance Test Suite Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create a performance test suite that measures publisher throughput, sustained load handling, and horizontal scaling across all store+transport combinations.

**Architecture:** A new xUnit + Testcontainers test project that reuses existing integration test patterns. Tests parameterized by (Store, Transport, PublisherCount). A `MetricsCollector` listens to existing `OutboxInstrumentation` via `MeterListener`. A `MessageProducer` handles both bulk seeding and sustained-rate insertion. Results are printed to console and written to a markdown report.

**Tech Stack:** .NET 10, xUnit, Testcontainers (PostgreSQL, Azure SQL Edge, Redpanda, EventHub emulator + Azurite), System.Diagnostics.Metrics

---

## File Structure

```
tests/Outbox.PerformanceTests/
  Outbox.PerformanceTests.csproj           # Project file with package/project references
  Fixtures/
    PerformanceFixture.cs                   # Testcontainers: all 4 containers, schema init
    PerformanceCollection.cs                # xUnit collection definition
  Helpers/
    TestMatrix.cs                           # Store/Transport enum + parameterized combinations
    PerfTestOptions.cs                      # Publisher options tuned for throughput
    MessageProducer.cs                      # Bulk seeding + sustained-rate SQL insertion
    MetricsCollector.cs                     # MeterListener on OutboxInstrumentation
    PerfReportWriter.cs                     # Markdown + console report generation
    PublisherHostBuilder.cs                 # Builds IHost for each store+transport combination
  Scenarios/
    BulkThroughputTests.cs                  # 1M message drain tests
    SustainedLoadTests.cs                   # 1K msg/sec sustained insertion tests
  reports/                                  # Generated markdown reports (gitignored)
    .gitkeep
```

Also modified:
- `src/Outbox.slnx` — add new project
- `.gitignore` — ignore generated reports (if not already covered)

---

### Task 1: Project Scaffolding

**Files:**
- Create: `tests/Outbox.PerformanceTests/Outbox.PerformanceTests.csproj`
- Modify: `src/Outbox.slnx`
- Create: `tests/Outbox.PerformanceTests/reports/.gitkeep`

- [ ] **Step 1: Create the project file**

Create `tests/Outbox.PerformanceTests/Outbox.PerformanceTests.csproj`:

```xml
<Project Sdk="Microsoft.NET.Sdk">

  <PropertyGroup>
    <IsPackable>false</IsPackable>
    <IsTestProject>true</IsTestProject>
  </PropertyGroup>

  <ItemGroup>
    <PackageReference Include="coverlet.collector" Version="8.*" />
    <PackageReference Include="Microsoft.NET.Test.Sdk" Version="17.*" />
    <PackageReference Include="xunit" Version="2.*" />
    <PackageReference Include="xunit.runner.visualstudio" Version="2.*" />
    <PackageReference Include="Testcontainers.PostgreSql" Version="4.*" />
    <PackageReference Include="Testcontainers.Redpanda" Version="4.*" />
    <PackageReference Include="Confluent.Kafka" Version="2.8.0" />
    <PackageReference Include="Npgsql" Version="9.*" />
    <PackageReference Include="Microsoft.Data.SqlClient" Version="6.*" />
    <PackageReference Include="Microsoft.Extensions.Hosting" Version="10.*" />
    <PackageReference Include="Microsoft.Extensions.Configuration.Binder" Version="10.*" />
    <PackageReference Include="Azure.Messaging.EventHubs" Version="5.*" />
  </ItemGroup>

  <ItemGroup>
    <ProjectReference Include="../../src/Outbox.Core/Outbox.Core.csproj" />
    <ProjectReference Include="../../src/Outbox.PostgreSQL/Outbox.PostgreSQL.csproj" />
    <ProjectReference Include="../../src/Outbox.SqlServer/Outbox.SqlServer.csproj" />
    <ProjectReference Include="../../src/Outbox.Kafka/Outbox.Kafka.csproj" />
    <ProjectReference Include="../../src/Outbox.EventHub/Outbox.EventHub.csproj" />
  </ItemGroup>

</Project>
```

- [ ] **Step 2: Add project to solution**

Edit `src/Outbox.slnx` — add before the closing `</Solution>` tag:

```xml
  <Project Path="../tests/Outbox.PerformanceTests/Outbox.PerformanceTests.csproj" />
```

- [ ] **Step 3: Create reports directory with .gitkeep**

Create `tests/Outbox.PerformanceTests/reports/.gitkeep` (empty file).

- [ ] **Step 4: Verify it builds**

Run: `dotnet build tests/Outbox.PerformanceTests/`
Expected: Build succeeded.

- [ ] **Step 5: Commit**

```bash
git add tests/Outbox.PerformanceTests/Outbox.PerformanceTests.csproj \
        tests/Outbox.PerformanceTests/reports/.gitkeep \
        src/Outbox.slnx
git commit -m "chore: scaffold performance test project"
```

---

### Task 2: Test Matrix and Options

**Files:**
- Create: `tests/Outbox.PerformanceTests/Helpers/TestMatrix.cs`
- Create: `tests/Outbox.PerformanceTests/Helpers/PerfTestOptions.cs`

- [ ] **Step 1: Create TestMatrix.cs**

Create `tests/Outbox.PerformanceTests/Helpers/TestMatrix.cs`:

```csharp
namespace Outbox.PerformanceTests.Helpers;

public enum StoreType
{
    SqlServer,
    PostgreSql
}

public enum TransportType
{
    Redpanda,
    EventHub
}

public sealed record TestCombination(StoreType Store, TransportType Transport, int PublisherCount)
{
    public string Label => $"{Store}+{Transport} {PublisherCount}P";

    public override string ToString() => Label;
}

public static class TestMatrix
{
    private static readonly StoreType[] Stores = [StoreType.SqlServer, StoreType.PostgreSql];
    private static readonly TransportType[] Transports = [TransportType.Redpanda, TransportType.EventHub];
    private static readonly int[] PublisherCounts = [1, 2, 4];

    public static IEnumerable<object[]> AllCombinations()
    {
        foreach (var store in Stores)
        foreach (var transport in Transports)
        foreach (var count in PublisherCounts)
            yield return [new TestCombination(store, transport, count)];
    }
}
```

- [ ] **Step 2: Create PerfTestOptions.cs**

Create `tests/Outbox.PerformanceTests/Helpers/PerfTestOptions.cs`:

```csharp
using Outbox.Core.Options;

namespace Outbox.PerformanceTests.Helpers;

public static class PerfTestOptions
{
    public static readonly Action<OutboxPublisherOptions> Default = o =>
    {
        o.BatchSize = 500;
        o.MaxRetryCount = 5;
        o.MinPollIntervalMs = 50;
        o.MaxPollIntervalMs = 1000;
        o.HeartbeatIntervalMs = 5_000;
        o.HeartbeatTimeoutSeconds = 30;
        o.PartitionGracePeriodSeconds = 10;
        o.RebalanceIntervalMs = 5_000;
        o.OrphanSweepIntervalMs = 5_000;
        o.DeadLetterSweepIntervalMs = 60_000;
        o.CircuitBreakerFailureThreshold = 3;
        o.CircuitBreakerOpenDurationSeconds = 30;
        o.PublishThreadCount = 4;
    };
}
```

- [ ] **Step 3: Verify it builds**

Run: `dotnet build tests/Outbox.PerformanceTests/`
Expected: Build succeeded.

- [ ] **Step 4: Commit**

```bash
git add tests/Outbox.PerformanceTests/Helpers/TestMatrix.cs \
        tests/Outbox.PerformanceTests/Helpers/PerfTestOptions.cs
git commit -m "feat(perf): add test matrix and publisher options"
```

---

### Task 3: Performance Fixture (Testcontainers)

**Files:**
- Create: `tests/Outbox.PerformanceTests/Fixtures/PerformanceFixture.cs`
- Create: `tests/Outbox.PerformanceTests/Fixtures/PerformanceCollection.cs`

- [ ] **Step 1: Create PerformanceFixture.cs**

This fixture starts all 4 containers (PostgreSQL, Azure SQL Edge, Redpanda, Azurite + EventHub emulator) and initializes schemas. The EventHub emulator requires a config file mounted and Azurite as a dependency — since Testcontainers doesn't have a native EventHub module, we use `ContainerBuilder` with a custom network so the emulator can reach Azurite by hostname.

Create `tests/Outbox.PerformanceTests/Fixtures/PerformanceFixture.cs`:

```csharp
using System.Text;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Configurations;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Testcontainers.PostgreSql;
using Testcontainers.Redpanda;

namespace Outbox.PerformanceTests.Fixtures;

public sealed class PerformanceFixture : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder()
        .WithImage("postgres:16-alpine")
        .Build();

    private readonly RedpandaContainer _redpanda = new RedpandaBuilder(
            "docker.redpanda.com/redpandadata/redpanda:v24.2.18")
        .Build();

    private INetwork _eventHubNetwork = null!;
    private IContainer _azurite = null!;
    private IContainer _eventHubEmulator = null!;

    // Azure SQL Edge — same approach as integration tests
    private IContainer _sqlServer = null!;
    private const string SqlServerPassword = "YourStrong!Passw0rd";

    public string PostgreSqlConnectionString => _postgres.GetConnectionString();
    public string SqlServerConnectionString { get; private set; } = "";
    public string BootstrapServers => _redpanda.GetBootstrapAddress();
    public string EventHubConnectionString { get; private set; } = "";

    public async Task InitializeAsync()
    {
        // Start independent containers in parallel
        _eventHubNetwork = new NetworkBuilder().Build();
        await _eventHubNetwork.CreateAsync();

        _azurite = new ContainerBuilder()
            .WithImage("mcr.microsoft.com/azure-storage/azurite:latest")
            .WithNetwork(_eventHubNetwork)
            .WithNetworkAliases("azurite")
            .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(10000))
            .Build();

        _sqlServer = new ContainerBuilder()
            .WithImage("mcr.microsoft.com/azure-sql-edge:latest")
            .WithPortBinding(0, 1433)
            .WithEnvironment("ACCEPT_EULA", "Y")
            .WithEnvironment("SA_PASSWORD", SqlServerPassword)
            .WithWaitStrategy(Wait.ForUnixContainer()
                .UntilOperationIsSucceeded(async () =>
                {
                    try
                    {
                        var port = _sqlServer.GetMappedPublicPort(1433);
                        var host = _sqlServer.Hostname;
                        var cs = $"Server={host},{port};Database=master;User Id=sa;Password={SqlServerPassword};TrustServerCertificate=True;Connect Timeout=5;";
                        await using var conn = new SqlConnection(cs);
                        await conn.OpenAsync();
                        await using var cmd = new SqlCommand("SELECT 1", conn);
                        await cmd.ExecuteScalarAsync();
                        return true;
                    }
                    catch { return false; }
                }, retries: 30, retryInterval: TimeSpan.FromSeconds(2)))
            .Build();

        // Start postgres, redpanda, sql server, azurite in parallel
        await Task.WhenAll(
            _postgres.StartAsync(),
            _redpanda.StartAsync(),
            _sqlServer.StartAsync(),
            _azurite.StartAsync());

        // Build SQL Server connection string
        var sqlPort = _sqlServer.GetMappedPublicPort(1433);
        var sqlHost = _sqlServer.Hostname;
        SqlServerConnectionString = $"Server={sqlHost},{sqlPort};Database=master;User Id=sa;Password={SqlServerPassword};TrustServerCertificate=True;";

        // Start EventHub emulator (depends on Azurite being ready)
        var configJson = BuildEventHubConfig();
        _eventHubEmulator = new ContainerBuilder()
            .WithImage("mcr.microsoft.com/azure-messaging/eventhubs-emulator:latest")
            .WithNetwork(_eventHubNetwork)
            .WithNetworkAliases("eventhubs-emulator")
            .WithPortBinding(0, 5672)
            .WithEnvironment("BLOB_SERVER", "azurite")
            .WithEnvironment("METADATA_SERVER", "azurite")
            .WithEnvironment("ACCEPT_EULA", "Y")
            .WithResourceMapping(Encoding.UTF8.GetBytes(configJson),
                "/Eventhubs_Emulator/ConfigFiles/Config.json")
            .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(5672))
            .Build();

        await _eventHubEmulator.StartAsync();

        var ehPort = _eventHubEmulator.GetMappedPublicPort(5672);
        var ehHost = _eventHubEmulator.Hostname;
        EventHubConnectionString = $"Endpoint=sb://{ehHost}:{ehPort};SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;";

        // Initialize schemas
        await Task.WhenAll(
            RunPostgresSchemaAsync(),
            RunSqlServerSchemaAsync());
    }

    public async Task DisposeAsync()
    {
        await Task.WhenAll(
            _postgres.DisposeAsync().AsTask(),
            _redpanda.DisposeAsync().AsTask(),
            _sqlServer.DisposeAsync().AsTask(),
            _eventHubEmulator.DisposeAsync().AsTask(),
            _azurite.DisposeAsync().AsTask());

        await _eventHubNetwork.DisposeAsync();
    }

    private static string BuildEventHubConfig()
    {
        // Create entities for the 3 perf test topics
        return """
        {
            "UserConfig": {
                "NamespaceConfig": [
                    {
                        "Type": "EventHub",
                        "Name": "emulatorNs1",
                        "Entities": [
                            { "Name": "perf-topic-0", "PartitionCount": "8", "ConsumerGroups": [] },
                            { "Name": "perf-topic-1", "PartitionCount": "8", "ConsumerGroups": [] },
                            { "Name": "perf-topic-2", "PartitionCount": "8", "ConsumerGroups": [] }
                        ]
                    }
                ],
                "LoggingConfig": { "Type": "File" }
            }
        }
        """;
    }

    private async Task RunPostgresSchemaAsync()
    {
        var schemaPath = FindSchemaFile("src/Outbox.PostgreSQL/db_scripts/install.sql");
        var sql = await File.ReadAllTextAsync(schemaPath);

        await using var conn = new NpgsqlConnection(_postgres.GetConnectionString());
        await conn.OpenAsync();
        await using var cmd = new NpgsqlCommand(sql, conn);
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task RunSqlServerSchemaAsync()
    {
        var schemaPath = FindSchemaFile("src/Outbox.SqlServer/db_scripts/install.sql");
        var sql = await File.ReadAllTextAsync(schemaPath);

        await using var conn = new SqlConnection(SqlServerConnectionString);
        await conn.OpenAsync();

        // SQL Server cannot execute batches with GO — split and execute each
        foreach (var batch in sql.Split(["GO", "go"], StringSplitOptions.RemoveEmptyEntries))
        {
            var trimmed = batch.Trim();
            if (string.IsNullOrEmpty(trimmed)) continue;

            await using var cmd = new SqlCommand(trimmed, conn);
            await cmd.ExecuteNonQueryAsync();
        }
    }

    private static string FindSchemaFile(string relativePath)
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !File.Exists(Path.Combine(dir.FullName, relativePath)))
            dir = dir.Parent;

        return dir is not null
            ? Path.Combine(dir.FullName, relativePath)
            : throw new FileNotFoundException($"Could not find {relativePath} from {AppContext.BaseDirectory}");
    }
}
```

- [ ] **Step 2: Create PerformanceCollection.cs**

Create `tests/Outbox.PerformanceTests/Fixtures/PerformanceCollection.cs`:

```csharp
namespace Outbox.PerformanceTests.Fixtures;

[CollectionDefinition(Name)]
public class PerformanceCollection : ICollectionFixture<PerformanceFixture>
{
    public const string Name = "Performance";
}
```

- [ ] **Step 3: Verify it builds**

Run: `dotnet build tests/Outbox.PerformanceTests/`
Expected: Build succeeded.

- [ ] **Step 4: Commit**

```bash
git add tests/Outbox.PerformanceTests/Fixtures/
git commit -m "feat(perf): add performance fixture with all containers"
```

---

### Task 4: MetricsCollector

**Files:**
- Create: `tests/Outbox.PerformanceTests/Helpers/MetricsCollector.cs`

- [ ] **Step 1: Create MetricsCollector.cs**

This class uses `MeterListener` to subscribe to the `Outbox` meter (or `{groupName}.Outbox`). It captures counter increments and histogram recordings in-memory for later percentile calculation.

Create `tests/Outbox.PerformanceTests/Helpers/MetricsCollector.cs`:

```csharp
using System.Collections.Concurrent;
using System.Diagnostics.Metrics;

namespace Outbox.PerformanceTests.Helpers;

public sealed class MetricsCollector : IDisposable
{
    private readonly MeterListener _listener;
    private readonly ConcurrentDictionary<string, long> _counters = new();
    private readonly ConcurrentDictionary<string, ConcurrentBag<double>> _histograms = new();
    private readonly ConcurrentBag<(DateTimeOffset Time, long Pending)> _pendingSnapshots = [];

    private readonly Timer? _pendingTimer;
    private readonly string _meterNamePrefix;
    private long _lastPendingValue;

    public MetricsCollector(string? groupName = null, TimeSpan? pendingSampleInterval = null)
    {
        _meterNamePrefix = groupName is not null ? $"{groupName}.Outbox" : "Outbox";

        _listener = new MeterListener();
        _listener.InstrumentPublished = (instrument, listener) =>
        {
            if (instrument.Meter.Name == _meterNamePrefix)
                listener.EnableMeasurementEvents(instrument);
        };

        _listener.SetMeasurementEventCallback<long>(OnLongMeasurement);
        _listener.SetMeasurementEventCallback<double>(OnDoubleMeasurement);
        _listener.SetMeasurementEventCallback<int>(OnIntMeasurement);
        _listener.Start();

        var interval = pendingSampleInterval ?? TimeSpan.FromSeconds(5);
        _pendingTimer = new Timer(_ => SnapshotPending(), null, interval, interval);
    }

    public long GetCounter(string name) =>
        _counters.GetValueOrDefault(name, 0);

    public IReadOnlyList<double> GetHistogramValues(string name) =>
        _histograms.TryGetValue(name, out var bag) ? [.. bag] : [];

    public IReadOnlyList<(DateTimeOffset Time, long Pending)> GetPendingSnapshots() =>
        [.. _pendingSnapshots];

    public long GetLastPendingValue() => Volatile.Read(ref _lastPendingValue);

    public static double Percentile(IReadOnlyList<double> sortedValues, double p)
    {
        if (sortedValues.Count == 0) return 0;
        var index = (int)Math.Ceiling(p / 100.0 * sortedValues.Count) - 1;
        return sortedValues[Math.Max(0, Math.Min(index, sortedValues.Count - 1))];
    }

    public HistogramStats GetHistogramStats(string name)
    {
        var values = GetHistogramValues(name);
        if (values.Count == 0)
            return new HistogramStats(0, 0, 0, 0, 0);

        var sorted = values.OrderBy(v => v).ToList();
        return new HistogramStats(
            Count: sorted.Count,
            P50: Percentile(sorted, 50),
            P95: Percentile(sorted, 95),
            P99: Percentile(sorted, 99),
            Max: sorted[^1]);
    }

    private void OnLongMeasurement(Instrument instrument, long value,
        ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state)
    {
        var name = instrument.Name;

        if (instrument is Counter<long>)
        {
            _counters.AddOrUpdate(name, value, (_, old) => old + value);
        }
        else if (instrument is ObservableGauge<long>)
        {
            Volatile.Write(ref _lastPendingValue, value);
        }
    }

    private void OnDoubleMeasurement(Instrument instrument, double value,
        ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state)
    {
        var bag = _histograms.GetOrAdd(instrument.Name, _ => []);
        bag.Add(value);
    }

    private void OnIntMeasurement(Instrument instrument, int value,
        ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state)
    {
        // batch_size is Histogram<int> — store as double for unified percentile calc
        var bag = _histograms.GetOrAdd(instrument.Name, _ => []);
        bag.Add(value);
    }

    private void SnapshotPending()
    {
        _listener.RecordObservableInstruments();
        var current = Volatile.Read(ref _lastPendingValue);
        _pendingSnapshots.Add((DateTimeOffset.UtcNow, current));
    }

    public void Dispose()
    {
        _pendingTimer?.Dispose();
        _listener.Dispose();
    }
}

public sealed record HistogramStats(int Count, double P50, double P95, double P99, double Max);
```

- [ ] **Step 2: Verify it builds**

Run: `dotnet build tests/Outbox.PerformanceTests/`
Expected: Build succeeded.

- [ ] **Step 3: Commit**

```bash
git add tests/Outbox.PerformanceTests/Helpers/MetricsCollector.cs
git commit -m "feat(perf): add MetricsCollector using MeterListener"
```

---

### Task 5: MessageProducer

**Files:**
- Create: `tests/Outbox.PerformanceTests/Helpers/MessageProducer.cs`

- [ ] **Step 1: Create MessageProducer.cs**

This handles bulk insertion (for pre-seeding) and sustained-rate insertion (for load tests). Uses raw SQL with multi-row inserts for performance.

Create `tests/Outbox.PerformanceTests/Helpers/MessageProducer.cs`:

```csharp
using System.Data.Common;
using System.Diagnostics;
using System.Text;
using Microsoft.Data.SqlClient;
using Npgsql;

namespace Outbox.PerformanceTests.Helpers;

public static class MessageProducer
{
    private const int TopicCount = 3;
    private const int PartitionKeyCount = 10;
    private static readonly byte[] FixedPayload = Encoding.UTF8.GetBytes(
        """{"perf":true,"data":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}""");

    // ---------------------------------------------------------------
    // Bulk seeding — used by BulkThroughputTests
    // ---------------------------------------------------------------

    public static async Task BulkSeedAsync(StoreType store, string connectionString,
        int totalMessages, int batchSize = 10_000)
    {
        for (var offset = 0; offset < totalMessages; offset += batchSize)
        {
            var count = Math.Min(batchSize, totalMessages - offset);

            if (store == StoreType.PostgreSql)
                await InsertBatchPostgreSql(connectionString, offset, count);
            else
                await InsertBatchSqlServer(connectionString, offset, count);
        }
    }

    // ---------------------------------------------------------------
    // Sustained-rate insertion — used by SustainedLoadTests
    // ---------------------------------------------------------------

    public static async Task ProduceAtRateAsync(StoreType store, string connectionString,
        int messagesPerSecond, CancellationToken ct)
    {
        const int insertsPerSecond = 10; // 10 micro-batches per second
        var batchSize = messagesPerSecond / insertsPerSecond;
        var interval = TimeSpan.FromMilliseconds(1000.0 / insertsPerSecond);
        var sw = Stopwatch.StartNew();
        var batchIndex = 0;

        while (!ct.IsCancellationRequested)
        {
            var expectedTime = interval * batchIndex;
            var elapsed = sw.Elapsed;
            var delay = expectedTime - elapsed;

            if (delay > TimeSpan.Zero)
                await Task.Delay(delay, ct);

            var offset = batchIndex * batchSize;

            try
            {
                if (store == StoreType.PostgreSql)
                    await InsertBatchPostgreSql(connectionString, offset, batchSize);
                else
                    await InsertBatchSqlServer(connectionString, offset, batchSize);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }

            batchIndex++;
        }
    }

    // ---------------------------------------------------------------
    // PostgreSQL multi-row insert
    // ---------------------------------------------------------------

    private static async Task InsertBatchPostgreSql(string connectionString, int offset, int count)
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();

        var sb = new StringBuilder();
        sb.Append("""
            INSERT INTO outbox (topic_name, partition_key, event_type, payload, event_datetime_utc, event_ordinal)
            VALUES
            """);

        for (var i = 0; i < count; i++)
        {
            var globalIndex = offset + i;
            var topic = $"perf-topic-{globalIndex % TopicCount}";
            var key = $"pk-{globalIndex % PartitionKeyCount}";

            if (i > 0) sb.Append(',');
            sb.Append($"('{topic}','{key}','PerfEvent',@p,clock_timestamp(),{i})");
        }

        await using var cmd = new NpgsqlCommand(sb.ToString(), conn);
        cmd.Parameters.Add(new NpgsqlParameter("p", FixedPayload));
        await cmd.ExecuteNonQueryAsync();
    }

    // ---------------------------------------------------------------
    // SQL Server multi-row insert
    // ---------------------------------------------------------------

    private static async Task InsertBatchSqlServer(string connectionString, int offset, int count)
    {
        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync();

        var sb = new StringBuilder();
        sb.Append("""
            INSERT INTO dbo.Outbox (TopicName, PartitionKey, EventType, Payload, EventDateTimeUtc, EventOrdinal)
            VALUES
            """);

        for (var i = 0; i < count; i++)
        {
            var globalIndex = offset + i;
            var topic = $"perf-topic-{globalIndex % TopicCount}";
            var key = $"pk-{globalIndex % PartitionKeyCount}";

            if (i > 0) sb.Append(',');
            sb.Append($"('{topic}','{key}','PerfEvent',@p,SYSUTCDATETIME(),{i})");
        }

        await using var cmd = new SqlCommand(sb.ToString(), conn);
        cmd.Parameters.Add(new SqlParameter("@p", System.Data.SqlDbType.VarBinary) { Value = FixedPayload });
        await cmd.ExecuteNonQueryAsync();
    }
}
```

- [ ] **Step 2: Verify it builds**

Run: `dotnet build tests/Outbox.PerformanceTests/`
Expected: Build succeeded.

- [ ] **Step 3: Commit**

```bash
git add tests/Outbox.PerformanceTests/Helpers/MessageProducer.cs
git commit -m "feat(perf): add MessageProducer for bulk and sustained insertion"
```

---

### Task 6: PublisherHostBuilder

**Files:**
- Create: `tests/Outbox.PerformanceTests/Helpers/PublisherHostBuilder.cs`

- [ ] **Step 1: Create PublisherHostBuilder.cs**

Builds an `IHost` for any (Store, Transport) combination. Unlike integration tests which use `FaultyTransportWrapper`, this uses real transports.

Create `tests/Outbox.PerformanceTests/Helpers/PublisherHostBuilder.cs`:

```csharp
using Confluent.Kafka;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;
using Outbox.Core.Builder;
using Outbox.Core.Options;
using Outbox.EventHub;
using Outbox.Kafka;
using Outbox.PostgreSQL;
using Outbox.SqlServer;
using System.Data.Common;

namespace Outbox.PerformanceTests.Helpers;

public static class PublisherHostBuilder
{
    public static IHost Build(
        TestCombination combo,
        string pgConnectionString,
        string sqlServerConnectionString,
        string bootstrapServers,
        string eventHubConnectionString,
        Action<OutboxPublisherOptions>? configureOptions = null)
    {
        return Host.CreateDefaultBuilder()
            .ConfigureAppConfiguration((_, config) =>
            {
                config.AddInMemoryCollection(new Dictionary<string, string?>
                {
                    ["Outbox:Publisher:BatchSize"] = "500"
                });
            })
            .ConfigureServices((ctx, services) =>
            {
                services.AddOutbox(ctx.Configuration, outbox =>
                {
                    // Store
                    ConfigureStore(outbox, combo.Store, pgConnectionString, sqlServerConnectionString);

                    // Transport
                    ConfigureTransport(outbox, combo.Transport, bootstrapServers, eventHubConnectionString);

                    // Publisher options
                    outbox.ConfigurePublisher(o =>
                    {
                        PerfTestOptions.Default(o);
                        configureOptions?.Invoke(o);
                    });
                });
            })
            .Build();
    }

    private static void ConfigureStore(IOutboxBuilder outbox, StoreType store,
        string pgConnectionString, string sqlServerConnectionString)
    {
        switch (store)
        {
            case StoreType.PostgreSql:
                outbox.UsePostgreSql(options =>
                {
                    options.ConnectionString = pgConnectionString;
                });
                break;
            case StoreType.SqlServer:
                outbox.UseSqlServer(options =>
                {
                    options.ConnectionString = sqlServerConnectionString;
                });
                break;
        }
    }

    private static void ConfigureTransport(IOutboxBuilder outbox, TransportType transport,
        string bootstrapServers, string eventHubConnectionString)
    {
        switch (transport)
        {
            case TransportType.Redpanda:
                outbox.UseKafka(options =>
                {
                    options.BootstrapServers = bootstrapServers;
                    options.Acks = "All";
                    options.EnableIdempotence = true;
                    options.LingerMs = 5;
                });
                break;
            case TransportType.EventHub:
                outbox.UseEventHub(options =>
                {
                    options.ConnectionString = eventHubConnectionString;
                });
                break;
        }
    }
}
```

- [ ] **Step 2: Verify it builds**

Run: `dotnet build tests/Outbox.PerformanceTests/`
Expected: Build succeeded.

- [ ] **Step 3: Commit**

```bash
git add tests/Outbox.PerformanceTests/Helpers/PublisherHostBuilder.cs
git commit -m "feat(perf): add PublisherHostBuilder for all store+transport combos"
```

---

### Task 7: PerfReportWriter

**Files:**
- Create: `tests/Outbox.PerformanceTests/Helpers/PerfReportWriter.cs`

- [ ] **Step 1: Create PerfReportWriter.cs**

Create `tests/Outbox.PerformanceTests/Helpers/PerfReportWriter.cs`:

```csharp
using System.Runtime.InteropServices;
using System.Text;
using Xunit.Abstractions;

namespace Outbox.PerformanceTests.Helpers;

public sealed record BulkResult(
    TestCombination Combo,
    int TotalMessages,
    TimeSpan Duration,
    HistogramStats PollDuration,
    HistogramStats PublishDuration,
    HistogramStats BatchSize);

public sealed record SustainedResult(
    TestCombination Combo,
    int TargetRate,
    TimeSpan TestDuration,
    double AvgDrainRate,
    long PeakPending,
    long FinalPending,
    HistogramStats PollDuration,
    HistogramStats PublishDuration,
    IReadOnlyList<(DateTimeOffset Time, long Pending)> PendingSnapshots);

public static class PerfReportWriter
{
    public static void WriteBulkToConsole(BulkResult r, ITestOutputHelper output)
    {
        var msgPerSec = r.TotalMessages / r.Duration.TotalSeconds;
        output.WriteLine($"[Bulk] {r.Combo.Label}: {r.TotalMessages:N0} msgs in {r.Duration:mm\\:ss} " +
                         $"({msgPerSec:N0} msg/sec) | Poll p50={r.PollDuration.P50:F1}ms p95={r.PollDuration.P95:F1}ms | " +
                         $"Pub p50={r.PublishDuration.P50:F1}ms p95={r.PublishDuration.P95:F1}ms");
    }

    public static void WriteSustainedToConsole(SustainedResult r, ITestOutputHelper output)
    {
        var keptUp = r.FinalPending < r.TargetRate; // less than 1 second of backlog
        output.WriteLine($"[Sustained] {r.Combo.Label}: target={r.TargetRate}/s drain={r.AvgDrainRate:N0}/s " +
                         $"peak={r.PeakPending:N0} final={r.FinalPending:N0} kept_up={keptUp}");
    }

    public static async Task WriteMarkdownReportAsync(
        IReadOnlyList<BulkResult> bulkResults,
        IReadOnlyList<SustainedResult> sustainedResults,
        string reportsDir)
    {
        Directory.CreateDirectory(reportsDir);
        var timestamp = DateTimeOffset.UtcNow.ToString("yyyy-MM-ddTHH-mm-ss");
        var path = Path.Combine(reportsDir, $"{timestamp}-perf-report.md");

        var sb = new StringBuilder();
        sb.AppendLine($"# Performance Test Report - {DateTimeOffset.UtcNow:yyyy-MM-dd HH:mm:ss} UTC");
        sb.AppendLine();

        // Environment
        sb.AppendLine("## Environment");
        sb.AppendLine($"- OS: {RuntimeInformation.OSDescription}");
        sb.AppendLine($"- .NET: {RuntimeInformation.FrameworkDescription}");
        sb.AppendLine($"- Architecture: {RuntimeInformation.OSArchitecture}");
        sb.AppendLine($"- Processors: {Environment.ProcessorCount}");
        sb.AppendLine();

        // Bulk results
        if (bulkResults.Count > 0)
        {
            sb.AppendLine("## Bulk Throughput");
            sb.AppendLine("| Store | Transport | Publishers | Messages | Duration | Msg/sec | Poll p50 | Poll p95 | Pub p50 | Pub p95 |");
            sb.AppendLine("|-------|-----------|------------|----------|----------|---------|----------|----------|---------|---------|");
            foreach (var r in bulkResults)
            {
                var msgSec = r.TotalMessages / r.Duration.TotalSeconds;
                sb.AppendLine($"| {r.Combo.Store} | {r.Combo.Transport} | {r.Combo.PublisherCount} " +
                              $"| {r.TotalMessages:N0} | {r.Duration:mm\\:ss} | {msgSec:N0} " +
                              $"| {r.PollDuration.P50:F1}ms | {r.PollDuration.P95:F1}ms " +
                              $"| {r.PublishDuration.P50:F1}ms | {r.PublishDuration.P95:F1}ms |");
            }
            sb.AppendLine();
        }

        // Sustained results
        if (sustainedResults.Count > 0)
        {
            sb.AppendLine("## Sustained Load");
            sb.AppendLine("| Store | Transport | Publishers | Target | Drain Rate | Peak Pending | Final Pending | Kept Up? |");
            sb.AppendLine("|-------|-----------|------------|--------|------------|--------------|---------------|----------|");
            foreach (var r in sustainedResults)
            {
                var keptUp = r.FinalPending < r.TargetRate;
                sb.AppendLine($"| {r.Combo.Store} | {r.Combo.Transport} | {r.Combo.PublisherCount} " +
                              $"| {r.TargetRate}/s | {r.AvgDrainRate:N0}/s | {r.PeakPending:N0} " +
                              $"| {r.FinalPending:N0} | {(keptUp ? "Yes" : "No")} |");
            }
            sb.AppendLine();

            // Pending over time table
            sb.AppendLine("## Pending Over Time (Sustained Load)");
            var headers = sustainedResults.Select(r => r.Combo.Label).ToList();
            sb.Append("| Time |");
            foreach (var h in headers) sb.Append($" {h} |");
            sb.AppendLine();
            sb.Append("|------|");
            foreach (var _ in headers) sb.Append("------|");
            sb.AppendLine();

            // Align by snapshot index (all should have same interval)
            var maxSnapshots = sustainedResults.Max(r => r.PendingSnapshots.Count);
            for (var i = 0; i < maxSnapshots; i++)
            {
                sb.Append($"| {i * 5}s |");
                foreach (var r in sustainedResults)
                {
                    var val = i < r.PendingSnapshots.Count ? r.PendingSnapshots[i].Pending : 0;
                    sb.Append($" {val:N0} |");
                }
                sb.AppendLine();
            }
            sb.AppendLine();
        }

        await File.WriteAllTextAsync(path, sb.ToString());
    }
}
```

- [ ] **Step 2: Verify it builds**

Run: `dotnet build tests/Outbox.PerformanceTests/`
Expected: Build succeeded.

- [ ] **Step 3: Commit**

```bash
git add tests/Outbox.PerformanceTests/Helpers/PerfReportWriter.cs
git commit -m "feat(perf): add PerfReportWriter for console and markdown output"
```

---

### Task 8: Cleanup Helpers

**Files:**
- Create: `tests/Outbox.PerformanceTests/Helpers/CleanupHelper.cs`

- [ ] **Step 1: Create CleanupHelper.cs**

We need cleanup and query helpers for both stores that work with the perf test scenarios.

Create `tests/Outbox.PerformanceTests/Helpers/CleanupHelper.cs`:

```csharp
using Microsoft.Data.SqlClient;
using Npgsql;

namespace Outbox.PerformanceTests.Helpers;

public static class CleanupHelper
{
    public static async Task CleanupAsync(StoreType store, string connectionString)
    {
        if (store == StoreType.PostgreSql)
            await CleanupPostgreSqlAsync(connectionString);
        else
            await CleanupSqlServerAsync(connectionString);
    }

    public static async Task<long> GetPendingCountAsync(StoreType store, string connectionString)
    {
        if (store == StoreType.PostgreSql)
            return await QueryScalarPostgreSql(connectionString, "SELECT COUNT(*) FROM outbox");
        else
            return await QueryScalarSqlServer(connectionString, "SELECT COUNT(*) FROM dbo.Outbox");
    }

    private static async Task CleanupPostgreSqlAsync(string connectionString)
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await using var cmd = new NpgsqlCommand("""
            TRUNCATE outbox, outbox_dead_letter;
            DELETE FROM outbox_publishers;
            UPDATE outbox_partitions SET owner_publisher_id = NULL, owned_since_utc = NULL, grace_expires_utc = NULL;
            """, conn);
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task CleanupSqlServerAsync(string connectionString)
    {
        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync();
        await using var cmd = new SqlCommand("""
            DELETE FROM dbo.Outbox;
            DELETE FROM dbo.OutboxDeadLetter;
            DELETE FROM dbo.OutboxPublishers;
            UPDATE dbo.OutboxPartitions SET OwnerPublisherId = NULL, OwnedSinceUtc = NULL, GraceExpiresUtc = NULL;
            """, conn);
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task<long> QueryScalarPostgreSql(string connectionString, string sql)
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await using var cmd = new NpgsqlCommand(sql, conn);
        return (long)(await cmd.ExecuteScalarAsync())!;
    }

    private static async Task<long> QueryScalarSqlServer(string connectionString, string sql)
    {
        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync();
        await using var cmd = new SqlCommand(sql, conn);
        return (long)(int)(await cmd.ExecuteScalarAsync())!;
    }
}
```

- [ ] **Step 2: Verify it builds**

Run: `dotnet build tests/Outbox.PerformanceTests/`
Expected: Build succeeded.

- [ ] **Step 3: Commit**

```bash
git add tests/Outbox.PerformanceTests/Helpers/CleanupHelper.cs
git commit -m "feat(perf): add cleanup and query helpers for both stores"
```

---

### Task 9: BulkThroughputTests

**Files:**
- Create: `tests/Outbox.PerformanceTests/Scenarios/BulkThroughputTests.cs`

- [ ] **Step 1: Create BulkThroughputTests.cs**

Create `tests/Outbox.PerformanceTests/Scenarios/BulkThroughputTests.cs`:

```csharp
using System.Diagnostics;
using Outbox.PerformanceTests.Fixtures;
using Outbox.PerformanceTests.Helpers;
using Xunit.Abstractions;

namespace Outbox.PerformanceTests.Scenarios;

[Collection(PerformanceCollection.Name)]
public class BulkThroughputTests
{
    private const int TotalMessages = 1_000_000;
    private const int StabilizationDelayMs = 15_000;

    private readonly PerformanceFixture _fixture;
    private readonly ITestOutputHelper _output;

    // Collect results across test runs for final report
    private static readonly List<BulkResult> Results = [];

    public BulkThroughputTests(PerformanceFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    [Theory(Timeout = 600_000)] // 10 minute timeout per combination
    [MemberData(nameof(TestMatrix.AllCombinations), MemberType = typeof(TestMatrix))]
    public async Task BulkDrain(TestCombination combo)
    {
        var storeConnStr = combo.Store == StoreType.PostgreSql
            ? _fixture.PostgreSqlConnectionString
            : _fixture.SqlServerConnectionString;

        // Cleanup
        await CleanupHelper.CleanupAsync(combo.Store, storeConnStr);

        // Pre-seed messages
        _output.WriteLine($"[Bulk] {combo.Label}: Seeding {TotalMessages:N0} messages...");
        var seedSw = Stopwatch.StartNew();
        await MessageProducer.BulkSeedAsync(combo.Store, storeConnStr, TotalMessages);
        seedSw.Stop();
        _output.WriteLine($"[Bulk] {combo.Label}: Seeding complete in {seedSw.Elapsed:mm\\:ss}");

        // Verify seed count
        var seeded = await CleanupHelper.GetPendingCountAsync(combo.Store, storeConnStr);
        Assert.Equal(TotalMessages, seeded);

        // Start metrics collector
        using var metrics = new MetricsCollector();

        // Start publishers
        var hosts = new List<IHost>();
        for (var i = 0; i < combo.PublisherCount; i++)
        {
            var host = PublisherHostBuilder.Build(
                combo,
                _fixture.PostgreSqlConnectionString,
                _fixture.SqlServerConnectionString,
                _fixture.BootstrapServers,
                _fixture.EventHubConnectionString);
            hosts.Add(host);
        }

        var sw = Stopwatch.StartNew();

        // Start all publisher hosts
        foreach (var host in hosts)
            await host.StartAsync();

        // Wait for stabilization (partition rebalancing) if multi-publisher
        if (combo.PublisherCount > 1)
        {
            _output.WriteLine($"[Bulk] {combo.Label}: Waiting {StabilizationDelayMs}ms for rebalancing...");
            await Task.Delay(StabilizationDelayMs);
        }

        // Wait for all messages to drain
        var timeout = TimeSpan.FromMinutes(8);
        var pollInterval = TimeSpan.FromSeconds(2);
        var lastLog = Stopwatch.StartNew();

        while (sw.Elapsed < timeout)
        {
            var pending = await CleanupHelper.GetPendingCountAsync(combo.Store, storeConnStr);

            if (lastLog.Elapsed > TimeSpan.FromSeconds(10))
            {
                var published = TotalMessages - pending;
                var rate = published / sw.Elapsed.TotalSeconds;
                _output.WriteLine($"[Bulk] {combo.Label}: {published:N0}/{TotalMessages:N0} " +
                                  $"({100.0 * published / TotalMessages:F1}%) — {rate:N0} msg/sec");
                lastLog.Restart();
            }

            if (pending == 0) break;
            await Task.Delay(pollInterval);
        }

        sw.Stop();

        // Stop publishers
        foreach (var host in hosts)
            await host.StopAsync(TimeSpan.FromSeconds(10));
        foreach (var host in hosts)
            host.Dispose();

        // Verify drain
        var remaining = await CleanupHelper.GetPendingCountAsync(combo.Store, storeConnStr);
        Assert.Equal(0, remaining);

        // Collect results
        var result = new BulkResult(
            combo,
            TotalMessages,
            sw.Elapsed,
            metrics.GetHistogramStats("outbox.poll.duration"),
            metrics.GetHistogramStats("outbox.publish.duration"),
            metrics.GetHistogramStats("outbox.poll.batch_size"));

        PerfReportWriter.WriteBulkToConsole(result, _output);

        lock (Results)
        {
            Results.Add(result);
        }
    }
}
```

- [ ] **Step 2: Verify it builds**

Run: `dotnet build tests/Outbox.PerformanceTests/`
Expected: Build succeeded.

- [ ] **Step 3: Commit**

```bash
git add tests/Outbox.PerformanceTests/Scenarios/BulkThroughputTests.cs
git commit -m "feat(perf): add BulkThroughputTests for 1M message drain"
```

---

### Task 10: SustainedLoadTests

**Files:**
- Create: `tests/Outbox.PerformanceTests/Scenarios/SustainedLoadTests.cs`

- [ ] **Step 1: Create SustainedLoadTests.cs**

Create `tests/Outbox.PerformanceTests/Scenarios/SustainedLoadTests.cs`:

```csharp
using System.Diagnostics;
using Outbox.PerformanceTests.Fixtures;
using Outbox.PerformanceTests.Helpers;
using Xunit.Abstractions;

namespace Outbox.PerformanceTests.Scenarios;

[Collection(PerformanceCollection.Name)]
public class SustainedLoadTests
{
    private const int TargetRate = 1_000; // messages per second
    private static readonly TimeSpan TestDuration = TimeSpan.FromMinutes(5);
    private const int StabilizationDelayMs = 15_000;

    private readonly PerformanceFixture _fixture;
    private readonly ITestOutputHelper _output;

    private static readonly List<SustainedResult> Results = [];

    public SustainedLoadTests(PerformanceFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    [Theory(Timeout = 600_000)] // 10 minute timeout per combination
    [MemberData(nameof(TestMatrix.AllCombinations), MemberType = typeof(TestMatrix))]
    public async Task SustainedRate(TestCombination combo)
    {
        var storeConnStr = combo.Store == StoreType.PostgreSql
            ? _fixture.PostgreSqlConnectionString
            : _fixture.SqlServerConnectionString;

        // Cleanup
        await CleanupHelper.CleanupAsync(combo.Store, storeConnStr);

        // Start metrics collector
        using var metrics = new MetricsCollector(pendingSampleInterval: TimeSpan.FromSeconds(5));

        // Start publishers
        var hosts = new List<IHost>();
        for (var i = 0; i < combo.PublisherCount; i++)
        {
            var host = PublisherHostBuilder.Build(
                combo,
                _fixture.PostgreSqlConnectionString,
                _fixture.SqlServerConnectionString,
                _fixture.BootstrapServers,
                _fixture.EventHubConnectionString);
            hosts.Add(host);
        }

        foreach (var host in hosts)
            await host.StartAsync();

        // Wait for stabilization if multi-publisher
        if (combo.PublisherCount > 1)
        {
            _output.WriteLine($"[Sustained] {combo.Label}: Waiting {StabilizationDelayMs}ms for rebalancing...");
            await Task.Delay(StabilizationDelayMs);
        }

        // Start sustained producer
        using var producerCts = new CancellationTokenSource(TestDuration);
        var producerTask = MessageProducer.ProduceAtRateAsync(
            combo.Store, storeConnStr, TargetRate, producerCts.Token);

        // Monitor progress
        var sw = Stopwatch.StartNew();
        var lastLog = Stopwatch.StartNew();
        long peakPending = 0;

        while (sw.Elapsed < TestDuration)
        {
            await Task.Delay(TimeSpan.FromSeconds(5));

            var pending = await CleanupHelper.GetPendingCountAsync(combo.Store, storeConnStr);
            if (pending > peakPending) peakPending = pending;

            if (lastLog.Elapsed > TimeSpan.FromSeconds(15))
            {
                _output.WriteLine($"[Sustained] {combo.Label}: pending={pending:N0} peak={peakPending:N0} " +
                                  $"published={metrics.GetCounter("outbox.messages.published"):N0} " +
                                  $"elapsed={sw.Elapsed:mm\\:ss}");
                lastLog.Restart();
            }
        }

        // Wait for producer to finish
        try { await producerTask; }
        catch (OperationCanceledException) { }

        // Give publishers a few seconds to drain remaining
        await Task.Delay(TimeSpan.FromSeconds(10));

        var finalPending = await CleanupHelper.GetPendingCountAsync(combo.Store, storeConnStr);
        if (finalPending > peakPending) peakPending = finalPending;

        // Stop publishers
        foreach (var host in hosts)
            await host.StopAsync(TimeSpan.FromSeconds(10));
        foreach (var host in hosts)
            host.Dispose();

        // Calculate drain rate
        var totalPublished = metrics.GetCounter("outbox.messages.published");
        var avgDrainRate = totalPublished / TestDuration.TotalSeconds;

        // Collect results
        var result = new SustainedResult(
            combo,
            TargetRate,
            TestDuration,
            avgDrainRate,
            peakPending,
            finalPending,
            metrics.GetHistogramStats("outbox.poll.duration"),
            metrics.GetHistogramStats("outbox.publish.duration"),
            metrics.GetPendingSnapshots());

        PerfReportWriter.WriteSustainedToConsole(result, _output);

        lock (Results)
        {
            Results.Add(result);
        }
    }
}
```

- [ ] **Step 2: Verify it builds**

Run: `dotnet build tests/Outbox.PerformanceTests/`
Expected: Build succeeded.

- [ ] **Step 3: Commit**

```bash
git add tests/Outbox.PerformanceTests/Scenarios/SustainedLoadTests.cs
git commit -m "feat(perf): add SustainedLoadTests for 1K msg/sec load testing"
```

---

### Task 11: Report Generation Trigger

**Files:**
- Create: `tests/Outbox.PerformanceTests/Scenarios/ReportGenerationFixture.cs`

- [ ] **Step 1: Create ReportGenerationFixture.cs**

xUnit doesn't have a built-in "after all tests" hook, so we use a collection fixture's `DisposeAsync` to write the final markdown report after all tests in the collection complete.

We need to refactor the results storage from static lists in the test classes to a shared fixture. Update `PerformanceFixture.cs` to hold results and write the report on dispose.

Edit `tests/Outbox.PerformanceTests/Fixtures/PerformanceFixture.cs` — add to the class:

```csharp
    // Results collected during test runs
    private readonly List<BulkResult> _bulkResults = [];
    private readonly List<SustainedResult> _sustainedResults = [];
    private readonly object _resultsLock = new();

    public void AddBulkResult(BulkResult result)
    {
        lock (_resultsLock) _bulkResults.Add(result);
    }

    public void AddSustainedResult(SustainedResult result)
    {
        lock (_resultsLock) _sustainedResults.Add(result);
    }
```

And update `DisposeAsync` to write the report before disposing containers:

```csharp
    public async Task DisposeAsync()
    {
        // Write report before tearing down containers
        List<BulkResult> bulkCopy;
        List<SustainedResult> sustainedCopy;
        lock (_resultsLock)
        {
            bulkCopy = [.. _bulkResults];
            sustainedCopy = [.. _sustainedResults];
        }

        if (bulkCopy.Count > 0 || sustainedCopy.Count > 0)
        {
            var reportsDir = FindReportsDir();
            await PerfReportWriter.WriteMarkdownReportAsync(bulkCopy, sustainedCopy, reportsDir);
        }

        await Task.WhenAll(
            _postgres.DisposeAsync().AsTask(),
            _redpanda.DisposeAsync().AsTask(),
            _sqlServer.DisposeAsync().AsTask(),
            _eventHubEmulator.DisposeAsync().AsTask(),
            _azurite.DisposeAsync().AsTask());

        await _eventHubNetwork.DisposeAsync();
    }

    private static string FindReportsDir()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !Directory.Exists(Path.Combine(dir.FullName, "tests/Outbox.PerformanceTests")))
            dir = dir.Parent;

        return dir is not null
            ? Path.Combine(dir.FullName, "tests/Outbox.PerformanceTests/reports")
            : Path.Combine(AppContext.BaseDirectory, "reports");
    }
```

- [ ] **Step 2: Update BulkThroughputTests to use fixture for results**

In `BulkThroughputTests.cs`, remove the `static readonly List<BulkResult> Results` field. Replace the result collection at the end of the test:

Replace:
```csharp
        lock (Results)
        {
            Results.Add(result);
        }
```

With:
```csharp
        _fixture.AddBulkResult(result);
```

- [ ] **Step 3: Update SustainedLoadTests to use fixture for results**

In `SustainedLoadTests.cs`, remove the `static readonly List<SustainedResult> Results` field. Replace the result collection at the end of the test:

Replace:
```csharp
        lock (Results)
        {
            Results.Add(result);
        }
```

With:
```csharp
        _fixture.AddSustainedResult(result);
```

- [ ] **Step 4: Add missing using to PerformanceFixture**

Add to the top of `PerformanceFixture.cs`:
```csharp
using Outbox.PerformanceTests.Helpers;
```

- [ ] **Step 5: Verify it builds**

Run: `dotnet build tests/Outbox.PerformanceTests/`
Expected: Build succeeded.

- [ ] **Step 6: Commit**

```bash
git add tests/Outbox.PerformanceTests/
git commit -m "feat(perf): add report generation via fixture dispose"
```

---

### Task 12: Build Verification and Smoke Test

**Files:**
- No new files — verification only

- [ ] **Step 1: Full build**

Run: `dotnet build tests/Outbox.PerformanceTests/`
Expected: Build succeeded with no warnings relevant to our code.

- [ ] **Step 2: Verify test discovery**

Run: `dotnet test tests/Outbox.PerformanceTests/ --list-tests`
Expected: Should list 24 test methods (12 for BulkDrain, 12 for SustainedRate) — one per (Store, Transport, PublisherCount) combination.

- [ ] **Step 3: Commit any remaining fixes**

If any build or discovery issues were found, fix and commit:
```bash
git add -A tests/Outbox.PerformanceTests/
git commit -m "fix(perf): resolve build issues from verification"
```

---

## Notes for Execution

- **EventHub emulator** is the most complex container to set up. If it causes issues during test runs, it may need additional wait strategies or health checks. The emulator requires Azurite to be fully ready before it can accept connections.
- **SQL Server bulk inserts** with multi-row VALUES have a limit of ~1000 parameters per statement. The `MessageProducer` uses a single `@p` parameter for the fixed payload and string-interpolated topic/key values (safe because they're internal constants, not user input). If this causes issues with large batches, consider using `SqlBulkCopy` instead.
- **The `MeterListener` approach** for `MetricsCollector` requires that the meter instances are created in the same process. Since the publisher hosts run in-process, this works. If the listener doesn't capture metrics from a specific host, ensure the `MeterListener` is created before the host starts.
- **Parallel test execution** — xUnit runs tests within a collection sequentially by default, which is what we want to avoid container resource contention. Tests in different collections would run in parallel.
