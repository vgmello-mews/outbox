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
