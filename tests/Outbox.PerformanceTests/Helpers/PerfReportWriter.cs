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

        AppendEnvironmentSection(sb);
        AppendBulkSection(sb, bulkResults);
        AppendSustainedSection(sb, sustainedResults);

        await File.WriteAllTextAsync(path, sb.ToString());
    }

    private static void AppendEnvironmentSection(StringBuilder sb)
    {
        sb.AppendLine("## Environment");
        sb.AppendLine($"- OS: {RuntimeInformation.OSDescription}");
        sb.AppendLine($"- .NET: {RuntimeInformation.FrameworkDescription}");
        sb.AppendLine($"- Architecture: {RuntimeInformation.OSArchitecture}");
        sb.AppendLine($"- Processors: {Environment.ProcessorCount}");
        sb.AppendLine();
    }

    private static void AppendBulkSection(StringBuilder sb, IReadOnlyList<BulkResult> bulkResults)
    {
        if (bulkResults.Count == 0) return;

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

    private static void AppendSustainedSection(StringBuilder sb, IReadOnlyList<SustainedResult> sustainedResults)
    {
        if (sustainedResults.Count == 0) return;

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

        AppendPendingOverTimeSection(sb, sustainedResults);
    }

    private static void AppendPendingOverTimeSection(StringBuilder sb, IReadOnlyList<SustainedResult> sustainedResults)
    {
        sb.AppendLine("## Pending Over Time (Sustained Load)");
        var headers = sustainedResults.Select(r => r.Combo.Label).ToList();
        sb.Append("| Time |");
        foreach (var h in headers) sb.Append($" {h} |");
        sb.AppendLine();
        sb.Append("|------|");
        foreach (var _ in headers) sb.Append("------|");
        sb.AppendLine();

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
}
