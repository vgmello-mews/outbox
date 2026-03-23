using Microsoft.Extensions.Options;
using Outbox.Core.Options;
using Xunit;

namespace Outbox.Core.Tests;

public class OutboxPublisherOptionsValidatorTests
{
    private static OutboxPublisherOptions ValidOptions() => new()
    {
        BatchSize = 100,
        MaxRetryCount = 10,
        CircuitBreakerFailureThreshold = 3,
        CircuitBreakerOpenDurationSeconds = 30,
        LeaseDurationSeconds = 45,
        PartitionGracePeriodSeconds = 60,
        HeartbeatIntervalMs = 10_000,
        HeartbeatTimeoutSeconds = 30,
        MinPollIntervalMs = 100,
        MaxPollIntervalMs = 5000,
        RebalanceIntervalMs = 30_000,
        OrphanSweepIntervalMs = 60_000,
        DeadLetterSweepIntervalMs = 60_000,
    };

    private static ValidateOptionsResult Validate(OutboxPublisherOptions options)
    {
        var validator = new OutboxPublisherOptionsValidator();
        return validator.Validate(null, options);
    }

    [Fact]
    public void ValidOptions_ReturnsSuccess()
    {
        var result = Validate(ValidOptions());
        Assert.True(result.Succeeded);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void BatchSize_Invalid_ReturnsFailure(int value)
    {
        var options = ValidOptions();
        options.BatchSize = value;
        var result = Validate(options);
        Assert.True(result.Failed);
        Assert.Contains("BatchSize", result.FailureMessage);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void LeaseDurationSeconds_Invalid_ReturnsFailure(int value)
    {
        var options = ValidOptions();
        options.LeaseDurationSeconds = value;
        var result = Validate(options);
        Assert.True(result.Failed);
        Assert.Contains("LeaseDurationSeconds", result.FailureMessage);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void MaxRetryCount_Invalid_ReturnsFailure(int value)
    {
        var options = ValidOptions();
        options.MaxRetryCount = value;
        var result = Validate(options);
        Assert.True(result.Failed);
        Assert.Contains("MaxRetryCount", result.FailureMessage);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void MinPollIntervalMs_Invalid_ReturnsFailure(int value)
    {
        var options = ValidOptions();
        options.MinPollIntervalMs = value;
        var result = Validate(options);
        Assert.True(result.Failed);
        Assert.Contains("MinPollIntervalMs", result.FailureMessage);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void MaxPollIntervalMs_Invalid_ReturnsFailure(int value)
    {
        var options = ValidOptions();
        options.MaxPollIntervalMs = value;
        var result = Validate(options);
        Assert.True(result.Failed);
        Assert.Contains("MaxPollIntervalMs", result.FailureMessage);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void HeartbeatIntervalMs_Invalid_ReturnsFailure(int value)
    {
        var options = ValidOptions();
        options.HeartbeatIntervalMs = value;
        var result = Validate(options);
        Assert.True(result.Failed);
        Assert.Contains("HeartbeatIntervalMs", result.FailureMessage);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void HeartbeatTimeoutSeconds_Invalid_ReturnsFailure(int value)
    {
        var options = ValidOptions();
        options.HeartbeatTimeoutSeconds = value;
        var result = Validate(options);
        Assert.True(result.Failed);
        Assert.Contains("HeartbeatTimeoutSeconds", result.FailureMessage);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void PartitionGracePeriodSeconds_Invalid_ReturnsFailure(int value)
    {
        var options = ValidOptions();
        options.PartitionGracePeriodSeconds = value;
        var result = Validate(options);
        Assert.True(result.Failed);
        Assert.Contains("PartitionGracePeriodSeconds", result.FailureMessage);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void RebalanceIntervalMs_Invalid_ReturnsFailure(int value)
    {
        var options = ValidOptions();
        options.RebalanceIntervalMs = value;
        var result = Validate(options);
        Assert.True(result.Failed);
        Assert.Contains("RebalanceIntervalMs", result.FailureMessage);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void OrphanSweepIntervalMs_Invalid_ReturnsFailure(int value)
    {
        var options = ValidOptions();
        options.OrphanSweepIntervalMs = value;
        var result = Validate(options);
        Assert.True(result.Failed);
        Assert.Contains("OrphanSweepIntervalMs", result.FailureMessage);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void DeadLetterSweepIntervalMs_Invalid_ReturnsFailure(int value)
    {
        var options = ValidOptions();
        options.DeadLetterSweepIntervalMs = value;
        var result = Validate(options);
        Assert.True(result.Failed);
        Assert.Contains("DeadLetterSweepIntervalMs", result.FailureMessage);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void CircuitBreakerFailureThreshold_Invalid_ReturnsFailure(int value)
    {
        var options = ValidOptions();
        options.CircuitBreakerFailureThreshold = value;
        var result = Validate(options);
        Assert.True(result.Failed);
        Assert.Contains("CircuitBreakerFailureThreshold", result.FailureMessage);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void CircuitBreakerOpenDurationSeconds_Invalid_ReturnsFailure(int value)
    {
        var options = ValidOptions();
        options.CircuitBreakerOpenDurationSeconds = value;
        var result = Validate(options);
        Assert.True(result.Failed);
        Assert.Contains("CircuitBreakerOpenDurationSeconds", result.FailureMessage);
    }

    [Fact]
    public void MaxPollIntervalMs_LessThanMinPollIntervalMs_ReturnsFailure()
    {
        var options = ValidOptions();
        options.MinPollIntervalMs = 5000;
        options.MaxPollIntervalMs = 100;
        var result = Validate(options);
        Assert.True(result.Failed);
        Assert.Contains("MaxPollIntervalMs must be >= MinPollIntervalMs", result.FailureMessage);
    }

    [Fact]
    public void CrossField_PartitionGracePeriodLessThanLeaseDuration_ReturnsFailure()
    {
        var options = ValidOptions();
        options.LeaseDurationSeconds = 60;
        options.PartitionGracePeriodSeconds = 45; // less than LeaseDurationSeconds
        var result = Validate(options);
        Assert.True(result.Failed);
        Assert.Contains("PartitionGracePeriodSeconds", result.FailureMessage);
        Assert.Contains("LeaseDurationSeconds", result.FailureMessage);
    }

    [Fact]
    public void CrossField_PartitionGracePeriodEqualToLeaseDuration_ReturnsFailure()
    {
        var options = ValidOptions();
        options.LeaseDurationSeconds = 60;
        options.PartitionGracePeriodSeconds = 60; // equal — must be strictly greater
        var result = Validate(options);
        Assert.True(result.Failed);
        Assert.Contains("PartitionGracePeriodSeconds", result.FailureMessage);
        Assert.Contains("strictly greater", result.FailureMessage);
    }

    [Fact]
    public void CrossField_HeartbeatTimeoutTooShort_ReturnsFailure()
    {
        var options = ValidOptions();
        // HeartbeatTimeoutSeconds * 1000 < HeartbeatIntervalMs * 3
        // e.g. 10s timeout, 5000ms interval → 10000 < 15000
        options.HeartbeatIntervalMs = 5000;
        options.HeartbeatTimeoutSeconds = 10;
        var result = Validate(options);
        Assert.True(result.Failed);
        Assert.Contains("HeartbeatTimeoutSeconds", result.FailureMessage);
        Assert.Contains("HeartbeatIntervalMs", result.FailureMessage);
    }

    [Fact]
    public void CrossField_MaxRetryCountNotGreaterThanCircuitBreakerThreshold_ReturnsFailure()
    {
        var options = ValidOptions();
        options.MaxRetryCount = 3;
        options.CircuitBreakerFailureThreshold = 3; // MaxRetryCount <= threshold
        var result = Validate(options);
        Assert.True(result.Failed);
        Assert.Contains("MaxRetryCount", result.FailureMessage);
        Assert.Contains("CircuitBreakerFailureThreshold", result.FailureMessage);
    }
}
