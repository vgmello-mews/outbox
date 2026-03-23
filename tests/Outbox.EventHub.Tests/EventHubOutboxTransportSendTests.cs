using System.Text;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Outbox.Core.Abstractions;
using Outbox.Core.Models;
using Xunit;

namespace Outbox.EventHub.Tests;

public class EventHubOutboxTransportSendTests
{
    private static EventHubOutboxTransport CreateTransport(
        string eventHubName = "test-hub",
        EventHubProducerClient? client = null,
        IEnumerable<ITransportMessageInterceptor<EventData>>? interceptors = null)
    {
        client ??= Substitute.For<EventHubProducerClient>();
        var options = Options.Create(new EventHubTransportOptions
        {
            EventHubName = eventHubName,
            MaxBatchSizeBytes = 1_048_576,
            SendTimeoutSeconds = 15,
            ConnectionString = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=key;SharedAccessKey=abc="
        });
        var logger = NullLogger<EventHubOutboxTransport>.Instance;
        return new EventHubOutboxTransport(client, options, logger,
            interceptors ?? Enumerable.Empty<ITransportMessageInterceptor<EventData>>());
    }

    private static OutboxMessage CreateMessage(string topicName = "test-hub") =>
        new OutboxMessage(
            SequenceNumber: 1L,
            TopicName: topicName,
            PartitionKey: "pk1",
            EventType: "TestEvent",
            Headers: null,
            Payload: Encoding.UTF8.GetBytes("{}"),
            PayloadContentType: "application/json",
            EventDateTimeUtc: DateTimeOffset.UtcNow,
            EventOrdinal: 0,
            RetryCount: 0,
            CreatedAtUtc: DateTimeOffset.UtcNow);

    [Fact]
    public async Task SendAsync_WrongTopicName_ThrowsInvalidOperationException()
    {
        var transport = CreateTransport(eventHubName: "test-hub");
        var messages = new[] { CreateMessage(topicName: "wrong-hub") };

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => transport.SendAsync("wrong-hub", "pk1", messages, CancellationToken.None));
    }

    [Fact]
    public async Task SendAsync_MatchingTopicName_DoesNotThrowTopicMismatch()
    {
        // Verifies no InvalidOperationException is thrown for topic name validation.
        // Note: EventDataBatch has no public constructor, making full send-path mocking
        // impractical. This test validates that topic name matching succeeds and only
        // fails when CreateBatchAsync is not set up (NSubstitute returns null by default).
        var client = Substitute.For<EventHubProducerClient>();
        var transport = CreateTransport(eventHubName: "test-hub", client: client);
        var messages = new[] { CreateMessage(topicName: "test-hub") };

        // The call will fail at CreateBatchAsync (null return from substitute),
        // but NOT with an InvalidOperationException from topic name validation.
        var ex = await Record.ExceptionAsync(
            () => transport.SendAsync("test-hub", "pk1", messages, CancellationToken.None));

        Assert.False(
            ex is InvalidOperationException ioe &&
            ioe.Message.Contains("transport is configured for"),
            "Should not throw a topic-mismatch InvalidOperationException");
    }

    [Fact]
    public async Task DisposeAsync_ReturnsCompletedTask()
    {
        // Cover lines 117-120: DisposeAsync returns ValueTask.CompletedTask without disposing client.
        var client = Substitute.For<EventHubProducerClient>();
        var transport = CreateTransport(eventHubName: "test-hub", client: client);

        await transport.DisposeAsync();

        // Client must NOT be disposed (it's owned by DI)
        await client.DidNotReceive().CloseAsync(Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task SendAsync_EmptyTopicName_DoesNotThrowTopicMismatch()
    {
        // Cover the branch where topicName is empty — the topic-mismatch check is skipped.
        var client = Substitute.For<EventHubProducerClient>();
        var transport = CreateTransport(eventHubName: "test-hub", client: client);
        var messages = new[] { CreateMessage(topicName: "") };

        // Verify that empty topic skips the mismatch check. The substitute returns null
        // from CreateBatchAsync, so a different exception is expected (not topic validation).
        var ex = await Record.ExceptionAsync(
            () => transport.SendAsync("", "pk1", messages, CancellationToken.None));

        Assert.False(
            ex is InvalidOperationException ioe &&
            ioe.Message.Contains("transport is configured for"),
            "Empty topic should skip the mismatch check");
    }

    [Fact]
    public async Task SendAsync_NullConfiguredEventHubName_DoesNotThrowTopicMismatch()
    {
        // Cover the branch where _configuredEventHubName is null/empty — mismatch check is skipped.
        var client = Substitute.For<EventHubProducerClient>();
        var transport = CreateTransport(eventHubName: "", client: client);
        var messages = new[] { CreateMessage(topicName: "any-topic") };

        var ex = await Record.ExceptionAsync(
            () => transport.SendAsync("any-topic", "pk1", messages, CancellationToken.None));

        Assert.False(
            ex is InvalidOperationException ioe &&
            ioe.Message.Contains("transport is configured for"),
            "Empty configured EventHub name should skip the mismatch check");
    }

    [Fact]
    public async Task SendAsync_WhenCreateBatchAsyncThrows_ExceptionPropagatesWithoutPartialSend()
    {
        // Cover the exception path before any messages are sent.
        // When CreateBatchAsync throws, sentSequenceNumbers is empty so no PartialSendException
        // is thrown — the original exception propagates directly.
        var client = Substitute.For<EventHubProducerClient>();
        client.CreateBatchAsync(
                Arg.Any<CreateBatchOptions>(),
                Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("EventHub unavailable"));

        var transport = CreateTransport(eventHubName: "test-hub", client: client);
        var messages = new[] { CreateMessage(topicName: "test-hub") };

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => transport.SendAsync("test-hub", "pk1", messages, CancellationToken.None));

        Assert.Equal("EventHub unavailable", ex.Message);
        // Must NOT be wrapped in PartialSendException since nothing was sent
        Assert.IsNotType<PartialSendException>(ex);
    }
}
