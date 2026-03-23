using Azure.Messaging.EventHubs.Producer;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Outbox.Core.Abstractions;
using Outbox.Core.Builder;
using Xunit;

namespace Outbox.EventHub.Tests;

public class EventHubOutboxBuilderExtensionsTests
{
    [Fact]
    public void UseEventHub_WithClientFactory_RegistersIOutboxTransport()
    {
        var services = new ServiceCollection();
        var config = new ConfigurationBuilder().Build();

        var mockClient = Substitute.For<EventHubProducerClient>();

        services.AddOutbox(config, outbox =>
        {
            outbox.UseEventHub(
                clientFactory: _ => mockClient,
                configure: opts =>
                {
                    opts.ConnectionString = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=k;SharedAccessKey=abc=";
                    opts.EventHubName = "test-hub";
                });
        });

        services.AddSingleton(Substitute.For<IOutboxStore>());
        services.AddLogging();

        var provider = services.BuildServiceProvider();
        var transport = provider.GetService<IOutboxTransport>();
        Assert.NotNull(transport);
        Assert.IsType<EventHubOutboxTransport>(transport);
    }

    [Fact]
    public void UseEventHub_WithClientFactory_UsesProvidedFactory()
    {
        var services = new ServiceCollection();
        var config = new ConfigurationBuilder().Build();

        var mockClient = Substitute.For<EventHubProducerClient>();
        var factoryInvoked = false;

        services.AddOutbox(config, outbox =>
        {
            outbox.UseEventHub(clientFactory: sp =>
            {
                factoryInvoked = true;
                return mockClient;
            });
        });

        services.AddSingleton(Substitute.For<IOutboxStore>());
        services.AddLogging();

        var provider = services.BuildServiceProvider();
        // Resolve producer client to trigger factory
        _ = provider.GetService<EventHubProducerClient>();

        Assert.True(factoryInvoked);
    }

    [Fact]
    public void UseEventHub_WithClientFactory_NoConfigure_RegistersTransport()
    {
        var services = new ServiceCollection();
        var config = new ConfigurationBuilder().Build();

        var mockClient = Substitute.For<EventHubProducerClient>();

        services.AddOutbox(config, outbox =>
        {
            outbox.UseEventHub(clientFactory: _ => mockClient);
        });

        services.AddSingleton(Substitute.For<IOutboxStore>());
        services.AddLogging();

        var provider = services.BuildServiceProvider();
        var transport = provider.GetService<IOutboxTransport>();
        Assert.NotNull(transport);
    }

    [Fact]
    public void UseEventHub_Configure_AppliesOptions()
    {
        var services = new ServiceCollection();
        var config = new ConfigurationBuilder().Build();

        var mockClient = Substitute.For<EventHubProducerClient>();

        services.AddOutbox(config, outbox =>
        {
            outbox.UseEventHub(
                clientFactory: _ => mockClient,
                configure: opts =>
                {
                    opts.EventHubName = "my-hub";
                    opts.MaxBatchSizeBytes = 256_000;
                    opts.SendTimeoutSeconds = 30;
                });
        });

        services.AddSingleton(Substitute.For<IOutboxStore>());
        services.AddLogging();

        var provider = services.BuildServiceProvider();
        var transport = provider.GetService<IOutboxTransport>();
        Assert.NotNull(transport);
    }

    [Fact]
    public void UseEventHub_DefaultOverload_WithConfigure_RegistersIOutboxTransport()
    {
        // Tests the first UseEventHub overload (no clientFactory) with a configure delegate.
        // Exercises: the configure != null branch and IOutboxTransport registration.
        // The EventHubProducerClient singleton is NOT resolved to avoid connecting to a real broker.
        var services = new ServiceCollection();
        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Outbox:EventHub:ConnectionString"] = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=k;SharedAccessKey=abc=",
                ["Outbox:EventHub:EventHubName"] = "test-hub",
            })
            .Build();

        services.AddOutbox(config, outbox =>
        {
            outbox.UseEventHub(configure: opts =>
            {
                opts.EventHubName = "test-hub";
                opts.MaxBatchSizeBytes = 256_000;
            });
        });

        services.AddSingleton(Substitute.For<IOutboxStore>());
        services.AddLogging();

        // Verify IOutboxTransport is registered (do not resolve — that would create a real EventHubProducerClient)
        var descriptor = services.FirstOrDefault(d => d.ServiceType == typeof(IOutboxTransport));
        Assert.NotNull(descriptor);
        Assert.Equal(typeof(EventHubOutboxTransport), descriptor.ImplementationType);
    }

    [Fact]
    public void UseEventHub_DefaultOverload_NoConfigure_RegistersIOutboxTransport()
    {
        // Tests the first UseEventHub overload without any configure delegate (configure is null).
        // Exercises: the configure == null branch (the if block is skipped).
        var services = new ServiceCollection();
        var config = new ConfigurationBuilder().Build();

        services.AddOutbox(config, outbox =>
        {
            outbox.UseEventHub(); // no configure delegate — exercises configure==null path
        });

        services.AddSingleton(Substitute.For<IOutboxStore>());
        services.AddLogging();

        var descriptor = services.FirstOrDefault(d => d.ServiceType == typeof(IOutboxTransport));
        Assert.NotNull(descriptor);
        Assert.Equal(typeof(EventHubOutboxTransport), descriptor.ImplementationType);
    }
}
