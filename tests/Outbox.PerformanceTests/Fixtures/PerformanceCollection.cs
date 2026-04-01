using Xunit;

namespace Outbox.PerformanceTests.Fixtures;

[CollectionDefinition(Name)]
public class PerformanceCollection : ICollectionFixture<PerformanceFixture>
{
    public const string Name = "Performance";
}
