using Outbox.PostgreSQL;
using Xunit;

namespace Outbox.Store.Tests;

public class PostgreSqlStoreOptionsTests
{
    [Fact]
    public void DefaultSchemaName_IsPublic()
    {
        var options = new PostgreSqlStoreOptions();
        Assert.Equal("public", options.SchemaName);
    }

    [Fact]
    public void DefaultCommandTimeoutSeconds_Is30()
    {
        var options = new PostgreSqlStoreOptions();
        Assert.Equal(30, options.CommandTimeoutSeconds);
    }

    [Fact]
    public void DefaultTransientRetryMaxAttempts_Is6()
    {
        var options = new PostgreSqlStoreOptions();
        Assert.Equal(6, options.TransientRetryMaxAttempts);
    }

    [Fact]
    public void DefaultTransientRetryBackoffMs_Is1000()
    {
        var options = new PostgreSqlStoreOptions();
        Assert.Equal(1000, options.TransientRetryBackoffMs);
    }

    [Theory]
    [InlineData("public")]
    [InlineData("outbox")]
    [InlineData("my_schema")]
    [InlineData("_private")]
    [InlineData("Schema123")]
    public void ValidSchemaName_SetsSuccessfully(string schemaName)
    {
        var options = new PostgreSqlStoreOptions();
        options.SchemaName = schemaName;
        Assert.Equal(schemaName, options.SchemaName);
    }

    [Theory]
    [InlineData("")]
    [InlineData("123abc")]
    [InlineData("my-schema")]
    [InlineData("my schema")]
    [InlineData("schema;DROP TABLE")]
    [InlineData("public.outbox")]
    public void InvalidSchemaName_ThrowsArgumentException(string schemaName)
    {
        var options = new PostgreSqlStoreOptions();
        Assert.Throws<ArgumentException>(() => options.SchemaName = schemaName);
    }

    [Fact]
    public void TablePrefix_DefaultsToEmpty()
    {
        var opts = new PostgreSqlStoreOptions();
        Assert.Equal("", opts.TablePrefix);
    }

    [Theory]
    [InlineData("orders_")]
    [InlineData("my_prefix_")]
    [InlineData("Foo")]
    [InlineData("_prefix")]
    public void TablePrefix_ValidPrefixes_Accepted(string prefix)
    {
        var opts = new PostgreSqlStoreOptions();
        opts.TablePrefix = prefix;
        Assert.Equal(prefix, opts.TablePrefix);
    }

    [Theory]
    [InlineData("123abc")]
    [InlineData("my-prefix")]
    [InlineData("prefix;DROP")]
    [InlineData("has space")]
    [InlineData("   ")]
    public void TablePrefix_InvalidPrefixes_ThrowsArgumentException(string prefix)
    {
        var opts = new PostgreSqlStoreOptions();
        Assert.Throws<ArgumentException>(() => opts.TablePrefix = prefix);
    }

    [Fact]
    public void TablePrefix_EmptyString_Allowed()
    {
        var opts = new PostgreSqlStoreOptions();
        opts.TablePrefix = "something";
        opts.TablePrefix = "";
        Assert.Equal("", opts.TablePrefix);
    }

    [Fact]
    public void TablePrefix_Null_ThrowsArgumentNullException()
    {
        var opts = new PostgreSqlStoreOptions();
        Assert.Throws<ArgumentNullException>(() => opts.TablePrefix = null!);
    }
}
