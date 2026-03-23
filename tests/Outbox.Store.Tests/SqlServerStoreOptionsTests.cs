using Outbox.SqlServer;
using Xunit;

namespace Outbox.Store.Tests;

public class SqlServerStoreOptionsTests
{
    [Fact]
    public void DefaultSchemaName_IsDbo()
    {
        var options = new SqlServerStoreOptions();
        Assert.Equal("dbo", options.SchemaName);
    }

    [Fact]
    public void DefaultCommandTimeoutSeconds_Is30()
    {
        var options = new SqlServerStoreOptions();
        Assert.Equal(30, options.CommandTimeoutSeconds);
    }

    [Fact]
    public void DefaultTransientRetryMaxAttempts_Is6()
    {
        var options = new SqlServerStoreOptions();
        Assert.Equal(6, options.TransientRetryMaxAttempts);
    }

    [Fact]
    public void DefaultTransientRetryBackoffMs_Is1000()
    {
        var options = new SqlServerStoreOptions();
        Assert.Equal(1000, options.TransientRetryBackoffMs);
    }

    [Theory]
    [InlineData("dbo")]
    [InlineData("outbox")]
    [InlineData("my_schema")]
    [InlineData("_private")]
    [InlineData("Schema123")]
    public void ValidSchemaName_SetsSuccessfully(string schemaName)
    {
        var options = new SqlServerStoreOptions();
        options.SchemaName = schemaName;
        Assert.Equal(schemaName, options.SchemaName);
    }

    [Theory]
    [InlineData("")]
    [InlineData("123abc")]
    [InlineData("my-schema")]
    [InlineData("my space")]
    [InlineData("schema;DROP TABLE")]
    [InlineData("public.outbox")]
    public void InvalidSchemaName_ThrowsArgumentException(string schemaName)
    {
        var options = new SqlServerStoreOptions();
        Assert.Throws<ArgumentException>(() => options.SchemaName = schemaName);
    }

    [Fact]
    public void TablePrefix_DefaultsToEmpty()
    {
        var opts = new SqlServerStoreOptions();
        Assert.Equal("", opts.TablePrefix);
    }

    [Theory]
    [InlineData("Orders")]
    [InlineData("my_prefix_")]
    [InlineData("Foo")]
    [InlineData("_prefix")]
    public void TablePrefix_ValidPrefixes_Accepted(string prefix)
    {
        var opts = new SqlServerStoreOptions();
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
        var opts = new SqlServerStoreOptions();
        Assert.Throws<ArgumentException>(() => opts.TablePrefix = prefix);
    }

    [Fact]
    public void TablePrefix_EmptyString_Allowed()
    {
        var opts = new SqlServerStoreOptions();
        opts.TablePrefix = "Something";
        opts.TablePrefix = "";
        Assert.Equal("", opts.TablePrefix);
    }

    [Fact]
    public void TablePrefix_Null_ThrowsArgumentNullException()
    {
        var opts = new SqlServerStoreOptions();
        Assert.Throws<ArgumentNullException>(() => opts.TablePrefix = null!);
    }
}
