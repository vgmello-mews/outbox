using Npgsql;
using Outbox.Core.Builder;
using Outbox.EventHub;
using Outbox.PostgreSQL;

var builder = WebApplication.CreateSlimBuilder(args);

var connectionString = builder.Configuration.GetConnectionString("OutboxDb")
    ?? throw new InvalidOperationException("ConnectionStrings:OutboxDb is required.");

builder.Services.AddOutbox(builder.Configuration, outbox =>
{
    outbox.UsePostgreSql(async (sp, ct) =>
    {
        var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync(ct);
        return conn;
    });
    outbox.UseEventHub();
});

var app = builder.Build();
app.MapHealthChecks("/health/internal");
await app.RunAsync();
