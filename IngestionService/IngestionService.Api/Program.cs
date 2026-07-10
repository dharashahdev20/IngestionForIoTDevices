using IngestionService.Core.Aggregation;
using IngestionService.Core.Ingestion;

var builder = WebApplication.CreateBuilder(args);

// One store for the process lifetime, shared across all requests -
// registered as a singleton since it's internally thread-safe
// (ConcurrentDictionary + per-device locks).
builder.Services.AddSingleton<AggregatorStore>();

var app = builder.Build();

// POST /readings
// Accepts a JSON array of up to 50,000 readings. Streams directly off the
// request body's PipeReader - see ReadingStreamParser for why.
app.MapPost("/readings", async (HttpContext ctx, AggregatorStore store) =>
{
var accepted = await ReadingStreamParser.IngestAsync(
    ctx.Request.BodyReader,
    store,
    DateTime.UtcNow,
    ctx.RequestAborted);

return Results.Ok(new { accepted });
});

// GET /readings/{deviceId}/aggregate
// Returns count/min/max/average over the trailing 5-minute window.
app.MapGet("/readings/{deviceId}/aggregate", (string deviceId, AggregatorStore store) =>
{
if (!store.TryGetSnapshot(deviceId, DateTime.UtcNow, out var result))
{
return Results.NotFound(new { deviceId, message = "No readings in the active window." });
}

return Results.Ok(new
{
result.DeviceId,
result.Count,
result.Min,
result.Max,
result.Average
});
});

app.Run();

// Exposed for WebApplicationFactory-based integration tests.
public partial class Program { }

