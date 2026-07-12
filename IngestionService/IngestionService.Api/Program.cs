using IngestionService.Core.Aggregation;
using IngestionService.Core.Ingestion;
using System.Diagnostics;
using System.Text.Json;
using IngestionService.Api.Extensions;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddEndpointsApiExplorer();

builder.Services.AddSwaggerGen(options =>
{
    options.SwaggerDoc("v1", new()
    {
        Title = "IoT Device Ingestion Service",
        Version = "v1",
        Description = "High-throughput ingestion service for IoT device."
    });
});

//with TimeProvider.System, we can use DateTime.UtcNow in the code, but for testing we can inject a mock time provider to control the current time.
builder.Services.AddSingleton(TimeProvider.System);

// One store for the process lifetime, shared across all requests -
// registered as a singleton since it's internally thread-safe
// (ConcurrentDictionary + per-device locks).
builder.Services.AddSingleton<AggregatorStore>();

var app = builder.Build();

app.Logger.LogInformation("IoT Ingestion Service started in {Environment}", app.Environment.EnvironmentName);

app.UseGlobalExceptionHandling();

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI(options =>
    {
        options.DocumentTitle = "IoT Ingestion API";
        options.SwaggerEndpoint("/swagger/v1/swagger.json", "IoT Ingestion API v1");
    });
}
app.MapGet("/", () => Results.Redirect("/swagger"));

// POST /readings
// Accepts a JSON array of up to 50,000 readings. Streams directly off the
// request body's PipeReader - see ReadingStreamParser for why.
app.MapPost("/readings", async (HttpContext ctx, AggregatorStore store, ILogger<Program> logger) =>
{
    logger.LogInformation("Received ingestion request from {RemoteIp}", ctx.Connection.RemoteIpAddress);

    var stopwatch = Stopwatch.StartNew();
            var accepted = await ReadingStreamParser.IngestAsync(
            ctx.Request.BodyReader,
            store,
            ctx.RequestAborted);

        stopwatch.Stop();

        logger.LogInformation("Accepted {AcceptedCount} readings in {ElapsedMilliseconds} ms",accepted,stopwatch.ElapsedMilliseconds);

        return Results.Accepted(value: new { accepted });
    
})
.WithName("IngestReadings")
.WithSummary("Ingest a stream of IoT device readings.")
.WithDescription(
    "Accepts a JSON array of readings and processes them using a streaming parser.")
.Produces(StatusCodes.Status202Accepted)
.Produces(StatusCodes.Status400BadRequest);

// GET /readings/{deviceId}/aggregate
// Returns count/min/max/average over the trailing 5-minute window.
app.MapGet("/readings/{deviceId}/aggregate", (string deviceId, AggregatorStore store, ILogger<Program> logger) =>
{
    if (!store.TryGetSnapshot(deviceId, out var result))
    {
        logger.LogWarning(
        "Statistics requested for unknown device {DeviceId}",
        deviceId);

        return Results.NotFound(new { deviceId, message = "No readings in the active window." });
    }

    logger.LogInformation("Statistics returned for device {DeviceId}",deviceId);

    return Results.Ok(new
    {
        result.DeviceId,
        result.Count,
        result.Min,
        result.Max,
        result.Average
    });
})
 .WithName("GetDeviceStatistics")
    .WithSummary("Returns the current five-minute statistics for a device.")
    .Produces<AggregateResult>(StatusCodes.Status200OK)
    .Produces(StatusCodes.Status404NotFound);


app.Run();

// Exposed for WebApplicationFactory-based integration tests.
public partial class Program { }

