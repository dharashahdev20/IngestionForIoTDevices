using IngestionService.Core.Aggregation;
using Xunit;

namespace IngestionService.Tests;

public class ConcurrencyTests
{
    [Fact]
    public async Task ConcurrentWrites_ToSameDevice_NoLostUpdates()
    {
        var store = new AggregatorStore();
        var now = DateTime.UtcNow;
        const int writersCount = 32;
        const int writesPerWriter = 2_000;

        var tasks = Enumerable.Range(0, writersCount).Select(_ => Task.Run(() =>
        {
            for (var i = 0; i < writesPerWriter; i++)
            {
                // Spread across the last ~250s so writes land in different
                // buckets and some collide - exercising both the "new
                // bucket" and "existing bucket" code paths concurrently.
                var ts = now.AddSeconds(-(i % 250));
                store.Ingest("device-A", ts, 1.0, now);
            }
        })).ToArray();

        await Task.WhenAll(tasks);

        Assert.True(store.TryGetSnapshot("device-A", now, out var result));
        Assert.Equal(writersCount * writesPerWriter, result.Count);
    }

    [Fact]
    public async Task ConcurrentWrites_AndReads_DoNotObserveTornState()
    {
        var store = new AggregatorStore();
        var now = DateTime.UtcNow;
        using var cts = new CancellationTokenSource();

        var writer = Task.Run(() =>
        {
            for (var i = 0; i < 50_000 && !cts.IsCancellationRequested; i++)
            {
                store.Ingest("device-B", now.AddSeconds(-(i % 250)), i, now);
            }
        });

        var readerErrors = new List<string>();
        var reader = Task.Run(() =>
        {
            for (var i = 0; i < 5_000; i++)
            {
                if (store.TryGetSnapshot("device-B", now, out var snap))
                {
                    // Invariant that must hold no matter when we sample:
                    // min <= average <= max, count > 0.
                    if (snap.Count <= 0 || snap.Min > snap.Average || snap.Average > snap.Max)
                    {
                        readerErrors.Add($"Torn read: count={snap.Count} min={snap.Min} avg={snap.Average} max={snap.Max}");
                    }
                }
            }
        });

        await writer;
        cts.Cancel();
        await reader;

        Assert.Empty(readerErrors);
    }

    [Fact]
    public async Task ConcurrentWrites_AcrossManyDevices_EachDeviceIndependentlyCorrect()
    {
        var store = new AggregatorStore();
        var now = DateTime.UtcNow;
        const int deviceCount = 100;
        const int writesPerDevice = 500;

        var tasks = Enumerable.Range(0, deviceCount).Select(d => Task.Run(() =>
        {
            var deviceId = $"device-{d}";
            for (var i = 0; i < writesPerDevice; i++)
            {
                store.Ingest(deviceId, now, 1.0, now);
            }
        })).ToArray();

        await Task.WhenAll(tasks);

        for (var d = 0; d < deviceCount; d++)
        {
            Assert.True(store.TryGetSnapshot($"device-{d}", now, out var result));
            Assert.Equal(writesPerDevice, result.Count);
        }
    }
}
