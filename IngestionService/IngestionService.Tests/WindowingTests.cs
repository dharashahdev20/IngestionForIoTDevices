using IngestionService.Core.Aggregation;
using Xunit;

namespace IngestionService.Tests;

public class WindowingTests
{
    [Fact]
    public void Snapshot_WithNoReadings_ReturnsNoData()
    {
        var aggregator = new DeviceAggregator();
        var result = aggregator.Snapshot("d1", DateTime.UtcNow);

        Assert.False(result.HasData);
        Assert.Equal(0, result.Count);
    }

    [Fact]
    public void Snapshot_WithReadingsInsideWindow_AggregatesCorrectly()
    {
        var aggregator = new DeviceAggregator();
        var now = new DateTime(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc);

        aggregator.Add(now.AddSeconds(-10), 10, now);
        aggregator.Add(now.AddSeconds(-5), 20, now);
        aggregator.Add(now.AddSeconds(-1), 30, now);

        var result = aggregator.Snapshot("d1", now);

        Assert.True(result.HasData);
        Assert.Equal(3, result.Count);
        Assert.Equal(10, result.Min);
        Assert.Equal(30, result.Max);
        Assert.Equal(20, result.Average);
    }

    [Fact]
    public void Snapshot_ReadingJustOutsideWindow_IsExcluded()
    {
        var aggregator = new DeviceAggregator();
        var now = new DateTime(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc);

        // exactly 300s old -> outside the window (window is [now-300s, now))
        aggregator.Add(now.AddSeconds(-300), 999, now);
        var result = aggregator.Snapshot("d1", now);
        Assert.False(result.HasData);

        // 299s old -> just inside the window
        aggregator.Add(now.AddSeconds(-299), 42, now);
        result = aggregator.Snapshot("d1", now);
        Assert.True(result.HasData);
        Assert.Equal(1, result.Count);
        Assert.Equal(42, result.Average);
    }

    [Fact]
    public void Add_ReadingsArrivingOutOfOrder_StillAggregatedCorrectly()
    {
        var aggregator = new DeviceAggregator();
        var now = new DateTime(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc);

        // Arrive out of chronological order.
        aggregator.Add(now.AddSeconds(-2), 30, now);
        aggregator.Add(now.AddSeconds(-30), 10, now);
        aggregator.Add(now.AddSeconds(-15), 20, now);

        var result = aggregator.Snapshot("d1", now);

        Assert.Equal(3, result.Count);
        Assert.Equal(10, result.Min);
        Assert.Equal(30, result.Max);
        Assert.Equal(20, result.Average);
    }

    [Fact]
    public void Snapshot_AsWindowAdvances_OldReadingsAgeOut()
    {
        var aggregator = new DeviceAggregator();
        var t0 = new DateTime(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc);

        aggregator.Add(t0, 100, t0);

        var stillIn = aggregator.Snapshot("d1", t0.AddSeconds(299));
        Assert.True(stillIn.HasData);

        var agedOut = aggregator.Snapshot("d1", t0.AddSeconds(300));
        Assert.False(agedOut.HasData);
    }

    [Fact]
    public void Add_BucketReusedAfterFullRotation_OverwritesStaleData()
    {
        var aggregator = new DeviceAggregator();
        var t0 = new DateTime(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc);

        aggregator.Add(t0, 1, t0);

        // Exactly one full window later (300s), the same bucket index is
        // reused for a new second - old value must not leak into the sum.
        var t1 = t0.AddSeconds(300);
        aggregator.Add(t1, 500, t1);

        var result = aggregator.Snapshot("d1", t1);

        Assert.Equal(1, result.Count);
        Assert.Equal(500, result.Average);
    }

    [Fact]
    public void Add_ReadingOlderThanWindow_IsIgnored()
    {
        var aggregator = new DeviceAggregator();
        var now = new DateTime(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc);

        aggregator.Add(now.AddSeconds(-600), 999, now); // way outside window

        var result = aggregator.Snapshot("d1", now);
        Assert.False(result.HasData);
    }
}
