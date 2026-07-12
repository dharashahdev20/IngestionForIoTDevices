using System.Collections.Concurrent;

namespace IngestionService.Core.Aggregation;

/// <summary>
/// Owns one DeviceAggregator per deviceId. ConcurrentDictionary gives us
/// lock-free-ish reads and fine-grained (bucketed internally) locking on
/// insert, so devices are independent of one another - this is the layer
/// that lets us avoid a single global lock across all ~10,000 devices.
/// </summary>
public sealed class AggregatorStore(TimeProvider timeProvider)
{
    private readonly TimeProvider _timeProvider = timeProvider;
    private readonly ConcurrentDictionary<string, DeviceAggregator> _devices = new();

    // Cached so GetOrAdd doesn't allocate a new closure/delegate per call
    // on the hot path.
    private static readonly Func<string, DeviceAggregator> CreateAggregator = _ => new DeviceAggregator();

    public void Ingest(string deviceId, DateTime timestampUtc, double value)
    {
        var nowUtc = _timeProvider.GetUtcNow().UtcDateTime;

        var aggregator = _devices.GetOrAdd(deviceId, CreateAggregator);
        aggregator.Add(timestampUtc, value, nowUtc);
    }

    public bool TryGetSnapshot(string deviceId, out AggregateResult result)
    {
        var nowUtc = _timeProvider.GetUtcNow().UtcDateTime;

        if (_devices.TryGetValue(deviceId, out var aggregator))
        {
            result = aggregator.Snapshot(deviceId, nowUtc);
            return result.HasData;
        }

        result = default;
        return false;
    }
}