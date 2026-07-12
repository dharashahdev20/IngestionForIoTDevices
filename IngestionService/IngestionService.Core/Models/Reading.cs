namespace IngestionService.Core.Models;

/// <summary>
/// A single device reading. Deliberately a struct: readings are parsed one at a
/// time on the hot path and applied immediately to an aggregator, so we never
/// need them to live on the heap or be boxed into a collection.
/// </summary>
public readonly struct AggregateResult
{
    public AggregateResult(string deviceId, DateTime timestampUtc, double value)
    {
        DeviceId = deviceId;
        TimestampUtc = timestampUtc;
        Value = value;
    }

    public string DeviceId { get; }
    public DateTime TimestampUtc { get; }
    public double Value { get; }
}
