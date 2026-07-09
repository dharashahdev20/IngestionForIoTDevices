namespace IngestionService.Core.Aggregation;

/// <summary>
/// Snapshot of a device's rolling aggregates at the moment of the query.
/// This is a plain immutable value returned to callers - it is a copy,
/// never a reference into live aggregator state, so it can be handed to the
/// API layer and serialized without any further locking.
/// </summary>
public readonly record struct AggregateResult(
    string DeviceId,
    long Count,
    double Min,
    double Max,
    double Average,
    bool HasData);
