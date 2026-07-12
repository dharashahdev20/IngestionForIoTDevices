namespace IngestionService.Core.Aggregation;

/// <summary>
/// Maintains rolling count/min/max/sum for a single device over a 5-minute
/// window, approximated as 300 one-second buckets in a fixed circular array.
///
/// Why buckets instead of storing raw readings:
///   - Storing every raw reading and evicting individually (e.g. via a deque)
///     means unbounded memory proportional to traffic, plus per-reading
///     eviction bookkeeping under lock.
///   - 300 fixed-size buckets give O(1) ingestion, O(1) eviction (a bucket
///     is simply overwritten once its second rolls back around), and a hard
///     memory ceiling per device regardless of throughput.
///   - Cost: precision is to the nearest second rather than exact
///     millisecond eviction. For a 5-minute rolling window over dashboards /
///     alerting, sub-second precision is not meaningfully useful - this is
///     the "bucketed approximation" tradeoff called out as acceptable in
///     the spec.
///
/// Concurrency: a single lock guards this device's 300-slot array. Writes
/// are O(1) inside the lock; reads are O(300) inside the lock. Because each
/// device has its own instance, contention only occurs when the SAME device
/// is written/read concurrently, which is rare relative to ~10,000 devices.
/// A plain Monitor lock (via C# `lock`) was chosen over ReaderWriterLockSlim
/// because the critical sections are extremely short (no I/O, no allocation)
/// - the extra bookkeeping RWLS needs to track reader/writer state costs more
/// than the exclusive lock it would save you from, at these hold times.
/// </summary>
public sealed class DeviceAggregator
{
    private const int WindowSeconds = 300; // 5 minutes

    // Sentinel meaning "this slot has never been written / is not
    // currently representing any second". Chosen instead of 0 because
    // epoch-second 0 (1970-01-01) is a legitimate, if unlikely, value.
    private const long Empty = long.MinValue;

    private struct Bucket
    {
        public long EpochSecond;
        public long Count;
        public double Sum;
        public double Min;
        public double Max;
    }

    private readonly Bucket[] _buckets;
    private readonly object _gate = new();

    public DeviceAggregator()
    {
        _buckets = new Bucket[WindowSeconds];
        for (var i = 0; i < WindowSeconds; i++)
        {
            _buckets[i].EpochSecond = Empty;
        }
    }

    /// <summary>
    /// Applies one reading. Readings whose timestamp already fell outside
    /// the window relative to "now" are silently dropped - they would never
    /// be visible in any query anyway, so accepting them would only waste
    /// work and briefly corrupt a bucket a live reading might reuse.
    /// </summary>
    public void Add(DateTime timestampUtc, double value, DateTime nowUtc)
    {
        var epoch = ToEpochSeconds(timestampUtc);
        var nowEpoch = ToEpochSeconds(nowUtc);

        if (nowEpoch - epoch >= WindowSeconds || epoch > nowEpoch)
        {
            // Too old to matter, or clock-skewed into the future - ignore
            // rather than let it corrupt a bucket that belongs to "now".
            return;
        }

        var index = (int)(((epoch % WindowSeconds) + WindowSeconds) % WindowSeconds);

        lock (_gate)
        {
            ref var bucket = ref _buckets[index];

            if (bucket.EpochSecond != epoch)
            {
                // This slot currently represents a different second (either
                // stale from ~5 minutes ago, or never used) - reinitialize
                // it to represent this reading's second.
                bucket.EpochSecond = epoch;
                bucket.Count = 0;
                bucket.Sum = 0;
                bucket.Min = value;
                bucket.Max = value;
            }

            bucket.Count++;
            bucket.Sum += value;
            if (value < bucket.Min) bucket.Min = value;
            if (value > bucket.Max) bucket.Max = value;
        }
    }

    /// <summary>
    /// Computes the current rolling aggregate by summing only buckets that
    /// still fall inside the window relative to "now". This is what
    /// naturally "ages out" old data without ever needing a background
    /// sweep or timer - a bucket that has rolled out of the window is just
    /// skipped here, and will be overwritten in place the next time its
    /// second comes back around.
    /// </summary>
    public AggregateResult Snapshot(string deviceId, DateTime nowUtc)
    {
        var nowEpoch = ToEpochSeconds(nowUtc);

        long count = 0;
        double sum = 0;
        var min = double.PositiveInfinity;
        var max = double.NegativeInfinity;

        lock (_gate)
        {
            for (var i = 0; i < WindowSeconds; i++)
            {
                ref var bucket = ref _buckets[i];
                if (bucket.EpochSecond == Empty) continue;
                if (nowEpoch - bucket.EpochSecond >= WindowSeconds) continue; // aged out
                if (bucket.EpochSecond > nowEpoch) continue; // clock skew guard

                count += bucket.Count;
                sum += bucket.Sum;
                if (bucket.Min < min) min = bucket.Min;
                if (bucket.Max > max) max = bucket.Max;
            }
        }

        if (count == 0)
        {
            return new AggregateResult(deviceId, 0, 0, 0, 0, HasData: false);
        }

        return new AggregateResult(deviceId, count, min, max, sum / count, HasData: true);
    }

    private static long ToEpochSeconds(DateTime utc) =>
        new DateTimeOffset(DateTime.SpecifyKind(utc, DateTimeKind.Utc)).ToUnixTimeSeconds();
}
