using System.Buffers;
using System.IO.Pipelines;
using System.Text.Json;
using IngestionService.Core.Aggregation;

namespace IngestionService.Core.Ingestion;

/// <summary>
/// Parses a JSON array of readings straight off the request body's PipeReader
/// using Utf8JsonReader, applying each reading to the AggregatorStore as
/// soon as it is parsed.
///
/// Why this shape instead of System.Text.Json.JsonSerializer.DeserializeAsync
/// &lt;List&lt;Reading&gt;&gt;:
///   - DeserializeAsync&lt;List&lt;Reading&gt;&gt; would materialize up to 50,000
///     Reading objects (or a 50k-element List) purely to throw them away
///     immediately after use - all allocation, no benefit.
///   - Utf8JsonReader is a ref struct that reads tokens directly from the
///     buffer segments handed to it by the PipeReader, with no intermediate
///     copy of the request body and no per-reading heap object beyond the
///     device-id string itself (which we need to keep anyway as the
///     dictionary key).
///   - The only unavoidable per-reading allocation is the deviceId string,
///     because ConcurrentDictionary&lt;string, ...&gt; needs a string key. A
///     small string-interning cache could remove even that for the common
///     case of ~10,000 repeating device ids; noted as a follow-up in the
///     design doc rather than built here, to avoid over-engineering before
///     measuring whether it earns its keep.
/// </summary>
public static class ReadingStreamParser
{
    public static async Task<long> IngestAsync(
        PipeReader body,
        AggregatorStore store,
        DateTime nowUtc,
        CancellationToken cancellationToken)
    {
        long accepted = 0;
        var state = new JsonReaderState(new JsonReaderOptions { AllowTrailingCommas = false });

        // A reading object can be split across two pipe reads (e.g. the
        // buffer ends mid-way through `{"deviceId":"d1","time` ). Partial
        // field state therefore has to survive across ProcessBuffer calls,
        // not just across tokens within one call.
        var partial = new PartialReading();

        while (true)
        {
            var result = await body.ReadAsync(cancellationToken).ConfigureAwait(false);
            var buffer = result.Buffer;

            var consumed = ProcessBuffer(ref buffer, ref state, result.IsCompleted, store, nowUtc, ref accepted, ref partial);

            body.AdvanceTo(consumed, buffer.End);

            if (result.IsCompleted)
            {
                break;
            }
        }

        return accepted;
    }

    private struct PartialReading
    {
        public string? DeviceId;
        public DateTime Timestamp;
        public double Value;
        public bool HaveDeviceId;
        public bool HaveTimestamp;
        public bool HaveValue;

        public void Reset()
        {
            DeviceId = null;
            HaveDeviceId = false;
            HaveTimestamp = false;
            HaveValue = false;
        }
    }

    private static SequencePosition ProcessBuffer(
        ref ReadOnlySequence<byte> buffer,
        ref JsonReaderState state,
        bool isFinalBlock,
        AggregatorStore store,
        DateTime nowUtc,
        ref long accepted,
        ref PartialReading partial)
    {
        var reader = new Utf8JsonReader(buffer, isFinalBlock, state);

        while (reader.Read())
        {
            switch (reader.TokenType)
            {
                case JsonTokenType.PropertyName:
                    {
                        // reader.Read() below advances to the value token.
                        var propertyName = reader.ValueSpan;
                        reader.Read();

                        if (propertyName.SequenceEqual("deviceId"u8))
                        {
                            partial.DeviceId = reader.GetString();
                            partial.HaveDeviceId = partial.DeviceId is not null;
                        }
                        else if (propertyName.SequenceEqual("timestamp"u8))
                        {
                            partial.Timestamp = reader.GetDateTime().ToUniversalTime();
                            partial.HaveTimestamp = true;
                        }
                        else if (propertyName.SequenceEqual("value"u8))
                        {
                            partial.Value = reader.GetDouble();
                            partial.HaveValue = true;
                        }
                        break;
                    }
                case JsonTokenType.EndObject:
                    {
                        if (partial.HaveDeviceId && partial.HaveTimestamp && partial.HaveValue)
                        {
                            store.Ingest(partial.DeviceId!, partial.Timestamp, partial.Value, nowUtc);
                            accepted++;
                        }

                        partial.Reset();
                        break;
                    }
            }
        }

        state = reader.CurrentState;
        return buffer.GetPosition(reader.BytesConsumed);
    }
}