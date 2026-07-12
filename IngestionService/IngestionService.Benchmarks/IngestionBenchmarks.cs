using System.IO.Pipelines;
using System.Text;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using IngestionService.Core.Aggregation;
using IngestionService.Core.Ingestion;

BenchmarkRunner.Run<IngestionBenchmarks>();

[MemoryDiagnoser]
[ThreadingDiagnoser]
public class IngestionBenchmarks
{
    private byte[] _payload50k = Array.Empty<byte>();
    private byte[] _payload1k = Array.Empty<byte>();

    [GlobalSetup]
    public void Setup()
    {
        _payload50k = BuildPayload(readingCount: 50_000, deviceCount: 10_000);
        _payload1k = BuildPayload(readingCount: 1_000, deviceCount: 200);
    }

    [Benchmark(Description = "Ingest 50,000 readings (single request, streaming parse)")]
    public async Task<long> Ingest_50k_SingleBatch()
    {
        var store = new AggregatorStore(TimeProvider.System);
        var pipe = new Pipe();
        var writeTask = pipe.Writer.WriteAsync(_payload50k).AsTask()
            .ContinueWith(_ => pipe.Writer.Complete());

        var accepted = await ReadingStreamParser.IngestAsync(
            pipe.Reader, store, CancellationToken.None);

        await writeTask;
        return accepted;
    }

    [Benchmark(Description = "Ingest 20 concurrent batches of 1,000 readings each")]
    public async Task<long> Ingest_20xConcurrent_1k()
    {
        var store = new AggregatorStore(TimeProvider.System);

        var tasks = Enumerable.Range(0, 20).Select(async _ =>
        {
            var pipe = new Pipe();
            var writeTask = pipe.Writer.WriteAsync(_payload1k).AsTask()
                .ContinueWith(_ => pipe.Writer.Complete());

            var accepted = await ReadingStreamParser.IngestAsync(
                pipe.Reader, store, CancellationToken.None);

            await writeTask;
            return accepted;
        });

        var results = await Task.WhenAll(tasks);
        return results.Sum();
    }

    private static byte[] BuildPayload(int readingCount, int deviceCount)
    {
        var sb = new StringBuilder(readingCount * 70);
        sb.Append('[');
        var now = DateTime.UtcNow;
        var rnd = new Random(42);

        for (var i = 0; i < readingCount; i++)
        {
            if (i > 0) sb.Append(',');
            var deviceId = $"device-{i % deviceCount}";
            var ts = now.AddSeconds(-rnd.Next(0, 290)).ToString("O");
            var value = rnd.NextDouble() * 100;
            sb.Append($"{{\"deviceId\":\"{deviceId}\",\"timestamp\":\"{ts}\",\"value\":{value:F3}}}");
        }

        sb.Append(']');
        return Encoding.UTF8.GetBytes(sb.ToString());
    }
}
