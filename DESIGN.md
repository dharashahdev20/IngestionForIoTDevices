# Design Document

## 1. Data structures

**`AggregatorStore`**: `ConcurrentDictionary<string, DeviceAggregator>`, one
entry per device. Chosen over a single shared structure (e.g. one big lock
around a `Dictionary`) because it gives each device independent
synchronization - two threads writing to different devices never contend
with each other, only writes to the *same* device serialize.

**`DeviceAggregator`**: a fixed `Bucket[300]` circular array, one slot per
second of the 5-minute window. Each `Bucket` is a small mutable struct:
`{ EpochSecond, Count, Sum, Min, Max }`.

Why buckets instead of storing raw readings (e.g. a per-device deque of
`(timestamp, value)` pairs trimmed on every write):

| | Raw reading deque | 300-second buckets |
|---|---|---|
| Memory per device | grows with traffic (could be huge at 50k/batch) | fixed, ~7.2KB regardless of load |
| Ingestion cost | O(1) amortized append + O(k) eviction of expired entries | O(1) always |
| Query cost | O(n) scan or maintained running aggregate with O(k) eviction | O(300) always, bounded |
| Precision | exact, millisecond | ±1 second |

At 10,000 devices, a fixed-size approach matters: 10,000 unbounded deques is
a memory-growth risk under sustained high traffic, while 10,000 × 300
buckets is a hard, predictable ceiling (~72MB worst case for the bucket
arrays alone) that doesn't move regardless of how much traffic arrives.

The tradeoff is precision: a reading is bucketed by the second, not the
millisecond, so the window boundary is "give or take a second." For 5-minute
rolling dashboards/alerting this precision loss is not observable to a human
or a threshold-based alert, so it was judged an acceptable approximation
rather than something requiring exact eviction.

## 2. Allocation strategy on the ingestion path

- **Streaming JSON parse** (`ReadingStreamParser`): reads the request body
  via its `PipeReader` and parses tokens with `Utf8JsonReader` directly over
  the pipe's buffer segments. No `List<Reading>` or `Reading[]` is ever
  materialized for the batch - each reading is applied to the aggregator the
  moment its closing `}` is seen, then discarded.
- **`Reading` model is unused on the hot path** - it exists in `Core.Models`
  as the conceptual wire shape / for tests, but the parser writes parsed
  fields into a small mutable struct (`PartialReading`) rather than
  allocating a `Reading` instance per object.
- **Buckets are structs**, not classes, stored inline in the `Bucket[]`
  array - no per-bucket heap allocation, no pointer chasing during the
  O(300) scan on query.
- **Remaining allocation**: the `deviceId` string itself, once per reading
  (`Utf8JsonReader.GetString()`), because `ConcurrentDictionary<string, ...>`
  needs a string key. This was left as-is rather than built out further,
  because interning/caching device ID strings adds real complexity (a
  concurrent string pool, additional hashing) for a payoff that should be
  measured, not assumed - the benchmark suite is the tool to decide whether
  it's worth adding, and it's called out in the README as a next step.
- `ServerGarbageCollection` + `ConcurrentGarbageCollection` are enabled in
  the API project for throughput under sustained allocation (favors overall
  throughput and multi-core scaling over minimizing individual GC pause
  latency, which fits a batch-ingestion workload better than a
  latency-sensitive one).
- - **Time abstraction (`TimeProvider`)**: The service avoids directly calling
  `DateTime.UtcNow` by injecting `TimeProvider`. This improves testability by
  allowing unit and integration tests to substitute a deterministic time source
  without changing production code. The production API registers
  `TimeProvider.System`.

## 3. Concurrency model

- **Per-device lock, not a global lock.** Each `DeviceAggregator` owns a
  private `object _gate` guarding only its own 300-slot array. Two requests
  touching different devices never block each other. Contention is possible
  only when the same device is written/read concurrently - realistic, but
  short-lived, since the critical section is O(1) for a write and O(300) for
  a read, with no I/O or allocation inside the lock.
- **Why a plain `lock` (Monitor) and not lock-free CAS or
  `ReaderWriterLockSlim`:**
  - A fully lock-free bucket update would need to atomically update four
    fields (count, sum, min, max) together, which isn't expressible with a
    single CAS without packing them into one word (losing precision) or
    using a version-stamped copy-on-write scheme (more allocation, more
    complexity) - not justified at this hold-time.
  - `ReaderWriterLockSlim` optimizes for many concurrent readers with rare
    writers. This workload is the opposite: ingestion (writes) is explicitly
    the hot path and dominates traffic, while queries are comparatively
    infrequent. RWLS's extra bookkeeping to track reader/writer state would
    cost more than the plain exclusive lock it would save.
  - `ConcurrentDictionary.GetOrAdd` handles the "new device" race safely
    without any additional locking at the store level.
- **Correctness argument**: every mutation of a bucket's four fields happens
  inside the same `lock (_gate)` block, and every read of those same fields
  (in `Snapshot`) happens inside the same lock, on the same object, per
  device. This gives standard mutual exclusion - no interleaving of a
  read and a write on the same device can observe a torn/partial update
  (verified by `ConcurrentTests.ConcurrentWrites_AndReads_DoNotObserveTornState`,
  which asserts `min <= average <= max` holds under concurrent load).
  - **Singleton lifetime**: `AggregatorStore` is registered as a singleton in
  dependency injection. Since it is internally thread-safe
  (`ConcurrentDictionary` + per-device synchronization), a single shared
  instance safely serves all requests while preserving aggregated state for
  the lifetime of the application.

## 4. Windowing approach and tradeoffs

Already covered in §1 - bucketed-by-second approximation of a continuously
sliding 5-minute window. Restated tradeoff: O(1) writes and a fixed memory
footprint, at the cost of ~1-second precision on the window boundary, which
was judged appropriate for a dashboard/alerting consumer rather than an
exact-audit use case.

**Out-of-order arrival** is handled naturally: a reading is routed to the
bucket for *its own* timestamp's second, not "now" - so a reading arriving
slightly late still lands in the correct historical bucket, as long as that
bucket hasn't since been overwritten by a reading from a later occurrence of
that same second-of-cycle (i.e., 300 seconds later). Readings that arrive
already older than the window are dropped at write time - they could never
appear in any future query, so applying them would only risk corrupting a
bucket a live reading is about to reuse.

## 5. Beyond a single instance

Not built (explicitly out of scope), but the credible path:

- **Partition by `deviceId`** across N ingestion nodes (consistent hashing
  on device ID), so all readings for a given device always land on the same
  node - this preserves the current single-writer-per-device model without
  needing distributed locks or coordination for aggregation itself.
- **Ingestion fan-in**: a lightweight router/gateway (or client-side
  hashing, if devices/gateways know their shard) forwards each reading to
  its owning node.
- **Query fan-out is avoided** by the same partitioning - a query for a
  specific `deviceId` goes straight to the one node that owns it, no
  scatter-gather needed.
- **Failure/rebalancing**: this is the part that genuinely needs new design
  - a 5-minute in-memory-only window is inherently lossy across a node
    restart, so at true scale you'd want the ingestion node to also publish
    raw readings to a durable log (e.g. Kafka) partitioned by `deviceId`,
    letting a replacement node "catch up" the last 5 minutes on failover
    instead of starting cold. That crosses into persistence/durability,
    which is explicitly out of scope here, but it's the natural next
    building block.

## 6. Benchmark results


| Method                                                     | Mean       | Error     | StdDev    | Completed Work Items | Lock Contentions | Gen0       | Gen1       | Gen2      | Allocated |
|----------------------------------------------------------- |-----------:|----------:|----------:|---------------------:|-----------------:|-----------:|-----------:|----------:|----------:|
| 'Ingest 50,000 readings (single request, streaming parse)' | 100.281 ms | 1.9950 ms | 4.5436 ms |               3.0000 |                - | 14833.3333 | 14666.6667 | 4833.3333 | 120.79 MB |
| 'Ingest 20 concurrent batches of 1,000 readings each'      |   3.916 ms | 0.0408 ms | 0.0381 ms |              60.0000 |                - |   265.6250 |   257.8125 |         - |   3.27 MB |

you can run beanchmark project locally in release mode and check the output. For reference I have added my result in Bechmark_Output.txt


## 7. API Discoverability

Swagger (OpenAPI) is enabled to provide interactive API documentation.

Benefits include:

- Endpoint discovery without reading source code.
- Request and response schema visualization.
- Ability to execute requests directly from the browser.
- Clear documentation of HTTP status codes.
- Helpful summaries and descriptions for each endpoint.

This significantly improves developer experience and makes the API easier to
consume and validate.

## 8. Global Exception Handling

The API uses centralized exception handling middleware rather than wrapping
every endpoint in try/catch blocks.

Benefits:

- Keeps endpoint handlers focused solely on business logic.
- Ensures a consistent JSON error response format.
- Prevents leaking internal exception details.
- Allows structured logging of unexpected failures.
- Makes future exception mapping (validation, authorization, etc.) easier.

Malformed JSON requests are translated into HTTP 400 (Bad Request), while
unexpected server errors return HTTP 500 (Internal Server Error).

## 9. Structured Logging

The service uses the built-in `ILogger<T>` abstraction for structured logging.

Logging is added at key points:

- Application startup
- Reading ingestion requests
- Batch processing duration
- Number of accepted readings
- Aggregate query requests
- Unknown device lookups
- Unexpected exceptions (via middleware)

Structured logging was preferred over string interpolation because named
properties (e.g. `{DeviceId}`, `{AcceptedCount}`) can be indexed by log
aggregation platforms such as Seq, Elasticsearch or Azure Monitor, making
searching and filtering significantly easier.

## 10. Testing Strategy

The solution contains multiple layers of automated testing.

### Unit Tests

Unit tests validate the aggregation logic in isolation, including:

- Rolling five-minute window
- Min/Max/Average calculation
- Out-of-order readings
- Concurrent updates
- Expiration of old readings

### Integration Tests

Integration tests use `WebApplicationFactory` to host the API in-memory and
exercise the complete request pipeline.

Scenarios covered include:

- Successful ingestion
- Aggregate retrieval
- Unknown device handling (404)
- Invalid JSON requests (400)

The application uses `TimeProvider`, allowing deterministic testing of
time-dependent behavior without changing production code.