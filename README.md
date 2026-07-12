# High-Throughput IoT Event Ingestion Service

A lightweight, high-throughput ASP.NET Core Minimal API for ingesting IoT device readings and maintaining rolling five-minute statistics in memory.

The service is optimized for:

- High ingestion throughput
- Low memory allocation
- Thread-safe concurrent updates
- Constant-time writes
- Fixed memory usage per device

The complete design rationale, concurrency model and implementation trade-offs are documented in **DESIGN.md**.

---

# Prerequisites

- .NET 10 SDK
- Visual Studio 2022 (or later) / VS Code (optional)

---

# Build

```bash
dotnet build
```

---

# Run the API

```bash
dotnet run --project src/IngestionService.Api
```

Once started, the console will display the listening URLs.

Example:

```
https://localhost:7058
http://localhost:5058
```

---

# API Documentation (Swagger)

Swagger/OpenAPI is enabled for easier exploration of the API.

After starting the application, browse to:

```
https://localhost:<port>/swagger
```

or simply

```
https://localhost:<port>
```

The root endpoint automatically redirects to Swagger UI.

---

# Run Tests

Run all unit and integration tests.

```bash
dotnet test
```

---

# Run Benchmarks

The solution includes a BenchmarkDotNet project for measuring ingestion performance.

```bash
dotnet run -c Release --project benchmarks/IngestionService.Benchmarks
```

---

# API Endpoints

## POST /readings

Accepts a JSON array containing up to **50,000** readings.

Example:

```json
[
  {
    "deviceId": "sensor-1",
    "timestamp": "2026-07-08T10:15:00Z",
    "value": 23.4
  },
  {
    "deviceId": "sensor-2",
    "timestamp": "2026-07-08T10:15:01Z",
    "value": 19.9
  }
]
```

Successful response:

```json
{
  "accepted": 2
}
```

Status Codes

| Status | Description |
|---------|-------------|
|202 Accepted|Batch accepted and processed|
|400 Bad Request|Malformed JSON request|
|500 Internal Server Error|Unexpected server error|

---

## GET /readings/{deviceId}/aggregate

Returns rolling statistics for the last five minutes.

Example response:

```json
{
  "deviceId": "sensor-1",
  "count": 42,
  "min": 12.3,
  "max": 98.1,
  "average": 45.6
}
```

If no readings exist in the active window:

```
404 Not Found
```

---

# Project Highlights

- Streaming JSON parser using `Utf8JsonReader`
- Constant-time ingestion using fixed-size rolling buckets
- Thread-safe aggregation with per-device synchronization
- `ConcurrentDictionary` for device-level isolation
- Low allocation design
- Swagger/OpenAPI documentation
- Structured logging using `ILogger`
- Centralized global exception handling middleware
- Unit, concurrency and integration tests
- BenchmarkDotNet performance benchmarks
- Time abstraction using `TimeProvider` for improved testability

---

# Assumptions

1. **Rolling Window**

   The active window is the previous **five minutes**. Readings older than the window are ignored during ingestion.

2. **Malformed Readings**

   Individual malformed readings (missing required fields) are skipped without rejecting the entire batch. The `accepted` count reflects only successfully processed readings.

3. **Late Arriving Events**

   Readings already outside the rolling window are ignored because they can never contribute to future aggregates.

4. **Time Source**

   The application depends on `TimeProvider` instead of directly calling `DateTime.UtcNow`.

   Benefits include:

   - Improved testability
   - Consistent time source throughout the application
   - Ability to inject deterministic time providers during testing

5. **Unknown Devices**

   Devices with no active readings return:

   ```
   404 Not Found
   ```

   The implementation intentionally does not distinguish between:

   - a device that has never been seen
   - a device whose readings have expired

6. **API Documentation**

   Swagger is enabled by default in Development mode.

7. **Exception Handling**

   A centralized middleware handles unexpected exceptions by:

   - Logging the exception
   - Returning consistent JSON error responses
   - Mapping malformed JSON to HTTP 400
   - Preventing internal exception details from being returned to clients

---

# Logging

Structured logging is implemented using the built-in `ILogger<T>` abstraction.

The application logs:

- Application startup
- Incoming ingestion requests
- Number of accepted readings
- Batch processing duration
- Aggregate requests
- Unknown device lookups
- Unhandled exceptions

Structured logging enables better filtering and querying in centralized logging platforms such as Seq, ELK and Azure Monitor.

---

# Testing

The solution includes:

- Unit Tests
- Concurrency Tests
- Integration Tests (using `WebApplicationFactory`)
- BenchmarkDotNet Performance Benchmarks

---

# Future Improvements

Given additional time, the following enhancements would be valuable:

- Intern/cache `deviceId` strings to reduce the remaining per-reading allocation.
- Add end-to-end HTTP load testing using tools such as **k6** or **Bombardier**.
- Remove inactive devices from `AggregatorStore` after prolonged inactivity.
- Expose metrics using OpenTelemetry.
- Add Health Checks.
- Introduce Rate Limiting.
- Persist readings through Kafka/Event Hubs for recovery after node failures.

---

For a detailed explanation of the design decisions, concurrency model and implementation trade-offs, please refer to **DESIGN.md**.