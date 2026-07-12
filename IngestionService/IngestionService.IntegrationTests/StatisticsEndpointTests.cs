using IngestionService.Core.Aggregation;
using System.Net;
using System.Net.Http.Json;
using System.Text;

namespace IngestionService.IntegrationTests;

public class StatisticsEndpointTests : IClassFixture<ApiFactory>
{
    private readonly HttpClient _client;

    public StatisticsEndpointTests(ApiFactory factory)
    {
        _client = factory.CreateClient();
    }

    [Fact]
    public async Task Should_Return_Aggregated_Statistics()
    {
        var json = """
        [
          {
            "deviceId":"device-1",
            "timestamp":"2026-07-12T10:00:00Z",
            "value":10
          },
          {
            "deviceId":"device-1",
            "timestamp":"2026-07-12T10:00:01Z",
            "value":20
          }
        ]
        """;

        await _client.PostAsync(
            "/readings",
            new StringContent(json, Encoding.UTF8, "application/json"));

        var response = await _client.GetAsync("/readings/device-1/aggregate");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        var result = await response.Content.ReadFromJsonAsync<AggregateResult>();
      
        Assert.Equal(2, result.Count);
        Assert.Equal(10, result.Min);
        Assert.Equal(20, result.Max);
        Assert.Equal(15, result.Average);
    }

    [Fact]
    public async Task Should_Return_NotFound_For_Unknown_Device()
    {
        var response = await _client.GetAsync("/readings/unknown/aggregate");
        
        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);        
    }
}