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
        var now = DateTimeOffset.UtcNow;

        var json = $$"""
[
  {
    "deviceId":"device-1",
   "timestamp":"{{now.AddMilliseconds(-500):O}}",
    "value":10
  },
  {
    "deviceId":"device-1",
    "timestamp":"{{now.AddMilliseconds(-100):O}}",
    "value":20
  }
]
""";

        var postResponse = await _client.PostAsync(
            "/readings",
            new StringContent(json, Encoding.UTF8, "application/json"));

        Assert.Equal(HttpStatusCode.Accepted, postResponse.StatusCode);

        var response = await _client.GetAsync("/readings/device-1/aggregate");

        var getContent = await response.Content.ReadAsStringAsync();
        Console.WriteLine(getContent);

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