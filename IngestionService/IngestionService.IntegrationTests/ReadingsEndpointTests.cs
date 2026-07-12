using System.Net;
using System.Text;

namespace IngestionService.IntegrationTests;

public class ReadingsEndpointTests : IClassFixture<ApiFactory>
{
    private readonly HttpClient _client;

    public ReadingsEndpointTests(ApiFactory factory)
    {
        _client = factory.CreateClient();
    }

    [Fact]
    public async Task Should_Accept_Valid_Readings()
    {
        var json = """
        [
          {
            "deviceId":"device-1",
            "timestamp":"2026-07-12T10:00:00Z",
            "value":20.5
          }
        ]
        """;

        var response = await _client.PostAsync(
            "/readings",
            new StringContent(json, Encoding.UTF8, "application/json"));

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
    }

    [Fact]
    public async Task Should_Return_BadRequest_For_Invalid_Json()
    {
        var json = "[{";

        var response = await _client.PostAsync(
            "/readings",
            new StringContent(json, Encoding.UTF8, "application/json"));

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);        
    }
}