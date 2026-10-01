using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using WalletConsumer.Models;

namespace WalletConsumer.Services;

public class OpenSearchIndexingService
{
    private readonly ILogger<OpenSearchIndexingService> _logger;
    private readonly HttpClient _httpClient;
    private readonly string _openSearchUrl;

    public OpenSearchIndexingService(ILogger<OpenSearchIndexingService> logger, string openSearchUrl = "http://localhost:9200")
    {
        _logger = logger;
        _openSearchUrl = openSearchUrl.TrimEnd('/');
        _httpClient = new HttpClient();
        _httpClient.Timeout = TimeSpan.FromSeconds(5);
    }

    public async Task InitializeOpenSearchIndicesAsync()
    {
        try
        {
            string walletIndexMapping = @"
            {
              ""settings"": {
                ""index.knn"": true
              },
              ""mappings"": {
                ""properties"": {
                  ""id"": { ""type"": ""keyword"" },
                  ""title"": { ""type"": ""text"" },
                  ""content"": { ""type"": ""text"" },
                  ""category"": { ""type"": ""keyword"" },
                  ""timestamp"": { ""type"": ""date"" },
                  ""vector_embedding"": {
                    ""type"": ""knn_vector"",
                    ""dimension"": 128,
                    ""method"": {
                      ""name"": ""hnsw"",
                      ""space_type"": ""cosinesimil"",
                      ""engine"": ""nmslib""
                    }
                  }
                }
              }
            }";

            var content = new StringContent(walletIndexMapping, Encoding.UTF8, "application/json");
            var response = await _httpClient.PutAsync($"{_openSearchUrl}/wallet-audit-semantic-index", content);
            _logger.LogInformation("[OPENSEARCH INIT] Response Status: {Status}", response.StatusCode);
        }
        catch (Exception ex)
        {
            _logger.LogWarning("[OPENSEARCH INIT NOTICE] Cluster connecting or initializing at {Url}: {Message}", _openSearchUrl, ex.Message);
        }
    }

    public async Task IndexTransactionAuditDocumentAsync(string transactionId, string userId, string type, decimal amount, string description)
    {
        try
        {
            var doc = new SemanticDocument
            {
                Id = transactionId,
                Title = $"Giao dich vi {type} - User {userId}",
                Content = $"Giao dich tai chinh {type} so tien {amount:N0} VND. Chi tiet: {description}",
                Category = "WalletTransaction",
                VectorEmbedding = GenerateSyntheticEmbedding(description),
                Timestamp = DateTime.UtcNow
            };

            string json = JsonSerializer.Serialize(doc);
            var content = new StringContent(json, Encoding.UTF8, "application/json");
            var response = await _httpClient.PutAsync($"{_openSearchUrl}/wallet-audit-semantic-index/_doc/{transactionId}", content);

            if (response.IsSuccessStatusCode)
            {
                _logger.LogInformation("[OPENSEARCH INDEXED] Transaction ID: {TxId} | Type: {Type} | User: {UserId}", transactionId, type, userId);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning("[OPENSEARCH INDEX WARNING] Failed to index transaction {TxId}: {Msg}", transactionId, ex.Message);
        }
    }

    public async Task IndexWikipediaDocumentAsync(string wikiEventId, string wiki, string title, string user, string comment)
    {
        try
        {
            var doc = new SemanticDocument
            {
                Id = $"wiki-{wikiEventId}",
                Title = $"Wikipedia Edit [{wiki}]: {title}",
                Content = $"User {user} edited Wikipedia article '{title}'. Comment: {comment}",
                Category = "WikipediaArticle",
                VectorEmbedding = GenerateSyntheticEmbedding($"{title} {comment}"),
                Timestamp = DateTime.UtcNow
            };

            string json = JsonSerializer.Serialize(doc);
            var content = new StringContent(json, Encoding.UTF8, "application/json");
            await _httpClient.PutAsync($"{_openSearchUrl}/wallet-audit-semantic-index/_doc/wiki-{wikiEventId}", content);

            _logger.LogInformation("[OPENSEARCH INDEXED WIKIPEDIA] Wiki: {Wiki} | Title: {Title}", wiki, title);
        }
        catch (Exception ex)
        {
            _logger.LogWarning("[OPENSEARCH WIKI INDEX WARNING]: {Msg}", ex.Message);
        }
    }

    public static float[] GenerateSyntheticEmbedding(string text)
    {
        float[] vector = new float[128];
        int hash = text.GetHashCode();
        var rand = new Random(hash);
        for (int i = 0; i < 128; i++)
        {
            vector[i] = (float)rand.NextDouble();
        }
        return vector;
    }
}
