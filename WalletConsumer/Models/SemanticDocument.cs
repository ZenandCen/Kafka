using System.Text.Json.Serialization;

namespace WalletConsumer.Models;

public class SemanticDocument
{
    [JsonPropertyName("id")]
    public string Id { get; set; } = Guid.NewGuid().ToString();

    [JsonPropertyName("title")]
    public string Title { get; set; } = string.Empty;

    [JsonPropertyName("content")]
    public string Content { get; set; } = string.Empty;

    [JsonPropertyName("category")]
    public string Category { get; set; } = "WalletTransaction";

    [JsonPropertyName("vector_embedding")]
    public float[] VectorEmbedding { get; set; } = Array.Empty<float>();

    [JsonPropertyName("metadata_json")]
    public string MetadataJson { get; set; } = "{}";

    [JsonPropertyName("timestamp")]
    public DateTime Timestamp { get; set; } = DateTime.UtcNow;
}
