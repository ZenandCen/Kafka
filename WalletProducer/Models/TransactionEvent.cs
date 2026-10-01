using System.Text.Json.Serialization;

namespace WalletProducer.Models;

public class TransactionEvent
{
    [JsonPropertyName("transactionId")]
    public string TransactionId { get; set; } = Guid.NewGuid().ToString();

    [JsonPropertyName("userId")]
    public string UserId { get; set; } = string.Empty;

    [JsonPropertyName("type")]
    public string Type { get; set; } = "DEPOSIT";

    [JsonPropertyName("amount")]
    public decimal Amount { get; set; }

    [JsonPropertyName("referenceId")]
    public string ReferenceId { get; set; } = string.Empty;

    [JsonPropertyName("description")]
    public string Description { get; set; } = string.Empty;

    [JsonPropertyName("timestamp")]
    public DateTime Timestamp { get; set; } = DateTime.UtcNow;
}
