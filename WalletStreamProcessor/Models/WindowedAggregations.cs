using System.Text.Json.Serialization;

namespace WalletStreamProcessor.Models;

public class UserTransactionWindow
{
    public string UserId { get; set; } = string.Empty;
    public int TransactionCount { get; set; }
    public decimal TotalAmount { get; set; }
    public DateTime WindowStart { get; set; }
    public DateTime WindowEnd { get; set; }
    public bool FraudAlert { get; set; }
    public string AlertReason { get; set; } = string.Empty;
}

public class FraudAlertEvent
{
    [JsonPropertyName("alertId")]
    public string AlertId { get; set; } = Guid.NewGuid().ToString();

    [JsonPropertyName("userId")]
    public string UserId { get; set; } = string.Empty;

    [JsonPropertyName("transactionCount")]
    public int TransactionCount { get; set; }

    [JsonPropertyName("totalAmount")]
    public decimal TotalAmount { get; set; }

    [JsonPropertyName("alertReason")]
    public string AlertReason { get; set; } = string.Empty;

    [JsonPropertyName("timestamp")]
    public DateTime Timestamp { get; set; } = DateTime.UtcNow;
}
