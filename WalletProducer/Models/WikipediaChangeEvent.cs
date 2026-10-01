using System.Text.Json.Serialization;

namespace WalletProducer.Models;

public class WikipediaChangeEvent
{
    [JsonPropertyName("id")]
    public long Id { get; set; }

    [JsonPropertyName("type")]
    public string Type { get; set; } = "edit";

    [JsonPropertyName("title")]
    public string Title { get; set; } = string.Empty;

    [JsonPropertyName("title_url")]
    public string TitleUrl { get; set; } = string.Empty;

    [JsonPropertyName("comment")]
    public string Comment { get; set; } = string.Empty;

    [JsonPropertyName("timestamp")]
    public long Timestamp { get; set; }

    [JsonPropertyName("user")]
    public string User { get; set; } = string.Empty;

    [JsonPropertyName("bot")]
    public bool Bot { get; set; }

    [JsonPropertyName("wiki")]
    public string Wiki { get; set; } = string.Empty;

    [JsonPropertyName("server_name")]
    public string ServerName { get; set; } = string.Empty;

    [JsonPropertyName("length")]
    public LengthInfo? Length { get; set; }
}

public class LengthInfo
{
    [JsonPropertyName("old")]
    public int Old { get; set; }

    [JsonPropertyName("new")]
    public int New { get; set; }
}
