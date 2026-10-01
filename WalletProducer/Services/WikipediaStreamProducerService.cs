using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using WalletProducer.Models;

namespace WalletProducer.Services;

public class WikipediaStreamProducerService
{
    private readonly ILogger<WikipediaStreamProducerService> _logger;
    private readonly IProducer<string, string> _producer;
    private readonly string _topic = "wikipedia-events";
    private readonly HttpClient _httpClient;

    public WikipediaStreamProducerService(ILogger<WikipediaStreamProducerService> logger, string bootstrapServers = "localhost:9092")
    {
        _logger = logger;
        _httpClient = new HttpClient();
        _httpClient.Timeout = TimeSpan.FromMinutes(10);

        var config = new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            Acks = Acks.All,
            EnableIdempotence = true,
            LingerMs = 10,
            BatchSize = 32768,
            CompressionType = CompressionType.Snappy
        };

        _producer = new ProducerBuilder<string, string>(config).Build();
    }

    public async Task StartWikipediaStreamAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("==========================================");
        _logger.LogInformation("  STARTING WIKIPEDIA EVENTSTREAM PRODUCER ");
        _logger.LogInformation("==========================================");

        string streamUrl = "https://stream.wikimedia.org/v2/stream/recentchange";

        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, streamUrl);
            using var response = await _httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
            response.EnsureSuccessStatusCode();

            using var stream = await response.Content.ReadAsStreamAsync(cancellationToken);
            using var reader = new StreamReader(stream);

            _logger.LogInformation("Connected to Wikimedia SSE stream. Ingesting live edits into Kafka topic: {Topic}...", _topic);

            while (!reader.EndOfStream && !cancellationToken.IsCancellationRequested)
            {
                string? line = await reader.ReadLineAsync(cancellationToken);
                if (string.IsNullOrWhiteSpace(line) || !line.StartsWith("data: ")) continue;

                string json = line.Substring(6).Trim();
                try
                {
                    var wikiEvent = JsonSerializer.Deserialize<WikipediaChangeEvent>(json);
                    if (wikiEvent == null) continue;

                    string partitionKey = !string.IsNullOrEmpty(wikiEvent.Wiki) ? wikiEvent.Wiki : wikiEvent.User;

                    _producer.Produce(_topic, new Message<string, string>
                    {
                        Key = partitionKey,
                        Value = json
                    }, report =>
                    {
                        if (report.Error.IsError)
                        {
                            _logger.LogError("[Wiki Stream Error]: {Reason}", report.Error.Reason);
                        }
                        else
                        {
                            _logger.LogInformation("[Wiki Stream -> Kafka] Topic: {Topic} | Key ({Wiki}): {Key} | Partition: {Partition} | Offset: {Offset} | Title: {Title}",
                                _topic, wikiEvent.Wiki, partitionKey, report.Partition, report.Offset, wikiEvent.Title);
                        }
                    });

                    _producer.Flush(TimeSpan.FromMilliseconds(100));
                }
                catch (Exception ex)
                {
                    _logger.LogWarning("[Wiki Stream Parse Exception]: {Msg}", ex.Message);
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning("[Wiki Stream Fallback] SSE Connection unavailable/offline ({Msg}). Running Synthetic Wikipedia Event Generator...", ex.Message);
            await GenerateSyntheticWikipediaEventsAsync(cancellationToken);
        }
    }

    public async Task GenerateSyntheticWikipediaEventsAsync(CancellationToken cancellationToken)
    {
        var wikis = new[] { "enwiki", "viwiki", "frwiki", "dewiki", "jawiki", "wikidatawiki" };
        var users = new[] { "Editor_Alpha", "WikiBot_01", "FinanceContributor", "EWalletAnalyst", "DataMaster" };
        var titles = new[]
        {
            "Apache Kafka Architecture & Partitioning",
            "Elasticsearch Vector Search & Semantic Search",
            "OpenSearch Enterprise Analytics Engine",
            "E-Wallet Financial Transaction Protocols",
            "Real-time Stream Processing with Kafka Streams",
            "Kafka Connect Architecture and Sink Connectors",
            "Optimistic Locking & Redis Distributed Cache"
        };

        var random = new Random();

        while (!cancellationToken.IsCancellationRequested)
        {
            string wiki = wikis[random.Next(wikis.Length)];
            string user = users[random.Next(users.Length)];
            string title = titles[random.Next(titles.Length)];

            var wikiEvent = new WikipediaChangeEvent
            {
                Id = random.Next(100000, 999999),
                Type = "edit",
                Title = title,
                TitleUrl = $"https://en.wikipedia.org/wiki/{Uri.EscapeDataString(title)}",
                Comment = $"Updated documentation for {title} - Optimizing throughput and indexing.",
                Timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
                User = user,
                Bot = user.Contains("Bot"),
                Wiki = wiki,
                ServerName = $"{wiki}.wikipedia.org",
                Length = new LengthInfo { Old = random.Next(1000, 5000), New = random.Next(5001, 10000) }
            };

            string json = JsonSerializer.Serialize(wikiEvent);

            _producer.Produce(_topic, new Message<string, string>
            {
                Key = wiki,
                Value = json
            }, report =>
            {
                _logger.LogInformation("[Synthetic Wiki -> Kafka] Wiki: {Wiki} | Title: {Title} | Partition: {Partition} | Offset: {Offset}",
                    wiki, title, report.Partition, report.Offset);
            });

            _producer.Flush(TimeSpan.FromMilliseconds(500));
            await Task.Delay(1000, cancellationToken);
        }
    }
}
