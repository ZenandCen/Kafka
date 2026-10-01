using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace WalletConsumer.Services;

public class WalletConsumerWorker : BackgroundService
{
    private readonly ILogger<WalletConsumerWorker> _logger;
    private readonly WalletRedisStateService _redisStateService;
    private readonly OpenSearchIndexingService _indexingService;
    private readonly string _bootstrapServers;

    public WalletConsumerWorker(
        ILogger<WalletConsumerWorker> logger,
        WalletRedisStateService redisStateService,
        OpenSearchIndexingService indexingService,
        string bootstrapServers = "localhost:9092")
    {
        _logger = logger;
        _redisStateService = redisStateService;
        _indexingService = indexingService;
        _bootstrapServers = bootstrapServers;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        string instanceId = Environment.GetEnvironmentVariable("POD_NAME") ?? $"wallet-consumer-instance-{Guid.NewGuid().ToString().Substring(0, 4)}";

        var config = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = "wallet-enterprise-processing-group",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            PartitionAssignmentStrategy = PartitionAssignmentStrategy.Range | PartitionAssignmentStrategy.CooperativeSticky,
            GroupInstanceId = instanceId
        };

        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe(new[] { "wallet-transactions", "wikipedia-events" });

        _logger.LogInformation("==========================================================================");
        _logger.LogInformation("  STARTING ENTERPRISE KAFKA CONSUMER WORKER");
        _logger.LogInformation("  Group Instance ID: {InstanceId}", instanceId);
        _logger.LogInformation("==========================================================================");

        await _indexingService.InitializeOpenSearchIndicesAsync();

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var consumeResult = consumer.Consume(TimeSpan.FromMilliseconds(500));
                    if (consumeResult == null) continue;

                    string topic = consumeResult.Topic;
                    string key = consumeResult.Message.Key;
                    string value = consumeResult.Message.Value;

                    if (topic == "wallet-transactions")
                    {
                        using var doc = JsonDocument.Parse(value);
                        var root = doc.RootElement;

                        string txId = root.GetProperty("transactionId").GetString() ?? Guid.NewGuid().ToString();
                        string userId = root.GetProperty("userId").GetString() ?? "unknown";
                        string type = root.GetProperty("type").GetString() ?? "DEPOSIT";
                        decimal amount = root.GetProperty("amount").GetDecimal();
                        string refId = root.GetProperty("referenceId").GetString() ?? "REF-000";
                        string desc = root.TryGetProperty("description", out var descEl) ? descEl.GetString() ?? "" : "";

                        _logger.LogInformation("[KAFKA CONSUME] Topic: {Topic} | Partition: {Partition} | Offset: {Offset} | User: {UserId} | Type: {Type} | Amount: {Amount:N0} VND",
                            topic, consumeResult.Partition, consumeResult.Offset, userId, type, amount);

                        bool redisOk = false;
                        if (type == "DEPOSIT")
                        {
                            redisOk = await _redisStateService.ProcessDepositWithOptimisticLockAsync(userId, amount, txId);
                        }
                        else
                        {
                            redisOk = await _redisStateService.ProcessPaymentOrSetPendingAsync(userId, amount, refId, txId);
                        }

                        if (redisOk)
                        {
                            await _indexingService.IndexTransactionAuditDocumentAsync(txId, userId, type, amount, desc);
                            consumer.Commit(consumeResult);
                            _logger.LogInformation("[MANUAL COMMIT SUCCESS] Offset {Offset} committed.", consumeResult.Offset);
                        }
                    }
                    else if (topic == "wikipedia-events")
                    {
                        _logger.LogInformation("[WIKIPEDIA STREAM CONSUME] Partition: {Partition} | Offset: {Offset} | Wiki/Key: {Key}",
                            consumeResult.Partition, consumeResult.Offset, key);

                        try
                        {
                            using var doc = JsonDocument.Parse(value);
                            var root = doc.RootElement;

                            string title = root.TryGetProperty("title", out var tEl) ? tEl.GetString() ?? "Untitled" : "Untitled";
                            string wiki = root.TryGetProperty("wiki", out var wEl) ? wEl.GetString() ?? "wiki" : "wiki";
                            string user = root.TryGetProperty("user", out var uEl) ? uEl.GetString() ?? "User" : "User";
                            string comment = root.TryGetProperty("comment", out var cEl) ? cEl.GetString() ?? "" : "";
                            string eventId = root.TryGetProperty("id", out var idEl) ? idEl.GetInt64().ToString() : Guid.NewGuid().ToString();

                            await _indexingService.IndexWikipediaDocumentAsync(eventId, wiki, title, user, comment);
                            consumer.Commit(consumeResult);
                        }
                        catch
                        {
                            consumer.Commit(consumeResult);
                        }
                    }
                }
                catch (ConsumeException ex)
                {
                    _logger.LogError("[CONSUME EXCEPTION]: {Reason}", ex.Error.Reason);
                }
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("[Consumer Worker] Cancellation requested. Shutting down...");
        }
        finally
        {
            _logger.LogInformation("[Graceful Shutdown] Calling consumer.Close()...");
            consumer.Close();
            _logger.LogInformation("[Graceful Shutdown] Consumer closed gracefully.");
        }
    }
}
