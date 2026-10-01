using System.Collections.Concurrent;
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using WalletStreamProcessor.Models;

namespace WalletStreamProcessor.Services;

public class TransactionStreamProcessorWorker : BackgroundService
{
    private readonly ILogger<TransactionStreamProcessorWorker> _logger;
    private readonly string _inputTopic = "wallet-transactions";
    private readonly string _alertTopic = "wallet-fraud-alerts";
    private readonly IConsumer<string, string> _consumer;
    private readonly IProducer<string, string> _producer;

    private readonly ConcurrentDictionary<string, List<(DateTime Timestamp, decimal Amount)>> _userWindows = new();

    public TransactionStreamProcessorWorker(ILogger<TransactionStreamProcessorWorker> logger, string bootstrapServers = "localhost:9092")
    {
        _logger = logger;

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = "wallet-stream-processor-group",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            PartitionAssignmentStrategy = PartitionAssignmentStrategy.Range | PartitionAssignmentStrategy.CooperativeSticky
        };

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            Acks = Acks.All,
            EnableIdempotence = true
        };

        _consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
        _producer = new ProducerBuilder<string, string>(producerConfig).Build();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("==========================================================================");
        _logger.LogInformation("  STARTING REAL-TIME KAFKA STREAM PROCESSOR & FRAUD VELOCITY WORKER");
        _logger.LogInformation("==========================================================================");

        _consumer.Subscribe(_inputTopic);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var result = _consumer.Consume(TimeSpan.FromMilliseconds(500));
                    if (result == null) continue;

                    string userId = result.Message.Key;
                    string json = result.Message.Value;

                    if (string.IsNullOrEmpty(userId)) continue;

                    using var doc = JsonDocument.Parse(json);
                    var root = doc.RootElement;
                    decimal amount = root.TryGetProperty("amount", out var amountEl) ? amountEl.GetDecimal() : 0m;
                    string type = root.TryGetProperty("type", out var typeEl) ? typeEl.GetString() ?? "UNKNOWN" : "UNKNOWN";

                    DateTime now = DateTime.UtcNow;
                    var history = _userWindows.GetOrAdd(userId, _ => new List<(DateTime, decimal)>());

                    lock (history)
                    {
                        history.Add((now, amount));
                        history.RemoveAll(x => (now - x.Timestamp).TotalSeconds > 60);

                        int txCount = history.Count;
                        decimal totalAmount = history.Sum(x => x.Amount);

                        _logger.LogInformation("[KAFKA STREAMS] User: {UserId} | Window Tx Count (1m): {Count} | Total: {Total:N0} VND",
                            userId, txCount, totalAmount);

                        if (txCount > 5 || totalAmount > 2000000m)
                        {
                            string reason = txCount > 5
                                ? $"Velocity Fraud Alert: {txCount} transactions in 60s"
                                : $"High Value Spike Alert: {totalAmount:N0} VND in 60s";

                            _logger.LogWarning("[FRAUD DETECTED] User: {UserId} | Reason: {Reason}", userId, reason);

                            var alert = new FraudAlertEvent
                            {
                                UserId = userId,
                                TransactionCount = txCount,
                                TotalAmount = totalAmount,
                                AlertReason = reason,
                                Timestamp = now
                            };

                            string alertJson = JsonSerializer.Serialize(alert);

                            try
                            {
                                _producer.Produce(_alertTopic, new Message<string, string>
                                {
                                    Key = userId,
                                    Value = alertJson
                                });
                                _producer.Flush(TimeSpan.FromMilliseconds(200));
                            }
                            catch (ProduceException<string, string> produceEx)
                            {
                                _logger.LogError("[FRAUD ALERT PRODUCE ERROR]: {Reason}", produceEx.Error.Reason);
                            }
                        }
                    }

                    _consumer.Commit(result);
                }
                catch (ConsumeException ex)
                {
                    _logger.LogError("[Stream Processor Consume Error]: {Reason}", ex.Error.Reason);
                }
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("[Stream Processor Worker] Shutting down gracefully...");
        }
        finally
        {
            _consumer.Close();
        }

        await Task.CompletedTask;
    }
}
