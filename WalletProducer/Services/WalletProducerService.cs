using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using WalletProducer.Models;

namespace WalletProducer.Services;

public class WalletProducerService
{
    private readonly ILogger<WalletProducerService> _logger;
    private readonly IProducer<string, string> _producer;
    private readonly string _topic = "wallet-transactions";

    public WalletProducerService(ILogger<WalletProducerService> logger, string bootstrapServers = "localhost:9092")
    {
        _logger = logger;

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

    public async Task<DeliveryResult<string, string>> SendTransactionAsync(TransactionEvent tx)
    {
        string json = JsonSerializer.Serialize(tx);

        var message = new Message<string, string>
        {
            Key = tx.UserId,
            Value = json
        };

        var tcs = new TaskCompletionSource<DeliveryResult<string, string>>();

        _producer.Produce(_topic, message, deliveryReport =>
        {
            if (deliveryReport.Error.IsError)
            {
                _logger.LogError("[PRODUCER ERROR] User {UserId}: {Reason}", tx.UserId, deliveryReport.Error.Reason);
                tcs.SetException(new Exception(deliveryReport.Error.Reason));
            }
            else
            {
                _logger.LogInformation("[PRODUCER SUCCESS] User: {UserId} -> Topic: {Topic} | Partition: {Partition} | Offset: {Offset}",
                    tx.UserId, deliveryReport.Topic, deliveryReport.Partition, deliveryReport.Offset);
                tcs.SetResult(deliveryReport);
            }
        });

        _producer.Flush(TimeSpan.FromSeconds(1));
        return await tcs.Task;
    }
}
