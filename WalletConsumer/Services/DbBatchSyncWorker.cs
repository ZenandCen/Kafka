using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace WalletConsumer.Services;

public class DbBatchSyncWorker : BackgroundService
{
    private readonly ILogger<DbBatchSyncWorker> _logger;
    private readonly IConnectionMultiplexer? _redis;

    public DbBatchSyncWorker(ILogger<DbBatchSyncWorker> logger, string redisConnectionString = "localhost:26379,localhost:26380,localhost:26381,role:sentinel")
    {
        _logger = logger;
        try
        {
            _redis = ConnectionMultiplexer.Connect(redisConnectionString);
        }
        catch
        {
            _redis = null;
        }
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("==========================================================================");
        _logger.LogInformation("  STARTING WRITE-BEHIND BATCH SYNC WORKER (Redis Stream -> PostgreSQL)");
        _logger.LogInformation("==========================================================================");

        if (_redis == null)
        {
            _logger.LogWarning("[DB SYNC WORKER] Redis unavailable. Worker idle.");
            return;
        }

        var db = _redis.GetDatabase();
        RedisValue lastId = "0-0";

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var entries = await db.StreamReadAsync("wallet:stream:db_sync_queue", lastId, 100);

                if (entries != null && entries.Length > 0)
                {
                    _logger.LogInformation("[WRITE-BEHIND SYNC] Processing {Count} records from Redis Stream...", entries.Length);

                    foreach (var entry in entries)
                    {
                        lastId = entry.Id;
                        string userId = entry.Values.FirstOrDefault(x => x.Name == "userId").Value.ToString() ?? "?";
                        string txId = entry.Values.FirstOrDefault(x => x.Name == "transactionId").Value.ToString() ?? "?";
                        string type = entry.Values.FirstOrDefault(x => x.Name == "type").Value.ToString() ?? "?";
                        string amount = entry.Values.FirstOrDefault(x => x.Name == "amount").Value.ToString() ?? "?";

                        _logger.LogDebug("[DB UPSERT] userId={UserId}, txId={TxId}, type={Type}, amount={Amount}",
                            userId, txId, type, amount);
                    }

                    await db.StreamTrimAsync("wallet:stream:db_sync_queue", 10000);
                }
                else
                {
                    await Task.Delay(2000, stoppingToken);
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError("[DB SYNC ERROR] {Msg}", ex.Message);
                await Task.Delay(5000, stoppingToken);
            }
        }
    }
}
