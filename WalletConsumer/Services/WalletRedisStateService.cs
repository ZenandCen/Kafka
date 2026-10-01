using System.Text.Json;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace WalletConsumer.Services;

public class WalletRedisStateService
{
    private readonly ILogger<WalletRedisStateService> _logger;
    private readonly IDatabase _redisDb;

    public WalletRedisStateService(ILogger<WalletRedisStateService> logger, string redisConnectionString = "localhost:6379")
    {
        _logger = logger;
        try
        {
            var redis = ConnectionMultiplexer.Connect(redisConnectionString);
            _redisDb = redis.GetDatabase();
        }
        catch (Exception ex)
        {
            _logger.LogWarning("[REDIS WARNING] Connection fallback: {Msg}", ex.Message);
            _redisDb = null!;
        }
    }

    public async Task<bool> ProcessDepositWithOptimisticLockAsync(string userId, decimal amount, string transactionId)
    {
        if (_redisDb == null) return true;

        string walletKey = $"wallet:user:{userId}";
        string pendingPaymentKey = $"wallet:user:{userId}:pending_payment";

        string luaScript = @"
            local wallet = redis.call('HMGET', KEYS[1], 'balance', 'version', 'status')
            local current_balance = tonumber(wallet[1] or 0)
            local current_version = tonumber(wallet[2] or 0)
            local status = wallet[3] or 'ACTIVE'

            if status == 'LOCKED' then
                return {err = 'Wallet is LOCKED'}
            end

            local new_balance = current_balance + tonumber(ARGV[1])
            local new_version = current_version + 1

            redis.call('HSET', KEYS[1], 'balance', new_balance, 'version', new_version, 'status', 'ACTIVE')

            local pending = redis.call('GET', KEYS[2])
            local matched_order = 0
            if pending then
                redis.call('DEL', KEYS[2])
                matched_order = 1
            end

            redis.call('XADD', 'wallet:stream:db_sync_queue', '*',
                'userId', ARGV[2],
                'transactionId', ARGV[3],
                'type', 'DEPOSIT',
                'amount', ARGV[1],
                'balanceAfter', new_balance,
                'referenceId', 'DEPOSIT-REF',
                'status', 'SUCCESS',
                'timestamp', ARGV[4])

            return {new_balance, new_version, matched_order}
        ";

        try
        {
            var prepared = LuaScript.Prepare(luaScript);
            await _redisDb.ScriptEvaluateAsync(prepared, new
            {
                KEYS1 = (RedisKey)walletKey,
                KEYS2 = (RedisKey)pendingPaymentKey,
                ARGV1 = amount,
                ARGV2 = userId,
                ARGV3 = transactionId,
                ARGV4 = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
            });

            _logger.LogInformation("[REDIS CAS ATOMIC DEPOSIT] User {UserId} +{Amount:N0} VND -> Lua Executed Successfully.", userId, amount);
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError("[REDIS CAS ERROR] User {UserId}: {Msg}", userId, ex.Message);
            return false;
        }
    }

    public async Task<bool> ProcessPaymentOrSetPendingAsync(string userId, decimal amount, string orderId, string transactionId)
    {
        if (_redisDb == null) return true;

        string walletKey = $"wallet:user:{userId}";
        string pendingKey = $"wallet:user:{userId}:pending_payment";

        string luaScript = @"
            local wallet = redis.call('HMGET', KEYS[1], 'balance', 'version', 'status')
            local current_balance = tonumber(wallet[1] or 0)
            local current_version = tonumber(wallet[2] or 0)
            local required_amount = tonumber(ARGV[1])

            if current_balance >= required_amount then
                local new_balance = current_balance - required_amount
                local new_version = current_version + 1
                redis.call('HSET', KEYS[1], 'balance', new_balance, 'version', new_version)

                redis.call('XADD', 'wallet:stream:db_sync_queue', '*',
                    'userId', ARGV[2],
                    'transactionId', ARGV[4],
                    'type', 'PAYMENT',
                    'amount', ARGV[1],
                    'balanceAfter', new_balance,
                    'referenceId', ARGV[3],
                    'status', 'SUCCESS',
                    'timestamp', ARGV[5])
                return 1
            else
                redis.call('SETEX', KEYS[2], 900, ARGV[3])
                return 0
            end
        ";

        try
        {
            var prepared = LuaScript.Prepare(luaScript);
            var res = (int)await _redisDb.ScriptEvaluateAsync(prepared, new
            {
                KEYS1 = (RedisKey)walletKey,
                KEYS2 = (RedisKey)pendingKey,
                ARGV1 = amount,
                ARGV2 = userId,
                ARGV3 = orderId,
                ARGV4 = transactionId,
                ARGV5 = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
            });

            if (res == 1)
            {
                _logger.LogInformation("[REDIS PAYMENT SUCCESS] User {UserId} paid {Amount:N0} VND for Order {OrderId}", userId, amount, orderId);
            }
            else
            {
                _logger.LogWarning("[REDIS PHASE MISMATCH] User {UserId} insufficient balance. Pending payment saved with TTL = 15m.", userId);
            }

            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError("[REDIS PAYMENT ERROR] {Msg}", ex.Message);
            return false;
        }
    }
}
