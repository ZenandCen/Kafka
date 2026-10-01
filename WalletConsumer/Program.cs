using System;
using System.Threading;
using Confluent.Kafka;

class Program
{
    static void Main(string[] args)
    {
        string topic = "wallet-transactions";
        string groupId = "wallet-processing-group";
        
        string instanceId = Environment.GetEnvironmentVariable("POD_NAME") ?? $"wallet-instance-{Guid.NewGuid().ToString().Substring(0, 4)}";

        var config = new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false, // Manual Commit chuẩn tài chính

            // Hỗ trợ chiến lược phân bổ Cooperative Rebalance
            PartitionAssignmentStrategy = PartitionAssignmentStrategy.Range | PartitionAssignmentStrategy.CooperativeSticky,

            // Static Group Membership chống rebalance thừa thãi khi restart app
            GroupInstanceId = instanceId
        };

        // Sử dụng CancellationTokenSource để điều phối việc dừng vòng lặp an toàn thay cho Wakeup() bên Java
        var cts = new CancellationTokenSource();

        // Đăng ký bắt sự kiện Ctrl+C (Graceful Shutdown)
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true; // Ngăn tiến trình bị kill ngay lập tức
            cts.Cancel();
            Console.WriteLine("\n[HỆ THỐNG] Nhận tín hiệu dừng, đang tiến hành đóng Consumer an toàn...");
        };

        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe(topic);

        Console.WriteLine("========================================");
        Console.WriteLine($"    WALLET CONSUMER (.NET CONFLUENT)     ");
        Console.WriteLine($"    Instance ID: {instanceId}");
        Console.WriteLine("========================================");
        Console.WriteLine("Đang lắng nghe giao dịch ví... Nhấn Ctrl+C để thoát.\n");

        try
        {
            // Vòng lặp lắng nghe chạy đến khi có tín hiệu hủy từ CancellationToken
            while (!cts.Token.IsCancellationRequested)
            {
                try
                {
                    // Truyền cts.Token vào hàm Consume để .NET tự động ngắt block ngay khi có tín hiệu dừng
                    var consumeResult = consumer.Consume(cts.Token);
                    if (consumeResult == null) continue;

                    string userId = consumeResult.Message.Key;
                    string payload = consumeResult.Message.Value;

                    Console.WriteLine($"[NHẬN] Partition: {consumeResult.Partition} | Offset: {consumeResult.Offset} | User: {userId} | Payload: {payload}");

                    // Giả lập xử lý nghiệp vụ Database
                    bool isDbSuccess = ProcessWalletTransaction(userId, payload);

                    if (isDbSuccess)
                    {
                        // Manual Commit an toàn sau khi Database đã ghi nhận
                        consumer.Commit(consumeResult);
                        Console.WriteLine($"[COMMIT] Đã lưu và commit offset {consumeResult.Offset} thành công.\n");
                    }
                    else
                    {
                        Console.WriteLine($"[CẢNH BÁO] Lỗi Database cho user {userId}, giữ nguyên offset để retry.\n");
                    }
                }
                catch (ConsumeException ex)
                {
                    Console.WriteLine($"[LỖI CONSUME] {ex.Error.Reason}");
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Ngoại lệ này xảy ra khi CancellationToken được kích hoạt, đây là hành vi hoàn toàn bình thường.
            Console.WriteLine("[INFO] Vòng lặp Consumer đã được yêu cầu dừng qua CancellationToken.");
        }
        finally
        {
            // Khối finally cực kỳ quan trọng: Gọi Close() để commit offset cuối cùng, 
            // rời khỏi Consumer Group và nhường partition cho các node khác một cách mượt mà.
            Console.WriteLine("[HỆ THỐNG] Đang gọi consumer.Close()...");
            consumer.Close();
            Console.WriteLine("[HỆ THỐNG] Consumer đã đóng gracefully hoàn toàn.");
        }
    }

    private static bool ProcessWalletTransaction(string userId, string payload)
    {
        Thread.Sleep(200); // Giả lập thời gian ghi DB
        return true;
    }
}