using System;
using System.Threading.Tasks;
using Confluent.Kafka;

class Program
{
    static async Task Main(string[] args)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = "localhost:9092",
            Acks = Acks.All,             // Đảm bảo an toàn tuyệt đối
            EnableIdempotence = true     // Chống trùng lặp tin nhắn
        };

        using var producer = new ProducerBuilder<string, string>(config).Build();
        string topic = "wallet-transactions";

        Console.WriteLine("========================================");
        Console.WriteLine("    WALLET PRODUCER (FULL STÉPHANE DEMO) ");
        Console.WriteLine("========================================");
        Console.WriteLine("Cú pháp: <UserId> <Nội dung giao dịch>");
        Console.WriteLine("Gõ 'exit' để thoát.\n");

        while (true)
        {
            Console.Write("Nhập giao dịch mới: ");
            string input = Console.ReadLine();
            
            if (string.IsNullOrWhiteSpace(input) || input.ToLower() == "exit")
                break;

            var parts = input.Split(' ', 2);
            if (parts.Length < 2)
            {
                Console.WriteLine("⚠️ Sai định dạng! Nhập: <UserId> <Nội dung>");
                continue;
            }

            string userId = parts[0];  // Key bắt buộc là userId để bảo toàn thứ tự
            string payload = parts[1];

            var message = new Message<string, string>
            {
                Key = userId,
                Value = payload
            };

            // Sử dụng Callback (DeliveryReport) chuẩn mô hình Stéphane hướng dẫn
            producer.Produce(topic, message, deliveryReport =>
            {
                if (deliveryReport.Error.IsError)
                {
                    Console.WriteLine($"[LỖI PRODUCER] Gửi thất bại cho User {userId}: {deliveryReport.Error.Reason}");
                }
                else
                {
                    Console.WriteLine($"[THÀNH CÔNG] User {userId} -> Partition: {deliveryReport.Partition} | Offset: {deliveryReport.Offset}");
                }
            });

            // Đảm bảo message được đẩy đi ngay lập tức
            producer.Flush(TimeSpan.FromSeconds(1));
        }
    }
}