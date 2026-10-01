using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using WalletProducer.Models;
using WalletProducer.Services;

class Program
{
    static async Task Main(string[] args)
    {
        Console.WriteLine("==========================================================================");
        Console.WriteLine("    WALLET KAFKA SYSTEM - PRODUCER CLI & WIKIPEDIA EVENTSTREAM INGESTION");
        Console.WriteLine("==========================================================================");

        var host = Host.CreateDefaultBuilder(args)
            .ConfigureServices((_, services) =>
            {
                services.AddSingleton(sp => new WalletProducerService(
                    sp.GetRequiredService<ILogger<WalletProducerService>>()));
                services.AddSingleton(sp => new WikipediaStreamProducerService(
                    sp.GetRequiredService<ILogger<WikipediaStreamProducerService>>()));
            })
            .Build();

        var walletService = host.Services.GetRequiredService<WalletProducerService>();
        var wikiService = host.Services.GetRequiredService<WikipediaStreamProducerService>();

        Console.WriteLine("Chon che do Producer:");
        Console.WriteLine("1. Nhap giao dich thu cong (DEPOSIT / PAYMENT)");
        Console.WriteLine("2. Gia phong luong Lech Pha (Phase Mismatch Simulation)");
        Console.WriteLine("3. Khoi dong Wikipedia Live EventStream Producer (Wikimedia -> Kafka)");
        Console.WriteLine("4. Ban High Throughput 1,000 giao dich vi (Benchmark Batching & Snappy)");
        Console.Write("Lua chon (1-4): ");

        string? choice = Console.ReadLine();

        if (choice == "3")
        {
            using var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };
            await wikiService.StartWikipediaStreamAsync(cts.Token);
        }
        else if (choice == "2")
        {
            string userId = "user_mismatch_" + Guid.NewGuid().ToString().Substring(0, 5);
            Console.WriteLine($"\n[MO PHONG LECH PHA] User ID: {userId}");

            Console.WriteLine("1. Ban PAYMENT (300,000 VND) TRUOC...");
            await walletService.SendTransactionAsync(new TransactionEvent
            {
                UserId = userId,
                Type = "PAYMENT",
                Amount = 300000,
                ReferenceId = "ORDER-PAYMENT-FIRST-001",
                Description = "Thanh toan don hang thuong mai dien tu qua vi"
            });

            await Task.Delay(2000);

            Console.WriteLine("2. Ban DEPOSIT (500,000 VND) SAU...");
            await walletService.SendTransactionAsync(new TransactionEvent
            {
                UserId = userId,
                Type = "DEPOSIT",
                Amount = 500000,
                ReferenceId = "DEPOSIT-AFTER-PAYMENT-002",
                Description = "Nap tien tu ngan hang Vietcombank vao vi"
            });

            Console.WriteLine("[OK] Da ban xong 2 event lech pha! Xem Wallet xu ly Auto Matching.");
        }
        else if (choice == "4")
        {
            Console.WriteLine("\n[BENCHMARK] Dang gui 1,000 message giao dich vi...");
            var random = new Random();
            for (int i = 1; i <= 1000; i++)
            {
                string userId = $"user_{random.Next(1, 10):D3}";
                string type = random.Next(100) > 30 ? "DEPOSIT" : "PAYMENT";
                decimal amount = random.Next(1, 100) * 10000m;

                await walletService.SendTransactionAsync(new TransactionEvent
                {
                    UserId = userId,
                    Type = type,
                    Amount = amount,
                    ReferenceId = $"BENCHMARK-TX-{i:D5}",
                    Description = $"Giao dich benchmark high throughput so {i}"
                });
            }
            Console.WriteLine("[OK] Da hoan tat 1,000 message.");
        }
        else
        {
            Console.WriteLine("\n[THU CONG] Nhap theo dinh dang: <UserId> <Type: DEPOSIT/PAYMENT> <Amount>");
            Console.WriteLine("Vi du: user_123 DEPOSIT 500000");
            Console.WriteLine("Go 'exit' de thoat.\n");

            while (true)
            {
                Console.Write("Giao dich moi: ");
                string? input = Console.ReadLine();
                if (string.IsNullOrWhiteSpace(input) || input.ToLower() == "exit") break;

                var parts = input.Split(' ', 3);
                if (parts.Length < 3 || !decimal.TryParse(parts[2], out decimal amount))
                {
                    Console.WriteLine("[WARN] Nhap sai! Dinh dang: <UserId> <Type> <Amount>");
                    continue;
                }

                await walletService.SendTransactionAsync(new TransactionEvent
                {
                    UserId = parts[0],
                    Type = parts[1].ToUpper(),
                    Amount = amount,
                    ReferenceId = "MANUAL-" + Guid.NewGuid().ToString().Substring(0, 6),
                    Description = $"Giao dich thu cong {parts[1]} cho user {parts[0]}"
                });
            }
        }
    }
}
