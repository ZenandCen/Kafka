using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using WalletConsumer.Services;

class Program
{
    static async Task Main(string[] args)
    {
        Console.WriteLine("==========================================================================");
        Console.WriteLine("  WALLET KAFKA SYSTEM - ENTERPRISE CONSUMER, OPENSEARCH & SEMANTIC SEARCH");
        Console.WriteLine("==========================================================================");

        var host = Host.CreateDefaultBuilder(args)
            .ConfigureServices((_, services) =>
            {
                services.AddSingleton<WalletRedisStateService>();
                services.AddSingleton<OpenSearchIndexingService>();
                services.AddSingleton<SemanticSearchService>();
                services.AddHostedService<WalletConsumerWorker>();
                services.AddHostedService<DbBatchSyncWorker>();
            })
            .Build();

        if (args.Length > 0 && args[0] == "--semantic-search")
        {
            string query = args.Length > 1 ? args[1] : "Thanh toan don hang bi thieu tien va nap tien";
            var searchService = host.Services.GetRequiredService<SemanticSearchService>();
            await searchService.SearchSemanticVectorAsync(query);
            return;
        }

        await host.RunAsync();
    }
}
