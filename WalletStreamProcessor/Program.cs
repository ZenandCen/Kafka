using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using WalletStreamProcessor.Services;

class Program
{
    static void Main(string[] args)
    {
        Console.WriteLine("==========================================================================");
        Console.WriteLine("        WALLET KAFKA STREAM PROCESSOR (.NET BACKGROUND SERVICE)");
        Console.WriteLine("==========================================================================");

        var host = Host.CreateDefaultBuilder(args)
            .ConfigureServices((_, services) =>
            {
                services.AddHostedService<TransactionStreamProcessorWorker>();
            })
            .Build();

        host.Run();
    }
}
