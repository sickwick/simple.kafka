using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Simple.Kafka.Consumer;

public class KafkaConsumerHostedService : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;

    public KafkaConsumerHostedService(IServiceProvider serviceProvider)
    {
        _serviceProvider = serviceProvider;
    }
    
    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var services = _serviceProvider.GetServices<IKafkaConsumer>();
        foreach (var service in services)
        {
            Task.Run(async () => await service.DoWorkAsync(stoppingToken), stoppingToken);
        }
        
        return Task.CompletedTask;
    }
}