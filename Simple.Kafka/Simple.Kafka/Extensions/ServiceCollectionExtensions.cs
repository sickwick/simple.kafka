using System.Reflection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Simple.Kafka.Consumer;
using Simple.Kafka.Settings;

namespace Simple.Kafka.Extensions;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddKafkaConsumer<TEvent>(this IServiceCollection services)
    where TEvent: class
    {
        services.AddSingleton<IKafkaConsumer>((provider) =>
        {
            var settings = provider.GetRequiredService<IOptions<SimpleKafkaSettings>>();
            var logger = provider.GetRequiredService<ILogger<KafkaConsumer<TEvent>>>();

            return new KafkaConsumer<TEvent>(settings, logger, provider);
        });

        return services;
    }

    public static IServiceCollection AddSimpleKafka(this IServiceCollection services, IConfiguration configuration,
        params Assembly[] assemblies)
    {
        services.Configure<SimpleKafkaSettings>(configuration.GetSection("SimpleKafka"));
        services.AddMediatR(cfg => cfg.RegisterServicesFromAssemblies(assemblies));
        services.AddHostedService<KafkaConsumerHostedService>();

        return services;
    }
}