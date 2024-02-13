using System.Reflection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Simple.Kafka.Consumer;
using Simple.Kafka.Producer;
using Simple.Kafka.Settings;

namespace Simple.Kafka.Extensions;

public static class ServiceCollectionExtensions
{
    private const string SettingName = "SimpleKafka";
    public static IServiceCollection AddKafkaConsumer<TEvent>(this IServiceCollection services, IConfiguration configuration)
    where TEvent: class
    {
        services.AddSingleton<IKafkaConsumer>((provider) =>
        {
            var settings = provider.GetRequiredService<IOptions<SimpleKafkaSettings>>();
            var logger = provider.GetRequiredService<ILogger<KafkaConsumer<TEvent>>>();
            var retryService = provider.GetRequiredService<IRetryProducerService>();

            return new KafkaConsumer<TEvent>(settings, logger, provider, retryService);
        });

        var settings = configuration.GetSection(SettingName).Get<SimpleKafkaSettings>();

        if (settings is null
            || !settings.Consumers.TryGetValue(typeof(TEvent).Name, out var consumer)
            || consumer.RetryTopics is null or {Length: 0})
        {
            return services;
        }

        // services.AddKafkaReplyProducer<byte[], TEvent>(consumer.RetryTopics);

        return services;
    }

    public static IServiceCollection AddSimpleKafka(this IServiceCollection services, IConfiguration configuration,
        params Assembly[] assemblies)
    {
        // services.AddOptions<SimpleKafkaSettings>()
        //     .BindConfiguration(SettingName)
        //     .ValidateOnStart();
        services.Configure<SimpleKafkaSettings>(configuration.GetSection(SettingName));
        services.AddMediatR(cfg => cfg.RegisterServicesFromAssemblies(assemblies));
        services.AddHostedService<KafkaConsumerHostedService>();

        return services;
    }

    public static IServiceCollection AddKafkaProducer<TKey, TValue>(this IServiceCollection services)
    {
        services.AddSingleton<IKafkaProducer<TKey, TValue>>((provider) =>
        {
            var options = provider.GetRequiredService<IOptions<SimpleKafkaSettings>>();
            return new KafkaProducer<TKey, TValue>(options);
        });

        services.AddSingleton<IRetryProducerFactory, RetryProducerFactory>();
        services.AddSingleton<IRetryProducerService, RetryProducerService>();

        return services;
    }
    
    public static IServiceCollection AddKafkaReplyProducer<TKey, TValue>(this IServiceCollection services, params string[] topics)
    {
        foreach (var topic in topics)
        {
           services.TryAddEnumerable(ServiceDescriptor.KeyedSingleton<IRetryKafkaProducer<TKey, TValue>>(topic, (provider, _) =>
           {
               var options = provider.GetRequiredService<IOptions<SimpleKafkaSettings>>();
               return new RetryKafkaProducer<TKey, TValue>(topic, options);
           }));
        }
        
        return services;
    }
}