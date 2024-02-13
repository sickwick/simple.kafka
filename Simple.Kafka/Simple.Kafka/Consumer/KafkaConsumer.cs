using Confluent.Kafka;
using MediatR;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Simple.Kafka.Extensions;
using Simple.Kafka.Producer;
using Simple.Kafka.Serializers;
using Simple.Kafka.Settings;

namespace Simple.Kafka.Consumer;

public class KafkaConsumer<TEvent> : IKafkaConsumer
    where TEvent : class
{
    private readonly SimpleKafkaSettings _settings;
    private readonly SimpleKafkaConsumerSettings _consumerSettings;
    private readonly ILogger _logger;
    private readonly IServiceProvider _serviceProvider;
    private ConsumerConfig _consumerConfig;
    private readonly IRetryProducerService _producerService;

    public KafkaConsumer(IOptions<SimpleKafkaSettings> settings, ILogger<KafkaConsumer<TEvent>> logger,
        IServiceProvider serviceProvider, IRetryProducerService producerService)
    {
        _logger = logger;
        _serviceProvider = serviceProvider;
        _producerService = producerService;
        _settings = settings.Value;

        if (!_settings.Consumers.TryGetValue(typeof(TEvent).Name, out _consumerSettings))
        {
            throw new Exception($"Not found settings for {typeof(TEvent).Name} consumer");
        }

        BuildConfiguration();
    }

    public async Task DoWorkAsync(CancellationToken cancellationToken)
    {
        var name = string.Empty;

        try
        {
            using var consumer = CreateConsumer();
            name = consumer.Name;

            _logger.LogDebug("Consumer {0} starting", name);

            while (!cancellationToken.IsCancellationRequested)
            {
                await ProcessEventAsync(consumer, name, cancellationToken);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Consumer {0} has error", name);
        }
    }

    private async Task ProcessEventAsync(IConsumer<byte[], TEvent> consumer, string name,
        CancellationToken cancellationToken)
    {
        ConsumeResult<byte[], TEvent> kafkaEvent = new();
        try
        {
            kafkaEvent = consumer.Consume(cancellationToken);

            if (kafkaEvent is null) return;

            _logger.LogDebug("{0}: New message consumed", name);

            await using var scope = _serviceProvider.CreateAsyncScope();
            var mediatr = scope.ServiceProvider.GetRequiredService<IMediator>();
            await mediatr.Send(kafkaEvent.Message.Value, cancellationToken);

            if (!_consumerConfig.EnableAutoCommit ?? false)
            {
                consumer.Commit(kafkaEvent);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error while working with event");

            if (_consumerSettings.RetryTopics is null or { Length: 0 })
            {
                return;
            }

            try
            {
                await _producerService.SendToRetryAsync(kafkaEvent, _consumerSettings.RetryTopics, cancellationToken);
            }
            catch (Exception e)
            {
                _logger.LogError(ex, "Error while send to reply");
            }

        }
    }

    private IConsumer<byte[], TEvent> CreateConsumer()
    {
        var consumer = new ConsumerBuilder<byte[], TEvent>(_consumerConfig)
            .SetErrorHandler((c, e) => _logger.LogError("{0} Error in consumer: {1}, reason: {2}",
                _consumerSettings.Topic, c.Name, e.Reason))
            .SetPartitionsAssignedHandler((_, p) =>
                _logger.LogDebug("{0} Assigned partitions: [{1}]", _consumerSettings.Topic, string.Join(',', p)))
            .SetPartitionsRevokedHandler((_, p) =>
                _logger.LogDebug("{0} Revoked partitions: [{1}]", _consumerSettings.Topic, string.Join(',', p)))
            .SetLogHandler((_, m) => _logger.LogDebug("Consumer {0} log with message: {1}", m.Name, m.Message))
            .SetKeyDeserializer(Deserializers.ByteArray)
            .SetValueDeserializer(new JsonDeserializer<TEvent>())
            .Build();

        var topics = new List<string> { _consumerSettings.Topic };
        if (_consumerSettings.RetryTopics is not null and { Length: > 0 })
        {
            topics.AddRange(_consumerSettings.RetryTopics);
        }
        
        consumer.Subscribe(topics);

        return consumer;
    }

    private void BuildConfiguration()
    {
        var nativeConfig = _consumerSettings.NativeConfig ?? new Dictionary<string, string>();

        if (_consumerSettings.IsSharedNativeConfigEnabled)
        {
            nativeConfig = nativeConfig.MapConfigs(_settings.SharedConsumerNativeConfig);
        }

        var consumerConfig = new ConsumerConfig(nativeConfig)
        {
            BootstrapServers = _settings.BootstrapServers,
            GroupId = _consumerSettings.Group
        };

        _consumerConfig = consumerConfig;
    }
}