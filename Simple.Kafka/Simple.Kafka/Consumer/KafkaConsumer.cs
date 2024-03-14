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
    protected readonly SimpleKafkaConsumerSettings _consumerSettings;
    private readonly ILogger _logger;
    private readonly IServiceProvider _serviceProvider;
    protected ConsumerConfig _consumerConfig;
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

        using var consumer = CreateConsumer();
        if (consumer == null)
        {
            return;
        }

        try
        {
            name = consumer.Name;

            _logger.LogDebug("Consumer {0} starting", name);
            var semaphore = new Semaphore(1, 10);
            var i = 0;

            while (!cancellationToken.IsCancellationRequested)
            {
                // if (i < 10)
                // {
                //     var t = new Thread(async () =>
                //     {
                //         semaphore.WaitOne();
                //         i++;
                //         await ProcessEventAsync(consumer, name, cancellationToken);
                //         semaphore.Release();
                //         i--;
                //     });
                //     t.Start();
                // }
                // if (_tasks.Count(c => c is
                //         { IsCompleted: false, IsCanceled: false, IsFaulted: false, IsCompletedSuccessfully: false }) <
                //     100)
                // {
                //     var task = Task.Run(async () => { await ProcessEventAsync(consumer, name, cancellationToken); },
                //         cancellationToken);
                //     _tasks.Add(task);
                // }
                // else
                // {
                //     await Task.Delay(5000, cancellationToken);
                //     _tasks.RemoveAll(c => c.IsCanceled || c.IsCompleted || c.IsFaulted || c.IsCompletedSuccessfully);
                // }
                await ProcessEventAsync(consumer, name, cancellationToken);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Consumer {0} has error", name);
        }
        finally
        {
            consumer.Close();
        }
    }

    protected virtual async Task ProcessEventAsync(IConsumer<byte[], TEvent> consumer, string name,
        CancellationToken cancellationToken)
    {
        ConsumeResult<byte[], TEvent> kafkaEvent = new();
        var paused = false;
        try
        {
            kafkaEvent = consumer.Consume();

            if (kafkaEvent is null) return;

            // if (_consumerSettings.RetryTopics.Contains(kafkaEvent.Topic))
            // {
            //     var delayTime =
            //         _delayCalculator.Calculate(kafkaEvent.Message.Timestamp, GetBackoffMultiplier(kafkaEvent.Topic),
            //             _consumerSettings.InitialDelay);
            //     if (delayTime > DateTimeOffset.UtcNow)
            //     {
            //         paused = true;
            //         var partitions = new List<TopicPartition>() { kafkaEvent.TopicPartition };
            //         consumer.Pause(partitions);
            //         await DelayTillRetryTime(delayTime, cancellationToken);
            //     }
            // }

            _logger.LogDebug("{0}: New message consumed", name);

            await using (var scope = _serviceProvider.CreateAsyncScope())
            {
                var mediatr = scope.ServiceProvider.GetRequiredService<IMediator>();
                await mediatr.Send(kafkaEvent.Message.Value, cancellationToken);
            }
            
            if (!_consumerConfig.EnableAutoCommit ?? false)
            {
                consumer.Commit(kafkaEvent);
            }
        }
        catch (Exception ex)
        {
            // _logger.LogError(ex, "Error while working with event");

            if (_consumerSettings.RetryTopics is null or { Length: 0 })
            {
                return;
            }

            try
            {
                Task.Run(async () => await _producerService.SendToRetryAsync(kafkaEvent, _consumerSettings.RetryTopics, cancellationToken));
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Error while send to reply");
            }
        }
        finally
        {
            if (paused)
                consumer.Resume(new List<TopicPartition>() { kafkaEvent.TopicPartition });
        }
    }

    protected virtual IConsumer<byte[], TEvent> CreateConsumer()
    {
        try
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

            var topics = GetTopicsToSubscribe();

            consumer.Subscribe(topics);

            return consumer;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, ex.Message);
            return null;
        }
    }

    protected virtual List<string> GetTopicsToSubscribe()
    {
        var topics = new List<string> { _consumerSettings.Topic };
        // if (_consumerSettings.RetryTopics is not null and { Length: > 0 })
        // {
        //     topics.AddRange(_consumerSettings.RetryTopics);
        // }

        return topics;
    }

    protected virtual void BuildConfiguration()
    {
        var nativeConfig = _consumerSettings.NativeConfig ?? new Dictionary<string, string>();

        if (_consumerSettings.IsSharedNativeConfigEnabled)
        {
            nativeConfig = nativeConfig.MapConfigs(_settings.SharedConsumerNativeConfig);
        }

        var consumerConfig = new ConsumerConfig(nativeConfig)
        {
            BootstrapServers = _settings.BootstrapServers,
            GroupId = _consumerSettings.Group,
        };

        _consumerConfig = consumerConfig;
    }
}