using Confluent.Kafka;
using MediatR;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Simple.Kafka.Producer;
using Simple.Kafka.Settings;

namespace Simple.Kafka.Consumer;

public class KafkaRetryConsumer<TEvent> : KafkaConsumer<TEvent>
    where TEvent : class
{
    private readonly IOptions<SimpleKafkaSettings> _settings;
    private readonly ILogger<KafkaConsumer<TEvent>> _logger;
    private readonly IServiceProvider _serviceProvider;
    private readonly IRetryProducerService _producerService;
    private readonly IDelayCalculator _delayCalculator;
    // public string Topic { get; set; }
    public List<string> Topics { get; set; }

    public KafkaRetryConsumer(IOptions<SimpleKafkaSettings> settings, ILogger<KafkaConsumer<TEvent>> logger, IServiceProvider serviceProvider, IRetryProducerService producerService, IDelayCalculator delayCalculator) : base(settings, logger, serviceProvider, producerService)
    {
        _settings = settings;
        _logger = logger;
        _serviceProvider = serviceProvider;
        _producerService = producerService;
        _delayCalculator = delayCalculator;
    }

    protected override List<string> GetTopicsToSubscribe()
    {
        return Topics;
    }

    protected override async Task ProcessEventAsync(IConsumer<byte[], TEvent> consumer, string name, CancellationToken cancellationToken)
    {
        ConsumeResult<byte[], TEvent> kafkaEvent = new();
        var paused = false;
        try
        {
            kafkaEvent = consumer.Consume();

            if (kafkaEvent is null) return;

            if (_consumerSettings.RetryTopics.Contains(kafkaEvent.Topic))
            {
                var delayTime =
                    _delayCalculator.Calculate(kafkaEvent.Message.Timestamp, GetBackoffMultiplier(kafkaEvent.Topic),
                        _consumerSettings.InitialDelay);
                if (delayTime > DateTimeOffset.UtcNow)
                {
                    paused = true;
                    var partitions = new List<TopicPartition>() { kafkaEvent.TopicPartition };
                    consumer.Pause(partitions);
                    await DelayTillRetryTime(delayTime, cancellationToken);
                }
            }

            _logger.LogDebug("{0}: New message consumed", name);

            await using (var scope = _serviceProvider.CreateAsyncScope())
            {
                var mediatr = scope.ServiceProvider.GetRequiredService<IMediator>();
                await mediatr.Send(kafkaEvent.Message.Value);
            }
            
            if (!_consumerConfig.EnableAutoCommit ?? false)
            {
                consumer.Commit(kafkaEvent);
            }
        }catch (Exception ex)
        {
            // _logger.LogError(ex, "Error while working with event");

            if (_consumerSettings.RetryTopics is null or { Length: 0 })
            {
                return;
            }
            
            Task.Run(async () =>
            {
                try
                {
                    await _producerService.SendToRetryAsync(kafkaEvent, _consumerSettings.RetryTopics,
                        cancellationToken);
                }
                catch (Exception e)
                {
                    _logger.LogError(e, "Error while send to reply");
                }
            });
        }
        finally
        {
            if (paused)
                consumer.Resume(new List<TopicPartition>() { kafkaEvent.TopicPartition });
        }
    }
    private static async Task DelayTillRetryTime(DateTimeOffset delayTime, CancellationToken cancellationToken)
    {
        var delayTimespan = delayTime - DateTimeOffset.UtcNow;
        var delayMs = Math.Max(Convert.ToInt32(delayTimespan.TotalMilliseconds), 0);
        await Task.Delay(delayMs, cancellationToken);
    }
    
    
    private int GetBackoffMultiplier(string topicName)
        => Array.IndexOf(_consumerSettings.RetryTopics, topicName) + 1;
}