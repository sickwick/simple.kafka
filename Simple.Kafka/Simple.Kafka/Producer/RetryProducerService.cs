using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Simple.Kafka.Producer;

public class RetryProducerService : IRetryProducerService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly IRetryProducerFactory _producerFactory;
    private readonly ILogger<RetryProducerService> _logger;

    public RetryProducerService(IServiceProvider serviceProvider, IRetryProducerFactory producerFactory, ILogger<RetryProducerService> logger)
    {
        _serviceProvider = serviceProvider;
        _producerFactory = producerFactory;
        _logger = logger;
    }

    public async Task SendToRetryAsync<TEvent>(
        ConsumeResult<byte[], TEvent> result,
        IEnumerable<string> retryTopics,
        CancellationToken cancellationToken)
    {
        var nextTopic = GetNextTopic(result, retryTopics);

        if (nextTopic is null)
        {
            _logger.LogWarning("Не найден топик повтора");
            return;
        }

        // await using var scope = _serviceProvider.CreateAsyncScope();
        // var services = scope.ServiceProvider.GetServices<IRetryKafkaProducer<byte[], TEvent>>();
        // var producer = services.FirstOrDefault(c => c.TopicName.Equals(nextTopic));
        var producer = _producerFactory.GetProducer<byte[], TEvent>(nextTopic);

        if (producer is null)
        {
            return;
        }
        
        await producer.RetryAsync(result.Message, cancellationToken);
    }

    private string? GetNextTopic<TEvent>(ConsumeResult<byte[], TEvent> result, IEnumerable<string> retryTopics)
    {
        var currentTopic = result.Topic;
        
        var nextTopic = retryTopics.FirstOrDefault();

        if (retryTopics.Contains(currentTopic))
        {
            nextTopic = retryTopics.SkipWhile(c => c != currentTopic).Skip(1).FirstOrDefault();   
        }
        return nextTopic;
    }
}