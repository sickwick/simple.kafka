using Confluent.Kafka;

namespace Simple.Kafka.Producer;

public interface IRetryProducerService
{
    Task SendToRetryAsync<TEvent>(
        ConsumeResult<byte[], TEvent> result,
        IEnumerable<string> retryTopics,
        CancellationToken cancellationToken);
}