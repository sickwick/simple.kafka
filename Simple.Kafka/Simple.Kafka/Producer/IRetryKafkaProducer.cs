using Confluent.Kafka;

namespace Simple.Kafka.Producer;

public interface IRetryKafkaProducer<TKey, TValue>
{
    string TopicName { get; set; }
    
    Task RetryAsync(Message<TKey, TValue> message, CancellationToken cancellationToken);
}