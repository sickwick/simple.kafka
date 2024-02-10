using Confluent.Kafka;

namespace Simple.Kafka.Producer;

public interface IKafkaProducer<TKey, TValue>
{
    Task<DeliveryResult<byte[], TValue>> ProduceAsync(TKey key, TValue value, CancellationToken cancellationToken,
        Headers? headers = null);
}