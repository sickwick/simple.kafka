namespace Simple.Kafka.Producer;

public interface IRetryProducerFactory
{
    IRetryKafkaProducer<TKey, TValue> GetProducer<TKey, TValue>(string topic);
}