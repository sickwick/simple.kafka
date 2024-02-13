using Microsoft.Extensions.Options;
using Simple.Kafka.Settings;

namespace Simple.Kafka.Producer;

public class RetryProducerFactory : IRetryProducerFactory
{
    private readonly IOptions<SimpleKafkaSettings> _settings;

    public RetryProducerFactory(IOptions<SimpleKafkaSettings> settings)
    {
        _settings = settings;
    }
    
    public IRetryKafkaProducer<TKey, TValue> GetProducer<TKey, TValue>(string topic)
    {
        return new RetryKafkaProducer<TKey, TValue>(topic, _settings);
    }
}