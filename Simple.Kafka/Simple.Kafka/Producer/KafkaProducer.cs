using System.Text;
using Confluent.Kafka;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using Simple.Kafka.Extensions;
using Simple.Kafka.Serializers;
using Simple.Kafka.Settings;

namespace Simple.Kafka.Producer;

public class KafkaProducer<TKey, TValue> : IKafkaProducer<TKey, TValue>
{
    private ProducerConfig _producerConfig;
    private SimpleKafkaProducerSettings _producerSettings;
    private SimpleKafkaSettings _settings;
    private IProducer<byte[], TValue> _producer;
    
    public KafkaProducer(IOptions<SimpleKafkaSettings> settings)
    {
        _settings = settings.Value;
        
        if (!_settings.Producers.TryGetValue(typeof(TValue).Name, out _producerSettings))
        {
            throw new Exception($"Not found settings for {typeof(TValue).Name} producer");
        }

        BuildConfiguration();
        _producer = CreateProducer();
    }

    public async Task<DeliveryResult<byte[], TValue>> ProduceAsync(
        TKey key,
        TValue value,
        CancellationToken cancellationToken,
        Headers? headers = null)
    {
        headers ??= new Headers();

        var message = new Message<byte[], TValue>()
        {
            Key = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(key)),
            Value = value,
            Headers = headers
        };
        
        return  await _producer.ProduceAsync(_producerSettings.Topic, message);
    }

    private IProducer<byte[], TValue> CreateProducer()
    {
        var producer = new ProducerBuilder<byte[], TValue>(_producerConfig)
            .SetKeySerializer(Confluent.Kafka.Serializers.ByteArray)
            .SetValueSerializer(new JsonSerializer<TValue>())
            .Build();
        return producer;
    }

    private void BuildConfiguration()
    {
        var nativeConfig = _producerSettings.NativeConfig ?? new Dictionary<string, string>();

        if (_producerSettings.IsSharedNativeConfigEnabled)
        {
            nativeConfig = nativeConfig.MapConfigs(_settings.SharedProducerNativeConfig);
        }
        
        var config = new ProducerConfig(nativeConfig)
        {
            BootstrapServers = _settings.BootstrapServers,
        };

        _producerConfig = config;
    }
}