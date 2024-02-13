using System.Text;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using Simple.Kafka.Extensions;
using Simple.Kafka.Serializers;
using Simple.Kafka.Settings;

namespace Simple.Kafka.Producer;

public class RetryKafkaProducer<TKey, TValue> : IRetryKafkaProducer<TKey, TValue>
{
    private readonly SimpleKafkaSettings _settings;

    public string TopicName { get; set; }
    
    public RetryKafkaProducer(string topic, IOptions<SimpleKafkaSettings> settings)
    {
        TopicName = topic;
        _settings = settings.Value;
    }

    public async Task RetryAsync(Message<TKey, TValue> message, CancellationToken cancellationToken)
    {
        var producer = CreateProducer();
        var newMessage = new Message<byte[], TValue>()
        {
            Key = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(message.Key)),
            Value = message.Value,
            Headers = message.Headers
        };
        
        var result =  await producer.ProduceAsync(TopicName, newMessage, cancellationToken); 
        // TODO: add logging
    }

    private IProducer<byte[], TValue> CreateProducer()
    {
        var config = BuildConfiguration();
        var producer = new ProducerBuilder<byte[], TValue>(config)
            .SetKeySerializer(Confluent.Kafka.Serializers.ByteArray)
            .SetValueSerializer(new JsonSerializer<TValue>())
            .Build();
        return producer;
    }

    private ProducerConfig BuildConfiguration()
    {
        if (!_settings.Consumers.TryGetValue(typeof(TValue).Name, out var consumerSettings))
        {
            throw new Exception("Consumer not found");
        }
        var nativeConfig = consumerSettings.RetryProducerNativeConfig ?? new Dictionary<string, string>();

        if (consumerSettings.IsSharedRetryProducerNativeConfigEnabled)
        {
            nativeConfig = nativeConfig.MapConfigs(_settings.SharedProducerNativeConfig);
        }
        
        var config = new ProducerConfig(nativeConfig)
        {
            BootstrapServers = _settings.BootstrapServers,
        };

        return config;
    }
}