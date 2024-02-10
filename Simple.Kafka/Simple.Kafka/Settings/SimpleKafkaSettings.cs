using System.ComponentModel.DataAnnotations;

namespace Simple.Kafka.Settings;

public record SimpleKafkaSettings
{
    [Required] public string BootstrapServers { get; init; }

    public Dictionary<string, SimpleKafkaConsumerSettings> Consumers { get; init; }
    public IDictionary<string, string> SharedConsumerNativeConfig { get; init; }

    public Dictionary<string, SimpleKafkaProducerSettings> Producers { get; init; }

    public IDictionary<string, string> SharedProducerNativeConfig { get; init; }
}