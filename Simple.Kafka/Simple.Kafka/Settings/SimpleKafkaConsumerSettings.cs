using System.ComponentModel.DataAnnotations;
using Confluent.Kafka;

namespace Simple.Kafka.Settings;

public record SimpleKafkaConsumerSettings
{
    public string Group { get; init; }

    [Required]
    public string Topic { get; init; }

    public IDictionary<string, string> NativeConfig { get; set; }

    public bool IsSharedNativeConfigEnabled { get; init; }

    public string[] RetryTopics { get; init; }

    public IDictionary<string, string> RetryProducerNativeConfig { get; init; }
    
    public bool IsSharedRetryProducerNativeConfigEnabled { get; init; }

    public TimeSpan InitialDelay { get; init; }
}