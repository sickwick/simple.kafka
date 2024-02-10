using System.ComponentModel.DataAnnotations;

namespace Simple.Kafka.Settings;

public record SimpleKafkaConsumerSettings
{
    public string Group { get; init; }

    [Required]
    public string Topic { get; init; }

    public IDictionary<string, string> NativeConfig { get; set; }

    public bool IsSharedNativeConfigEnabled { get; init; }
}