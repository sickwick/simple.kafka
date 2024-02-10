using System.ComponentModel.DataAnnotations;

namespace Simple.Kafka.Settings;

public class SimpleKafkaProducerSettings
{
    [Required]
    public string Topic { get; init; }

    public IDictionary<string, string> NativeConfig { get; set; }

    public bool IsSharedNativeConfigEnabled { get; init; }
}