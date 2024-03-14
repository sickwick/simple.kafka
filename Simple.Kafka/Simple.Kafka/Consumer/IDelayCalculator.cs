using Confluent.Kafka;

namespace Simple.Kafka.Consumer;

public interface IDelayCalculator
{
    DateTimeOffset Calculate(Timestamp messageTimestamp, int retryIndex, TimeSpan initialDelay);
}