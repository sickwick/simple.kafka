namespace Simple.Kafka.Consumer;

public interface IKafkaConsumer
{
    Task DoWorkAsync(CancellationToken cancellationToken);
}