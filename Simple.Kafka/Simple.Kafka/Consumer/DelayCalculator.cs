using Confluent.Kafka;

namespace Simple.Kafka.Consumer;

public class DelayCalculator : IDelayCalculator
{
    public DateTimeOffset Calculate(Timestamp messageTimestamp, int retryIndex, TimeSpan initialDelay)
    {
        var unixTimestampMs = messageTimestamp.UnixTimestampMs;
        var next = GetNextInProgression(retryIndex);
        var timeToAdd = next * initialDelay.TotalMilliseconds;
        var retryTime = UnixTimestampMsToDateTime(unixTimestampMs + timeToAdd);
        return new DateTimeOffset(retryTime, TimeSpan.Zero);
    }

    private static DateTime UnixTimestampMsToDateTime(double unixMillisecondsTimestamp)
    {
        return Timestamp.UnixTimeEpoch + TimeSpan.FromMilliseconds(unixMillisecondsTimestamp);
    }

    private double GetNextInProgression(double time)
    {
        return new Random().NextDouble() * 2 + time;
    }
}