using Confluent.Kafka;
using Newtonsoft.Json;

namespace Simple.Kafka.Serializers;

public class JsonDeserializer<TEvent> : IDeserializer<TEvent> where TEvent : class
{
    public TEvent Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
    {
        if (isNull)
            return null;

        try
        {
            var array = data.ToArray();

            using var stream = new MemoryStream(array);
            using var streamReader = new StreamReader(stream);
            var message = streamReader.ReadToEnd();
            return JsonConvert.DeserializeObject<TEvent>(message);
        }
        catch (AggregateException ex)
        {
            throw ex.InnerException;
        }
    }
}