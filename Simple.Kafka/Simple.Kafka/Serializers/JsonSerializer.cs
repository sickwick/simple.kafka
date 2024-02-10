using System.Text;
using Confluent.Kafka;
using Newtonsoft.Json;

namespace Simple.Kafka.Serializers;

public class JsonSerializer<TValue> : ISerializer<TValue>
{
    public byte[] Serialize(TValue data, SerializationContext context)
    {
        try
        {
            var content = JsonConvert.SerializeObject(data);
            return Encoding.UTF8.GetBytes(content);
        }
        catch (AggregateException ex)
        {
            throw ex.InnerException;
        }
    }
}