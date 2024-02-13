using System.Text;
using Microsoft.AspNetCore.Mvc;
using Simple.Kafka.Producer;

namespace TestClient.Controllers;

[ApiController]
[Route("")]
public class TestController : ControllerBase
{
    private readonly IKafkaProducer<byte[], TestModel> _producer;

    public TestController(IKafkaProducer<byte[], TestModel> producer)
    {
        _producer = producer;
    }
    
    [HttpGet]
    public async Task ProduceAsync(CancellationToken cancellationToken)
    {
        var key = Encoding.UTF8.GetBytes("test");
        await _producer.ProduceAsync(key, new TestModel(){TesMessage = "hello"}, cancellationToken);
    }
}