using MediatR;

namespace TestClient;

public class TestModel : IRequest
{
    public string TesMessage { get; set; }
}