using MediatR;

namespace TestClient.Consumers;

public class TestModelConsumer : IRequestHandler<TestModel>
{
    public Task Handle(TestModel request, CancellationToken cancellationToken)
    {
        Console.WriteLine(request.TesMessage);
        throw new Exception("for retry");
        return Task.CompletedTask;
    }
}