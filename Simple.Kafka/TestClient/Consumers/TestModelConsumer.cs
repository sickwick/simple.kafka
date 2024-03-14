using MediatR;

namespace TestClient.Consumers;

public class TestModelConsumer : IRequestHandler<TestModel>
{
    public Task Handle(TestModel request, CancellationToken cancellationToken)
    {
        if (request.TesMessage.Equals("error"))
        {
            throw new Exception();
        }
        Console.WriteLine(request.TesMessage);
        // throw new Exception("for retry");
        return Task.CompletedTask;
    }
}