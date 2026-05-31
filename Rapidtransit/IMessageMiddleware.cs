namespace Rapidtransit;

public interface IMessageMiddleware
{
    Task Handle(object message, Func<Task> next, CancellationToken cancellationToken = default);
}
