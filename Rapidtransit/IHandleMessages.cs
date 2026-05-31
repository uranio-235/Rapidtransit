namespace Rapidtransit;

public interface IHandleMessages<in TMessage>
{
    Task Handle(TMessage message, CancellationToken cancellationToken = default);
}
