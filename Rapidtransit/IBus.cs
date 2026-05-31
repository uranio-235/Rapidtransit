namespace Rapidtransit;

public interface IBus
{
    ValueTask Send<TMessage>(TMessage message, CancellationToken cancellationToken = default);
}
