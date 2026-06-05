namespace Rapidtransit;

public interface IBus
{
    ValueTask Send<TMessage>(TMessage message, object? partition = null, CancellationToken cancellationToken = default);
}
