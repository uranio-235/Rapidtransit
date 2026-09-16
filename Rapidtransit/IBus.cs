namespace Rapidtransit;

public interface IBus
{
    ValueTask Send<TMessage>(
        TMessage message,
        object? partition = null,
        TimeSpan? giveupTime = null,
        CancellationToken cancellationToken = default);

    ValueTask Send<TMessage>(TMessage message, object? partition, CancellationToken cancellationToken);
}
