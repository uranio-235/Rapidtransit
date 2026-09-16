namespace Rapidtransit;

public interface IBus
{
    ValueTask Send<TMessage>(
        TMessage message,
        object? partition = null,
        DeliveryMode deliveryMode = DeliveryMode.EveryoneGetsAChance,
        CancellationToken cancellationToken = default);

    ValueTask Send<TMessage>(TMessage message, object? partition, CancellationToken cancellationToken);
}
