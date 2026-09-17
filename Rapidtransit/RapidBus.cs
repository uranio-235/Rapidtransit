using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace Rapidtransit;

internal sealed class RapidBus(
    Channel<Envelope> channel,
    LatestWinsRegistry latestWinsRegistry,
    ILogger<IBus> logger) : IBus
{
    private long _sequence;

    public async ValueTask Send<TMessage>(
        TMessage message,
        object? partition = null,
        DeliveryMode deliveryMode = DeliveryMode.EveryoneGetsAChance,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);

        if (!Enum.IsDefined(deliveryMode))
            throw new ArgumentOutOfRangeException(nameof(deliveryMode));

        if (deliveryMode is DeliveryMode.LatestWins or DeliveryMode.Busy && partition is null)
            throw new ArgumentException($"{deliveryMode} requires a partition key.", nameof(partition));

        var partitionKey = partition?.ToString();
        var envelope = new Envelope(message, partitionKey, deliveryMode, Interlocked.Increment(ref _sequence));

        await channel.Writer.WriteAsync(envelope, cancellationToken);

        logger.LogInformation(
            "Message {MessageType} entered the {Partition} queue with delivery mode {DeliveryMode}.",
            message.GetType().Name,
            partitionKey ?? "global",
            deliveryMode);

        if (deliveryMode == DeliveryMode.LatestWins)
            latestWinsRegistry.Register((message.GetType(), partitionKey!), envelope);
    }

    public ValueTask Send<TMessage>(TMessage message, object? partition, CancellationToken cancellationToken)
        => Send(message, partition, DeliveryMode.EveryoneGetsAChance, cancellationToken);
}
