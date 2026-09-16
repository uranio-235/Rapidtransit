using System.Diagnostics;
using System.Threading.Channels;

namespace Rapidtransit;

internal sealed class RapidBus(Channel<Envelope> channel) : IBus
{
    public ValueTask Send<TMessage>(
        TMessage message,
        object? partition = null,
        TimeSpan? giveupTime = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);

        if (giveupTime < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(giveupTime), "Give-up time cannot be negative.");

        return channel.Writer.WriteAsync(
            new Envelope(message, partition?.ToString(), giveupTime, Stopwatch.GetTimestamp()),
            cancellationToken);
    }

    public ValueTask Send<TMessage>(TMessage message, object? partition, CancellationToken cancellationToken)
        => Send(message, partition, giveupTime: null, cancellationToken);
}
