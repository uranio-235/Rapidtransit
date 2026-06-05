using System.Threading.Channels;

namespace Rapidtransit;

internal sealed class RapidBus(Channel<Envelope> channel) : IBus
{
    public ValueTask Send<TMessage>(TMessage message, object? partition = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        return channel.Writer.WriteAsync(new Envelope(message, partition?.ToString()), cancellationToken);
    }
}
