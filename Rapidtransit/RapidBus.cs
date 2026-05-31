using System.Threading.Channels;

namespace Rapidtransit;

internal sealed class RapidBus(Channel<object> channel) : IBus
{
    public ValueTask Send<TMessage>(TMessage message, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        return channel.Writer.WriteAsync(message, cancellationToken);
    }
}
