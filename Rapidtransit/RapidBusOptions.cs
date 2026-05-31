using System.Reflection;

namespace Rapidtransit;

public sealed class RapidBusOptions
{
    public int MaxParallelism { get; set; } = 5;
    public int ChannelCapacity { get; set; } = 1000;

    internal List<Type> MiddlewareTypes { get; } = [];
    internal List<Assembly> HandlerAssemblies { get; } = [];

    public RapidBusOptions Use<TMiddleware>() where TMiddleware : IMessageMiddleware
    {
        MiddlewareTypes.Add(typeof(TMiddleware));
        return this;
    }

    public RapidBusOptions RegisterHandlersFrom<TMarker>()
    {
        HandlerAssemblies.Add(typeof(TMarker).Assembly);
        return this;
    }
}
