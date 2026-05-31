using Microsoft.Extensions.DependencyInjection;
using System.Reflection;
using System.Runtime.ExceptionServices;

namespace Rapidtransit;

internal sealed class HandlerRegistry
{
    // message type → handler service type
    private readonly Dictionary<Type, Type> _map = [];

    internal void Register(Type handlerType)
    {
        foreach (var iface in handlerType.GetInterfaces())
        {
            if (!iface.IsGenericType || iface.GetGenericTypeDefinition() != typeof(IHandleMessages<>))
                continue;

            var messageType = iface.GetGenericArguments()[0];
            _map[messageType] = handlerType;
        }
    }

    internal void AutoRegisterFromAssembly(Assembly assembly)
    {
        foreach (var type in assembly.GetTypes())
        {
            if (type.IsAbstract || type.IsInterface) continue;
            if (type.GetInterfaces().Any(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IHandleMessages<>)))
                Register(type);
        }
    }

    internal async Task Dispatch(object message, IServiceProvider services, CancellationToken ct)
    {
        var messageType = message.GetType();

        if (!_map.TryGetValue(messageType, out var handlerType))
            throw new InvalidOperationException($"No handler registered for message type '{messageType.FullName}'.");

        var handler = services.GetRequiredService(handlerType);

        var iface = typeof(IHandleMessages<>).MakeGenericType(messageType);
        var method = iface.GetMethod(nameof(IHandleMessages<object>.Handle))!;

        try
        {
            await (Task)method.Invoke(handler, [message, ct])!;
        }
        catch (TargetInvocationException ex) when (ex.InnerException is not null)
        {
            ExceptionDispatchInfo.Capture(ex.InnerException).Throw();
        }
    }

    internal IEnumerable<Type> HandlerTypes => _map.Values;
}
