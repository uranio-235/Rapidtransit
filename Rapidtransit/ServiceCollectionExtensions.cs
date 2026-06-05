using System.Reflection;
using System.Threading.Channels;
using Microsoft.Extensions.DependencyInjection;

namespace Rapidtransit;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddRapidtransit(
        this IServiceCollection services,
        Action<RapidBusOptions>? configure = null)
    {
        var opts = new RapidBusOptions();
        configure?.Invoke(opts);

        var channel = Channel.CreateBounded<Envelope>(new BoundedChannelOptions(opts.ChannelCapacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
        });

        var registry = new HandlerRegistry();

        foreach (var assembly in opts.HandlerAssemblies)
            RegisterHandlersFromAssembly(services, registry, assembly);

        foreach (var middlewareType in opts.MiddlewareTypes)
            services.AddTransient(middlewareType);

        services.AddSingleton(opts);
        services.AddSingleton(channel);
        services.AddSingleton(registry);
        services.AddSingleton<IBus, RapidBus>();
        services.AddHostedService<DispatchWorker>();

        return services;
    }

    // escape hatch: registrar handlers fuera del fluent si hace falta
    public static IServiceCollection AutoRegisterHandlersFromAssemblyOf<T>(this IServiceCollection services)
        => services.AutoRegisterHandlersFromAssembly(typeof(T).Assembly);

    public static IServiceCollection AutoRegisterHandlersFromAssembly(this IServiceCollection services, Assembly assembly)
    {
        var descriptor = services.FirstOrDefault(d => d.ServiceType == typeof(HandlerRegistry));
        var registry = descriptor?.ImplementationInstance as HandlerRegistry
            ?? throw new InvalidOperationException("Call AddRapidtransit() before AutoRegisterHandlersFromAssembly().");

        RegisterHandlersFromAssembly(services, registry, assembly);
        return services;
    }

    private static void RegisterHandlersFromAssembly(IServiceCollection services, HandlerRegistry registry, Assembly assembly)
    {
        foreach (var type in assembly.GetTypes())
        {
            if (type.IsAbstract || type.IsInterface) continue;
            if (!type.GetInterfaces().Any(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IHandleMessages<>)))
                continue;

            services.AddTransient(type);
            registry.Register(type);
        }
    }
}
