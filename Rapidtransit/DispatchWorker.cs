using System.Threading.Channels;
using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Rapidtransit;

internal sealed class DispatchWorker(
    Channel<object> channel,
    HandlerRegistry registry,
    IServiceScopeFactory scopeFactory,
    RapidBusOptions options,
    ILogger<DispatchWorker> logger) : BackgroundService
{
    private readonly ConcurrentDictionary<Type, SemaphoreSlim> _sequentialHandlerGates = new();

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var semaphore = new SemaphoreSlim(options.MaxParallelism);

        await foreach (var message in channel.Reader.ReadAllAsync(stoppingToken))
        {
            await semaphore.WaitAsync(stoppingToken);

            _ = Task.Run(async () =>
            {
                SemaphoreSlim? sequentialGate = null;

                try
                {
                    if (registry.TryGetHandlerType(message.GetType(), out var handlerType)
                        && registry.IsSequentialHandler(handlerType))
                    {
                        sequentialGate = _sequentialHandlerGates.GetOrAdd(handlerType, _ => new SemaphoreSlim(1, 1));
                        await sequentialGate.WaitAsync(stoppingToken);
                    }

                    using var scope = scopeFactory.CreateScope();

                    Func<Task> pipeline = () => registry.Dispatch(message, scope.ServiceProvider, stoppingToken);

                    foreach (var middlewareType in options.MiddlewareTypes.AsEnumerable().Reverse())
                    {
                        var next = pipeline;
                        var mw = (IMessageMiddleware)scope.ServiceProvider.GetRequiredService(middlewareType);
                        pipeline = () => mw.Handle(message, next, stoppingToken);
                    }

                    await pipeline();
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Unhandled exception dispatching {MessageType}", message.GetType().Name);
                }
                finally
                {
                    sequentialGate?.Release();
                    semaphore.Release();
                }
            }, stoppingToken);
        }
    }
}
