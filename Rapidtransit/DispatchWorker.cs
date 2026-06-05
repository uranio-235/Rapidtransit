using System.Threading.Channels;
using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Rapidtransit;

internal sealed class DispatchWorker(
    Channel<Envelope> channel,
    HandlerRegistry registry,
    IServiceScopeFactory scopeFactory,
    RapidBusOptions options,
    ILogger<DispatchWorker> logger) : BackgroundService
{
    private readonly ConcurrentDictionary<(Type, string), SemaphoreSlim> _partitionGates = new();

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var semaphore = new SemaphoreSlim(options.MaxParallelism);

        await foreach (var envelope in channel.Reader.ReadAllAsync(stoppingToken))
        {
            await semaphore.WaitAsync(stoppingToken);

            _ = Task.Run(async () =>
            {
                SemaphoreSlim? partitionGate = null;

                try
                {
                    if (envelope.Partition is not null)
                    {
                        var key = (envelope.Message.GetType(), envelope.Partition);
                        partitionGate = _partitionGates.GetOrAdd(key, _ => new SemaphoreSlim(1, 1));
                        await partitionGate.WaitAsync(stoppingToken);
                    }

                    using var scope = scopeFactory.CreateScope();

                    Func<Task> pipeline = () => registry.Dispatch(envelope.Message, scope.ServiceProvider, stoppingToken);

                    foreach (var middlewareType in options.MiddlewareTypes.AsEnumerable().Reverse())
                    {
                        var next = pipeline;
                        var mw = (IMessageMiddleware)scope.ServiceProvider.GetRequiredService(middlewareType);
                        pipeline = () => mw.Handle(envelope.Message, next, stoppingToken);
                    }

                    await pipeline();
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Unhandled exception dispatching {MessageType}", envelope.Message.GetType().Name);
                }
                finally
                {
                    partitionGate?.Release();
                    semaphore.Release();
                }
            }, stoppingToken);
        }
    }
}
