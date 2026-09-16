using System.Threading.Channels;
using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Rapidtransit;

internal sealed class DispatchWorker(
    Channel<Envelope> channel,
    HandlerRegistry registry,
    LatestWinsRegistry latestWinsRegistry,
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
            var partitionKey = envelope.Partition is null
                ? ((Type, string)?)null
                : (envelope.Message.GetType(), envelope.Partition);

            await semaphore.WaitAsync(stoppingToken);

            _ = Task.Run(async () =>
            {
                SemaphoreSlim? partitionGate = null;
                var partitionGateAcquired = false;

                try
                {
                    if (partitionKey is not null)
                    {
                        var key = partitionKey.Value;
                        partitionGate = _partitionGates.GetOrAdd(key, _ => new SemaphoreSlim(1, 1));
                        if (envelope.DeliveryMode == DeliveryMode.Busy)
                        {
                            if (!partitionGate.Wait(0))
                                return;
                        }
                        else
                        {
                            await partitionGate.WaitAsync(stoppingToken);
                        }

                        partitionGateAcquired = true;

                        if (envelope.DeliveryMode == DeliveryMode.LatestWins &&
                            !latestWinsRegistry.IsLatest(key, envelope))
                        {
                            return;
                        }
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
                    if (partitionKey is not null && envelope.DeliveryMode == DeliveryMode.LatestWins)
                        latestWinsRegistry.Remove(partitionKey.Value, envelope);

                    if (partitionGateAcquired)
                        partitionGate!.Release();
                    semaphore.Release();
                }
            }, stoppingToken);
        }
    }
}
