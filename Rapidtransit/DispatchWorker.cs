using System.Threading.Channels;
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
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var semaphore = new SemaphoreSlim(options.MaxParallelism);

        await foreach (var message in channel.Reader.ReadAllAsync(stoppingToken))
        {
            await semaphore.WaitAsync(stoppingToken);

            _ = Task.Run(async () =>
            {
                try
                {
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
                    semaphore.Release();
                }
            }, stoppingToken);
        }
    }
}
