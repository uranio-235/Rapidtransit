using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Rapidtransit;

namespace Rapidtransit.Tests;

public class MiddlewareTests
{
    [Fact]
    public async Task Middleware_wraps_handler_and_next_is_called()
    {
        var log = new List<string>();

        var host = Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddSingleton(log);
                services.AddRapidtransit(o => o.Use<LoggingMiddleware>().RegisterHandlersFrom<LoggingHandler>());
            })
            .Build();

        await host.StartAsync();

        var bus = host.Services.GetRequiredService<IBus>();
        await bus.Send(new LogMessage("hi"));
        await Task.Delay(200);

        Assert.Equal(["before", "handler", "after"], log);

        await host.StopAsync();
    }

    [Fact]
    public async Task Middleware_catches_handler_exception()
    {
        var tcs = new TaskCompletionSource<Exception>();

        var host = Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddSingleton(tcs);
                services.AddRapidtransit(o => o.Use<ErrorCatchingMiddleware>().RegisterHandlersFrom<ExplodingHandler>());
            })
            .Build();

        await host.StartAsync();

        var bus = host.Services.GetRequiredService<IBus>();
        await bus.Send(new BombMessage());

        var caught = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.IsType<InvalidOperationException>(caught);

        await host.StopAsync();
    }

    [Fact]
    public async Task Multiple_middlewares_execute_in_registration_order()
    {
        var log = new List<string>();

        var host = Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddSingleton(log);
                services.AddRapidtransit(o => o
                    .Use<FirstMiddleware>()
                    .Use<SecondMiddleware>()
                    .RegisterHandlersFrom<OrderHandler>());
            })
            .Build();

        await host.StartAsync();

        var bus = host.Services.GetRequiredService<IBus>();
        await bus.Send(new OrderMessage());
        await Task.Delay(200);

        Assert.Equal(["first-in", "second-in", "handler", "second-out", "first-out"], log);

        await host.StopAsync();
    }
}

// --- Messages ---

record LogMessage(string Text);
record BombMessage();
record OrderMessage();

// --- Handlers ---

class LoggingHandler(List<string> log) : IHandleMessages<LogMessage>
{
    public Task Handle(LogMessage message, CancellationToken cancellationToken = default)
    {
        log.Add("handler");
        return Task.CompletedTask;
    }
}

class ExplodingHandler : IHandleMessages<BombMessage>
{
    public Task Handle(BombMessage message, CancellationToken cancellationToken = default)
        => throw new InvalidOperationException("boom");
}

class OrderHandler(List<string> log) : IHandleMessages<OrderMessage>
{
    public Task Handle(OrderMessage message, CancellationToken cancellationToken = default)
    {
        log.Add("handler");
        return Task.CompletedTask;
    }
}

// --- Middlewares ---

class LoggingMiddleware(List<string> log) : IMessageMiddleware
{
    public async Task Handle(object message, Func<Task> next, CancellationToken cancellationToken = default)
    {
        log.Add("before");
        await next();
        log.Add("after");
    }
}

class ErrorCatchingMiddleware(TaskCompletionSource<Exception> tcs) : IMessageMiddleware
{
    public async Task Handle(object message, Func<Task> next, CancellationToken cancellationToken = default)
    {
        try
        {
            await next();
        }
        catch (Exception ex)
        {
            tcs.TrySetResult(ex);
        }
    }
}

class FirstMiddleware(List<string> log) : IMessageMiddleware
{
    public async Task Handle(object message, Func<Task> next, CancellationToken cancellationToken = default)
    {
        log.Add("first-in");
        await next();
        log.Add("first-out");
    }
}

class SecondMiddleware(List<string> log) : IMessageMiddleware
{
    public async Task Handle(object message, Func<Task> next, CancellationToken cancellationToken = default)
    {
        log.Add("second-in");
        await next();
        log.Add("second-out");
    }
}
