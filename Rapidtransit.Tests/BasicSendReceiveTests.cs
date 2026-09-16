using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Rapidtransit;

namespace Rapidtransit.Tests;

public class BasicSendReceiveTests
{
    [Fact]
    public async Task Send_delivers_message_to_registered_handler()
    {
        var received = new TaskCompletionSource<PingMessage>();

        var host = Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddSingleton(received);
                services.AddRapidtransit(o => o.RegisterHandlersFrom<PingHandler>());
            })
            .Build();

        await host.StartAsync();

        var bus = host.Services.GetRequiredService<IBus>();
        await bus.Send(new PingMessage("hello"));

        var result = await received.Task.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal("hello", result.Content);

        await host.StopAsync();
    }

    [Fact]
    public async Task Send_multiple_messages_all_delivered()
    {
        const int count = 10;
        var counter = new CountdownLatch(count);

        var host = Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddSingleton(counter);
                services.AddRapidtransit(o => { o.MaxParallelism = 3; o.RegisterHandlersFrom<CountHandler>(); });
            })
            .Build();

        await host.StartAsync();

        var bus = host.Services.GetRequiredService<IBus>();

        for (var i = 0; i < count; i++)
            await bus.Send(new CountMessage(i));

        var done = await counter.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.True(done, "Not all messages were delivered in time.");

        await host.StopAsync();
    }

    [Fact]
    public async Task Handler_receives_injected_dependency()
    {
        var store = new MessageStore();

        var host = Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddSingleton(store);
                services.AddRapidtransit(o => o.RegisterHandlersFrom<StoringHandler>());
            })
            .Build();

        await host.StartAsync();

        var bus = host.Services.GetRequiredService<IBus>();
        await bus.Send(new StoreMessage("world"));

        await Task.Delay(200);

        Assert.Contains("world", store.Items);

        await host.StopAsync();
    }

    [Fact]
    public async Task Send_discards_message_that_exceeds_giveup_time()
    {
        var probe = new GiveupTimeProbe();

        var host = Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddSingleton(probe);
                services.AddRapidtransit(o =>
                {
                    o.MaxParallelism = 2;
                    o.RegisterHandlersFrom<GiveupTimeHandler>();
                });
            })
            .Build();

        await host.StartAsync();

        var bus = host.Services.GetRequiredService<IBus>();
        await bus.Send(new GiveupTimeMessage(1), partition: "same-key");
        await probe.FirstStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await bus.Send(
            new GiveupTimeMessage(2),
            partition: "same-key",
            giveupTime: TimeSpan.FromMilliseconds(50));

        await Task.Delay(100);
        probe.ReleaseFirst.TrySetResult();
        await probe.FirstFinished.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.ThrowsAsync<TimeoutException>(() =>
            probe.SecondHandled.Task.WaitAsync(TimeSpan.FromMilliseconds(250)));

        await host.StopAsync();
    }

}

// --- Messages ---

record PingMessage(string Content);
record CountMessage(int Index);
record StoreMessage(string Value);
record GiveupTimeMessage(int Number);

// --- Handlers ---

class PingHandler(TaskCompletionSource<PingMessage> tcs) : IHandleMessages<PingMessage>
{
    public Task Handle(PingMessage message, CancellationToken cancellationToken = default)
    {
        tcs.TrySetResult(message);
        return Task.CompletedTask;
    }
}

class CountHandler(CountdownLatch latch) : IHandleMessages<CountMessage>
{
    public Task Handle(CountMessage message, CancellationToken cancellationToken = default)
    {
        latch.Signal();
        return Task.CompletedTask;
    }
}

class StoringHandler(MessageStore store) : IHandleMessages<StoreMessage>
{
    public Task Handle(StoreMessage message, CancellationToken cancellationToken = default)
    {
        store.Items.Add(message.Value);
        return Task.CompletedTask;
    }
}

class GiveupTimeHandler(GiveupTimeProbe probe) : IHandleMessages<GiveupTimeMessage>
{
    public async Task Handle(GiveupTimeMessage message, CancellationToken cancellationToken = default)
    {
        if (message.Number == 1)
        {
            probe.FirstStarted.TrySetResult();
            await probe.ReleaseFirst.Task.WaitAsync(cancellationToken);
            probe.FirstFinished.TrySetResult();
            return;
        }

        probe.SecondHandled.TrySetResult();
    }
}

// --- Helpers ---

class MessageStore
{
    public List<string> Items { get; } = [];
}

class GiveupTimeProbe
{
    public TaskCompletionSource FirstStarted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public TaskCompletionSource ReleaseFirst { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public TaskCompletionSource FirstFinished { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public TaskCompletionSource SecondHandled { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
}

class CountdownLatch(int count)
{
    private int _remaining = count;
    private readonly TaskCompletionSource _tcs = new();

    public void Signal()
    {
        if (Interlocked.Decrement(ref _remaining) == 0)
            _tcs.TrySetResult();
    }

    public async Task<bool> WaitAsync(TimeSpan timeout)
    {
        var delay = Task.Delay(timeout);
        var winner = await Task.WhenAny(_tcs.Task, delay);
        return winner == _tcs.Task;
    }
}
