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
    public async Task Send_latest_wins_discards_pending_messages_in_same_partition()
    {
        var probe = new LatestWinsProbe();

        var host = Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddSingleton(probe);
                services.AddRapidtransit(o =>
                {
                    o.MaxParallelism = 2;
                    o.RegisterHandlersFrom<LatestWinsHandler>();
                });
            })
            .Build();

        await host.StartAsync();

        var bus = host.Services.GetRequiredService<IBus>();
        await bus.Send(new LatestWinsMessage(1), partition: "same-key");
        await probe.FirstStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await bus.Send(
            new LatestWinsMessage(2),
            partition: "same-key",
            deliveryMode: DeliveryMode.LatestWins);

        await bus.Send(
            new LatestWinsMessage(3),
            partition: "same-key",
            deliveryMode: DeliveryMode.LatestWins);

        await Task.Delay(100);
        probe.ReleaseFirst.TrySetResult();
        await probe.FirstFinished.Task.WaitAsync(TimeSpan.FromSeconds(5));

        var handled = await probe.LastHandled.Task.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal(3, handled);
        Assert.DoesNotContain(2, probe.Handled);

        await host.StopAsync();
    }

    [Fact]
    public async Task Send_busy_discards_messages_while_partition_is_busy()
    {
        var probe = new BusyProbe();

        var host = Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddSingleton(probe);
                services.AddRapidtransit(o =>
                {
                    o.MaxParallelism = 2;
                    o.RegisterHandlersFrom<BusyHandler>();
                });
            })
            .Build();

        await host.StartAsync();

        var bus = host.Services.GetRequiredService<IBus>();
        await bus.Send(new BusyMessage(1), partition: "same-key", deliveryMode: DeliveryMode.Busy);
        await probe.FirstStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await bus.Send(new BusyMessage(2), partition: "same-key", deliveryMode: DeliveryMode.Busy);
        await Task.Delay(100);
        probe.ReleaseFirst.TrySetResult();
        await probe.FirstFinished.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Task.Delay(100);

        await bus.Send(new BusyMessage(3), partition: "same-key", deliveryMode: DeliveryMode.Busy);
        var handled = await probe.LastHandled.Task.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal(3, handled);
        Assert.DoesNotContain(2, probe.Handled);

        await host.StopAsync();
    }

}

// --- Messages ---

record PingMessage(string Content);
record CountMessage(int Index);
record StoreMessage(string Value);
record LatestWinsMessage(int Number);
record BusyMessage(int Number);

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

class LatestWinsHandler(LatestWinsProbe probe) : IHandleMessages<LatestWinsMessage>
{
    public async Task Handle(LatestWinsMessage message, CancellationToken cancellationToken = default)
    {
        if (message.Number == 1)
        {
            probe.FirstStarted.TrySetResult();
            await probe.ReleaseFirst.Task.WaitAsync(cancellationToken);
            probe.FirstFinished.TrySetResult();
            return;
        }

        probe.Handled.Add(message.Number);
        probe.LastHandled.TrySetResult(message.Number);
    }
}

class BusyHandler(BusyProbe probe) : IHandleMessages<BusyMessage>
{
    public async Task Handle(BusyMessage message, CancellationToken cancellationToken = default)
    {
        if (message.Number == 1)
        {
            probe.FirstStarted.TrySetResult();
            await probe.ReleaseFirst.Task.WaitAsync(cancellationToken);
            probe.FirstFinished.TrySetResult();
            return;
        }

        probe.Handled.Add(message.Number);
        probe.LastHandled.TrySetResult(message.Number);
    }
}

// --- Helpers ---

class MessageStore
{
    public List<string> Items { get; } = [];
}

class LatestWinsProbe
{
    public TaskCompletionSource FirstStarted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public TaskCompletionSource ReleaseFirst { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public TaskCompletionSource FirstFinished { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public TaskCompletionSource<int> LastHandled { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public List<int> Handled { get; } = [];
}

class BusyProbe
{
    public TaskCompletionSource FirstStarted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public TaskCompletionSource ReleaseFirst { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public TaskCompletionSource FirstFinished { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public TaskCompletionSource<int> LastHandled { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public List<int> Handled { get; } = [];
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
