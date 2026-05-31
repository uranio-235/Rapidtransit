using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Rapidtransit;

namespace Rapidtransit.Tests;

public class SequentialHandlerTests
{
    [Fact]
    public async Task Sequential_handler_processes_one_message_at_a_time()
    {
        const int count = 8;
        var probe = new ConcurrencyProbe();
        var latch = new CountdownLatch(count);

        var host = Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddSingleton(probe);
                services.AddSingleton(latch);
                services.AddRapidtransit(o =>
                {
                    o.MaxParallelism = count;
                    o.RegisterHandlersFrom<SequentialWorkHandler>();
                });
            })
            .Build();

        await host.StartAsync();

        var bus = host.Services.GetRequiredService<IBus>();

        for (var i = 0; i < count; i++)
            await bus.Send(new SequentialWorkMessage(i));

        var done = await latch.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.True(done, "Not all sequential messages were processed in time.");
        Assert.Equal(1, probe.MaxActive);

        await host.StopAsync();
    }

    [Fact]
    public async Task Default_handler_can_process_messages_in_parallel()
    {
        const int count = 8;
        var probe = new ConcurrencyProbe();
        var latch = new CountdownLatch(count);

        var host = Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddSingleton(probe);
                services.AddSingleton(latch);
                services.AddRapidtransit(o =>
                {
                    o.MaxParallelism = count;
                    o.RegisterHandlersFrom<ParallelWorkHandler>();
                });
            })
            .Build();

        await host.StartAsync();

        var bus = host.Services.GetRequiredService<IBus>();

        for (var i = 0; i < count; i++)
            await bus.Send(new ParallelWorkMessage(i));

        var done = await latch.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.True(done, "Not all parallel messages were processed in time.");
        Assert.True(probe.MaxActive > 1, $"Expected parallel processing, but max active was {probe.MaxActive}.");

        await host.StopAsync();
    }
}

record SequentialWorkMessage(int Value);
record ParallelWorkMessage(int Value);

[SequentialHandler]
class SequentialWorkHandler(ConcurrencyProbe probe, CountdownLatch latch) : IHandleMessages<SequentialWorkMessage>
{
    public async Task Handle(SequentialWorkMessage message, CancellationToken cancellationToken = default)
    {
        probe.Enter();

        try
        {
            await Task.Delay(75, cancellationToken);
        }
        finally
        {
            probe.Exit();
            latch.Signal();
        }
    }
}

class ParallelWorkHandler(ConcurrencyProbe probe, CountdownLatch latch) : IHandleMessages<ParallelWorkMessage>
{
    public async Task Handle(ParallelWorkMessage message, CancellationToken cancellationToken = default)
    {
        probe.Enter();

        try
        {
            await Task.Delay(75, cancellationToken);
        }
        finally
        {
            probe.Exit();
            latch.Signal();
        }
    }
}

class ConcurrencyProbe
{
    private int _active;
    private int _maxActive;

    public int MaxActive => Volatile.Read(ref _maxActive);

    public void Enter()
    {
        var currentActive = Interlocked.Increment(ref _active);

        while (true)
        {
            var snapshot = Volatile.Read(ref _maxActive);
            if (currentActive <= snapshot)
                return;

            if (Interlocked.CompareExchange(ref _maxActive, currentActive, snapshot) == snapshot)
                return;
        }
    }

    public void Exit() => Interlocked.Decrement(ref _active);
}