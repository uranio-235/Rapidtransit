using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Rapidtransit.Tests;

public class PartitionedHandlerTests
{
    [Fact]
    public async Task Same_partition_key_processes_one_message_at_a_time()
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
                    o.RegisterHandlersFrom<PartitionedWorkHandler>();
                });
            })
            .Build();

        await host.StartAsync();

        var bus = host.Services.GetRequiredService<IBus>();

        for (var i = 0; i < count; i++)
            await bus.Send(new PartitionedWorkMessage(i), partition: "same-key");

        var done = await latch.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.True(done, "Not all messages were processed in time.");
        Assert.Equal(1, probe.MaxActive);

        await host.StopAsync();
    }

    [Fact]
    public async Task Different_partition_keys_process_messages_in_parallel()
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
                    o.RegisterHandlersFrom<PartitionedWorkHandler>();
                });
            })
            .Build();

        await host.StartAsync();

        var bus = host.Services.GetRequiredService<IBus>();

        for (var i = 0; i < count; i++)
            await bus.Send(new PartitionedWorkMessage(i), partition: i.ToString());

        var done = await latch.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.True(done, "Not all messages were processed in time.");
        Assert.True(probe.MaxActive > 1, $"Expected parallel processing across partitions, but max active was {probe.MaxActive}.");

        await host.StopAsync();
    }

    [Fact]
    public async Task No_partition_processes_messages_in_parallel()
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
                    o.RegisterHandlersFrom<PartitionedWorkHandler>();
                });
            })
            .Build();

        await host.StartAsync();

        var bus = host.Services.GetRequiredService<IBus>();

        for (var i = 0; i < count; i++)
            await bus.Send(new PartitionedWorkMessage(i));

        var done = await latch.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.True(done, "Not all messages were processed in time.");
        Assert.True(probe.MaxActive > 1, $"Expected parallel processing without partition, but max active was {probe.MaxActive}.");

        await host.StopAsync();
    }
}

record PartitionedWorkMessage(int Value);

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
            if (currentActive <= snapshot) return;
            if (Interlocked.CompareExchange(ref _maxActive, currentActive, snapshot) == snapshot) return;
        }
    }

    public void Exit() => Interlocked.Decrement(ref _active);
}

class PartitionedWorkHandler(ConcurrencyProbe probe, CountdownLatch latch) : IHandleMessages<PartitionedWorkMessage>
{
    public async Task Handle(PartitionedWorkMessage message, CancellationToken cancellationToken = default)
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
