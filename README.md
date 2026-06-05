# Rapidtransit

The service bus you bought in Temu.

## Wait... What?

### 🥀 **The Virgin Masstransit**
- Requires a **full message broker** just to say hello — RabbitMQ, Kafka, Azure Service Bus, etc.
- Brings **infrastructure baggage** like it’s moving in permanently.
- “Hold on, let me configure 19 transports and 47 options.”
- Debugging requires **three dashboards and a prayer**.
- Sends a message only after negotiating with five external daemons.
- “It’s enterprise‑ready” (means: *you will suffer*).

### 💪 **The Chad Rapidtransit**
- **In‑process message bus** — no brokers, no clusters, no drama.
- Built on **System.Threading.Channels**, because real chads use the BCL.
- `bus.Send` + `IHandleMessages<T>` ergonomics without the ceremony.
- **Zero external infrastructure** — deploy and go.
- Debugging is literally “put a breakpoint here.”
- Moves messages faster than your PM can say “microservices.”

## Look a this logo

<img src="Rapidtransit/chadbus.png" alt="Chadbus" width="400" />

You have to admit it. It's awesome.

## Getting started

### 1. Install

```
dotnet add package Rapidtransit
```

Done.

### 2. Register

Oh! Look at this. It has a fluent API. What a pro!

```csharp
builder.Services.AddRapidtransit(o => o
    .RegisterHandlersFrom<Program>()   // scans the assembly for IHandleMessages<T> implementations
    .Use<ErrorLoggingMiddleware>());    // optional middleware pipeline
```

### 3. Define a message

```csharp
record OrderPlaced(Guid OrderId);
```

### 4. Write a handler

```csharp
class OrderPlacedHandler(ILogger<OrderPlacedHandler> logger) : IHandleMessages<OrderPlaced>
{
    public Task Handle(OrderPlaced message, CancellationToken cancellationToken = default)
    {
        logger.LogInformation("Order {Id} placed", message.OrderId);
        return Task.CompletedTask;
    }
}
```

### 5. Send

```csharp
var bus = app.Services.GetRequiredService<IBus>();
await bus.Send(new OrderPlaced(Guid.NewGuid()));
```

Handlers are discovered automatically at startup. No manual registration, no wiring, no drama. Just pure, uncut Chad‑level autodiscovery.

## Oh, look! It has middleware also.

The old reliable `await next()` for your try and catch.

```csharp
class ErrorLoggingMiddleware(ILogger<ErrorLoggingMiddleware> logger) : IMessageMiddleware
{
    public async Task Handle(object message, Func<Task> next, CancellationToken cancellationToken = default)
    {
        try
        {
            await next();
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error handling {MessageType}", message.GetType().Name);
            // swallow, rethrow, dead-letter, fuck off
        }
    }
}
```

Register it. Fluently of course. How else could be?

```csharp
services.AddRapidtransit(o => o
    .RegisterHandlersFrom<Program>()
    .Use<ErrorLoggingMiddleware>()
    .Use<CorrelationMiddleware>());
```

Middlewares execute in registration order (`ErrorLogging` → `Correlation` → handler).

## Oh! Look. It can be configured

```csharp
services.AddRapidtransit(o =>
{
    o.MaxParallelism  = 10;    // max concurrent handlers (default: 5)
    o.ChannelCapacity = 5000;  // bounded channel size (default: 1000)

    o.RegisterHandlersFrom<Program>();
    o.Use<MyMiddleware>();
});
```

| Option | Default | What it does |
|---|---|---|
| `MaxParallelism` | `5` | How many handlers can party simultaneously. Crank it up for throughput, dial it down to pretend you care about resource limits. |
| `ChannelCapacity` | `1000` | Queue depth before `Send` starts pushing back on callers. Think of it as a bouncer — polite, firm, and immune to bribes. |


## Partitioned handlers (concurrent, but not feral)

Your `OrderUpdateHandler` can handle 500 orders at once. Great. Until two messages for the *same* order race each other and you get a fun consistency bug at 3 AM.

Pass a partition key. The bus does the rest.

```csharp
await bus.Send(new OrderUpdate(orderId), partition: orderId);
```

That's it. No attributes. No locks. No `volatile bool _isProcessing` with a comment that says `// DO NOT REMOVE`.

- **Same key**: strict one-at-a-time. Two messages for order `42`? They queue. Politely.
- **Different keys**: full parallel. Order `42` and order `99` don't even know each other exist.
- **No key at all**: parallel free-for-all, bounded by `MaxParallelism`. The default Chad behavior.

The key can be anything with a `.ToString()`. A `Guid`, an `int`, a string, a social security number (please don't). The bus converts it internally and doesn't judge.

```csharp
// Same order → sequential
await bus.Send(new OrderUpdate(orderId), partition: orderId);

// Different orders → parallel
await bus.Send(new OrderUpdate(order1Id), partition: order1Id);
await bus.Send(new OrderUpdate(order2Id), partition: order2Id);

// No opinion → do whatever
await bus.Send(new AuditLog("something happened"));
```

- Middleware: doesn't care either way.

## Architecture

You asked for a diagram. Fine. Here is your useless diagram.

```
bus.Send(message, partition?)
    └─► Channel<Envelope>.Writer.WriteAsync()

DispatchWorker (BackgroundService)
    └─► Channel<Envelope>.Reader.ReadAllAsync()
        └─► per-message Task.Run (bounded by SemaphoreSlim)
            ├─► partition gate (SemaphoreSlim per (Type, key)) — only if partition != null
            └─► IServiceScope (fresh scope per message)
                └─► Middleware₁ → Middleware₂ → ... → IHandleMessages<T>.Handle()
```

Happy?

## Internal weirdness

- **One channel, one reader** — ordered delivery, zero drama on dequeue.
- **`SemaphoreSlim(MaxParallelism)`** — bounded concurrency without spawning a thread for every message like it's 2004.
- **`IServiceScope` per message** — scoped DI works correctly; handlers and middlewares share the scope so nothing leaks into the next message's business.
- **Middleware pipeline** — a `Func<Task>` chain, same as ASP.NET Core. You already know how `await next()` works. Good.
- **Exception propagation** — `TargetInvocationException` from reflection gets unwrapped via `ExceptionDispatchInfo`, so your `catch (OrderNotFoundException)` actually catches `OrderNotFoundException` instead of a reflection wrapper that makes you feel stupid.

That's the whole thing. No hidden services, no background registries, no "magic" that becomes someone else's problem at 3 AM.

## Testing

No Docker. No TestContainers. No RabbitMQ running in the background judging you.

Just spin up the host, send a message, and await the result like a normal person.

```csharp
[Fact]
public async Task Send_delivers_to_handler()
{
    var tcs = new TaskCompletionSource<OrderPlaced>();

    var host = Host.CreateDefaultBuilder()
        .ConfigureServices(services =>
        {
            services.AddSingleton(tcs);
            services.AddRapidtransit(o => o.RegisterHandlersFrom<OrderPlacedHandler>());
        })
        .Build();

    await host.StartAsync();

    await host.Services.GetRequiredService<IBus>().Send(new OrderPlaced(Guid.NewGuid()));

    var result = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
    Assert.NotNull(result);

    await host.StopAsync();
}
```

If this test fails, your handler is broken. Not the bus. Not the channel. Not a transient network hiccup between your app and a broker 200ms away. **Check your handler.**

Debugging at its finest.

## License

WTFPL

### DO WHAT THE FUCK YOU WANT TO PUBLIC LICENSE

Version 2, December 2004

Copyright (C) 2004 Sam Hocevar <sam@hocevar.net>

Everyone is permitted to copy and distribute verbatim or modified
copies of this license document, and changing it is allowed as long
as the name is changed.

#### TERMS AND CONDITIONS FOR COPYING, DISTRIBUTION AND MODIFICATION

0. You just DO WHAT THE FUCK YOU WANT TO.
