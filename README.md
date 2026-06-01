# Rapidtransit

The service bus you bought in Temu.

## Wait... What?

### 🥀 **The Virgin Masstransit**
- Requires a **full message broker** just to say hello — RabbitMQ, Kafka, Azure Service Bus, etc.
- Brings **infrastructure baggage** like it’s moving in permanently.
- “Hold on, let me configure 19 transports and 47 options.”
- Debugging requires **three dashboards and a prayer**.
- Sends a message only after negotiating with five external daemons.
- “It’s enterprise‑ready” (translation: *you will suffer*).

### 💪 **The Chad Rapidtransit**
- **In‑process message bus** — no brokers, no clusters, no drama.
- Built on **System.Threading.Channels**, because real chads use the BCL.
- `bus.Send` + `IHandleMessages<T>` ergonomics without the ceremony.
- **Zero external infrastructure** — deploy and go.
- Debugging is literally “put a breakpoint here.”
- Moves messages faster than your PM can say “microservices.”

---

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

Handlers are discovered automatically at startup. No manual registration, no wiring, no drama. Just pure, uncut **Chad‑level autodiscovery**.
---

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
            // swallow, rethrow, dead-letter, fuckoff
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

---

## Oh! It can be configured

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
| `MaxParallelism` | `5` | Max handlers running concurrently (backed by `SemaphoreSlim`) |
| `ChannelCapacity` | `1000` | Max pending messages before `Send` back-pressures the caller |

---

## Sequential handlers (one-at-a-time)

If a handler must **never** overlap with itself, just slap a `SequentialHandler` on it. No locks, no mutexes, no existential dread — just a handler so disciplined it queues its own reps.

```csharp
[SequentialHandler]
class InventoryProjectionHandler : IHandleMessages<InventoryAdjusted>
{
    public Task Handle(InventoryAdjusted message, CancellationToken cancellationToken = default)
        => Task.CompletedTask;
}
```

Behavior:

- Marked handlers: process one message at a time per handler type.
- Unmarked handlers: keep normal parallel processing (bounded by `MaxParallelism`).
- Middleware behavior is unchanged.

---

## Architecture

```
bus.Send(message)
    └─► Channel<object>.Writer.WriteAsync()

DispatchWorker (BackgroundService)
    └─► Channel<object>.Reader.ReadAllAsync()
        └─► per-message Task.Run (bounded by SemaphoreSlim)
            └─► IServiceScope (fresh scope per message)
                └─► Middleware₁ → Middleware₂ → ... → IHandleMessages<T>.Handle()
```

- **One channel, one reader** — ordered delivery, no race on dequeue.
- **`SemaphoreSlim(MaxParallelism)`** — bounded concurrency without a thread-per-message.
- **`IServiceScope` per message** — scoped DI dependencies work correctly; handlers and middlewares share the same scope.
- **Middleware pipeline** — built as a `Func<Task>` chain (same pattern as ASP.NET Core). Each middleware calls `await next()` to proceed.
- **Exception propagation** — `TargetInvocationException` from reflection is unwrapped via `ExceptionDispatchInfo` so middleware `catch` blocks see the original exception type.

---

## Testing

The framework is designed to be test-friendly: just host it with `Host.CreateDefaultBuilder()` in xUnit and use `TaskCompletionSource<T>` or a countdown latch to await dispatch.

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

---

## License

WTFPL

### DO WHAT THE FUCK YOU WANT TO PUBLIC LICENSE

Version 2, December 2004

Copyright (C) 2004 Sam Hocevar <sam@hocevar.net>

Everyone is permitted to copy and distribute verbatim or modified
copies of this license document, and changing it is allowed as long
as the name is changed.

### DO WHAT THE FUCK YOU WANT TO PUBLIC LICENSE

#### TERMS AND CONDITIONS FOR COPYING, DISTRIBUTION AND MODIFICATION

0. You just DO WHAT THE FUCK YOU WANT TO.
