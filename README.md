# Rapidtransit

An in-process message bus for .NET built on top of `System.Threading.Channels`. Inspired by Rebus — same `bus.Send` / `IHandleMessages<T>` ergonomics, zero external infrastructure.

```csharp
await bus.Send(new OrderPlaced(orderId));
```

That's it. The handler picks it up automatically.

---

## Why

External message brokers (RabbitMQ, Azure Service Bus, etc.) are great for distributed systems, but add significant operational overhead for in-process communication. Rapidtransit gives you the same clean handler-based programming model over a `Channel<T>` instead — no broker, no network, no serialization.

---

## Getting started

### 1. Install

```
dotnet add package Rapidtransit
```

### 2. Register

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

Handlers are discovered automatically at startup. No manual registration, no wiring.

---

## Middleware

Middleware wraps every message dispatch. Useful for logging, error tracking, retries, correlation IDs, etc.

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
            // swallow, rethrow, dead-letter — your call
        }
    }
}
```

Register it fluently:

```csharp
services.AddRapidtransit(o => o
    .RegisterHandlersFrom<Program>()
    .Use<ErrorLoggingMiddleware>()
    .Use<CorrelationMiddleware>());
```

Middlewares execute in registration order (`ErrorLogging` → `Correlation` → handler).

---

## Configuration

```csharp
services.AddRapidtransit(o =>
{
    o.MaxParallelism  = 10;    // max concurrent handlers (default: 5)
    o.ChannelCapacity = 5000;  // bounded channel size (default: 1000)

    o.RegisterHandlersFrom<Program>();
    o.Use<MyMiddleware>();
});
```

| Option | Default | Description |
|---|---|---|
| `MaxParallelism` | `5` | Max handlers running concurrently (backed by `SemaphoreSlim`) |
| `ChannelCapacity` | `1000` | Max pending messages before `Send` back-pressures the caller |

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

MIT
