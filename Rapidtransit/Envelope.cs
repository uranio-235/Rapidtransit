namespace Rapidtransit;

internal sealed record Envelope(object Message, string? Partition, TimeSpan? GiveupTime, long EnqueuedTimestamp);
