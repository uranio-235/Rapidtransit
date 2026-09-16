using System.Collections.Concurrent;

namespace Rapidtransit;

internal sealed class LatestWinsRegistry
{
    private readonly ConcurrentDictionary<(Type, string), Envelope> _latest = new();

    internal void Register((Type, string) key, Envelope envelope)
    {
        _latest.AddOrUpdate(
            key,
            envelope,
            (_, current) => envelope.Sequence > current.Sequence ? envelope : current);
    }

    internal bool IsLatest((Type, string) key, Envelope envelope)
        => _latest.TryGetValue(key, out var latest) && ReferenceEquals(latest, envelope);

    internal void Remove((Type, string) key, Envelope envelope)
        => ((ICollection<KeyValuePair<(Type, string), Envelope>>)_latest)
            .Remove(new KeyValuePair<(Type, string), Envelope>(key, envelope));
}