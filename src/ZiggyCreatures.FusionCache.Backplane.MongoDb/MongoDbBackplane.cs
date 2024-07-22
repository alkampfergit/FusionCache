using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace ZiggyCreatures.Caching.Fusion.Backplane.MongoDb;

/// <summary>
/// Broadcaster class that uses a collection in mongodb to allow for simple
/// broascasting of messages to multiple processes.
/// </summary>
public class MongoDbBackplane : IFusionCacheBackplane
{
	private static ConcurrentDictionary<string, InternalMongoDbBackplane> _backplanes = new(StringComparer.OrdinalIgnoreCase);

	private InternalMongoDbBackplane _backplane;
	private BackplaneSubscriptionOptions _options;

	/// <summary>
	/// Create the backplane on Mongodb.
	/// </summary>
	public MongoDbBackplane(
		MongoDbBackplaneOptions options,
		ILoggerFactory loggerFactory)
	{
		//Create the real backplane if not already in cache with the connection id.
		if (!_backplanes.TryGetValue(options.CacheConnectionId ?? string.Empty, out _backplane))
		{
			_backplane = new InternalMongoDbBackplane(options, loggerFactory);
			_backplanes.TryAdd(options.CacheConnectionId ?? string.Empty, _backplane);
		}
	}

	/// <inheritdoc />
	public void Subscribe(BackplaneSubscriptionOptions options)
	{
		_options = options;
		_backplane.Subscribe(options);
	}

	/// <inheritdoc />
	public void Unsubscribe()
	{
		_backplane.Unsubscribe(_options);
	}

	/// <inheritdoc />
	public ValueTask PublishAsync(BackplaneMessage message, FusionCacheEntryOptions options, CancellationToken token = default)
	{
		return _backplane.PublishAsync(message, options, token);
	}

	/// <inheritdoc />
	public void Publish(BackplaneMessage message, FusionCacheEntryOptions options, CancellationToken token = default)
	{
		_backplane.Publish(message, options, token);
	}
}
