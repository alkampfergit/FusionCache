using System;
using MongoDB.Driver;

namespace ZiggyCreatures.Caching.Fusion.Backplane.MongoDb;

/// <summary>
/// Configuratino for backplane based on MongoDb
/// </summary>
public class MongoDbBackplaneOptions
{
	/// <summary>
	/// Connection string used to connect to MongoDb, it is ignored
	/// if <see cref="MongoClientFactory"/> is set. The preferred way
	/// to configure this component is using MongoClientFactory parameter
	/// so you can create the client with all the possible option and you can use the
	/// single client approach.
	/// </summary>
	public string? ConnectionString { get; set; }

	/// <summary>
	/// Do not know if this is really needed, but this is used to allow more instances
	/// of <see cref="MongoDbBackplane"/> to share the same collection and thus the
	/// very same notification handlers.
	/// </summary>
	public string? CacheConnectionId { get; set; }

	/// <summary>
	/// Name of the database to use, it is mandatory.
	/// </summary>
	public string DatabaseName { get; set; } = null!;

	/// <summary>
	/// Name of the collection used to store messages to be exchanged.
	/// </summary>
	public string CollectionName { get; set; } = "fusion-cache-backplane";

	/// <summary>
	/// If you want to avoid using change tracking even if it is available this
	/// property allows you to force polling.
	/// </summary>
	public bool DisableChangeTracking { get; set; }

	/// <summary>
	/// If you do not have Change Tracking enabled, or if you disable it with
	/// <see cref="DisableChangeTracking"/> you can vary the polling time with this
	/// paramter. You will expect a poll for each interval specified.
	/// </summary>
	public TimeSpan PollingInterval { get; set; } = TimeSpan.FromSeconds(1);

	/// <summary>
	/// To connect to Mongodb we can use the connection string or the
	/// caller can directly configure a factory that create Mongo client
	/// with the desired configuration. This is especially useful to create
	/// a single <see cref="IMongoClient"/> for the entire application.
	/// If this value is different from null, <see cref="ConnectionString"/>
	/// is not used anymore.
	/// </summary>
	public Func<IMongoClient>? MongoClientFactory { get; set; }

	internal IMongoClient GetMongoDbClient()
	{
		if (MongoClientFactory is not null)
		{
			return MongoClientFactory();
		}

		if (string.IsNullOrWhiteSpace(ConnectionString))
		{
			throw new InvalidOperationException("ConnectionString is mandatory if MongoClientFactory is not set");
		}

		return new MongoClient(ConnectionString);
	}
}
