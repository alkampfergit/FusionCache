using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using MongoDB.Bson;
using MongoDB.Driver;

namespace ZiggyCreatures.Caching.Fusion.Backplane.MongoDb;

/// <summary>
/// Helps using change stream to be notified of changes in a collection it is really
/// more educated for mongodb than polling.
/// </summary>
internal class MongoDbChangeStreamHelper : IMongoDbChangeStreamHelper, IDisposable
{
	private readonly IMongoClient _client;

	/// <summary>
	/// For each key == database, we have a list of database notifications.
	/// </summary>
	private readonly ConcurrentDictionary<string, DatabaseNotifications> _notifications = new();

	private readonly ConcurrentDictionary<string, List<Func<ChangeStreamHelperChangedEventArgs, CancellationToken, Task>>> _collectionNotifications = new();

	private bool _disposedValue;
	private readonly ILogger _logger;
	private readonly CancellationTokenSource _source;
	private readonly CancellationToken _token;

	private static readonly ConcurrentDictionary<string, MongoDbChangeStreamHelper> _pool = new();

	/// <summary>
	/// Create a <see cref="IMongoDbChangeStreamHelper"/>
	/// </summary>
	/// <param name="mongoClient"></param>
	/// <param name="logger"></param>
	/// <returns></returns>
	public static IMongoDbChangeStreamHelper CreateFromUrl(IMongoClient mongoClient, ILogger<MongoDbChangeStreamHelper>? logger = null)
	{
		if (!_pool.TryGetValue(mongoClient.ToString(), out var helper))
		{
			helper = new MongoDbChangeStreamHelper(mongoClient, logger);
			_pool[mongoClient.ToString()] = helper;
		}
		return helper;
	}

	private MongoDbChangeStreamHelper(
		IMongoClient client, ILogger<MongoDbChangeStreamHelper>? logger = null)
	{
		_logger = logger ?? (ILogger)NullLogger.Instance;
		_source = new CancellationTokenSource();
		_token = _source.Token;

		_client = client;
	}

	/// <summary>
	/// Register an handler.
	/// </summary>
	/// <param name="database"></param>
	/// <param name="collection"></param>
	/// <param name="notificationFunction"></param>
	public void RegisterNotifier(
		string database,
		string collection,
		Func<ChangeStreamHelperChangedEventArgs, CancellationToken, Task> notificationFunction)
	{
		var supportNotification = SupportsChangeStreams();
		if (!supportNotification)
		{
			throw new Exception("Unable to register change stream, not supported by the server");
		}

		if (database.EndsWith("-readmodel"))
		{
			//we are polling lots of collections for readmodel database it is much more
			//efficient to poll the entire database. This technique is not smart for support
			//database where we poll only one or two collections.
			if (!_notifications.TryGetValue(database, out var notifications))
			{
				//double lock, create notification for the entire database, then add the consumer
				//for the single collection.
				lock (_notifications)
				{
					if (!_notifications.TryGetValue(database, out notifications))
					{
						notifications = new();
						_notifications[database] = notifications;

						SetupNotificationForDatabase(database, notifications);
					}
				}
			}
			notifications.AddConsumer(collection, notificationFunction);
		}
		else
		{
			var key = $"{database}/{collection}";
			if (!_collectionNotifications.TryGetValue(key, out var notifications))
			{
				//Double lock 
				lock (_collectionNotifications)
				{
					if (!_collectionNotifications.TryGetValue(key, out notifications))
					{
						notifications = new();
						_collectionNotifications[key] = notifications;

						SetupNotificationForCollection(database, collection);
					}
				}
			}
			notifications.Add(notificationFunction);
		}
	}

	private void SetupNotificationForCollection(string databaseName, string collectionName)
	{
		var db = _client.GetDatabase(databaseName);
		var collection = db.GetCollection<BsonDocument>(collectionName);

		//fire and forget :), let the task run intefinitely.
		Task.Run(() => MonitorCollection(databaseName, collectionName, collection));
	}

	private async Task MonitorCollection(string databaseName, string collectionName, IMongoCollection<BsonDocument> collection)
	{
		var key = $"{databaseName}/{collectionName}";
		while (!_token.IsCancellationRequested)
		{
			try
			{
				var options = new ChangeStreamOptions { FullDocument = ChangeStreamFullDocumentOption.UpdateLookup };
				var cursor = await collection.WatchAsync(options, _token);

				using (cursor)
				{
					await cursor.ForEachAsync(async change =>
					{
						if (_collectionNotifications.TryGetValue(key, out var notifications))
						{
							ChangeStreamHelperChangedEventArgs arg = new ChangeStreamHelperChangedEventArgs(collectionName, databaseName);
							foreach (var notification in notifications)
							{
								try
								{
									await notification(arg, _token);
								}
								catch (Exception ex)
								{
									_logger.LogError(ex, "Error dispatching change stream to collection {0} database {1}", collection, databaseName);
								}
							}
						}
					}, _token);
				}
			}
			catch (Exception ex)
			{
				_logger.LogError(ex, "Error in change stream helper for collection {0} database {1}", collection, databaseName);
			}
		}
	}

	/// <summary>
	/// Create notification for the entire database, this is used because we can have lots of poller so
	/// we can monitor the ENTIRE database then dispatch internally to the correct handler.
	/// </summary>
	/// <param name="databaseName"></param>
	/// <param name="notifications"></param>
	private void SetupNotificationForDatabase(string databaseName, DatabaseNotifications notifications)
	{
		var db = _client.GetDatabase(databaseName);

		//fire and forget :), let the task run intefinitely.
		Task.Run(() => MonitorDatabase(db, notifications));
	}

	private async Task MonitorDatabase(IMongoDatabase db, DatabaseNotifications notifications)
	{
		var dbName = db.DatabaseNamespace.DatabaseName;
		while (!_token.IsCancellationRequested)
		{
			try
			{
				var options = new ChangeStreamOptions
				{
					FullDocumentBeforeChange = ChangeStreamFullDocumentBeforeChangeOption.Off,
					BatchSize = 100,
				};

				var pipeline = new EmptyPipelineDefinition<ChangeStreamDocument<BsonDocument>>()
					 .Match(doc => !doc.CollectionNamespace.CollectionName.Equals("checkpoints")
						&& !doc.CollectionNamespace.CollectionName.Equals("framework.AtomicProjectionCheckpoint"))
					 .AppendStage<ChangeStreamDocument<BsonDocument>, ChangeStreamDocument<BsonDocument>, BsonDocument>(
						"{ $unset : \"fullDocument\" }");
				//.ReplaceRoot<ChangeStreamDocument<BsonDocument>, ChangeStreamDocument< BsonDocument >>(new BsonDocument { { "_id", "$$ROOT._id" } });

				var cursor = await db.WatchAsync(pipeline, options, _token);

				using (cursor)
				{
					await cursor.ForEachAsync(async change =>
					{
						try
						{
							//determine the collection name that is changed, this will be used to dispatch the notification to the
							//Correct handler.
							var collectionName = change["ns"]["coll"].AsString;
							ChangeStreamHelperChangedEventArgs arg = new(collectionName, dbName);
							await notifications.NotifyAsync(dbName, collectionName, arg, _logger, _token);
						}
						catch (Exception ex)
						{
							//we can increment only dbname because we do not know the real collection name.
							_logger.LogError(ex, "Error dispatching change stream to notifier {0} {1}", dbName, change);
						}
					}, _token);
				}
			}
			catch (Exception ex)
			{
				_logger.LogError(ex, "Error in change stream helper for  database {0}", dbName);
			}
		}
	}

	/// <summary>
	/// Check if the current server supports change streams.
	/// </summary>
	/// <returns></returns>
	public bool SupportsChangeStreams()
	{
		try
		{
			var adminDatabase = _client.GetDatabase("admin");

			// Check the server version
			var buildInfoCommand = new BsonDocumentCommand<BsonDocument>(new BsonDocument { { "buildInfo", 1 } });
			var buildInfo = adminDatabase.RunCommand(buildInfoCommand);
			var versionString = buildInfo.GetValue("version").AsString;
			var version = Version.Parse(versionString);

			// Check the deployment type
			var isMasterCommand = new BsonDocumentCommand<BsonDocument>(new BsonDocument { { "isMaster", 1 } });
			var isMaster = adminDatabase.RunCommand(isMasterCommand);
			var isReplicaSet = isMaster.Contains("setName") || isMaster.Contains("isreplicaset");
			var isSharded = isMaster.GetValue("msg", "").AsString == "isdbgrid";

			int serverNumbers = 1;
			if (isReplicaSet)
			{
				var replicaSetStatusCommand = new BsonDocumentCommand<BsonDocument>(new BsonDocument { { "replSetGetStatus", 1 } });
				var replicaSetStatus = adminDatabase.RunCommand(replicaSetStatusCommand);
				var members = replicaSetStatus.GetValue("members").AsBsonArray;
				serverNumbers = members.Count;
			}
			//If you are connected with atlas serverless, you have a single server count. We cannot use change
			//Stream in this situation because it is not supported https://www.mongodb.com/docs/atlas/reference/serverless-instance-limitations/

			// Change Streams are supported in MongoDB 3.6+ and on replica sets or sharded clusters
			return version >= new Version(4, 2) && (isReplicaSet || isSharded) && serverNumbers > 1;
		}
		catch (Exception ex)
		{
			_logger.LogError(ex, "Unable to determine if mongodb supports change stream due to exception {0}", ex.Message);
			return false;
		}
	}

	/// <summary>
	/// Dispose the helper releaseing all the tracking.
	/// </summary>
	/// <param name="disposing"></param>
	protected virtual void Dispose(bool disposing)
	{
		if (!_disposedValue)
		{
			if (disposing)
			{
				_source.Cancel();
				_source.Dispose();
			}

			_disposedValue = true;
		}
	}

	/// <summary>
	/// Dispose the helper releaseing all the tracking.
	/// </summary>
	public void Dispose()
	{
		// Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
		Dispose(disposing: true);
		GC.SuppressFinalize(this);
	}

	/// <summary>
	/// Key is collection and value is all lamda that I need to notify for a single collection.
	/// </summary>
	private class DatabaseNotifications : ConcurrentDictionary<string, List<Func<ChangeStreamHelperChangedEventArgs, CancellationToken, Task>>>
	{
		public void AddConsumer(string collection, Func<ChangeStreamHelperChangedEventArgs, CancellationToken, Task> notification)
		{
			if (!this.TryGetValue(collection, out var notifications))
			{
				notifications = new();
				this[collection] = notifications;
			}
			notifications.Add(notification);
		}

		public async Task NotifyAsync(
			string databaseName,
			string collection,
			ChangeStreamHelperChangedEventArgs arg,
			ILogger logger,
			CancellationToken token)
		{
			if (this.TryGetValue(collection, out var notifications))
			{
				foreach (var notification in notifications)
				{
					try
					{
						await notification(arg, token);
					}
					catch (Exception ex)
					{
						logger.LogError(ex, "Error dispatching change stream to collection {0} database {1}", collection, databaseName);
					}
				}
			}
		}
	}
}

/// <summary>
/// Interface to abstract the concept of the stream helper.
/// </summary>
internal interface IMongoDbChangeStreamHelper
{
	/// <summary>
	/// Register a notifier on a database collection with a notification function.
	/// </summary>
	/// <param name="database"></param>
	/// <param name="collection"></param>
	/// <param name="notificationFunction"></param>
	void RegisterNotifier(string database, string collection, Func<ChangeStreamHelperChangedEventArgs, CancellationToken, Task> notificationFunction);

	/// <summary>
	/// Analyze feature flags, and actual connection string to mongo to understand if a change stream is supported.
	/// </summary>
	/// <returns></returns>
	bool SupportsChangeStreams();
}

internal struct ChangeStreamHelperChangedEventArgs
{
	public ChangeStreamHelperChangedEventArgs(string collectionName, string dataBaseName) : this()
	{
		CollectionName = collectionName;
		DataBaseName = dataBaseName;
	}

	public string CollectionName { get; }

	public string DataBaseName { get; }
}
