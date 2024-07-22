using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using MongoDB.Driver;

namespace ZiggyCreatures.Caching.Fusion.Backplane.MongoDb;

/// <summary>
/// This is the real instance that will perform the backplane operations
/// </summary>
internal class InternalMongoDbBackplane
{
	/// <summary>
	/// This is the list of the internal subscriptions.
	/// </summary>
	private ConcurrentDictionary<BackplaneSubscriptionOptions, BackplaneSubscriptionOptions> _subscriptionOptions = new();

	private readonly ILogger<MongoDbBackplane> _logger;
	private readonly MongoDbBackplaneOptions _options;
	private readonly IMongoClient _client;
	private readonly IMongoDbChangeStreamHelper _changeStreamHelper;
	private readonly string _identity;

	/// <summary>
	/// Create the backplane on Mongodb.
	/// </summary>
	public InternalMongoDbBackplane(
		MongoDbBackplaneOptions options,
		ILoggerFactory loggerFactory)
	{
		_logger = loggerFactory.CreateLogger<MongoDbBackplane>();
		_options = options;
		_client = _options.GetMongoDbClient();
		_changeStreamHelper = MongoDbChangeStreamHelper.CreateFromUrl(_client, loggerFactory.CreateLogger<MongoDbChangeStreamHelper>());

		var process = System.Diagnostics.Process.GetCurrentProcess();
		_identity = $"{Environment.MachineName}-{process.Id}";

		//check for databasename null
		if (string.IsNullOrWhiteSpace(_options.DatabaseName))
		{
			throw new InvalidOperationException("DatabaseName is mandatory for MongoDbBackplane");
		}
		//check also for collection name
		if (string.IsNullOrWhiteSpace(_options.CollectionName))
		{
			throw new InvalidOperationException("CollectionName is mandatory for MongoDbBackplane");
		}
		var db = _client.GetDatabase(_options.DatabaseName);
		_collection = db.GetCollection<MongoDbBackplaneMessage>(_options.CollectionName);

		// Create an index to automatically remove expired notifications
		var indexKeys = Builders<MongoDbBackplaneMessage>.IndexKeys.Ascending(n => n.ExpireTime);
		var indexOptions = new CreateIndexOptions { ExpireAfter = TimeSpan.Zero };
		var indexModel = new CreateIndexModel<MongoDbBackplaneMessage>(indexKeys, indexOptions);
		_collection.Indexes.CreateOne(indexModel);

		//Create an index on the DispatchedTo field
		_collection.Indexes.CreateOne(
			new CreateIndexModel<MongoDbBackplaneMessage>(
			Builders<MongoDbBackplaneMessage>.IndexKeys.Ascending(n => n.DispatchedTo)));
	}

	/// <inheritdoc />
	public void Subscribe(BackplaneSubscriptionOptions options)
	{
		if (options is null)
			throw new ArgumentNullException(nameof(options));

		if (options.ChannelName is null)
			throw new NullReferenceException("The BackplaneSubscriptionOptions.ChannelName cannot be null");

		if (options.IncomingMessageHandler is null)
			throw new NullReferenceException("The BackplaneSubscriptionOptions.MessageHandler cannot be null");

		if (options.ConnectHandler is null)
			throw new NullReferenceException("The BackplaneSubscriptionOptions.ConnectHandler cannot be null");

		lock (_subscriptionOptions)
		{
			_subscriptionOptions[options] = options;

			//I want to start polling when the first subscription will be active
			if (_timer == null)
			{
				_timer = new Timer(TimerCallBack, null, 1000, _options.PollingInterval.Milliseconds);
			}
		}
	}

	/// <inheritdoc />
	public void Unsubscribe(BackplaneSubscriptionOptions options)
	{
		lock (_subscriptionOptions)
		{
			//ok remove from the list
			_subscriptionOptions.TryRemove(options, out _);

			//if there are no more subscriptions we can stop polling
			if (_subscriptionOptions.Count == 0)
			{
				//stop polling, stop the timer, stop everything.
				_timer?.Dispose();
				_timer = null;
			}
		}
	}

	/// <inheritdoc />
	public async ValueTask PublishAsync(BackplaneMessage message, FusionCacheEntryOptions options, CancellationToken token = default)
	{
		var mongoMessage = MongoDbBackplaneMessage.Create(message, options.Duration);
		//ok just insert the message in the collection
		await _collection.InsertOneAsync(mongoMessage, cancellationToken: token);
	}

	/// <inheritdoc />
	public void Publish(BackplaneMessage message, FusionCacheEntryOptions options, CancellationToken token = default)
	{
		var mongoMessage = MongoDbBackplaneMessage.Create(message, options.Duration);
		//ok just insert the message in the collection
		_collection.InsertOne(mongoMessage, cancellationToken: token);
	}

	private int _pollerGate = 0;

	private Timer? _timer;
	private IMongoCollection<MongoDbBackplaneMessage> _collection;

	/// <summary>
	/// In the callback we simply poll the collection for new messages to be dispatched.
	/// </summary>
	/// <param name="state"></param>
	private void TimerCallBack(object state)
	{
		if (Interlocked.CompareExchange(ref _pollerGate, 1, 0) == 0)
		{
			try
			{
				var filter = Builders<MongoDbBackplaneMessage>.Filter.Ne("DispatchedTo", _identity);
				var update = Builders<MongoDbBackplaneMessage>.Update.AddToSet(n => n.DispatchedTo, _identity);
				var sort = Builders<MongoDbBackplaneMessage>.Sort.Ascending(n => n.QueueTime);
				var options = new FindOneAndUpdateOptions<MongoDbBackplaneMessage> { ReturnDocument = ReturnDocument.After };

				//ok now go until we find stuff.
				MongoDbBackplaneMessage notification;

				do
				{
					//TODO: standard timer is not async, we should probably use a Task.run + await Task.delay
					notification = _collection.FindOneAndUpdate(filter, update, options);

					if (notification != null)
					{
						foreach (var item in _subscriptionOptions.Values)
						{
							if (item.IncomingMessageHandler != null)
							{
								item.IncomingMessageHandler(notification.Message);
							}
						}
					}
				} while (notification != null && _timer != null);
			}
			catch (Exception ex)
			{
				_logger.LogError(ex, "Error in polling change stream");
			}
			finally
			{
				Interlocked.Exchange(ref _pollerGate, 0);
			}
		}
	}
}
