using System;
using System.Collections.Generic;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace ZiggyCreatures.Caching.Fusion.Backplane.MongoDb;

internal class MongoDbBackplaneMessage
{
	public static MongoDbBackplaneMessage Create(BackplaneMessage broadcastMessage, TimeSpan timeToLive)
	{
		var now = DateTime.UtcNow;
		return new MongoDbBackplaneMessage()
		{
			Id = ObjectId.GenerateNewId(),
			Message = broadcastMessage,
			ExpireTime = now.Add(timeToLive),
			QueueTime = now
		};
	}

	public ObjectId Id { get; set; }

	/// <summary>
	/// This is the real message that we want to send on the backplane.
	/// </summary>
	[BsonElement("m")]
	public BackplaneMessage Message { get; private set; } = null!;

	[BsonElement("e")]
	public DateTime ExpireTime { get; set; }

	[BsonElement("q")]
	public DateTime QueueTime { get; set; }

	[BsonElement("d")]
	public List<string> DispatchedTo { get; set; } = [];
}
