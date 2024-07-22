# FusionCache

![FusionCache logo](https://raw.githubusercontent.com/ZiggyCreatures/FusionCache/main/docs/logo-256x256.png)

### FusionCache is an easy to use, fast and robust hybrid cache with advanced resiliency features.

It was born after years of dealing with all sorts of different types of caches: memory caching, distributed caching, http caching, CDNs, browser cache, offline cache, you name it. So I've tried to put together these experiences and came up with FusionCache.

Find out [more](https://github.com/ZiggyCreatures/FusionCache).

## 📦 This package

This package is a backplane implementation on [MongoDb](https://www.mongodb.com/) Database. Clearly this can be sub-optimal if we consider other alternative that were born to support natively the pub/sub model but can help if you already are using MongoDb and does not want to introduce other piece of infrastructure.