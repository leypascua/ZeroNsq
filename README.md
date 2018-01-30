ZeroNSQ
================

A dead-simple .NET library for bit.ly's (http://nsq.io) which targets netstandard2.0

Compatible and tested with NSQ version 1.0.0-compat

Inspired by [NSQCore](https://github.com/Webjet/NSQCore)


Usage
-----

### Publishing Messages

    var connectionString = "nsqd=http://127.0.0.1:4151;";

    using (IPublisher publisher = Publisher.CreateInstance(connectionString))
    {
        publisher.Publish("topic-name", "message contents");
    }

### Subscribing to Messages

    var connectionString = "nsqd=tcp://127.0.0.1:4150;";

    using (ISubscriber subscriber = Subscriber.CreateInstance("topic-name", "topic-name-channel", connectionString))
    {
        subscriber
            .OnMessageReceived(ctx => ExecuteMessageHandler(ctx))
            .OnConnectionError(err => LogError(err))
            .Start();

        // wait for connections...
    }


License
-------
The MIT License. Use as you please, but don't blame me for problems
