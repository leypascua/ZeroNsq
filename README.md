ZeroNSQ
================

A dead-simple, fault-tolerant .NET library for bit.ly's (http://nsq.io) which targets netstandard2.0

Compatible and tested with NSQ version 1.0.0-compat

Inspired by [NSQCore](https://github.com/Webjet/NSQCore)

This is a work-in-progress, but will be used in production.

Usage
-----

### Download for netstandard2.0 via NuGet
    Install-Package ZeroNsq

### Connection strings

    // for publishers / producers:
    var publisherConnStr = "nsqd=http://127.0.0.1:4151;"; // A connection to a HTTP NSQD endpoint is preferred.

    // for subscribers / consumers:
    var consumerConnStr = "nsqd=tcp://127.0.0.1:4150; " + // Required. The nsqd host URI. Can be defined more than once.
                          "MessageTimeout=120; " + // Optional. the max amount of time in seconds a message can be in-flight before it times out
                          "HeartbeatIntervalInSeconds=30; " + // Optional. The NSQ protocol heartbeat interval
                          "MaxClientReconnectionAttempts=3; " + // Optional. The max number of attempts to reconnect to the NSQD instance
                          "MaxInFlight=1; " + // Optional. The max number of messages that are in-flight 
                          "MaxRetryAttempts=3;"; // Optional. The max NSQ command submission attempts when an operation fails

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

### Handling Messages
    
    public static void ExecuteMessageHandler(IMessageContext ctx) 
    {
        const int MaxMessageAttempts = 3;

        try 
        {
            // Message can be deserialized into JSON or something else.
            string utf8String = ctx.Message.ToUtf8String();

            while (IsLongProcessRunning(utf8String))
            {
                // Avoid message timeout.
                ctx.Touch();
                Thread.Sleep(1000);
            }

            Assert.Equal("message contents", utf8String);

            // Tell NSQ that we're done with the message.
            ctx.Finish();
        }
        catch 
        {
            if (ctx.Message.Attempts <= MaxMessageAttempts)
            {
                // requeue the message
                ctx.Requeue();
            }
            else 
            {
                // Do something to handle the message, maybe put to 
                // a custom dead letter queue.
                WriteToDeadLetterQueue(ctx);

                // Throw-out the message from the queue
                ctx.Finish();
            }
        }
    }


License
-------
The MIT License. Use as you please, but don't blame me for problems
