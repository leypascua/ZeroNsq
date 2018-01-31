using System;
using System.Collections.Generic;
using System.Text;

namespace ZeroNsq.MultiConsumerHost.Example
{
    public class Endpoint
    {
        private readonly string _topicName;
        private readonly string _channelName;
        private readonly MessageQueue _messageQueue;
        private Action _onShutdownReceivedCallback;

        public Endpoint(string topicName, string channelName, MessageQueue messageQueue)
        {
            _topicName = topicName;
            _channelName = channelName;
            _messageQueue = messageQueue;
        }

        public string Name { get { return string.Format("{0}@{1}", _channelName, _topicName); } }

        public ISubscriber Consumer { get; private set; }

        public void Start()
        {
            string connStr = "nsqd=tcp://127.0.0.1:4150;";
            Consumer = Subscriber.CreateInstance(_topicName, _channelName, SubscriberOptions.Parse(connStr));

            Consumer
                .OnMessageReceived(HandleMessage)
                .OnConnectionError(HandleError);

            Consumer.Start();
        }

        public Endpoint OnShutdownReceived(Action callback)
        {
            _onShutdownReceivedCallback = callback;

            return this;
        }

        public void Stop()
        {
            if (Consumer.IsActive)
            {
                Consumer.Stop();
            }
        }

        private void HandleError(ConnectionErrorContext errorContext)
        {
            EnqueueMessage("[{0}@{1}] Error Occurred: {2}", _channelName, _topicName, errorContext.Error.ToString());
        }

        private void HandleMessage(IMessageContext context)
        {
            string message = context.Message.ToUtf8String();
            EnqueueMessage("[{0}@{1}] Message Received: {2}", _channelName, _topicName, message);
            context.Finish();

            if (message == "shutdown")
            {
                if (_onShutdownReceivedCallback != null)
                {
                    _onShutdownReceivedCallback();
                }
            }
        }

        private void EnqueueMessage(string format, params string[] args)
        {
            string message = string.Format(format, args);
            _messageQueue.Enqueue(message);
        }

    }
}
