using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using ZeroNsq.Protocol;

namespace ZeroNsq
{
    public interface IMessageContext
    {
        Message Message { get; }
        string TopicName { get; }
        string ChannelName { get; }

        void Finish();
        void Requeue();
        void Touch();
    }

    internal class MessageContext : IMessageContext
    {
        private readonly Consumer _consumer;
        private readonly SubscriberOptions _options;

        public MessageContext(Consumer consumer, Message msg, SubscriberOptions options, string topic, string channel)
        {
            _consumer = consumer;
            _options = options;
            Message = msg;
            TopicName = topic;
            ChannelName = channel;
        }

        public Message Message { get; private set; }

        public string TopicName { get; private set; }

        public string ChannelName { get; private set; }

        public void Finish()
        {
            _consumer.Connection.SendRequest(Commands.Finish(Message.Id));
        }

        public void Requeue()
        {   
            int currentAttempts = (int)Message.Attempts;

            Trace.WriteLine(string.Format("Current attempts={0}; Max={1}", currentAttempts, _options.MaxRetryAttempts));

            if (currentAttempts <= _options.MaxRetryAttempts)
            {
                ///TODO: Get this value from somewhere... 
                int requeueDeferTimeout = 0;

                _consumer.Connection.SendRequest(Commands.Requeue(Message.Id, requeueDeferTimeout));
            }
            else
            {
                ///TODO: Possibly add a callback to handle this.
                throw new MessageRequeueException("MaxRetryAttempts for the message was exceeded.");
            }
        }

        public void Touch()
        {
            _consumer.Connection.SendRequest(Commands.Touch(Message.Id));
        }
    }
}
