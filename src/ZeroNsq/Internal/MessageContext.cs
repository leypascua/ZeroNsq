using System;
using System.Threading.Tasks;
using ZeroNsq.Protocol;

namespace ZeroNsq.Internal
{
    internal class MessageContext : IMessageContext
    {
        private readonly INsqConnection _connection;
        private readonly SubscriberOptions _options;
        private readonly Message _internalMessage;

        public MessageContext(INsqConnection connection, Message msg, SubscriberOptions options, string topic, string channel)
        {
            _connection = connection;
            _options = options;
            _internalMessage = msg;
            TopicName = topic;
            ChannelName = channel;
        }

        public IMessage Message { get { return _internalMessage; } }

        public string TopicName { get; private set; }

        public string ChannelName { get; private set; }

        public void Finish()
        {
            try
            {
                Task.Run(async () => await FinishAsync()).Wait();
            }
            catch (AggregateException ex)
            {
                throw ex.InnerException;
            }
        }

        public async Task FinishAsync()
        {
            await _connection.SendRequestAsync(Commands.Finish(_internalMessage.Id));
        }

        public void Requeue()
        {
            try
            {
                Task.Run(() => RequeueAsync()).Wait();
            }
            catch (AggregateException ex)
            {
                throw ex.InnerException;
            }
        }

        public async Task RequeueAsync()
        {
            int currentAttempts = (int)Message.Attempts;

            if (currentAttempts <= _options.MaxRetryAttempts)
            {
                ///TODO: Get this value from somewhere... 
                int requeueDeferTimeout = 0;

                await _connection.SendRequestAsync(Commands.Requeue(_internalMessage.Id, requeueDeferTimeout));
            }
            else
            {
                ///TODO: Possibly add a callback to handle this.
                throw new MessageRequeueException("MaxRetryAttempts for the message was exceeded.");
            }
        }

        public void Touch()
        {
            try
            {
                Task.Run(() => TouchAsync()).Wait();
            }
            catch (AggregateException ex)
            {
                throw ex.InnerException;
            }
        }

        public async Task TouchAsync()
        {
            await _connection.SendRequestAsync(Commands.Touch(_internalMessage.Id));
        }
    }
}
