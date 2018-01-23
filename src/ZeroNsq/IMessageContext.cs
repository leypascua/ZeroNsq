using System;
using System.Collections.Generic;
using System.Text;
using ZeroNsq.Protocol;

namespace ZeroNsq
{
    public interface IMessageContext
    {
        Message Message { get; }

        void Finish();
        void Requeue();
        void Touch();
    }

    internal class MessageContext : IMessageContext
    {
        private readonly INsqConnection _connection;
        private readonly SubscriberOptions _options;

        public MessageContext(INsqConnection conn, Message msg, SubscriberOptions options)
        {
            _connection = conn;
            _options = options;
            Message = msg;
        }

        public Message Message { get; private set; }

        public void Finish()
        {
            _connection.SendRequest(Commands.New(Commands.Finish(Message.Id)));
        }

        public void Requeue()
        {
            if (_options.MaxRetryAttempts < Message.Attempts)
            {
                ///TODO: Possibly add a callback to handle this.

                throw new InvalidOperationException("MaxRetryAttempts for the message exceeded.");
            }

            _connection.SendRequest(Commands.New(Commands.Requeue(Message.Id, 0)));
        }

        public void Touch()
        {
            _connection.SendRequest(Commands.New(Commands.Touch(Message.Id)));
        }
    }
}
