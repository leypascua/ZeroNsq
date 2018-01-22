using System;
using System.Collections.Generic;
using System.Text;

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
        public MessageContext(Message msg)
        {
            Message = msg;
        }

        public Message Message { get; private set; }

        public void Finish()
        {
            throw new NotImplementedException();
        }

        public void Requeue()
        {
            throw new NotImplementedException();
        }

        public void Touch()
        {
            throw new NotImplementedException();
        }
    }
}
