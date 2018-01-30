using System;
using System.Collections.Generic;
using System.Text;
using ZeroNsq.Protocol;

namespace ZeroNsq.Internal
{
    public class HandlerExecutionContext
    {
        public INsqConnection Connection { get; set; }
        public SubscriberOptions Options { get; set; }
        public string TopicName { get; set; }
        public string ChannelName { get; set; }
        public Action<IMessageContext> MessageReceivedCallback { get; set; } 
        public Message Message { get; set; } 
        public Action<ConnectionErrorContext> ErrorCallback { get; set; }

        public IMessageContext CreateMessageContext()
        {
            return new MessageContext(
                Connection,
                Message,
                Options,
                TopicName,
                ChannelName
            );
        }
    }
}
