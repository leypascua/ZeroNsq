using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ZeroNsq.WebAppExample.Models
{
    public class MessageSubscriber
    {
        public const string TopicName = "ZeroNsq.SimpleExample.Program";
        public readonly static Stack<string> ReceivedMessages = new Stack<string>();        

        public static ISubscriber CreateInstance(string topicName, string channelName, string connectionString)
        {
            var options = SubscriberOptions.Parse(connectionString);
            options.ClientId = "ZeroNsq.WebAppExample.Models.MessageSubscriber";

            ISubscriber subscriber = Subscriber.CreateInstance(topicName, channelName, options);

            subscriber
                .OnMessageReceived(HandleMessage)
                .OnConnectionError(errorContext =>
                {
                    string message = "Error: " + errorContext.Error.ToString();
                    ReceivedMessages.Push(message);
                })
                .Start();

            return subscriber;
        }

        private static void HandleMessage(IMessageContext context)
        {
            string incomingMessage = context.Message.ToUtf8String();

            // simulate an error
            if (incomingMessage == "err")
            {
                try
                {
                    context.Requeue();
                    
                    ReceivedMessages.Push(string.Format("Retry attempt for {0}: {1}", context.Message.IdString, context.Message.Attempts));
                    return;
                }
                catch (MessageRequeueException)
                {
                    context.Finish();
                    ReceivedMessages.Push(string.Format("Exhausted retry attempts for msg {0}", context.Message.IdString));
                    return;
                }
            }

            // simulate a successful message.            
            ReceivedMessages.Push(incomingMessage);
            context.Finish();
        }
    }
}
