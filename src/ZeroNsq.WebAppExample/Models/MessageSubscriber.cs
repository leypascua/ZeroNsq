using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
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
            options.MaxInFlight = 3;

            ISubscriber subscriber = Subscriber.CreateInstance(topicName, channelName, options);

            subscriber
                .OnMessageReceivedAsync(HandleMessageAsync)
                .OnConnectionError(errorContext =>
                {
                    string message = "Error: " + errorContext.Error.ToString();
                    ReceivedMessages.Push(message);
                })
                .Start();

            return subscriber;
        }

        private static async Task HandleMessageAsync(IMessageContext context)
        {
            string incomingMessage = context.Message.ToUtf8String();

            // simulate a long-running async call
            if (incomingMessage == "sleep")
            {
                await Sleep();
            }

            // simulate an error
            if (incomingMessage == "err")
            {
                try
                {
                    await context.RequeueAsync();
                    
                    ReceivedMessages.Push(string.Format("Retry attempt for {0}: {1}", context.Message.IdString, context.Message.Attempts));
                    return;
                }
                catch (MessageRequeueException)
                {
                    await context.FinishAsync();
                    ReceivedMessages.Push(string.Format("Exhausted retry attempts for msg {0}", context.Message.IdString));
                    return;
                }
            }

            // simulate a successful message.            
            ReceivedMessages.Push(incomingMessage);
            await context.FinishAsync();
        }

        private static async Task Sleep()
        {
            await Task.Run(() =>
            {
                string threadId = Thread.CurrentThread.ManagedThreadId.ToString();
                var resetEvent = new ManualResetEventSlim();
                ReceivedMessages.Push("Sleep: Thread " + threadId + " is waiting for 7 seconds");
                resetEvent.Wait(TimeSpan.FromSeconds(7));
                ReceivedMessages.Push("Sleep: Thread " + threadId + " is waking up");
            });
        }
    }
}
