using System;
using System.Threading;
using System.Threading.Tasks;

namespace ZeroNsq.ConsumerExample
{
    class Program
    {
        const string Localhost = "127.0.0.1";
        static ManualResetEventSlim resetEvent = new ManualResetEventSlim();

        static void Main(string[] args)
        {
            string topicName = args.Length == 0 ? "ZeroNsq.SimpleExample.Program" : args[0];
            string channelName = topicName + ".Channel";
            string connectionString = string.Format("nsqd=tcp://{0}:4150;", Localhost);

            using (ISubscriber subscriber = Subscriber.CreateInstance(topicName, channelName, SubscriberOptions.Parse(connectionString)))
            {
                subscriber
                    .OnMessageReceived(HandleMessage)
                    .OnConnectionError(errorContext => Console.WriteLine("Error: " + errorContext.Error.ToString()))
                    .Start();

                Console.WriteLine("Press [ctrl + c] to terminate.");
                Console.WriteLine("Subscribed to topic: " + topicName);

                resetEvent.Wait();

                Console.WriteLine("Stopping the subscriber");
                subscriber.Stop();
            }
        }

        static void HandleMessage(IMessageContext context)
        {
            string incomingMessage = context.Message.ToUtf8String();

            // simulate an error
            if (incomingMessage == "err")
            {
                try
                {
                    context.Requeue();
                    Console.WriteLine("Retry attempt for {0}: {1}", context.Message.IdString, context.Message.Attempts);
                    return;
                }
                catch (MessageRequeueException)
                {
                    context.Finish();
                    Console.WriteLine("Exhausted retry attempts for msg {0}", context.Message.IdString);
                    return;
                }
            }

            // simulate a long-running message handler
            if (incomingMessage == "long")
            {
                var longTask = Task.Factory.StartNew(() => Thread.Sleep(TimeSpan.FromSeconds(30)));

                while (!longTask.IsCompleted)
                {
                    Thread.Sleep(TimeSpan.FromSeconds(10));
                    context.Touch();
                }

                context.Finish();
            }

            // simulate a successful message.            
            Console.Write("pong>");
            Console.WriteLine(incomingMessage);
            context.Finish();

            if (incomingMessage.StartsWith("shutdown"))
            {
                Console.WriteLine();
                Console.WriteLine("Shutdown message received. Killing the process...");
                resetEvent.Set();
            }
        }
    }
}
