using System;
using System.Threading;
using System.Threading.Tasks;

namespace ZeroNsq.SimpleExample
{
    class Program
    {
        const string Localhost = "127.0.0.1";

        static void Main(string[] args)
        {
            string topicName = "ZeroNsq.SimpleExample.Program";
            string channelName = topicName + ".Channel";
            string connectionString = "nsqd=tcp://127.0.0.1:4150;";

            using (var subscriber = new Subscriber(topicName, channelName, SubscriberOptions.Parse(connectionString)))
            using (var publisher = new Publisher(Localhost, 4150))
            {
                subscriber
                    .OnMessageReceived(HandleMessage)
                    .OnConnectionError(errorContext => Console.WriteLine("Error: " + errorContext.Error.ToString()))
                    .Start();

                Console.WriteLine("Type a message then hit [enter] to publish.");
                Console.WriteLine("Press [ctrl + c] to terminate.");

                while (true)
                {
                    Console.Write("ping>");
                    string message = Console.ReadLine();
                    publisher.Publish(topicName, message);
                }
            }
        }

        static void HandleMessage(IMessageContext context)
        {
            string incomingMessage = context.Message.ToUtf8String();

            // simulate an error
            if (incomingMessage == "err")
            {
                context.Requeue();
                return;
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
            }

            // simulate a successful message.
            Console.Write("pong>");
            Console.WriteLine(incomingMessage);
            context.Finish();
        }
    }
}
