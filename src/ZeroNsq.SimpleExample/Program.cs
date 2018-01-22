using System;

namespace ZeroNsq.SimpleExample
{
    class Program
    {
        const string Localhost = "127.0.0.1";

        static void Main(string[] args)
        {
            string topicName = "ZeroNsq.SimpleExample.Program";
            string channelName = topicName + ".Channel";

            using (var subscriber = Subscriber.Create("nsqd=tcp://127.0.0.1:4150;"))
            using (var publisher = new Publisher(Localhost, 4150))
            {
                subscriber.Start(topicName, channelName, context =>
                {
                    Console.Write("pong>");
                    Console.WriteLine(context.Message.ToUtf8String());
                    context.Finish();
                });

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
    }
}
