using System;

namespace ZeroNsq.ProducerExample
{
    class Program
    {
        const string Localhost = "127.0.0.1";

        static void Main(string[] args)
        {
            // try to get the topic name from CLI arguments
            string topicName = args.Length == 0 ? "ZeroNsq.SimpleExample.Program" : args[0];

            string connectionString = string.Format("nsqd=http://{0}:4151;", Localhost);

#if DEBUG
            LogProvider.Configure()
                .UseDefault();
#endif      

            using (IPublisher publisher = Publisher.CreateInstance(connectionString))
            {
                Console.WriteLine("Type a message then hit [enter] to publish.");
                Console.WriteLine("Press [ctrl + c] or type 'exit' to terminate.");
                Console.WriteLine("Producing topic: " + topicName);
                Console.WriteLine();

                while (true)
                {
                    Console.Write("ping> ");
                    string message = Console.ReadLine().Replace(Environment.NewLine, string.Empty);

                    if (message.StartsWith("exit")) break;

                    publisher.Publish(topicName, message);
                }
            }

            Console.WriteLine("Done.");
        }
    }
}
