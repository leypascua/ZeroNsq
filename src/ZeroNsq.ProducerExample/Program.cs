using System;

namespace ZeroNsq.ProducerExample
{
    class Program
    {
        const string Localhost = "127.0.0.1";

        static void Main(string[] args)
        {
            string topicName = "ZeroNsq.SimpleExample.Program";
            string connectionString = string.Format("nsqd=tcp://{0}:4150;", Localhost);

            using (IPublisher publisher = new TcpPublisher(Localhost, 4150))
            {
                Console.WriteLine("Type a message then hit [enter] to publish.");
                Console.WriteLine("Press [ctrl + c] to terminate.");

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
