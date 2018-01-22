using System;

namespace ZeroNsq.SimpleExample
{
    class Program
    {
        const string Localhost = "127.0.0.1";

        static void Main(string[] args)
        {   
            using (var publisher = new Publisher(Localhost, 4150))
            {
                Console.WriteLine("Type a message then hit [enter] to publish.");
                Console.WriteLine("Press [ctrl + c] to terminate.");

                while (true)
                {
                    Console.Write("sample>");
                    string message = Console.ReadLine();
                    publisher.Publish("ZeroNsq.SimpleExample.Program", message);
                }
            }
        }
    }
}
