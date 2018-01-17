using System;

namespace ZeroNsq.SimpleExample
{
    class Program
    {
        static void Main(string[] args)
        {
            using (var client = new NsqdConnection("127.0.0.1", 4150))
            {
                // publish a single message
                client.Publish("ZeroNsq.SimpleExample.Program", "Hello world");                      
            }   
        }
    }
}
