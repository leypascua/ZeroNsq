using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace ZeroNsq.MultiConsumerHost.Example
{
    class Program
    {
        static readonly ManualResetEventSlim ResetEvent = new ManualResetEventSlim();


        static void Main(string[] args)
        {
            // Usage:
            // ConsumerHost {topic} {# of instances}
            //  example:
            //  console@localhost> ConsumerHost ZeroNsq.SimpleExample.Program 1
            Console.WriteLine("Usage:");
            Console.WriteLine("   ConsumerHost {topic} {# of instances} \n\n");

            var hostArgs = ConsumerHostArgs.Parse(args);
            Console.WriteLine("Starting {0} channels for topic {1}", hostArgs.InstanceCount, hostArgs.TopicName);

            var mq = new MessageQueue();
            IEnumerable<Endpoint> endpoints = StartEndpoints(hostArgs, mq);

            Console.WriteLine("Waiting for messages...\n\n");

            while (!ResetEvent.IsSet)
            {
                string message = mq.Dequeue();
                Console.WriteLine(message);
            }

            foreach (var ep in endpoints)
            {
                Console.WriteLine("Stopping {0}", ep.Name);
                ep.Stop();
            }

            Console.WriteLine("\n\n Done.");
        }

        private static IEnumerable<Endpoint> StartEndpoints(ConsumerHostArgs hostArgs, MessageQueue mq)
        {
            var endpoints = new List<Endpoint>();

            Parallel.For(0, hostArgs.InstanceCount, idx =>
            {
                string channelName = string.Format("{0}.{1}", hostArgs.TopicName, idx);
                var ep = new Endpoint(hostArgs.TopicName, channelName, mq);
                ep.OnShutdownReceived(() =>
                {
                    if (!ResetEvent.IsSet)
                    {
                        string shutdownMessage = string.Format("[{0}] Shutdown message received.", ep.Name);
                        mq.Enqueue(shutdownMessage);
                        ResetEvent.Set();
                    }
                }).Start();

                endpoints.Add(ep);
                Console.WriteLine("Channel {0} started.", ep.Name);
            });

            return endpoints;
        }

        class ConsumerHostArgs
        {
            const string DefaultTopicName = "ZeroNsq.SimpleExample.Program";

            public string TopicName { get; set; }

            public int InstanceCount { get; set; }

            public static ConsumerHostArgs Parse(string[] args)
            {
                string topicName = DefaultTopicName;
                int instanceCount = 1;

                if (args.Length == 1)
                {
                    topicName = (args[0] ?? string.Empty).Trim();
                }

                if (args.Length == 2)
                {
                    topicName = (args[0] ?? string.Empty).Trim();
                    instanceCount = Math.Max(1, int.Parse(args[1]));
                }

                return new ConsumerHostArgs
                {
                    TopicName = topicName,
                    InstanceCount = instanceCount
                };
            }
        }
    }
}
