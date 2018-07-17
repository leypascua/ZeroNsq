using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using ZeroNsq;
using ZeroNsq.WebAppExample.Models;

namespace ZeroNsq.WebAppExample
{
    public class Program
    {
        

        public static void Main(string[] args)
        {
            
            string channelName = MessageSubscriber.TopicName + ".Web";
            string connectionString = string.Format("nsqd=tcp://{0}:4150;", "127.0.0.1");

            using (ISubscriber subscriber = MessageSubscriber.CreateInstance(MessageSubscriber.TopicName, channelName, connectionString))
            {
                BuildWebHost(args).Run();
                subscriber.Stop();
            }   
        }

        public static IWebHost BuildWebHost(string[] args) =>
            WebHost.CreateDefaultBuilder(args)
                .UseStartup<Startup>()
                .Build();

        
    }
}
