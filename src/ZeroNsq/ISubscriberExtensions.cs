using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace ZeroNsq
{
    public static class ISubscriberExtensions
    {
        public static void Stop(this ISubscriber subscriber)
        {
            Task.Run(() => subscriber.StopAsync()).Wait();
        }
    }
}
