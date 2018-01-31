using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace ZeroNsq.MultiConsumerHost.Example
{
    public class MessageQueue
    {
        private readonly ConcurrentQueue<string> _internalQueue;
        private readonly ManualResetEventSlim ResetEvent = new ManualResetEventSlim();

        public MessageQueue()
        {
            _internalQueue = new ConcurrentQueue<string>();
        }

        public void Enqueue(string message)
        {
            _internalQueue.Enqueue(message);
            ResetEvent.Set();
        }

        public string Dequeue()
        {
            if (_internalQueue.Count == 0)
            {
                ResetEvent.Wait();
            }

            ResetEvent.Reset();

            string result = null;
            _internalQueue.TryDequeue(out result);

            return result;
        }
    }
}
