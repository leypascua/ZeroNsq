using System;
using System.Collections.Generic;
using System.Text;

namespace ZeroNsq
{
    public class Subscriber : IDisposable
    {
        private readonly SubscriberOptions _options;
        private readonly List<INsqConnection> _connections = new List<INsqConnection>();
        
        private Subscriber(SubscriberOptions options)
        {
            _options = options;
        }

        public static Subscriber Create(string connectionString)
        {
            return Create(SubscriberOptions.Parse(connectionString));
        }

        public static Subscriber Create(SubscriberOptions options)
        {
            var sub = new Subscriber(options)
                .Initialize();

            return sub;
        }

        public void Start(string topicName, string channelName, Action<IMessageContext> messageReceivedCallback)
        {
            ///TODO: Implement me

            // locate NSQD instances 

            // foreach nsqd instance 
            //    connect
            //    subscribe
            //    rdy
            //    start worker 
        }

        private Subscriber Initialize()
        {
            // start reconnection poller

            return this;
        }

        #region IDisposable members

        public void Dispose()
        {
            throw new NotImplementedException();
        }

        #endregion
    }
}
