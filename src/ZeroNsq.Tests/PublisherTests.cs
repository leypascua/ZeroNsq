using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using Xunit;
using ZeroNsq.Tests.Utils;


namespace ZeroNsq.Tests
{
    public class PublisherTests
    {   
        static readonly string Message = "Hello World";

        [Fact]            
        public void InvalidHostTest()
        {
            var options = ConnectionOptions.Default;

            using (var conn = new NsqConnectionProxy(Nsqd.Local, 31685, options))
            using (var pub = new Publisher(conn, ConnectionOptions.Default))
            {
                Assert.Throws<SocketException>(() => pub.Publish(Nsqd.DefaultTopicName, Message));
            }
        }

        [Fact]
        public void ValidHostTest()
        {
            var options = ConnectionOptions.Default;

            using (var nsqd = Nsqd.StartLocal(7111))
            using (var conn = new NsqConnectionProxy(nsqd.Host, nsqd.Port, options))
            using (var pub = new Publisher(conn, options))
            {
                pub.Publish(Nsqd.DefaultTopicName, Message);
            }
        }

        [Fact]
        public void VeryLongTopicNameTest()
        {
            var options = ConnectionOptions.Default;

            using (var nsqd = Nsqd.StartLocal(7112))
            using (var conn = new NsqConnectionProxy(nsqd.Host, nsqd.Port, options))
            using (var pub = new Publisher(conn, options))
            {
                string topicName = string.Format("{0}{1}{2}", Nsqd.DefaultTopicName, Guid.NewGuid(), Guid.NewGuid());
                Assert.Throws<ArgumentException>(() => pub.Publish(topicName, Message));
            }
        }

        [Fact]
        public void TopicNameWithInvalidCharactersTest()
        {
            var options = ConnectionOptions.Default;

            using (var nsqd = Nsqd.StartLocal(7113))
            using (var conn = new NsqConnectionProxy(nsqd.Host, nsqd.Port, options))
            using (var pub = new Publisher(conn, options))
            {
                string topicName = "This.Topic$";
                Assert.Throws<ArgumentException>(() => pub.Publish(topicName, Message));
            }
        }

        [Fact]
        public void ReconnectionAttemptTest()
        {
            var options = new ConnectionOptions { MaxClientReconnectionAttempts = 2, InitialBackoffTimeInSeconds = 1 };

            using (var nsqd = Nsqd.StartLocal(7114))
            using (var conn = new NsqConnectionProxy(nsqd.Host, nsqd.Port, options))
            using (var pub = new Publisher(conn, options))
            {
                pub.Publish(Nsqd.DefaultTopicName, Message);

                nsqd.Kill();

                try
                {
                    pub.Publish(Nsqd.DefaultTopicName, Message);
                }
                catch (BaseException) { }

                Assert.NotEqual(0, conn.ReconnectionAttempts);
            }
        }
    }
}
