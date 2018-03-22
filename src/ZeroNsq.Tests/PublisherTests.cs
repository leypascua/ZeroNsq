using System;
using Xunit;
using ZeroNsq.Internal;
using ZeroNsq.Tests.Utils;

namespace ZeroNsq.Tests
{
    public class PublisherTests
    {   
        static readonly string Message = "Hello World";

        [Fact]
        public void CreateInstanceThrowsExceptionOnInvalidNsqdTest()
        {
            Assert.Throws<FormatException>(() =>
            {
                Publisher.CreateInstance("Host=127.0.0.1:4151;");
            });
        }

        [Fact]
        public void CreateTcpInstanceByConnectionStringTest()
        {
            string connectionString = "nsqd=tcp://127.0.0.1:4150;";
            var publisher = Publisher.CreateInstance(connectionString);

            Assert.True(publisher is TcpPublisher);
        }

        [Fact]
        public void CreateHttpInstanceByConnectionStringTest()
        {
            string connectionString = "nsqd=http://127.0.0.1:4151;";
            var publisher = Publisher.CreateInstance(connectionString);

            Assert.True(publisher is HttpPublisher);
        }

        [Fact]
        public void CreateDefaultInstanceTest()
        {
            var publisher = Publisher.CreateInstance();
            Assert.True(publisher is HttpPublisher);
        }

        [Fact]
        public void CreateWithHostAndPortTest()
        {
            var publisher = Publisher.CreateInstance("127.0.0.1", 4151);
            Assert.True(publisher is HttpPublisher);
        }

        [Fact]
        public void CreateHttpInstanceTest()
        {
            var publisher = Publisher.CreateInstance(new Uri("http://127.0.0.1:4151"));
            Assert.True(publisher is HttpPublisher);
        }

        [Fact]
        public void CreateTcpInstanceTest()
        {
            var publisher = Publisher.CreateInstance(new Uri("tcp://127.0.0.1:4151"));
            Assert.True(publisher is TcpPublisher);
        }

        [Fact]
        public void HttpErrorResponseTest()
        {
            using (var nsqd = Nsqd.StartLocal(7110))
            using (var pub = Publisher.CreateInstance(host: nsqd.Host, port: nsqd.HttpPort, scheme: "http"))
            {
                string topicName = "Test.Topic";

                Assert.Throws<RequestException>(() => pub.Publish(topicName, string.Empty));
            }
        }

        [Fact]
        public void HttpPublisherVeryLongTopicNameTest()
        {
            using (var nsqd = Nsqd.StartLocal(7100))
            using (var pub = Publisher.CreateInstance(host: nsqd.Host, port: nsqd.HttpPort, scheme: "http"))
            {
                string topicName = "Test.Topic" + new string('z', 64);
                Assert.Throws<ArgumentException>(() => pub.Publish(topicName, Guid.NewGuid().ToString()));
            }
        }

        [Fact]            
        public void InvalidHostTest()
        {
            var options = ConnectionOptions.Default;

            using (var conn = new NsqConnectionProxy(Nsqd.Local, 31685, options))
            using (var pub = new TcpPublisher(conn, ConnectionOptions.Default))
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
            using (var pub = new TcpPublisher(conn, options))
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
            using (var pub = new TcpPublisher(conn, options))
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
            using (var pub = new TcpPublisher(conn, options))
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
            using (var pub = new TcpPublisher(conn, options))
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
