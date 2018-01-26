using System;
using System.IO;
using System.Threading;
using Xunit;
using ZeroNsq.Protocol;
using ZeroNsq.Tests.Utils;
using ZeroNsq.Helpers;
using System.Threading.Tasks;

namespace ZeroNsq.Tests
{
    public class NsqdConnectionTests 
    {
        [Fact]
        public void InvalidHostTest()
        {
            using (var conn = new NsqdConnection(Nsqd.Local, 31685))
            {
                Assert.Throws<SocketException>(() => conn.Connect());
            }
        }

        [Fact]
        public void RespondToHeartbeatTest()
        {
            var options = new ConnectionOptions
            {
                HeartbeatIntervalInSeconds = 3
            };

            using (var nsqd = Nsqd.StartLocal(9111))
            using (var conn = new NsqdConnection(nsqd.Host, nsqd.Port, options))
            {
                conn.Connect();

                // force idle time
                Thread.Sleep(TimeSpan.FromSeconds((options.HeartbeatIntervalInSeconds.Value * 2) + 1));

                // both requests should be alive.
                conn.SendRequest(new Publish(Nsqd.DefaultTopicName, "Hello World"));
                Assert.Throws<RequestException>(() => conn.SendRequest(new InvalidRequest()));
            }   
        }

        [Fact]
        public void ConcurrentPublishingTest()
        {
            using (var nsqd = Nsqd.StartLocal(9112))
            using (var conn = new NsqdConnection(nsqd.Host, nsqd.Port, ConnectionOptions.Default))
            {
                conn.Connect();

                string message = new string('#', 1024 * 1024);

                var results = Parallel.For(1, 32, idx =>
                {
                    conn.SendRequest(new Publish(Nsqd.DefaultTopicName, message));
                });

                Assert.True(results.IsCompleted);
            }
        }

        [Fact]
        public void DroppedConnectionTest()
        {
            using (var nsqd = Nsqd.StartLocal(9113))
            using (var conn = new NsqdConnection(nsqd.Host, nsqd.Port))
            {
                conn.Connect();
                nsqd.Kill();

                Assert.Throws<ConnectionException>(() =>
                    conn.SendRequest(new Publish(Nsqd.DefaultTopicName, "Hello World"))
                );
            }
        }

        [Fact]
        public void ReceiveMessageTest()
        {
            var resetEvent = new ManualResetEventSlim();
            int receivedMessages = 0;

            using (var nsqd = Nsqd.StartLocal(9114))
            using (var subscriber = new NsqdConnection(nsqd.Host, nsqd.Port))
            using (var publisher = new NsqdConnection(nsqd.Host, nsqd.Port))
            {
                subscriber.OnMessageReceived(msg =>
                {
                    subscriber.SendRequest(Commands.Finish(msg.Id));
                    receivedMessages += 1;
                    resetEvent.Set();
                });

                subscriber.Connect();
                subscriber.SendRequest(new Subscribe(Nsqd.DefaultTopicName, Nsqd.DefaultTopicName));

                publisher.Connect();
                publisher.SendRequest(new Publish(Nsqd.DefaultTopicName, "Hello World"));

                subscriber.SendRequest(new Ready(1));

                resetEvent.Wait(TimeSpan.FromSeconds(5));
            }

            Assert.NotEqual(0, receivedMessages);
        }

        class InvalidRequest : IRequestWithResponse
        {
            public byte[] ToByteArray()
            {
                using (var ms = new MemoryStream())
                {
                    ms.WriteBytes(Commands.PUB);
                    ms.WriteASCII(Nsqd.DefaultTopicName + "\n");
                    ms.WriteInt32(0);
                    ms.WriteUtf8(Nsqd.DefaultTopicName);

                    return ms.ToArray();
                }
            }
        }
    }
}
