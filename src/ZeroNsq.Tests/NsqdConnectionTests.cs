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

            using (var nsqd = Nsqd.StartLocal())
            using (var conn = new NsqdConnection(nsqd.Host, nsqd.Port, options))
            {
                conn.Connect();

                // force idle time
                Thread.Sleep(TimeSpan.FromSeconds(options.HeartbeatIntervalInSeconds.Value * 4));

                // both requests should be alive.
                conn.SendRequest(new Publish(Nsqd.DefaultTopicName, "Hello World"));
                Assert.Throws<RequestException>(() => conn.SendRequest(new InvalidRequest()));
            }   
        }

        [Fact]
        public void ConcurrentPublishingTest()
        {
            using (var nsqd = Nsqd.StartLocal())
            using (var conn = new NsqdConnection(nsqd.Host, nsqd.Port, ConnectionOptions.Default))
            {
                conn.Connect();

                string message = new string('#', 1024 * 1024);

                var results = Parallel.For(0, 100, idx =>
                {
                    conn.SendRequest(new Publish(Nsqd.DefaultTopicName, message));
                });

                Assert.True(results.IsCompleted);
            }
        }

        [Fact]
        public void DroppedConnectionTest()
        {
            using (var nsqd = Nsqd.StartLocal())
            using (var conn = new NsqdConnection(nsqd.Host, nsqd.Port))
            {
                conn.Connect();
                nsqd.Kill();

                Assert.Throws<ConnectionException>(() =>
                    conn.SendRequest(new Publish(Nsqd.DefaultTopicName, "Hello World"))
                );
            }
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
