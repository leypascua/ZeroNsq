using System;
using System.IO;
using System.Threading;
using Xunit;
using ZeroNsq.Protocol;
using ZeroNsq.Tests.Utils;
using ZeroNsq.Helpers;
using System.Threading.Tasks;
using ZeroNsq.Internal;

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
        public async Task RespondToHeartbeatTest()
        {
            bool isHeartbeatResponded = false;

            var options = new ConnectionOptions
            {
                HeartbeatIntervalInSeconds = 2
            };

            var resetEvent = new ManualResetEventSlim();

            using (var nsqd = Nsqd.StartLocal(9111))
            using (var conn = new NsqdConnection(nsqd.Host, nsqd.Port, options))
            {
                conn
                    .OnHeartbeatResponded(() => {
                        isHeartbeatResponded = true;
                        resetEvent.Set();
                    })
                    .Connect();

                // force idle time
                int waitTime = (options.HeartbeatIntervalInSeconds.Value * 2) + 1;
                resetEvent.Wait(TimeSpan.FromSeconds(waitTime));

                await conn.SendRequestAsync(new Publish(Nsqd.DefaultTopicName, "Hello World"));                

                Assert.True(isHeartbeatResponded);
            }   
        }

        [Fact]
        public void ConcurrentSendRequestTest()
        {
            using (var nsqd = Nsqd.StartLocal(9112))
            using (var conn = new NsqdConnection(nsqd.Host, nsqd.Port, ConnectionOptions.Default))
            {
                conn.Connect();

                string message = new string('#', 1024 * 1024);

                var results = Parallel.For(1, 32, async idx =>
                {
                    await conn.SendRequestAsync(new Publish(Nsqd.DefaultTopicName, message));
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

                Assert.ThrowsAsync<ConnectionException>(() =>
                    conn.SendRequestAsync(new Publish(Nsqd.DefaultTopicName, "Hello World"))
                );
            }
        }

        [Fact]
        public async Task ReceiveMessageTest()
        {
            var resetEvent = new ManualResetEventSlim();
            int receivedMessages = 0;

            using (var nsqd = Nsqd.StartLocal(9114))
            using (var subscriber = new NsqdConnection(nsqd.Host, nsqd.Port))
            using (var publisher = Publisher.CreateInstance(host: nsqd.Host, port: nsqd.HttpPort, scheme: "http"))
            {
                subscriber.OnMessageReceived(async msg =>
                {
                    await subscriber.SendRequestAsync(Commands.Finish(msg.Id));
                    receivedMessages += 1;
                    resetEvent.Set();
                });

                subscriber.Connect();
                await subscriber.SendRequestAsync(new Subscribe(Nsqd.DefaultTopicName, Nsqd.DefaultTopicName));

                publisher.Publish(Nsqd.DefaultTopicName, "Hello World");

                await subscriber.SendRequestAsync(new Ready(1));

                resetEvent.Wait();
            }

            Assert.NotEqual(0, receivedMessages);
        }

        [Fact]
        public void InvalidRequestThrowsExceptionTest()
        {
            using (var nsqd = Nsqd.StartLocal(9115))
            using (var conn = new NsqdConnection(nsqd.Host, nsqd.Port))
            {
                conn.Connect();                
                Assert.ThrowsAsync<RequestException>(() => conn.SendRequestAsync(new InvalidRequest()));
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
