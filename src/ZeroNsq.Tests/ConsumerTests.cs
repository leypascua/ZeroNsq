using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using Xunit;
using ZeroNsq.Tests.Utils;

namespace ZeroNsq.Tests
{
    public class ConsumerTests
    {
        [Fact]
        public void FinishTest()
        {
            string expectedMessage = "Hello world";
            string receivedMessage = null;
            var cancellationSource = new CancellationTokenSource();
            var opt = new SubscriberOptions();

            Action<IMessageContext> onMessageReceived = msg => {
                receivedMessage = msg.Message.ToUtf8String();
                msg.Finish();
            };

            using (var nsqd = Nsqd.StartLocal())
            using (var conn = new NsqdConnection(nsqd.Host, nsqd.Port, opt))
            using (var consumer = new Consumer(Nsqd.DefaultTopicName, conn, opt, cancellationSource.Token))
            using (var publisher = new Publisher(nsqd.Host, nsqd.Port, opt))
            {
                consumer.Start(Nsqd.DefaultTopicName, onMessageReceived, OnConnectionError);
                publisher.Publish(Nsqd.DefaultTopicName, expectedMessage);

                for (int idx = 0; idx <= 3; idx++)
                {
                    if (receivedMessage == null)
                    {
                        Thread.Sleep(TimeSpan.FromSeconds(2));
                    }
                }
            }

            Assert.Equal(expectedMessage, receivedMessage);
        }

        private void OnConnectionError(ConnectionErrorContext obj)
        {
            
        }
    }
}
