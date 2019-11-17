using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Xunit;
using ZeroNsq.Lookup;
using ZeroNsq.Tests.Utils;

namespace ZeroNsq.Tests
{
    public class NsqLookupDaemonServiceTests
    {
        string testMessage = "hello world";
        int lookupdTcpPort = 4990;
        string topicName = "reticle-commands";        

        [Fact]
        public async void LookupByTopic()
        {
            using (var lookupd = new Nsqlookupd(tcp: lookupdTcpPort, http: 4991))
            using (var nsqd = Nsqd.StartLocal(4992, lookupd: lookupdTcpPort))
            {
                var publisher = Publisher.CreateInstance($"nsqd=http://127.0.0.1:{nsqd.HttpPort}");
                await publisher.PublishAsync(topicName, testMessage);

                var lookupdClient = new NsqLookupDaemonService();

                Uri lookupdUri = new Uri($"http://localhost:{lookupd.HttpPort}");
                IEnumerable<ProducerEndpointData> endpoints = await lookupdClient.GetProducersAsync(lookupdUri, topicName);

                Assert.True(endpoints.Any());
            }
        }

        [Fact]
        public async void SubscribeViaLookupd()
        {
            using (var lookupd = new Nsqlookupd(tcp: lookupdTcpPort, http: 4992))
            using (var nsqd = Nsqd.StartLocal(4993, lookupd: lookupdTcpPort))
            {
                var subscriberOption = SubscriberOptions.Parse($"lookupd=http://127.0.0.1:{lookupd.HttpPort}/");

                using (var publisher = Publisher.CreateInstance($"nsqd=http://127.0.0.1:{nsqd.HttpPort}"))
                using (var subscriber = Subscriber.CreateInstance(topicName, $"{topicName}-testchannel", subscriberOption))
                {
                    var resetEvent = new ManualResetEventSlim();
                    string incomingMessage = null;

                    subscriber.OnMessageReceived(msgContext =>
                    {
                        incomingMessage = msgContext.Message.ToUtf8String();
                        resetEvent.Set();
                    });

                    subscriber.Start();

                    await publisher.PublishAsync(topicName, testMessage);

                    resetEvent.Wait(TimeSpan.FromSeconds(10));

                    Assert.Equal(testMessage, incomingMessage);
                }
            }
        }
    }
}
