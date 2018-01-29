using FakeItEasy;
using System;
using System.Linq;
using System.Net;
using System.Threading;
using Xunit;
using ZeroNsq.Internal;
using ZeroNsq.Lookup;
using ZeroNsq.Tests.Utils;

namespace ZeroNsq.Tests
{
    public class ConsumerFactoryTests
    {
        [Fact]
        public void GetSingleNsqdTest()
        {
            string connectionString = "nsqd=tcp://127.0.0.1:4150;";
            var options = SubscriberOptions.Parse(connectionString);
            var cancellationTokenSource = new CancellationTokenSource();

            var factory = new ConsumerFactory(options, cancellationTokenSource.Token);

            var results = factory.GetInstances(Nsqd.DefaultTopicName);

            Assert.NotNull(results.Single());
        }

        [Fact]
        public void GetMultipleNsqdTest()
        {
            string hostName = Dns.GetHostName();
            string connectionString = string.Format("nsqd=tcp://127.0.0.1:4150;nsqd=tcp://{0}:4150;", hostName);
            var options = SubscriberOptions.Parse(connectionString);
            var cancellationTokenSource = new CancellationTokenSource();

            var factory = new ConsumerFactory(options, cancellationTokenSource.Token);

            var results = factory.GetInstances(Nsqd.DefaultTopicName);

            Assert.Equal(2, results.Count());
        }

        [Fact]
        public void NsqdListChangedTest()
        {   
            string connectionString = "lookupd=http://127.0.0.1:4160;";
            var options = SubscriberOptions.Parse(connectionString);
            var cancellationTokenSource = new CancellationTokenSource();
            
            INsqLookupService mockLookupd = A.Fake<INsqLookupService>();
            var factory = new ConsumerFactory(options, cancellationTokenSource.Token, mockLookupd);

            var initialEndpoints = new ProducerEndpointData[]
            {
                new ProducerEndpointData { hostname = "foo", tcp_port = 4150 },
                new ProducerEndpointData { hostname = "bar", tcp_port = 4150 },
                new ProducerEndpointData { hostname = "mama", tcp_port = 4150 },
                new ProducerEndpointData { hostname = "papa", tcp_port = 4150 }
            };

            A.CallTo(() => mockLookupd.GetProducersAsync(null, null))
             .WithAnyArguments()
             .Returns(initialEndpoints);

            var initialResults = factory.GetInstances(Nsqd.DefaultTopicName);
            Assert.Equal(initialEndpoints.Length, initialResults.Count());

            ProducerEndpointData[] newEndpoints = new ProducerEndpointData[2];
            Array.Copy(initialEndpoints, newEndpoints, 2);
            A.CallTo(() => mockLookupd.GetProducersAsync(null, null))
             .WithAnyArguments()
             .Returns(newEndpoints);

            var finalResults = factory.GetInstances(Nsqd.DefaultTopicName).ToList();

            Assert.Equal(2, finalResults.Count);
            finalResults.All(x => initialResults.Contains(x));
        }
    }
}
