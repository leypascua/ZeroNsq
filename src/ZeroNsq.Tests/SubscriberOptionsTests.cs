using System.Linq;
using System.Net;
using Xunit;
using ZeroNsq.Internal;

namespace ZeroNsq.Tests
{
    public class SubscriberOptionsTests
    {
        [Fact]
        public void ParseWithAllParametersTest()
        {
            var options = SubscriberOptions.Parse("nsqd=tcp://127.0.0.1:4150;nsqd=tcp://127.0.0.2:4150;maxInFlight=3;maxRetryAttempts=4;");

            Assert.Equal(2, options.Nsqd.Length);
            Assert.Equal(3, options.MaxInFlight);
            Assert.Equal(4, options.MaxRetryAttempts);
        }

        [Fact]
        public void DefaultNsqdPortTest()
        {
            var options = SubscriberOptions.Parse("nsqd=127.0.0.1:4150;");
            Assert.Equal(ConnectionOptions.DefaultTcpPort, options.Nsqd.First().Port);
        }

        [Fact]
        public void LookupdTest()
        {
            string connStr = string.Format("lookupd=http://127.0.0.1:4160;lookupd=http://{0}:4160;", Dns.GetHostName());
            var options = SubscriberOptions.Parse(connStr);

            Assert.NotNull(options.Lookupd);
            Assert.Equal(2, options.Lookupd.Length);
        }
    }
}
