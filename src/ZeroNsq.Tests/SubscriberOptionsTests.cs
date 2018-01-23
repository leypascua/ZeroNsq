using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using Xunit;

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
    }
}
