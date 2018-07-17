using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace ZeroNsq.Tests
{
    public class SynchronousExtensionsTests
    {
        static readonly IPublisher publisher = new FakePublisher();
        //static readonly ISubscriber subscriber = FakeSubscriber();
                
        [Fact]
        public void PublishByteArrayTest()
        {
            Assert.Throws<NotSupportedException>(() => 
                publisher.Publish("topic", BitConverter.GetBytes(69)));
        }

        [Fact]
        public void PublishUtf8StringTest()
        {
            Assert.Throws<NotSupportedException>(() =>
                publisher.Publish("topic", "utf8string"));
        }

        //[Fact]
        public void SubscriberStopTest()
        {
            
        }

        class FakePublisher : IPublisher
        {
            public void Dispose()
            {
                throw new NotImplementedException();
            }

            public async Task PublishAsync(string topic, byte[] message)
            {
                await Task.Delay(10);
                throw new NotSupportedException();
            }
        }
    }
}
