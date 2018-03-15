using System;
using System.Collections.Generic;
using System.Text;
using Xunit;
using ZeroNsq.Helpers;

namespace ZeroNsq.Tests
{
    public class WaitTests
    {
        [Fact]
        public void WaitWithoutCallbackTest()
        {
            DateTime start = DateTime.UtcNow;                        
            TimeSpan threeSeconds = TimeSpan.FromSeconds(3);            

            Wait.For(threeSeconds)
                .Start();

            DateTime end = DateTime.UtcNow;

            Assert.True(end > start);
        }

        [Fact]
        public void WaitWithCallbackTest()
        {
            bool isInvoked = false;

            Wait.For(TimeSpan.FromSeconds(3))
                .Then(() => isInvoked = true)
                .Start();

            Assert.True(isInvoked);
        }
    }
}
