using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace ZeroNsq.Tests
{
    public class AdhocTests
    {
        [Fact]
        public void SuccessfulTest()
        {
            var successfulTask = Task.Factory.StartNew(() => { });
            Thread.Sleep(100);
            Assert.True(successfulTask.IsCompleted);            
        }

        [Fact]
        public void ErrorTaskTest()
        {
            var errorTask = Task.Factory.StartNew(() => throw new InvalidOperationException());
            Thread.Sleep(100);
            Assert.True(errorTask.IsCompleted && errorTask.IsFaulted);
        }

        [Fact]
        public void CancelledTaskTest()
        {
            var cancellationSource = new CancellationTokenSource();

            cancellationSource.Cancel();
            var cancelledTask = Task.Run(() =>
            {
                while (!cancellationSource.Token.IsCancellationRequested)
                {
                    Thread.Sleep(100);                    
                }

            }, cancellationSource.Token);

            Thread.Sleep(1000);

            Assert.True(cancelledTask.IsCompleted && cancelledTask.IsCanceled);
        }
    }
}
