using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using Xunit;
using ZeroNsq.Tests.Utils;

namespace ZeroNsq.Tests
{
    public class ConsumerTests
    {
        [Fact]
        public void MessageTimeoutTest()
        {
            string topicName = "MessageTimeout." + Guid.NewGuid().ToString();
            var cancellationSource = new CancellationTokenSource();
            var resetEvent = new ManualResetEventSlim();
            var opt = new SubscriberOptions
            {
                MessageTimeout = 5
            };

            IMessageContext messageContext = null;
            string expectedMessageId = null;

            Action<IMessageContext> onMessageReceived = msg => {

                if (string.IsNullOrEmpty(expectedMessageId))
                {
                    expectedMessageId = msg.Message.IdString;
                }

                // simulate a long process.
                Thread.Sleep(TimeSpan.FromSeconds(opt.MessageTimeout.Value + 2));                
                msg.Finish();

                if (msg.Message.Attempts > 1)
                {
                    messageContext = msg;
                    resetEvent.Set();
                    msg.Finish();
                }
            };

            using (var nsqd = Nsqd.StartLocal(8111))
            using (var conn = new NsqdConnection(nsqd.Host, nsqd.Port, opt))
            using (var consumer = new Consumer(topicName, conn, opt, cancellationSource.Token))
            using (var publisher = new Publisher(nsqd.Host, nsqd.Port, opt))
            {
                consumer.Start(topicName, onMessageReceived, OnConnectionError);
                publisher.Publish(topicName, "foo");

                resetEvent.Wait();
            }

            Assert.NotNull(messageContext);
            Assert.Equal(expectedMessageId, messageContext.Message.IdString);
        }

        [Fact]
        public void SubscribeAndFinishTest()
        {
            string topicName = "SubscribeAndFinish." + Guid.NewGuid().ToString();
            string expectedMessage = Guid.NewGuid().ToString();
            var cancellationSource = new CancellationTokenSource();
            var opt = new SubscriberOptions();
            IMessageContext messageContext = null;
            var resetEvent = new ManualResetEventSlim();

            Action<IMessageContext> onMessageReceived = msg => {
                messageContext = msg;
                resetEvent.Set();
                msg.Finish();
            };

            using (var nsqd = Nsqd.StartLocal(8112))
            using (var conn = new NsqdConnection(nsqd.Host, nsqd.Port, opt))
            using (var consumer = new Consumer(topicName, conn, opt, cancellationSource.Token))
            using (var publisher = new Publisher(nsqd.Host, nsqd.Port, opt))
            {
                consumer.Start(topicName, onMessageReceived, OnConnectionError);
                publisher.Publish(topicName, expectedMessage);

                resetEvent.Wait();
            }

            Assert.NotNull(messageContext);
            Assert.Equal(expectedMessage, messageContext.Message.ToUtf8String());
        }

        [Fact]
        public void RequeueAttemptsExceededTest()
        {
            string topicName = "RequeueAttemptsExceeded." + Guid.NewGuid().ToString();
            string expectedMessage = Guid.NewGuid().ToString();
            var cancellationSource = new CancellationTokenSource();
            var opt = new SubscriberOptions { MaxRetryAttempts = 5, MaxInFlight = 1 };
            var resetEvent = new ManualResetEventSlim();
            IMessageContext messageContext = null;
            string expectedMessageId = null;
            int actualRequeueCount = 0;

            Action<IMessageContext> onMessageReceived = msg => {
                try
                {
                    if (string.IsNullOrEmpty(expectedMessageId))
                    {
                        expectedMessageId = msg.Message.IdString;
                    }

                    msg.Requeue();
                    actualRequeueCount += 1;
                }
                catch (MessageRequeueException)
                {
                    messageContext = msg;
                    resetEvent.Set();
                    msg.Finish();
                }
            };

            using (var nsqd = Nsqd.StartLocal(8113))
            using (var conn = new NsqdConnection(nsqd.Host, nsqd.Port, opt))
            using (var consumer = new Consumer(topicName, conn, opt, cancellationSource.Token))
            using (var publisher = new Publisher(nsqd.Host, nsqd.Port, opt))
            {
                consumer.Start(topicName, onMessageReceived, OnConnectionError);
                publisher.Publish(topicName, expectedMessage);

                resetEvent.Wait();
            }

            Assert.Equal(expectedMessageId, messageContext.Message.IdString);
            Assert.Equal(opt.MaxRetryAttempts, actualRequeueCount);
        }

        [Fact]
        public void InvokeErrorCallbackTest()
        {
            string expectedErrorMessage = "foo: " + Guid.NewGuid().ToString();
            string topicName = "InvokeErrorCallback." + Guid.NewGuid().ToString();
            string expectedMessage = Guid.NewGuid().ToString();
            var cancellationSource = new CancellationTokenSource();
            var opt = new SubscriberOptions();
            ConnectionErrorContext errorContext = null;
            ManualResetEventSlim resetEvent = new ManualResetEventSlim();

            Action<IMessageContext> onMessageReceived = ctx =>
            {
                throw new InvalidOperationException(ctx.Message.ToUtf8String());
            };

            Action<ConnectionErrorContext> onConnectionError = ctx =>
            {
                errorContext = ctx;
                resetEvent.Set();
            };

            using (var nsqd = Nsqd.StartLocal(8114))
            using (var conn = new NsqdConnection(nsqd.Host, nsqd.Port, opt))
            using (var consumer = new Consumer(topicName, conn, opt, cancellationSource.Token))
            using (var publisher = new Publisher(nsqd.Host, nsqd.Port, opt))
            {
                consumer.Start(topicName, onMessageReceived, onConnectionError);
                publisher.Publish(topicName, expectedErrorMessage);

                resetEvent.Wait(TimeSpan.FromSeconds(5));
            }

            Assert.NotNull(errorContext);
            Assert.Equal(expectedErrorMessage, errorContext.Error.Message);
        }

        private void OnConnectionError(ConnectionErrorContext ctx)
        {
            Trace.WriteLine(ctx.Error.ToString());
        }
    }
}
