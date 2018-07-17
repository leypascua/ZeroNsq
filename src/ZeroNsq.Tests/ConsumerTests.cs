using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using Xunit;
using ZeroNsq.Internal;
using ZeroNsq.Tests.Utils;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using ZeroNsq.Helpers;

namespace ZeroNsq.Tests
{
    public class ConsumerTests
    {
        [Fact]
        public void TouchMessageTest()
        {
            string topicName = "TouchMessage." + Guid.NewGuid().ToString();
            var cancellationSource = new CancellationTokenSource();
            var resetEvent = new ManualResetEventSlim();
            var opt = new SubscriberOptions
            {
                MessageTimeout = 3
            };
            int incomingMessageCount = 0;

            Action<IMessageContext> onMessageReceived = ctx => 
            {
                Wait.For(TimeSpan.FromSeconds(opt.MessageTimeout.Value - 1)).Start();                
                ctx.Touch();
                Wait.For(TimeSpan.FromSeconds(opt.MessageTimeout.Value)).Start();
                ctx.Finish();                

                incomingMessageCount += 1;
                resetEvent.Set();
            };

            using (var nsqd = Nsqd.StartLocal(8110))
            using (var conn = new NsqdConnection(nsqd.Host, nsqd.Port, opt))
            using (var consumer = new Consumer(topicName, conn, opt, cancellationSource.Token))
            using (var publisher = new TcpPublisher(nsqd.Host, nsqd.Port, opt))
            {
                consumer.Start(topicName, onMessageReceived, OnConnectionError);

                publisher.Publish(topicName, "Hello world");

                resetEvent.Wait(TimeSpan.FromSeconds(opt.MessageTimeout.Value * 2));
            }

            Assert.Equal(1, incomingMessageCount);
        }

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
                Wait.For(TimeSpan.FromSeconds(opt.MessageTimeout.Value + 2))
                    .Start();
                
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
            using (var publisher = new TcpPublisher(nsqd.Host, nsqd.Port, opt))
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
            using (var publisher = new TcpPublisher(nsqd.Host, nsqd.Port, opt))
            {
                consumer.Start(topicName, onMessageReceived, OnConnectionError);
                publisher.Publish(topicName, expectedMessage);

                resetEvent.Wait();
            }

            Assert.NotNull(messageContext);
            Assert.Equal(expectedMessage, messageContext.Message.ToUtf8String());
        }

        [Fact]
        public async Task AsyncMessageCallbackTest()
        {
            string topicName = "SubscribeAndFinish." + Guid.NewGuid().ToString();
            string expectedMessage = Guid.NewGuid().ToString();
            var cancellationSource = new CancellationTokenSource();
            var opt = new SubscriberOptions();
            IMessageContext messageContext = null;
            var resetEvent = new ManualResetEventSlim();

            Func<IMessageContext, Task> onMessageReceivedAsync = async msg => {
                await Task.Delay(TimeSpan.FromSeconds(2));
                messageContext = msg;
                resetEvent.Set();
                await msg.FinishAsync();
            };

            using (var nsqd = Nsqd.StartLocal(8117))
            using (var conn = new NsqdConnection(nsqd.Host, nsqd.Port, opt))
            using (var consumer = new Consumer(topicName, conn, opt, cancellationSource.Token))
            using (var publisher = new TcpPublisher(nsqd.Host, nsqd.Port, opt))
            {
                await consumer.StartAsync(topicName, onMessageReceivedAsync, OnConnectionError);
                await publisher.PublishAsync(topicName, expectedMessage);

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
            using (var publisher = new TcpPublisher(nsqd.Host, nsqd.Port, opt))
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
            using (var publisher = new TcpPublisher(nsqd.Host, nsqd.Port, opt))
            {
                consumer.Start(topicName, onMessageReceived, onConnectionError);
                publisher.Publish(topicName, expectedErrorMessage);

                resetEvent.Wait(TimeSpan.FromSeconds(5));
            }

            Assert.NotNull(errorContext);
            Assert.Equal(expectedErrorMessage, errorContext.Error.Message);
        }

        [Fact]
        public void PublishSubscribeJsonTest()
        {
            string topicName = "PublishSubscribeJson." + Guid.NewGuid().ToString();
            var cancellationSource = new CancellationTokenSource();
            var resetEvent = new ManualResetEventSlim();
            var opt = new SubscriberOptions();
            var expected = new Foo
            {
                Id = Guid.NewGuid(),
                Data = new HandledMessageData
                {
                    Start = DateTime.UtcNow,
                    End = DateTime.UtcNow
                }
            };

            Foo result = null;

            Action<IMessageContext> onMessageReceived = ctx =>
            {
                string receivedJson = ctx.Message.ToUtf8String();
                result = JsonConvert.DeserializeObject<Foo>(receivedJson);
                resetEvent.Set();
            };

            using (var nsqd = Nsqd.StartLocal(8116))
            using (var conn = new NsqdConnection(nsqd.Host, nsqd.Port, opt))
            using (var consumer = new Consumer(topicName, conn, opt, cancellationSource.Token))
            using (var publisher = Publisher.CreateInstance(host: nsqd.Host, port: nsqd.HttpPort, scheme: "http"))
            {
                consumer.Start(topicName, onMessageReceived, OnConnectionError);

                string json = JsonConvert.SerializeObject(expected);
                publisher.Publish(topicName, json);
                resetEvent.Wait();

                Assert.Equal(expected.Id, result.Id);
                Assert.True(result.Data is JObject);
            }
        }

        [Fact]
        public void ParallelMessageHandlingTest()
        {
            string topicName = "ParallelMessageHandling." + Guid.NewGuid().ToString();
            var cancellationSource = new CancellationTokenSource();
            var opt = new SubscriberOptions { MaxInFlight = 3 };
            double sleepTime = 3;
            int expectedMessageCount = opt.MaxInFlight + 2;
            double expectedSleepTime = (expectedMessageCount - opt.MaxInFlight) * sleepTime + sleepTime;
            var resetEvent = new ManualResetEventSlim();

            var receivedMessages = new ConcurrentBag<HandledMessageData>();

            Action<IMessageContext> onMessageReceived = ctx =>
            {
                var receivedData = ctx.Message.Deserialize<HandledMessageData>();
                receivedData.ThreadId = Thread.CurrentThread.ManagedThreadId.ToString();
                receivedData.Start = DateTime.UtcNow;

                Wait.For(TimeSpan.FromSeconds(sleepTime))
                    .Start();
                
                receivedData.End = DateTime.UtcNow;
                receivedMessages.Add(receivedData);

                LogProvider.Current.Info("Finishing index " + receivedData.Index);
                ctx.Finish();

                if (receivedMessages.Count == expectedMessageCount)
                {
                    resetEvent.Set();
                }
            };

            int actualPublishedCount = 0;

            using (var nsqd = Nsqd.StartLocal(8115))
            using (var conn = new NsqdConnection(nsqd.Host, nsqd.Port, opt))
            using (var consumer = new Consumer(topicName, conn, opt, cancellationSource.Token))
            using (var publisher = Publisher.CreateInstance(host: nsqd.Host, port: nsqd.HttpPort, scheme: "http"))
            {
                consumer.Start(topicName, onMessageReceived, OnConnectionError);

                Parallel.For(0, expectedMessageCount, idx =>
                {
                    var data = new HandledMessageData { Index = idx, Published = DateTime.UtcNow };
                    publisher.PublishJson(topicName, data);
                    actualPublishedCount += 1;
                });

                resetEvent.Wait();
            }

            int uniqueThreadCount = receivedMessages
                .Select(x => x.ThreadId)
                .Distinct()
                .Count();

            Assert.Equal(expectedMessageCount, receivedMessages.Count);
            Assert.Equal(actualPublishedCount, receivedMessages.Count);
            Assert.True(uniqueThreadCount >= opt.MaxInFlight);
        }

        [Fact]
        public void RespondNOPOnLongRunningMessageHandlerTest()
        {
            LogProvider.Configure()
                .UseTrace();
                
            string topicName = "LongRunningMessage." + Guid.NewGuid().ToString();
            var cancellationSource = new CancellationTokenSource();
            var opt = new SubscriberOptions { MaxInFlight = 1, HeartbeatIntervalInSeconds = 2 };
            var resetEvent = new ManualResetEventSlim();
            bool isSuccessful = false;
            Exception caughtException = null;
            Action<IMessageContext> onMessageReceived = msg =>
            {
                try
                {
                    string text = msg.Message.ToUtf8String();
                    LogProvider.Current.Debug("MESSAGE HANDLER: Waiting... " + text);
                    SleepFor(TimeSpan.FromSeconds(20)).Wait();
                    LogProvider.Current.Debug("MESSAGE HANDLER: Done. Invoking FIN");
                    msg.Finish();
                    LogProvider.Current.Debug("MESSAGE HANDLER: Done. FIN Successful.");
                    isSuccessful = true;
                }
                catch(Exception ex)
                {
                    caughtException = ex;
                    isSuccessful = false;
                }

                resetEvent.Set();
            };

            using (var nsqd = Nsqd.StartLocal(8118))
            using (var conn = new NsqdConnection(nsqd.Host, nsqd.Port, opt))
            using (var consumer = new Consumer(topicName, conn, opt, cancellationSource.Token))
            using (var publisher = Publisher.CreateInstance(host: nsqd.Host, port: nsqd.HttpPort, scheme: "http"))
            {   
                consumer.Start(topicName, onMessageReceived, err => {
                    caughtException = err.Error;
                    isSuccessful = false;
                });

                SleepFor(TimeSpan.FromSeconds(opt.HeartbeatIntervalInSeconds.Value * 3)).Wait();
                publisher.Publish(topicName, "WAIT");
                resetEvent.Wait();
                publisher.Publish(topicName, "ANOTHER");
                SleepFor(TimeSpan.FromSeconds(opt.HeartbeatIntervalInSeconds.Value)).Wait();

                Assert.True(isSuccessful);
                Assert.Null(caughtException);
            }
        }

        private static async Task SleepFor(TimeSpan ts)
        {
            await Task.Run(() => Wait.For(ts).Start());
        }

        private void OnConnectionError(ConnectionErrorContext ctx)
        {
            Trace.WriteLine(ctx.Error.ToString());
        }

        class Foo
        {
            public Guid Id { get; set; }
            public object Data { get; set; }
        }

        class HandledMessageData
        {
            public DateTime Published { get; set; }
            public DateTime? Start { get; set; }
            public DateTime? End { get; set; }
            public string ThreadId { get; set; }
            public int Index { get; set; }
        }
    }
}
