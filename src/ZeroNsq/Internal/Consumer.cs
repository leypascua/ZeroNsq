using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ZeroNsq.Protocol;

namespace ZeroNsq.Internal
{
    public class Consumer : IDisposable
    {   
        private readonly SubscriberOptions _options;        
        private readonly CancellationToken _cancellationToken;
        private readonly object _connectionLock = new object();
        private readonly object _contextLock = new object();
        private readonly List<Task> _runningTasks = new List<Task>();
        private readonly string _topicName;        
        private bool _isReady;

        public Consumer(string topicName, INsqConnection connection, SubscriberOptions options, CancellationToken cancellationToken)
        {
            _topicName = topicName;            
            Connection = connection;
            _options = options;
            _cancellationToken = cancellationToken;
        }

        public bool IsConnected
        {
            get
            {
                return Connection != null && Connection.IsConnected;
            }
        }

        public bool IsReady
        {
            get
            {
                return IsConnected && _isReady;
            }
        }

        public INsqConnection Connection { get; private set; }

        public void Start(string channelName, Action<IMessageContext> callback, Action<ConnectionErrorContext> connectionErrorCallback, bool throwConnectionException = false)
        {
            try
            {
                Func<IMessageContext, Task> asyncCallback = ctx => {
                    try
                    {
                        return Task.Run(() => callback(ctx));
                    }
                    catch (AggregateException ex)
                    {
                        throw ex.InnerException;
                    }
                };

                Task.Run(() => StartAsync(channelName, asyncCallback, connectionErrorCallback, throwConnectionException)).Wait();
            }
            catch (AggregateException ex)
            {
                throw ex.InnerException;
            }
        }

        public async void StartAsync(string channelName, Func<IMessageContext, Task> callback, Action<ConnectionErrorContext> connectionErrorCallback, bool throwConnectionException = false)
        {
            if (IsConnected) return;

            try
            {
                Connection.OnMessageReceived(msg => {
                    ExecuteHandler(new HandlerExecutionContext
                    {
                        Connection = Connection,
                        Options = _options,
                        TopicName = _topicName,
                        ChannelName = channelName,
                        MessageReceivedCallbackAsync = callback,
                        Message = msg,
                        ErrorCallback = connectionErrorCallback
                    });
                });

                try
                {
                    await OpenConnectionAsync(Connection, _options, _topicName, channelName);
                }
                catch (AggregateException ex)
                {
                    throw ex.InnerException;
                }
            }
            catch (Exception ex)
            {
                LogProvider.Current.Fatal(string.Format("Consumer Error occurred. {0}", ex.ToString()));

                if (connectionErrorCallback != null)
                {
                    connectionErrorCallback(new ConnectionErrorContext(Connection, ex, channelName));
                }

                if (throwConnectionException && ex is ConnectionException)
                {
                    throw ex;
                }
            }
        }

        public void Stop()
        {
            lock (_connectionLock)
            {
                if (IsConnected)
                {
                    Connection.Close();
                    _isReady = false;
                    LogProvider.Current.Info(string.Format("Stopping consumer instance. Topic={0};", _topicName));
                }
            }
        }

        internal async Task AdviseReadyAsync(int maxInFlight)
        {
            if (Connection.IsConnected)
            {
                await Connection.SendRequestAsync(new Ready(maxInFlight));
                _isReady = maxInFlight > 0;
            }
        }

        private async Task OpenConnectionAsync(INsqConnection connection, SubscriberOptions options, string topicName, string channelName)
        {
            if (!IsConnected)
            {
                LogProvider.Current.Info(string.Format("Connecting consumer. Topic={0}; Channel={1};", _topicName, channelName));
                await connection.ConnectAsync();
                await connection.SendRequestAsync(new Subscribe(topicName, channelName));
                await AdviseReadyAsync(options.MaxInFlight);
                LogProvider.Current.Info(string.Format("Consumer started. Topic={0}; Channel={1}", _topicName, channelName));
            }
        }

        private void ExecuteHandler(HandlerExecutionContext handlerContext)
        {
            IMessageContext msgContext = handlerContext.CreateMessageContext();
            var handlerTask = ExecuteCallbackAsync(msgContext, handlerContext);

            EnqueueHandlerTask(handlerTask, handlerContext);
        }

        private void EnqueueHandlerTask(Task handlerTask, HandlerExecutionContext handlerContext)
        {
            _runningTasks.Add(handlerTask);

            while (MaxAllowableWorkerThreadsActive(_runningTasks, handlerContext.Options.MaxInFlight))
            {
                LogProvider.Current.Debug(string.Format("Max allowable workers (MaxInFlight={0}) exceeded. Waiting for a handler task to complete...", handlerContext.Options.MaxInFlight));
                Task.WaitAny(_runningTasks.ToArray());
            }

            var completedTasks = _runningTasks.Where(t => t.IsCompleted).ToList();
            foreach (var ct in completedTasks)
            {
                ct.Dispose();
                _runningTasks.Remove(ct);
            }
        }

        private async Task ExecuteCallbackAsync(IMessageContext msgContext, HandlerExecutionContext handlerContext)
        {
            try
            {
                LogProvider.Current.Debug("Executing consumer callback");
                await handlerContext.MessageReceivedCallbackAsync(msgContext);
            }
            catch (Exception ex)
            {
                LogProvider.Current.Error("Consumer exception caught: " + ex.ToString());

                if (handlerContext.ErrorCallback != null)
                {
                    handlerContext.ErrorCallback(new ConnectionErrorContext(handlerContext.Connection, ex, msgContext.ChannelName));
                }

                bool isKnownException = ex is BaseException;
                if (!isKnownException)
                {
                    // not caused by ZeroNsq. Stop incoming messages from flowing.
                    LogProvider.Current.Fatal("Unknown error detected. Advising RDY 0 to daemon.");
                    await AdviseReadyAsync(0);
                }
            }
        }

        private static bool MaxAllowableWorkerThreadsActive(IEnumerable<Task> runningTasks, int maxInFlight)
        {
            int activeTaskCount = runningTasks.Count(t => !t.IsCompleted);

            if (activeTaskCount == 0) return false;

            return activeTaskCount >= maxInFlight;
        }

        #region IDisposable members

        public void Dispose()
        {
            Stop();
        }

        #endregion
    }
}
