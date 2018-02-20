using System;
using System.Collections.Generic;
using System.Diagnostics;
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

                Task.Run(async () => await StartAsync(channelName, asyncCallback, connectionErrorCallback, throwConnectionException)).Wait();
            }
            catch (AggregateException ex)
            {
                throw ex.InnerException;
            }
        }

        public async Task StartAsync(string channelName, Func<IMessageContext, Task> callback, Action<ConnectionErrorContext> connectionErrorCallback, bool throwConnectionException = false)
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
                    throw;
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

            _runningTasks.Add(Task.Run(async () => await ExecuteCallbackAsync(msgContext, handlerContext)));
                        
            foreach (Task ct in _runningTasks.Where(t => t.IsCompleted).ToList())
            {
                try
                {
                    ct.Dispose();
                }
                catch { }
                
                _runningTasks.Remove(ct);
            }
        }

        private async Task ExecuteCallbackAsync(IMessageContext msgContext, HandlerExecutionContext handlerContext)
        {
            int threadId = Thread.CurrentThread.ManagedThreadId;

            try
            {   
                if (MaxAllowableWorkerThreadsActive(_runningTasks, handlerContext.Options.MaxInFlight))
                {
                    LogProvider.Current.Debug(string.Format("[T#{0}] Max allowable workers (MaxInFlight={1}) exceeded. Waiting for a handler task to complete...", 
                        threadId, handlerContext.Options.MaxInFlight));

                    Task.WaitAny(_runningTasks.ToArray());
                }

                LogProvider.Current.Debug(string.Format("[T#{0}] Executing consumer callback", threadId));
                var stopWatch = Stopwatch.StartNew();
                await handlerContext.MessageReceivedCallbackAsync(msgContext);
                LogProvider.Current.Debug(string.Format("[T#{0}] Consumer callback execution completed after {1} seconds.", threadId, stopWatch.Elapsed.TotalSeconds));
                stopWatch.Stop();
            }
            catch (Exception ex)
            {
                LogProvider.Current.Error(string.Format("[T#{0}] Consumer exception caught: {1}", threadId, ex.ToString()));

                if (handlerContext.ErrorCallback != null)
                {
                    handlerContext.ErrorCallback(new ConnectionErrorContext(handlerContext.Connection, ex, msgContext.ChannelName));
                }

                bool isKnownException = ex is BaseException;
                if (!isKnownException)
                {
                    // not caused by ZeroNsq. Stop incoming messages from flowing.
                    LogProvider.Current.Fatal(string.Format("[T#{0}] Unknown error detected. Advising RDY 0 to daemon.", threadId));
                    await AdviseReadyAsync(0);
                }
            }
        }

        private static bool MaxAllowableWorkerThreadsActive(IEnumerable<Task> runningTasks, int maxInFlight)
        {   
            int activeTaskCount = runningTasks.Count(t => !t.IsCompleted);

            if (activeTaskCount == 0) return false;

            return activeTaskCount > maxInFlight;
        }

        #region IDisposable members

        public void Dispose()
        {
            Stop();
        }

        #endregion
    }
}
