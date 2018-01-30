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
                        MessageReceivedCallback = callback,
                        Message = msg,
                        ErrorCallback = connectionErrorCallback
                    });
                });
            
                lock (_connectionLock)
                {
                    if (!IsConnected)
                    {
                        LogProvider.Current.Info(string.Format("Connecting consumer. Topic={0}; Channel={1};", _topicName, channelName));
                        Connection.Connect();
                        Connection.SendRequest(new Subscribe(_topicName, channelName));
                        AdviseReady(_options.MaxInFlight);
                        LogProvider.Current.Info(string.Format("Consumer started. Topic={0}; Channel={1}", _topicName, channelName));
                    }
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

        internal void AdviseReady(int maxInFlight)
        {
            if (Connection.IsConnected)
            {
                Connection.SendRequest(new Ready(maxInFlight));
                _isReady = maxInFlight > 0;
            }
        }

        private void ExecuteHandler(HandlerExecutionContext handlerContext)
        {
            IMessageContext msgContext = handlerContext.CreateMessageContext();

            var handlerTask = Task.Factory.StartNew(
                () => ExecuteCallback(msgContext, handlerContext),
                _cancellationToken,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Current
            );

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

        private void ExecuteCallback(IMessageContext msgContext, HandlerExecutionContext handlerContext)
        {
            try
            {
                LogProvider.Current.Debug("Executing consumer callback");
                handlerContext.MessageReceivedCallback(msgContext);
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
                    AdviseReady(0);
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
