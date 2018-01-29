using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ZeroNsq.Protocol;

namespace ZeroNsq
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
                    ExecuteHandler(channelName, callback, msg, connectionErrorCallback);
                });
            
                lock (_connectionLock)
                {
                    if (!IsConnected)
                    {
                        Connection.Connect();
                        Connection.SendRequest(new Subscribe(_topicName, channelName));
                        AdviseReady(_options.MaxInFlight);
                    }
                }
            }
            catch (Exception ex)
            {
                if (connectionErrorCallback != null)
                {
                    connectionErrorCallback(new ConnectionErrorContext(Connection, ex));
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

        private void ExecuteHandler(string channelName, Action<IMessageContext> callback, Message msg, Action<ConnectionErrorContext> errorCallback = null)
        {
            bool isSingleThreaded = _options.MaxInFlight == 1;
            IMessageContext context = CreateContext(msg, channelName);

            if (isSingleThreaded)
            {
                ExecuteCallback(context, callback, errorCallback, Connection);
            }
            else
            {
                ExecuteAsynchronously(context, callback, errorCallback);
            }
        }

        private void ExecuteCallback(IMessageContext messageContext, Action<IMessageContext> callback, Action<ConnectionErrorContext> errorCallback, INsqConnection conn)
        {
            try
            {
                callback(messageContext);
            }
            catch (Exception ex)
            {
                if (errorCallback != null)
                {
                    errorCallback(new ConnectionErrorContext(conn, ex));
                }

                bool isKnownException = ex is BaseException;
                if (!isKnownException)
                {
                    // not caused by ZeroNsq. Stop incoming messages from flowing.
                    AdviseReady(0);
                }
            }
        }

        private void ExecuteAsynchronously(IMessageContext context, Action<IMessageContext> callback, Action<ConnectionErrorContext> errorCallback)
        {   
            var callbackTask = Task.Factory.StartNew(
                s => ExecuteCallback(context, callback, errorCallback, Connection),
                TaskCreationOptions.LongRunning,
                _cancellationToken);

            if (!callbackTask.IsCompleted)
            {
                _runningTasks.Add(callbackTask);
            }
        }

        private IMessageContext CreateContext(Message msg, string channelName)
        {
            lock (_contextLock)
            {
                while (true)
                {
                    int activeTaskCount = _runningTasks.Count(t => !t.IsCompleted);

                    if (activeTaskCount <= _options.MaxInFlight) break;

                    Thread.Sleep(TimeSpan.FromSeconds(1));
                }

                var completedTasks = _runningTasks.Where(t => t.IsCompleted).ToList();
                foreach (var ct in completedTasks)
                {
                    ct.Dispose();
                    _runningTasks.Remove(ct);
                }
            }

            return new MessageContext(this, msg, _options, _topicName, channelName);
        }

        #region IDisposable members

        public void Dispose()
        {
            Stop();
        }

        #endregion
    }
}
