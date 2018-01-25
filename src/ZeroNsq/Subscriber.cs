using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using System.Linq;
using System.Timers;
using System.Collections.Concurrent;
using ZeroNsq.Protocol;
using System.Threading;

namespace ZeroNsq
{
    public class Subscriber : IDisposable
    {
        private static readonly int DefaultHeartbeatIntervalInSeconds = 60;
        private readonly TimeSpan _defaultHeartbeatInterval;
        private readonly object _startLock = new object();
        private readonly SubscriberOptions _options;        
        private readonly string _topicName;
        private readonly string _channelName;
        private readonly CancellationTokenSource _cancellationTokenSource;
        private readonly ConsumerFactory _consumerFactory;
        private Action<IMessageContext> _onMessageReceivedCallback = ctx => { };
        private Action<ConnectionErrorContext> _onConnectionErrorCallback = ctx => { };
        private Task _workerTask;        
        private bool _isRunning;

        public Subscriber(string topicName, string channelName, SubscriberOptions options) : this(topicName, channelName, options, null) { }

        public Subscriber(string topicName, string channelName, SubscriberOptions options, CancellationTokenSource cancellationTokenSource)
        {
            _topicName = topicName;
            _channelName = channelName;
            _options = options;
            _cancellationTokenSource = cancellationTokenSource ?? new CancellationTokenSource();
            _consumerFactory = new ConsumerFactory(options, _cancellationTokenSource.Token);
            _defaultHeartbeatInterval = TimeSpan.FromSeconds(options.HeartbeatIntervalInSeconds.GetValueOrDefault(DefaultHeartbeatIntervalInSeconds) * 2);            
        }

        public bool IsActive
        {
            get
            {
                bool isWorkerActive = _workerTask != null &&
                    !_workerTask.IsCompleted;

                return _isRunning && isWorkerActive && !_cancellationTokenSource.IsCancellationRequested;
            }
        }

        public Subscriber OnConnectionError(Action<ConnectionErrorContext> callback)
        {
            _onConnectionErrorCallback = callback;
            return this;
        }

        public Subscriber OnMessageReceived(Action<IMessageContext> callback)
        {
            _onMessageReceivedCallback = callback;
            return this;
        }

        public void Start()
        {
            if (IsActive) return;

            lock (_startLock)
            {
                MonitorConnections(true, throwConnectionException: true);

                _workerTask = Task.Factory.StartNew(
                    s => WorkerLoop(),
                    TaskCreationOptions.LongRunning,
                    _cancellationTokenSource.Token);

                _isRunning = true;
            }
        }

        public void Stop()
        {
            if (_consumerFactory != null)
            {
                _consumerFactory.Reset();
            }

            _isRunning = false;

            if (_cancellationTokenSource != null)
            {
                _cancellationTokenSource.CancelAfter(TimeSpan.FromSeconds(2));                
            }

            if (_workerTask != null)
            {
                _workerTask.Wait();
                _workerTask.Dispose();
                _workerTask = null;
            }
        }

        private void WorkerLoop()
        {
            while (IsActive)
            {
                bool isMonitored = MonitorConnections(IsActive, throwConnectionException: false);

                if (!isMonitored)
                {
                    break;
                }

                Thread.Sleep(_defaultHeartbeatInterval);
            }
        }

        private bool MonitorConnections(bool isRunning, bool throwConnectionException = false)
        {
            if (!isRunning) return false;
            if (_consumerFactory == null) return false;

            IEnumerable<Consumer> consumers = _consumerFactory.GetInstances(_topicName);
            
            foreach (Consumer consumer in consumers)
            {
                try
                {
                    bool isConnectionMonitorNeeded = isRunning && !_cancellationTokenSource.Token.IsCancellationRequested;
                    if (!isConnectionMonitorNeeded) return false;

                    if (!consumer.IsConnected)
                    {
                        consumer.Start(_channelName, _onMessageReceivedCallback, _onConnectionErrorCallback, throwConnectionException);
                    }
                }
                catch (BaseException ex)
                {
                    if (_onConnectionErrorCallback != null)
                    {
                        _onConnectionErrorCallback(new ConnectionErrorContext(consumer.Connection, ex));
                    }

                    if (throwConnectionException)
                    {
                        throw;
                    }
                }
            }

            return true;
        }

        #region IDisposable members

        public void Dispose()
        {
            Stop();

            if (_cancellationTokenSource != null)
            {
                _cancellationTokenSource.Dispose();
            }

            if (_consumerFactory != null)
            {
                _consumerFactory.Dispose();
            }
        }

        #endregion
    }
}
