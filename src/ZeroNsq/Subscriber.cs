using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Timers;
using ZeroNsq.Internal;
using ZeroNsq.Helpers;

namespace ZeroNsq
{
    /// <summary>
    /// An abstraction of NSQ's consumer connection.
    /// </summary>
    public class Subscriber : ISubscriber
    {
        private static readonly int DefaultHeartbeatIntervalInSeconds = 60;
        private readonly object _startLock = new object();
        private readonly SubscriberOptions _options;        
        private readonly string _topicName;
        private readonly string _channelName;
        private readonly CancellationTokenSource _cancellationTokenSource;
        private readonly ConsumerFactory _consumerFactory;
        private Action<IMessageContext> _onMessageReceivedCallback = ctx => { };
        private Action<ConnectionErrorContext> _onConnectionErrorCallback = ctx => { };
        private System.Timers.Timer _pollingTimer;
        private bool _isRunning;

        private Subscriber(string topicName, string channelName, SubscriberOptions options) : this(topicName, channelName, options, null) { }

        private Subscriber(string topicName, string channelName, SubscriberOptions options, CancellationTokenSource cancellationTokenSource)
        {
            _topicName = topicName;
            _channelName = channelName;
            _options = options;
            _cancellationTokenSource = cancellationTokenSource ?? new CancellationTokenSource();
            _consumerFactory = new ConsumerFactory(options, _cancellationTokenSource.Token);

            var pollingTimeout = TimeSpan.FromSeconds(DefaultHeartbeatIntervalInSeconds);
            
            _pollingTimer = new System.Timers.Timer(pollingTimeout.TotalMilliseconds);
            _pollingTimer.Elapsed += OnPollingTimerElapsed;

            LogProvider.Current.Debug(string.Format("New subscriber instance. Topic={0}; Channel={1}", topicName, channelName));
        }

        /// <summary>
        /// Gets if the subscriber instance is active
        /// </summary>
        public bool IsActive
        {
            get
            {
                bool isWorkerActive = _pollingTimer != null;
                return _isRunning && isWorkerActive && !_cancellationTokenSource.IsCancellationRequested;
            }
        }

        /// <summary>
        /// Creates a new subscriber instance
        /// </summary>
        /// <param name="topicName">The topic name</param>
        /// <param name="channelName">The channel name</param>
        /// <param name="options">The subscriber options</param>
        /// <returns>A subscriber instance</returns>
        public static ISubscriber CreateInstance(string topicName, string channelName, SubscriberOptions options)
        {
            return CreateInstance(topicName, channelName, options, null);
        }

        /// <summary>
        /// Creates a new subscriber instance
        /// </summary>
        /// <param name="topicName">The topic name</param>
        /// <param name="channelName">The channel name</param>
        /// <param name="options">The subscriber options</param>
        /// <param name="cancellationTokenSource">The cancellation token source</param>
        /// <returns>A subscriber instance</returns>
        public static ISubscriber CreateInstance(string topicName, string channelName, SubscriberOptions options, CancellationTokenSource cancellationTokenSource)
        {
            if (options.Nsqd == null || options.Nsqd.Length == 0 || 
                options.Lookupd == null || options.Lookupd.Length == 0)
            {
                throw new ArgumentException("NSQD or LOOKUPD instances are required.");
            }

            return new Subscriber(topicName.EnforceValidNsqName(), channelName.EnforceValidNsqName(), options, cancellationTokenSource ?? new CancellationTokenSource());
        }

        ISubscriber ISubscriber.OnConnectionError(Action<ConnectionErrorContext> callback)
        {
            _onConnectionErrorCallback = callback;
            return this;
        }

        ISubscriber ISubscriber.OnMessageReceived(Action<IMessageContext> callback)
        {
            _onMessageReceivedCallback = callback;
            return this;
        }

        void ISubscriber.Start()
        {
            if (IsActive) return;

            LogProvider.Current.Info(string.Format("Subscriber started. Topic={0}; Channel={1}", _topicName, _channelName));

            lock (_startLock)
            {
                MonitorConnections(true, throwConnectionException: true);
                _isRunning = true;
                _pollingTimer.Start();
            }
        }

        void ISubscriber.Stop()
        {
            lock (_startLock)
            {
                if (_pollingTimer != null)
                {
                    _pollingTimer.Stop();
                    _pollingTimer.Dispose();
                    _pollingTimer = null;
                }

                if (_consumerFactory != null)
                {
                    _consumerFactory.Reset();
                }

                _isRunning = false;
                LogProvider.Current.Info(string.Format("Subscriber stopped. Topic={0}; Channel={1}", _topicName, _channelName));
            }
        }

        private void OnPollingTimerElapsed(object sender, ElapsedEventArgs e)
        {
            var timer = sender as System.Timers.Timer;
            if (timer == null) return;

            LogProvider.Current.Info(string.Format("Subscriber polling timer elapsed. Monitoring connections. Topic={0}; Channel={1}", _topicName, _channelName));

            timer.Stop();

            bool isMonitored = MonitorConnections(IsActive, throwConnectionException: false);

            if (!isMonitored)
            {
                LogProvider.Current.Error(string.Format("Subscriber polling timer terminated. Topic={0}; Channel={1}", _topicName, _channelName));
                return;
            }

            if (_isRunning)
            {
                timer.Start();
            }
        }

        private bool MonitorConnections(bool isRunning, bool throwConnectionException = false)
        {
            if (!isRunning) return false;
            if (_consumerFactory == null) return false;

            IEnumerable<Consumer> consumers = _consumerFactory.GetInstances(_topicName).ToList();
            LogProvider.Current.Info(string.Format("Consumers found={0}. Topic={1}; Channel={2}", consumers.Count(), _topicName, _channelName));

            foreach (Consumer consumer in consumers)
            {
                try
                {
                    bool isConnectionMonitorNeeded = isRunning && !_cancellationTokenSource.Token.IsCancellationRequested;
                    if (!isConnectionMonitorNeeded) return false;

                    if (!consumer.IsConnected)
                    {
                        LogProvider.Current.Debug(string.Format("Starting consumer. Topic={0}; Channel={1}", _topicName, _channelName));
                        consumer.Start(_channelName, _onMessageReceivedCallback, _onConnectionErrorCallback, throwConnectionException);                        
                    }
                }
                catch (BaseException ex)
                {
                    LogProvider.Current.Error(string.Format("Subscriber error occurred. Topic={0}; Channel={1}; Reason=", _topicName, _channelName, ex.Message));

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

        /// <summary>
        /// Releases resources held by the subscriber instance
        /// </summary>
        public void Dispose()
        {
            (this as ISubscriber).Stop();

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
