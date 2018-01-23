using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using System.Linq;
using System.Timers;
using System.Collections.Concurrent;
using ZeroNsq.Protocol;

namespace ZeroNsq
{
    public class Subscriber : IDisposable
    {
        private static readonly TimeSpan DefaultHeartbeatInterval = TimeSpan.FromMinutes(1);
        private readonly object _monitorConnectionLock = new object();
        private readonly SubscriberOptions _options;
        private readonly NsqConnectionFactory _connectionFactory;
        private readonly ConcurrentDictionary<string, INsqConnection> _connections = new ConcurrentDictionary<string, INsqConnection>();
        private readonly string _topicName;
        private readonly string _channelName;
        private Action<IMessageContext> _onMessageReceivedCallback = ctx => { };
        private Action<ConnectionErrorContext> _onConnectionErrorCallback = ctx => { };
        private Timer _workerTimer = new Timer(DefaultHeartbeatInterval.TotalMilliseconds);

        public Subscriber(string topicName, string channelName, SubscriberOptions options)
        {
            _topicName = topicName;
            _channelName = channelName;
            _options = options;
            _connectionFactory = new NsqConnectionFactory(options);
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
            MonitorConnections().Wait();
            _workerTimer.Elapsed += OnWorkerTimerElapsed;
            _workerTimer.Start();
        }

        private void OnWorkerTimerElapsed(object sender, ElapsedEventArgs e)
        {   
            lock (_monitorConnectionLock)
            {
                _workerTimer.Stop();
                MonitorConnections().Wait();
                _workerTimer.Start();
            }
        }

        private async Task MonitorConnections()
        {
            var connections = await LocateNsqdInstances(_topicName, _connectionFactory, _connections);
            
            foreach (INsqConnection conn in connections)
            {
                LaunchConsumer(conn);
            }
        }

        private void LaunchConsumer(INsqConnection conn)
        {
            if (conn.IsConnected) return;

            if (_onMessageReceivedCallback != null)
            {
                //conn.OnMessageReceived(msg => {
                //    try
                //    {
                //        var ctx = new MessageContext(conn, msg, _options);
                //        _onMessageReceivedCallback();
                //    }
                //    catch (Exception ex)
                //    {
                //        if (_onConnectionErrorCallback != null)
                //        {
                //            _onConnectionErrorCallback(new ConnectionErrorContext(conn, ex));
                //        }
                //    }
                //});
            }

            try
            {
                conn.Connect();
                conn.SendRequest(new Subscribe(_topicName, _channelName));
                conn.SendRequest(new Ready(_options.MaxInFlight));
            }
            catch (Exception ex)
            {
                if (_onConnectionErrorCallback != null)
                {
                    _onConnectionErrorCallback(new ConnectionErrorContext(conn, ex));
                }
            }
        }

        private static async Task<IEnumerable<INsqConnection>> LocateNsqdInstances(string topicName, NsqConnectionFactory connectionFactory, ConcurrentDictionary<string, INsqConnection> currentConnections)
        {
            IDictionary<string, INsqConnection> newConnections = await connectionFactory.GetConnections(topicName);            
            
            // add
            foreach (string key in newConnections.Keys)
            {
               if (!currentConnections.ContainsKey(key))
                {
                    currentConnections.TryAdd(key, newConnections[key]);    
                }
            }

            // disconnect and remove
            var obsoleteKeys = currentConnections.Keys
                .Where(k => !newConnections.Keys.Contains(k));

            foreach (var key in obsoleteKeys)
            {
                currentConnections[key].Close();

                INsqConnection conn = null;
                currentConnections.TryRemove(key, out conn);
            }

            return currentConnections.Values;
        }

        #region IDisposable members

        public void Dispose()
        {
            if (_workerTimer != null)
            {
                _workerTimer.Stop();
                _workerTimer.Dispose();
                _workerTimer = null;
            }
        }

        #endregion
    }
}
