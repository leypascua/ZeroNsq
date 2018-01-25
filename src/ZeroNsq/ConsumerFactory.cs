using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;

namespace ZeroNsq
{
    public class ConsumerFactory : IDisposable
    {   
        private readonly SubscriberOptions _options;
        private readonly CancellationToken _cancellationToken;
        private readonly object syncLock = new object();
        private Dictionary<string, Consumer> _activeConsumers = new Dictionary<string, Consumer>();

        public ConsumerFactory(SubscriberOptions options, CancellationToken cancellationToken)
        {
            _options = options;
            _cancellationToken = cancellationToken;
        }

        public IEnumerable<Consumer> GetInstances(string topicName)
        {
            try
            {
                var task = GetInstancesAsync(topicName);
                task.Wait();
                return task.Result;
            }
            catch (AggregateException ex)
            {
                throw ex.InnerException;
            }
        }

        public async Task<IEnumerable<Consumer>> GetInstancesAsync(string topicName)
        {
            IDictionary<string, INsqConnection> locatedConnections = await GetConnections(topicName);

            lock (syncLock)
            {
                // add
                foreach (string key in locatedConnections.Keys)
                {
                    if (!_activeConsumers.ContainsKey(key))
                    {
                        var conn = locatedConnections[key];
                        var consumer = new Consumer(topicName, conn, _options, _cancellationToken);

                        _activeConsumers.Add(key, consumer);
                    }
                }

                // disconnect and remove
                var obsoleteKeys = _activeConsumers.Keys
                    .Where(k => !locatedConnections.Keys.Contains(k));

                foreach (var key in obsoleteKeys)
                {
                    _activeConsumers[key].Stop();
                    _activeConsumers.Remove(key);
                }
            }

            return _activeConsumers.Values;
        }

        public void Reset()
        {
            if (_activeConsumers != null)
            {
                foreach (var consumer in _activeConsumers.Values)
                {
                    if (consumer.IsConnected)
                    {
                        consumer.Stop();
                    }
                }

                _activeConsumers.Clear();
                _activeConsumers = null;
            }
        }

        private async Task<IDictionary<string, INsqConnection>> GetConnections(string topicName)
        {            
            var connections = await GetFromLookupd(topicName);

            IncludeFrom(_options.Nsqd, connections);

            return connections;
        }

        private void IncludeFrom(IEnumerable<DnsEndPoint> nsqdEndpoints, IDictionary<string, INsqConnection> connections)
        {
            foreach (DnsEndPoint endpoint in nsqdEndpoints)
            {
                string connectionId = NsqConnectionProxy.GenerateId(endpoint.Host, endpoint.Port);
                if (connections.ContainsKey(connectionId)) continue;

                connections.Add(connectionId, new NsqConnectionProxy(endpoint.Host, endpoint.Port, _options));
            }
        }

        private async Task<IDictionary<string, INsqConnection>> GetFromLookupd(string topicName)
        {
            ///TODO: Implement this. Download list from http://Lookupd.nsq.io:4160/lookup?topic={topicName}
            return await Task.Factory.StartNew(() => { return new Dictionary<string, INsqConnection>(); });
        }

        public void Dispose()
        {
            Reset();
        }
    }
}
