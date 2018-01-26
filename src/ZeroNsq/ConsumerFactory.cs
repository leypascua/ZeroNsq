using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
using ZeroNsq.Lookup;

namespace ZeroNsq
{
    public class ConsumerFactory : IDisposable
    {   
        private readonly SubscriberOptions _options;
        private readonly CancellationToken _cancellationToken;
        private readonly INsqLookupService _lookupd;
        private readonly object syncLock = new object();
        private Dictionary<string, Consumer> _activeConsumers = new Dictionary<string, Consumer>();

        public ConsumerFactory(SubscriberOptions options, CancellationToken cancellationToken, INsqLookupService lookupd = null)
        {
            _options = options;
            _cancellationToken = cancellationToken;
            _lookupd = lookupd ?? new NsqLookupDaemonService();
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
                    .Where(k => !locatedConnections.Keys.Contains(k))
                    .ToList();

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
            var results = new Dictionary<string, INsqConnection>();
            if (_options.Lookupd == null || !_options.Lookupd.Any()) return results;

            if (_lookupd == null)
            {   
                return await Task.Run(() => { return results; });
            }

            foreach (Uri endpointUri in _options.Lookupd)
            {
                IEnumerable<ProducerEndpointData> producers = await _lookupd.GetProducersAsync(endpointUri, topicName);

                foreach (var producer in producers)
                {
                    string key = NsqConnectionProxy.GenerateId(producer.hostname, producer.tcp_port);
                    if (!results.ContainsKey(key))
                    {
                        var conn = new NsqConnectionProxy(producer.hostname, producer.tcp_port, _options);
                        results.Add(key, conn);
                    }
                }
            }

            return results;
        }

        public void Dispose()
        {
            Reset();
        }
    }
}
