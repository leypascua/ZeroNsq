using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace ZeroNsq
{
    public class NsqConnectionFactory
    {
        private SubscriberOptions _options;

        public NsqConnectionFactory(SubscriberOptions options)
        {
            _options = options;
        }

        public async Task<IDictionary<string, INsqConnection>> GetConnections(string topicName)
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
            ///TODO: Implement this. Download list from from http://Lookupd.nsq.io:4160/lookup?topic={topicName}
            return await Task.Factory.StartNew(() => { return new Dictionary<string, INsqConnection>(); });
        }
    }
}
