using Jil;
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace ZeroNsq.Lookup
{
    public interface INsqLookupService
    {
        Task<IEnumerable<ProducerEndpointData>> GetProducersAsync(Uri lookupdHostUri, string topicName);
    }

    public class NsqLookupDaemonService : INsqLookupService
    {
        private readonly static HttpClient HttpClient = new HttpClient();

        public async Task<IEnumerable<ProducerEndpointData>> GetProducersAsync(Uri lookupdHostUri, string topicName)
        {
            ///TODO: Implement this. Download list from http://Lookupd.nsq.io:4160/lookup?topic={topicName}            

            var uriBuilder = new UriBuilder(lookupdHostUri);
            uriBuilder.Path = "lookup";
            uriBuilder.Query = "topic=" + topicName;

            string json = await HttpClient.GetStringAsync(uriBuilder.Uri);

            var response = JSON.Deserialize<LookupTopicProducersResponse>(json);

            if (response == null || response.status_code != 200)
            {
                throw new HttpRequestException("Unhandled exception ocurred on the request to " + uriBuilder.Uri.AbsoluteUri);
            }

            return response.data.producers;
        }
    }
}
