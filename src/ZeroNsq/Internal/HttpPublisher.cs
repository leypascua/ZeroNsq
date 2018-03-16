using System;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace ZeroNsq.Internal
{
    public class HttpPublisher : IPublisher
    {
        private const string Pub = "pub";
        private readonly static HttpClient HttpClient = new HttpClient();
        private readonly UriBuilder _uriBuilder;
        private readonly object _uriLock = new object();

        internal HttpPublisher(string host, int port)
        {
            _uriBuilder = new UriBuilder(string.Format("http://{0}:{1}", host, port));
        }

        public async Task PublishAsync(string topic, byte[] message)
        {
            if (message == null || message.Length == 0)
            {
                throw new RequestException("Message cannot be empty.");
            }

            string topicName = Helpers.StringHelpers.EnforceValidNsqName(topic);

            using (var memoryStream = new MemoryStream(message))
            using (var sc = new StreamContent(memoryStream))
            {
                LogProvider.Current.Debug(string.Format("Publishing a message on topic [{0}]", topic));
                await PostAsync(Pub, string.Format("topic={0}", topicName), sc);
            }
        }

        public void Dispose() {}

        private async Task PostAsync(string path, string query, HttpContent content)
        {
            string requestUri = BuildUri(path, query);
            var response = await HttpClient.PostAsync(requestUri, content);

            

            if (!response.IsSuccessStatusCode)
            {
                LogProvider.Current.Error("Request failed. Reason: " + response.ReasonPhrase);
                throw new RequestException(response.ReasonPhrase);
            }
        }

        private string BuildUri(string path, string query)
        {
            lock (_uriLock)
            {
                _uriBuilder.Path = path;
                _uriBuilder.Query = query;

                return _uriBuilder.Uri.AbsoluteUri;
            }
        }
    }
}
