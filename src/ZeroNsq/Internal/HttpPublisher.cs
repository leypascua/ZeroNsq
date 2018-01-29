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

        public void Publish(string topic, byte[] message)
        {
            try
            {
                PublishAsync(topic, message).Wait();
            }
            catch (AggregateException ex)
            {
                throw ex.InnerException;
            }
        }

        public async Task PublishAsync(string topic, byte[] message)
        {
            using (var ms = new MemoryStream(message))
            {
                await PublishAsync(topic, ms);
            }
        }

        public async Task PublishAsync(string topic, Stream messageStream)
        {
            string topicName = Helpers.StringHelpers.EnforceValidNsqName(topic);

            using (var sc = new StreamContent(messageStream))
            {
                await PostAsync(Pub, string.Format("topic={0}", topicName), sc);
            }
        }

        public void Publish(string topic, string utf8String)
        {
            try
            {
                using (var ms = new MemoryStream(Encoding.UTF8.GetBytes(utf8String)))
                {
                    PublishAsync(topic, ms).Wait();
                }
            }
            catch (AggregateException ex)
            {
                throw ex.InnerException;
            }
        }

        public void PublishJson<TMessage>(string topic, TMessage message) where TMessage : class, new()
        {
            string json = Jil.JSON.Serialize<TMessage>(message, Jil.Options.ExcludeNulls);
            Publish(topic, json);
        }

        public void Dispose() {}

        private async Task PostAsync(string path, string query, HttpContent content)
        {
            string requestUri = BuildUri(path, query);
            var response = await HttpClient.PostAsync(requestUri, content);

            if (!response.IsSuccessStatusCode)
            {
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
