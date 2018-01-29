using System;
using System.Collections.Generic;
using System.Text;

namespace ZeroNsq
{
    public static class Publisher
    {
        const string DefaultScheme = "http";
        const string DefaultHost = "127.0.0.1";
        const int DefaultPort = 4151;
        private readonly static string DefaultConnectionString = string.Format("nsqd={0}://{1}:{2}", DefaultScheme, DefaultHost, DefaultPort);

        public static IPublisher CreateInstance(string connectionString)
        {
            string uriString = ConnectionStringParser.GetMatch(ConnectionStringParser.NsqdKey, connectionString, DefaultConnectionString);
            var uri = new Uri(uriString);
            return CreateInstance(uri);
        }

        public static IPublisher CreateInstance(string host = null, int? port = null, string scheme = null)
        {
            string targetHost = (host ?? DefaultHost).Trim();
            string targetScheme = (scheme ?? DefaultScheme).Trim();

            if (port <= 0)
            {
                port = DefaultPort;
            }

            var builder = new UriBuilder();
            builder.Scheme = targetScheme;
            builder.Host = targetHost;
            builder.Port = port.GetValueOrDefault(DefaultPort);

            return CreateInstance(builder.Uri);
        }

        public static IPublisher CreateInstance(Uri endpointUri)
        {
            if (endpointUri.Scheme == "http")
            {
                return new HttpPublisher(endpointUri.Host, endpointUri.Port);
            }

            return new TcpPublisher(endpointUri.Host, endpointUri.Port);
        }
    }
}
