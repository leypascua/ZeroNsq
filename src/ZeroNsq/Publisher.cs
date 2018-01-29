using System;
using System.Collections.Generic;
using System.Text;

namespace ZeroNsq
{
    /// <summary>
    /// Exposes factory methods for getting an IPublisher instance.
    /// </summary>
    public static class Publisher
    {
        const string DefaultScheme = "http";
        const string DefaultHost = "127.0.0.1";
        const int DefaultPort = 4151;

        private readonly static string DefaultConnectionString = string.Format("nsqd={0}://{1}:{2}", DefaultScheme, DefaultHost, DefaultPort);

        /// <summary>
        /// Creates an IPublisher instance
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <returns>An IPublisher instance</returns>
        public static IPublisher CreateInstance(string connectionString)
        {
            string uriString = ConnectionStringParser.GetMatch(ConnectionStringParser.NsqdKey, connectionString, DefaultConnectionString);
            var uri = new Uri(uriString);
            return CreateInstance(uri);
        }

        /// <summary>
        /// Creates an IPublisher instance
        /// </summary>
        /// <param name="host">The host string</param>
        /// <param name="port">The port used by NSQD</param>
        /// <param name="scheme">The scheme to use (HTTP or TCP)</param>
        /// <returns>An IPublisher instance</returns>
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

        /// <summary>
        /// Creates an IPublisher instance
        /// </summary>
        /// <param name="endpointUri">The URI of the NSQD host API</param>
        /// <returns>An IPublisher instance</returns>
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
