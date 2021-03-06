﻿using System;
using ZeroNsq.Internal;

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
            if (string.IsNullOrEmpty(connectionString))
            {
                throw new ArgumentException("A non-empty connectionString is required.");
            }

            // A publisher only needs a single NSQD endpoint. Return the first one that we can get.
            string uriString = ConnectionStringParser.GetMatch(ConnectionStringParser.NsqdKey, (connectionString ?? string.Empty).Trim());

            if (string.IsNullOrEmpty(uriString))
            {
                throw new FormatException("NSQD is undefined on input: " + connectionString);
            }

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
                LogProvider.Current.Debug("Creating an instance of HttpPublisher");
                return new HttpPublisher(endpointUri.Host, endpointUri.Port);
            }

            LogProvider.Current.Debug("Creating an instance of TcpPublisher");
            return new TcpPublisher(endpointUri.Host, endpointUri.Port);
        }
    }
}
