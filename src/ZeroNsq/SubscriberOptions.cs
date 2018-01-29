using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using ZeroNsq.Internal;

namespace ZeroNsq
{
    /// <summary>
    /// An abstraction of available options for a consumer connection
    /// </summary>
    public class SubscriberOptions : ConnectionOptions
    {   
        private const string DefaultMaxInFlight = "1";
        private const string MaxInFlightKey = "MaxInFlight";
        private const string DefaultMaxRetryAttempts = "3";
        private const string MaxRetryAttemptsKey = "MaxRetryAttempts";        

        /// <summary>
        /// Gets or sets the DNS endpoints to connect to
        /// </summary>
        public DnsEndPoint[] Nsqd { get; set; }

        /// <summary>
        /// Gets or sets the URI of LOOKUPD instances to connect to
        /// </summary>
        public Uri[] Lookupd { get; set; }

        /// <summary>
        /// Gets or sets the number of messages in flight for the consumer connection.
        /// </summary>
        public int MaxInFlight { get; set; }

        /// <summary>
        /// Gets or sets the number of retry attempts allowed for any given message on the channel.
        /// </summary>
        public int MaxRetryAttempts { get; set; }

        /// <summary>
        /// Parses the input string into a new instance
        /// </summary>
        /// <param name="input">The input string</param>
        /// <returns>The instance</returns>
        public static SubscriberOptions Parse(string input)
        {
            var result = new SubscriberOptions
            {
                MaxInFlight = int.Parse(ConnectionStringParser.GetMatch(MaxInFlightKey, input, DefaultMaxInFlight)),
                MaxRetryAttempts = int.Parse(ConnectionStringParser.GetMatch(MaxRetryAttemptsKey, input, DefaultMaxRetryAttempts)),
                Nsqd = CreateEndpointsFrom(ConnectionStringParser.GetMatches(ConnectionStringParser.NsqdKey, input), "nsqd=").ToArray(),
                Lookupd = CreateUriFrom(ConnectionStringParser.GetMatches(ConnectionStringParser.LookupdKey, input), "lookupd=").ToArray(),
            };

            return result;
        }

        private static IEnumerable<Uri> CreateUriFrom(IEnumerable<string> items, string prefix)
        {
            foreach (string item in items)
            {
                string input = item
                    .Replace(prefix, string.Empty);

                if (!input.StartsWith("http"))
                {
                    input = "http://" + input;
                }

                yield return new Uri(input);
            }
        }

        private static IEnumerable<DnsEndPoint> CreateEndpointsFrom(IEnumerable<string> items, string prefix)
        {
            foreach (string item in items)
            {
                string input = item
                    .Replace(prefix, string.Empty);

                var builder = new UriBuilder(input);

                yield return new DnsEndPoint(builder.Uri.Host, builder.Uri.Port);
            }
        }
    }
}
