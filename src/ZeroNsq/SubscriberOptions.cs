using System;
using System.Collections.Generic;
using System.Linq;
using System.Collections;
using System.Text;
using System.Text.RegularExpressions;
using System.Net;

namespace ZeroNsq
{
    public class SubscriberOptions : ConnectionOptions
    {   
        private const string DefaultMaxInFlight = "1";
        private const string MaxInFlightKey = "MaxInFlight";
        private const string DefaultMaxRetryAttempts = "3";
        private const string MaxRetryAttemptsKey = "MaxRetryAttempts";        

        public DnsEndPoint[] Nsqd { get; set; }
        public Uri[] Lookupd { get; set; }
        public int MaxInFlight { get; set; }
        public int MaxRetryAttempts { get; set; }

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
