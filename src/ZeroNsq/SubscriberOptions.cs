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
        private const string NsqdKey = "Nsqd";
        private const string LookupdKey = "Lookupd";
        private const string DefaultMaxInFlight = "1";
        private const string MaxInFlightKey = "MaxInFlight";
        private const string DefaultMaxRetryAttempts = "3";
        private const string MaxRetryAttemptsKey = "MaxRetryAttempts";
        private const string CONFIG_REGEX = @"=([-A-Za-z0-9_.:/]+)";

        public DnsEndPoint[] Nsqd { get; set; }
        public Uri[] Lookupd { get; set; }
        public int MaxInFlight { get; set; }
        public int MaxRetryAttempts { get; set; }

        public static SubscriberOptions Parse(string input)
        {
            var result = new SubscriberOptions
            {
                MaxInFlight = int.Parse(GetMatch(MaxInFlightKey, input, DefaultMaxInFlight)),
                MaxRetryAttempts = int.Parse(GetMatch(MaxRetryAttemptsKey, input, DefaultMaxRetryAttempts)),
                Nsqd = CreateEndpointsFrom(GetMatches(NsqdKey, input), "nsqd=").ToArray(),
                Lookupd = CreateUriFrom(GetMatches(LookupdKey, input), "lookupd=").ToArray(),
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

        private static string GetMatch(string key, string source, string fallback = null)
        {
            string expression = key + CONFIG_REGEX;
            var match = Regex.Match(source, @expression, RegexOptions.IgnoreCase);

            return match.Success ?
                match.Groups[1].Value :
                fallback;
        }

        private static IEnumerable<string> GetMatches(string key, string source, string fallback = null)
        {
            string expression = key + CONFIG_REGEX;
            MatchCollection matches = Regex.Matches(source, @expression, RegexOptions.IgnoreCase);

            if (matches.Count == 0) yield break;

            foreach (Match match in matches)
            {
                yield return match.Value;
            }
        }
    }
}
