using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace ZeroNsq.Internal
{
    public static class ConnectionStringParser
    {
        internal const string NsqdKey = "Nsqd";
        internal const string LookupdKey = "Lookupd";                
        internal const string CONFIG_REGEX = @"=([-A-Za-z0-9_.:/]+)";

        internal static string GetMatch(string key, string source, string fallback = null)
        {
            string expression = key + CONFIG_REGEX;
            var match = Regex.Match(source, @expression, RegexOptions.IgnoreCase);

            return match.Success ?
                match.Groups[1].Value :
                fallback;
        }

        internal static IEnumerable<string> GetMatches(string key, string source, string fallback = null)
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
