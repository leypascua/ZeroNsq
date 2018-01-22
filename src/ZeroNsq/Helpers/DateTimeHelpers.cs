using System;
using System.Collections.Generic;
using System.Text;

namespace ZeroNsq.Helpers
{
    public static class DateTimeHelpers
    {
        public readonly static DateTime UnixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        public static long ToUnixTimestamp(this DateTime input, TimeZoneInfo localTimezone = null)
        {
            DateTime utcDate = input.Kind != DateTimeKind.Utc ?
                ToUtcFromLocalTime(input, localTimezone ?? TimeZoneInfo.Local) :
                input;

            long result = (long)utcDate.Subtract(UnixEpoch).TotalSeconds;
            return result;
        }

        public static DateTime ToUtcFromLocalTime(this DateTime input, TimeZoneInfo tzi)
        {
            return TimeZoneInfo.ConvertTime(input, tzi, TimeZoneInfo.Utc);
        }
    }
}
