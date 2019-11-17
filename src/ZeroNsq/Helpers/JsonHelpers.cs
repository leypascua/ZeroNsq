using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace ZeroNsq.Helpers
{
    internal static class JsonHelpers
    {
        private readonly static JsonSerializerSettings settings = new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore };

        public static string ToJson<T>(this T instance) where T : class
        {
            return JsonConvert.SerializeObject(instance, settings);
        }

        public static void ToJson(this object instance, TextWriter textWriter)
        {   
            using (var jwriter = new JsonTextWriter(textWriter) { CloseOutput = false })
            {
                JsonSerializer ser = new JsonSerializer();
                ser.Serialize(jwriter, instance);
            }
        }

        public static T DeserializeTo<T>(this string json) where T : class
        {
            return JsonConvert.DeserializeObject<T>(json);
        }

        public static T DeserializeTo<T>(this TextReader reader) where T : class
        {
            using (var jreader = new JsonTextReader(reader) { CloseInput = false })
            {
                JsonSerializer ser = new JsonSerializer();
                return ser.Deserialize<T>(jreader);
            }
        }
    }
}
