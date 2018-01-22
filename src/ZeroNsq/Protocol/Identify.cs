using Jil;
using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Text;

namespace ZeroNsq.Protocol
{   
    public class Identify : IRequest
    {   
        public string client_id { get; set; }

        public string hostname { get; set; }

        public bool feature_negotiation { get; set; }
        
        public int? heartbeat_interval { get; set; }
        
        public int? output_buffer_size { get; set; }
                
        public int? output_buffer_timeout { get; set; }

        public bool tls_v1 { get; set; }

        public bool snappy { get; set; }

        public bool deflate { get; set; }

        public int deflate_level { get; set; }

        public int sample_rate { get; set; }
        
        public int? msg_timeout { get; set; }

        public string user_agent { get { return "ZeroNsq/0.01b"; } }

        public byte[] ToByteArray()
        {
            using (var ms = new MemoryStream())            
            {   
                ms.WriteBytes(Commands.IDENTIFY);

                byte[] data = GetDataBytes(this);
                byte[] dataLength = BitConverter.GetBytes(data.Length);

                if (BitConverter.IsLittleEndian)
                {
                    Array.Reverse(dataLength);
                }

                ms.WriteBytes(dataLength);
                ms.WriteBytes(data);

                return ms.ToArray();
            }
        }

        private static byte[] GetDataBytes(object instance)
        {
            using (var dataStream = new MemoryStream())
            using (var textWriter = new StreamWriter(dataStream))
            {
                JSON.Serialize(instance, textWriter, Options.ExcludeNulls);
                textWriter.Flush();

                return dataStream.ToArray();
            }
        }
    }
}
