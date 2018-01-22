using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace ZeroNsq.Protocol
{
    public class Publish : IRequestWithResponse
    {
        public Publish(string topic, string utf8String)
        {
            TopicName = topic.EnforceValidNsqName();
            Data = Encoding.UTF8.GetBytes(utf8String);
        }

        public Publish(string topic, byte[] data)
        {
            TopicName = topic.EnforceValidNsqName();
            Data = data;
        }

        public string TopicName { get; set; }

        public byte[] Data { get; set; }

        public byte[] ToByteArray()
        {
            if (string.IsNullOrEmpty(TopicName)) throw new InvalidOperationException("TopicName is null or empty.");
            if (Data == null || Data.Length == 0) throw new InvalidOperationException("Data is empty.");

            using (var ms = new MemoryStream())
            {
                ms.WriteBytes(Commands.PUB);                
                ms.WriteASCII(TopicName + "\n");

                ms.WriteInt32(Data.Length);
                ms.WriteBytes(Data);

                return ms.ToArray();
            }
        }
    }
}
