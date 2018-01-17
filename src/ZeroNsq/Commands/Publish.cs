using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace ZeroNsq.Commands
{
    public class Publish : IProtocolCommandWithResponse
    {
        private static readonly byte[] CommandHeader = Encoding.ASCII.GetBytes("PUB ");

        public Publish(string topic, byte[] data)
        {
            TopicName = topic;
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
                ms.WriteBytes(CommandHeader);
                byte[] topicBytes = Encoding.ASCII.GetBytes(TopicName + "\n");
                ms.WriteASCII(TopicName + "\n");

                return ms.ToArray();
            }
        }
    }
}
