using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using ZeroNsq.Helpers;

namespace ZeroNsq.Protocol
{
    public class Subscribe : IRequestWithResponse
    {
        public Subscribe(string topic, string channel)
        {
            TopicName = topic.EnforceValidNsqName();
            ChannelName = channel.EnforceValidNsqName();
        }

        public string TopicName { get; set; }

        public string ChannelName { get; set; }

        public byte[] ToByteArray()
        {
            using (var ms = new MemoryStream())
            {
                ms.WriteBytes(Commands.SUB);
                ms.WriteASCII(TopicName + " ");
                ms.WriteASCII(ChannelName + "\n");

                return ms.ToArray();
            }
        }
    }
}
