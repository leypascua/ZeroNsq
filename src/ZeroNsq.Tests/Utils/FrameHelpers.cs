using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using ZeroNsq.Protocol;

namespace ZeroNsq.Tests.Utils
{
    public static class FrameHelpers
    {
        public static Frame MessageFrame(string data)
        {
            var msg = new Message(data);
            return Frame.Message(msg);
        }

        public static Stream MessageFrameStream(Message msg)
        {
            byte[] dataBuffer = msg.ToByteArray();
            byte[] sizeBuffer = BitConverter.GetBytes(dataBuffer.Length);
            byte[] frameTypeBuffer = BitConverter.GetBytes((int)FrameType.Message);

            var stream = new MemoryStream();
            int offset = 0;

            stream.Write(sizeBuffer, offset, sizeBuffer.Length);
            stream.Write(frameTypeBuffer, offset, frameTypeBuffer.Length);
            stream.Write(dataBuffer, offset, dataBuffer.Length);

            stream.Position = 0;

            return stream;
        }

        public static byte[] MessageFrameBytes(Message msg)
        {
            using (var stream = MessageFrameStream(msg) as MemoryStream)
            {
                return stream.ToArray();
            }
        }
    }
}
