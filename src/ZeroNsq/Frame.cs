using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace ZeroNsq
{
    public class Frame
    {
        /// <summary>
        /// Gets or sets the type of frame
        /// </summary>
        public FrameType Type { get; set; }

        public static Frame Response(string text)
        {
            var bytes = Encoding.UTF8.GetBytes(text);

            return new Frame
            {
                Type = FrameType.Response,
                MessageSize = bytes.Length,
                Data = bytes
            };
        }

        public static Frame Message(Message msg)
        {
            var bytes = msg.ToByteArray();

            return new Frame
            {
                Type = FrameType.Message,
                MessageSize = bytes.Length,
                Data = bytes
            };
        }

        public static Frame Error(string errorCode)
        {
            var bytes = Encoding.ASCII.GetBytes(errorCode);

            return new Frame
            {
                Type = FrameType.Error,
                MessageSize = bytes.Length,
                Data = bytes
            };
        }

        /// <summary>
        /// Gets or sets the message size in bytes
        /// </summary>
        public int MessageSize { get; set; }

        /// <summary>
        /// Gets or sets the data
        /// </summary>
        public byte[] Data { get; set; }
    }

    public static class FrameExtensions
    {
        public static Message ToMessage(this Frame frame)
        {
            if (frame.Type != FrameType.Message) return null;

            return new Message(frame.Data);
        }

        public static string ToErrorCode(this Frame frame)
        {
            if (frame.Type != FrameType.Error) return null;

            return Encoding.ASCII.GetString(frame.Data);
        }

        public static byte[] ToByteArray(this Frame frame)
        {   
            byte[] sizeBuffer = BitConverter.GetBytes(frame.Data.Length);
            byte[] frameTypeBuffer = BitConverter.GetBytes((int)frame.Type);

            using (var ms = new MemoryStream())
            {
                ms.WriteBytes(sizeBuffer);
                ms.WriteBytes(frameTypeBuffer);
                ms.WriteBytes(frame.Data);

                return ms.ToArray();
            }
        }
    }
}
