using System.IO;
using System.Text;
using ZeroNsq.Helpers;

namespace ZeroNsq.Protocol
{
    public class Frame
    {
        public const int FrameSizeLength = 4;
        public const int FrameTypeLength = 4;

        public Frame() { }

        public Frame(FrameType type, byte[] data)
        {
            Type = type;
            Data = data;
            MessageSize = data.Length;
        }

        /// <summary>
        /// Gets or sets the message size in bytes
        /// </summary>
        public int MessageSize { get; set; }

        /// <summary>
        /// Gets or sets the type of frame
        /// </summary>
        public FrameType Type { get; set; }

        /// <summary>
        /// Gets or sets the data
        /// </summary>
        public byte[] Data { get; set; }

        public static Frame Response(string text)
        {
            var bytes = Encoding.UTF8.GetBytes(text);
            return new Frame(FrameType.Response, bytes);
        }

        public static Frame Message(Message msg)
        {
            var bytes = msg.ToByteArray();
            return new Frame(FrameType.Message, bytes);
        }

        public static Frame Error(string errorCode)
        {
            var bytes = Encoding.ASCII.GetBytes(errorCode);
            return new Frame(FrameType.Error, bytes);
        }
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
            using (var ms = ToStream(frame) as MemoryStream)
            {   
                return ms.ToArray();
            }
        }

        public static string ToASCII(this Frame frame)
        {
            if (frame.Type == FrameType.Message) return ToMessage(frame).ToString();

            return Encoding.ASCII.GetString(frame.Data);
        }

        public static Stream ToStream(this Frame frame)
        {
            var ms = new MemoryStream();
            int frameLength = Frame.FrameTypeLength + frame.Data.Length;

            ms.WriteInt32(frameLength);
            ms.WriteInt32((int)frame.Type);
            ms.WriteBytes(frame.Data);

            ms.Position = 0;

            return ms;
        }
    }
}
