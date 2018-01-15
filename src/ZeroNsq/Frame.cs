using System;
using System.Collections.Generic;
using System.Text;

namespace ZeroNsq
{
    public class Frame
    {
        /// <summary>
        /// Gets or sets the type of frame
        /// </summary>
        public FrameType Type { get; set; }

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
    }
}
