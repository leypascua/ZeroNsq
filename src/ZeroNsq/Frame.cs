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

        /// <summary>
        /// Gets the readable UTF8 string data.
        /// </summary>
        /// <returns></returns>
        public string ToUtf8String()
        {
            if (Data == null || Data.Length == 0) return string.Empty;
            return Encoding.UTF8.GetString(Data, 0, Data.Length);
        }
    }
}
