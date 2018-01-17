﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace ZeroNsq
{
    public static class StreamUtils
    {
        public static Stream WriteASCII(this Stream stream, string asciiString)
        {
            byte[] buffer = Encoding.ASCII.GetBytes(asciiString);
            return WriteBytes(stream, buffer);
        }

        public static Stream WriteUtf8(this Stream stream, string utf8String)
        {
            byte[] buffer = Encoding.UTF8.GetBytes(utf8String);
            return WriteBytes(stream, buffer);
        }

        public static Stream WriteInt16(this Stream stream, short value)
        {
            byte[] buffer = BitConverter.GetBytes(value);
            return WriteBytes(stream, buffer);
        }

        public static Stream WriteInt32(this Stream stream, int value)
        {
            byte[] buffer = BitConverter.GetBytes(value);
            return WriteBytes(stream, buffer);
        }

        public static Stream WriteInt64(this Stream stream, long value)
        {
            byte[] buffer = BitConverter.GetBytes(value);
            return WriteBytes(stream, buffer);
        }

        public static Stream WriteBytes(this Stream stream, byte[] buffer)
        {
            stream.Write(buffer, 0, buffer.Length);
            return stream;
        }
    }
}
