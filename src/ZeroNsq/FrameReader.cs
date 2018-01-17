using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using System.Text;

namespace ZeroNsq
{
    public class FrameReader
    {
        private const int DefaultMaxFrameLength = 1048576;
        private const int FrameSizeLength = 4;
        private const int FrameTypeLength = 4;

        private readonly byte[] FrameSizeBuffer = new byte[FrameSizeLength];
        private readonly byte[] FrameTypeBuffer = new byte[FrameTypeLength];
        private readonly Stream _stream;

        public FrameReader(Stream stream)
        {
            _stream = stream;
        }

        public Frame ReadFrame()
        {
            int frameLength = ReadFrameLength();
            FrameType frameType = ReadFrameType();
            byte[] data = ReadFrameData(_stream, frameLength);

            return new Frame(frameType, data);
        }

        private int ReadFrameLength()
        {
            _stream.Read(FrameSizeBuffer, 0, FrameSizeLength);
            return ToInt32(FrameSizeBuffer);
        }

        private FrameType ReadFrameType()
        {
            _stream.Read(FrameTypeBuffer, 0, FrameTypeLength);
            return (FrameType)ToInt32(FrameTypeBuffer);
        }

        private static int ToInt32(byte[] buffer)
        {
            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(buffer);
            }

            return BitConverter.ToInt32(buffer, 0);
        }

        private static byte[] ReadFrameData(Stream stream, int frameLength)
        {
            byte[] buffer = new byte[frameLength];

            int offset = 0;
            buffer = ReadBytes(stream, buffer, offset, frameLength);

            return buffer;
        }

        private static byte[] ReadBytes(Stream stream, byte[] buffer, int offset, int length)
        {
            int bytesRead;
            int bytesLeft = length;

            while ((bytesRead = stream.Read(buffer, offset, bytesLeft)) > 0)
            {
                offset += bytesRead;
                bytesLeft -= bytesRead;
                if (offset > length) throw new InvalidOperationException("Buffer is longer than expected.");
                if (offset == length) break;
            }

            return buffer;
        }
    }
}
