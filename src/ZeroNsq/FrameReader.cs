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
        
        private readonly Stream _stream;        
        private readonly object _syncLock = new object();

        public FrameReader(Stream stream)
        {
            _stream = stream;
        }

        public Frame ReadFrame()
        {
            int frameLength = ReadFrameLength(_stream);
            FrameType frameType = ReadFrameType(_stream);
            byte[] data = ReadFrameData(_stream, frameLength);

            return new Frame
            {
                MessageSize = frameLength,
                Type = frameType,
                Data = data
            };
        }

        private static int ReadFrameLength(Stream stream)
        {
            byte[] buffer = ArrayPool<byte>.Shared.Rent(FrameSizeLength);
            int frameLength = 0;

            try
            {
                int offset = 0;
                ReadBytes(stream, buffer, offset, FrameSizeLength);
                frameLength = BitConverter.ToInt32(buffer, 0);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer, clearArray: true);
            }

            return frameLength;
        }

        private static FrameType ReadFrameType(Stream stream)
        {
            byte[] buffer = ArrayPool<byte>.Shared.Rent(FrameSizeLength);
            int frameType = 0;

            try
            {
                int offset = 0;
                ReadBytes(stream, buffer, offset, FrameTypeLength);
                frameType = BitConverter.ToInt32(buffer, 0);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer, clearArray: true);
            }

            return (FrameType)frameType;
        }

        private static byte[] ReadFrameData(Stream stream, int frameLength)
        {
            byte[] buffer = new byte[frameLength];

            int offset = 0;
            ReadBytes(stream, buffer, offset, frameLength);

            return buffer;
        }

        private static byte[] ReadFrame(Stream stream, int frameLength)
        {
            byte[] buffer = new byte[frameLength];
            int offset = 0;

            ReadBytes(stream, buffer, offset, frameLength);

            return buffer;
        }

        private static byte[] GetPooledBuffer(int length)
        {
            return ArrayPool<byte>.Shared.Rent(length);
        }

        private static void ReadBytes(Stream stream, byte[] buffer, int offset, int length)
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
        }
    }
}
