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

        public FrameReader(Stream stream)
        {
            _stream = stream;
        }

        public Frame ReadFrame()
        {
            int frameLength = ReadInt32(_stream, FrameSizeLength);
            FrameType frameType = (FrameType)ReadInt32(_stream, FrameTypeLength);
            byte[] data = ReadFrameData(_stream, frameLength);

            return new Frame
            {
                MessageSize = frameLength,
                Type = frameType,
                Data = data
            };
        }

        private static int ReadInt32(Stream stream, int bufferLength)
        {
            byte[] buffer = ArrayPool<byte>.Shared.Rent(bufferLength);
            int result = 0;

            try
            {
                int offset = 0;
                ReadBytes(stream, buffer, offset, FrameSizeLength);
                result = BitConverter.ToInt32(buffer, 0);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer, clearArray: true);
            }

            return result;
        }

        private static byte[] ReadFrameData(Stream stream, int frameLength)
        {
            byte[] buffer = new byte[frameLength];

            int offset = 0;
            ReadBytes(stream, buffer, offset, frameLength);

            return buffer;
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
