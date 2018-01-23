﻿using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using ZeroNsq.Protocol;

namespace ZeroNsq
{
    public class FrameReader
    {
        const int MaxMessageSize = 2097152;

        private readonly byte[] FrameSizeBuffer = new byte[Frame.FrameSizeLength];
        private readonly byte[] FrameTypeBuffer = new byte[Frame.FrameTypeLength];
        private readonly Stream _stream;        

        public FrameReader(Stream stream)
        {
            _stream = stream;
        }

        public bool IsBusy { get; private set; }

        public Frame ReadFrame()
        {
            try
            {
                var task = ReadFrameAsync();
                task.Wait();
                return task.Result;
            }
            catch (AggregateException ex)
            {
                throw ex.InnerException;
            }
        }

        public async Task<Frame> ReadFrameAsync()
        {
            try
            {
                IsBusy = true;

                int frameLength = await ReadFrameLengthAsync();
                FrameType frameType = await ReadFrameTypeAsync();
                int messageSize = frameLength - Frame.FrameTypeLength;

                if (messageSize > MaxMessageSize)
                {
                    throw new OverflowException("Messages greater than 2MB are not supported.");
                }

                byte[] data = await ReadFrameDataAsync(_stream, messageSize);

                IsBusy = false;

                return new Frame(frameType, data);
            }
            catch (IOException ex)
            {
                throw new ConnectionException("ReadFrame failed. Reason: " + ex.Message);
            }
            catch (ObjectDisposedException)
            {
                throw new ConnectionException("Connection has been disposed.");
            }
            finally
            {
                IsBusy = false;
            }
        }

        private async Task<int> ReadFrameLengthAsync()
        {   
            await _stream.ReadAsync(FrameSizeBuffer, 0, Frame.FrameSizeLength);
            return ToInt32(FrameSizeBuffer);
        }

        private async Task<FrameType> ReadFrameTypeAsync()
        {
            await _stream.ReadAsync(FrameTypeBuffer, 0, Frame.FrameTypeLength);
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

        private static async Task<byte[]> ReadFrameDataAsync(Stream stream, int frameLength)
        {
            byte[] buffer = new byte[frameLength];

            int offset = 0;
            buffer = await ReadBytesAsync(stream, buffer, offset, frameLength);

            return buffer;
        }

        private static async Task<byte[]> ReadBytesAsync(Stream stream, byte[] buffer, int offset, int length)
        {
            int bytesRead;
            int bytesLeft = length;

            while ((bytesRead = await stream.ReadAsync(buffer, offset, bytesLeft)) > 0)
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
