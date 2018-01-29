using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using ZeroNsq.Protocol;

namespace ZeroNsq.Internal
{
    public class FrameReader : IDisposable
    {
        const int MaxMessageSize = 2097152;

        private readonly byte[] FrameSizeBuffer = new byte[Frame.FrameSizeLength];
        private readonly byte[] FrameTypeBuffer = new byte[Frame.FrameTypeLength];
        private readonly Stream _stream;
        private readonly CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();

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

                byte[] data = await ReadFrameDataAsync(_stream, messageSize, _cancellationTokenSource.Token);

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

        public void Dispose()
        {
            if (_cancellationTokenSource.IsCancellationRequested) return;

            _cancellationTokenSource.Cancel();
            _cancellationTokenSource.Dispose();
        }

        private async Task<int> ReadFrameLengthAsync()
        {   
            await _stream.ReadAsync(FrameSizeBuffer, 0, Frame.FrameSizeLength, _cancellationTokenSource.Token);
            return ToInt32(FrameSizeBuffer);
        }

        private async Task<FrameType> ReadFrameTypeAsync()
        {
            await _stream.ReadAsync(FrameTypeBuffer, 0, Frame.FrameTypeLength, _cancellationTokenSource.Token);
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

        private static async Task<byte[]> ReadFrameDataAsync(Stream stream, int frameLength, CancellationToken cancellationToken)
        {
            byte[] buffer = new byte[frameLength];

            int offset = 0;
            buffer = await ReadBytesAsync(stream, buffer, offset, frameLength, cancellationToken);

            return buffer;
        }

        private static async Task<byte[]> ReadBytesAsync(Stream stream, byte[] buffer, int offset, int length, CancellationToken cancellationToken)
        {
            int bytesRead;
            int bytesLeft = length;

            while ((bytesRead = await stream.ReadAsync(buffer, offset, bytesLeft, cancellationToken)) > 0)
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
