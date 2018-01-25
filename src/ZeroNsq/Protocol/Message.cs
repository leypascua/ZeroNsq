using Jil;
using System;
using System.Buffers;
using System.IO;
using System.Text;
using ZeroNsq.Helpers;

namespace ZeroNsq
{
    public class Message
    {
        private const int MessageHeaderLength = 26;
        private const int TimestampHeaderLength = 8;
        private const int AttemptsHeaderLength = 2;
        private const int MessageIdHeaderLength = 16;

        public Message() { }

        public Message(string data)
        {
            Timestamp = DateTime.UtcNow.ToUnixTimestamp();
            Id = Encoding.UTF8.GetBytes(data.Md5().Substring(0, 16));
            Body = Encoding.UTF8.GetBytes(data);
        }

        public Message(byte[] data)
        {
            using (var ms = new MemoryStream(data))
            {
                Timestamp = ReadTimestamp(ms);
                Attempts = ReadAttempts(ms);
                Id = ReadMessageId(ms);
                Body = ReadBody(ms);
            }
        }

        public long Timestamp { get; set; }

        public short Attempts { get; set; }

        public byte[] Id { get; set; }

        public string IdString
        {
            get
            {
                return Encoding.ASCII.GetString(Id);
            }
        }

        public byte[] Body { get; set; }

        public byte[] ToByteArray()
        {
            EnsureNonEmptyBody();

            using (var ms = new MemoryStream())
            {
                WriteTo(ms);
                return ms.ToArray();
            }
        }

        public override string ToString()
        {
            return ToUtf8String();
        }

        public string ToUtf8String()
        {
            EnsureNonEmptyBody();
            return Encoding.UTF8.GetString(Body);
        }

        public TResult Deserialize<TResult>() where TResult : class, new()
        {
            if (Body == null || Body.Length == 0)
            {
                throw new InvalidOperationException("Unable to deserialize an empty message body.");
            }

            using (var ms = new MemoryStream(Body))
            using (var reader = new StreamReader(ms))
            {
                return JSON.Deserialize<TResult>(reader, Options.ExcludeNulls);
            }   
        }

        public void WriteTo(Stream stream)
        {
            stream.WriteInt64(Timestamp);
            stream.WriteInt16(Attempts);
            stream.WriteBytes(Id);
            stream.WriteBytes(Body);
            stream.Flush();
        }

        private static byte[] ReadBody(Stream ms)
        {
            long size = ms.Length - ms.Position;
            byte[] buffer = new byte[size];
            ms.Read(buffer, 0, (int)size);

            return buffer;
        }

        private static byte[] ReadMessageId(Stream ms)
        {
            byte[] buffer = new byte[MessageIdHeaderLength];
            ms.Read(buffer, 0, MessageIdHeaderLength);

            return buffer;
        }

        private static short ReadAttempts(Stream ms)
        {
            var buffer = ArrayPool<byte>.Shared.Rent(AttemptsHeaderLength);
            short result = 0;

            try
            {
                ms.Read(buffer, 0, AttemptsHeaderLength);
                result = BitConverter.ToInt16(buffer, 0);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer, clearArray: true);
            }

            return result;
        }

        private static long ReadTimestamp(Stream ms)
        {
            var buffer = ArrayPool<byte>.Shared.Rent(TimestampHeaderLength);
            long result = 0;

            try
            {
                ms.Read(buffer, 0, TimestampHeaderLength);
                result = BitConverter.ToInt64(buffer, 0);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer, clearArray: true);
            }

            return result;
        }

        private void EnsureNonEmptyBody()
        {
            if (Body == null || Body.Length == 0)
            {
                throw new InvalidOperationException("Body is empty.");
            }
        }
    }
}
