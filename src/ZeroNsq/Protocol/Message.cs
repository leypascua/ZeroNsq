using Jil;
using System;
using System.Buffers;
using System.IO;
using System.Text;
using ZeroNsq.Helpers;

namespace ZeroNsq
{
    /// <summary>
    /// An abstraction of NSQ's incoming message
    /// </summary>
    public class Message
    {
        private const int MessageHeaderLength = 26;
        private const int TimestampHeaderLength = 8;
        private const int AttemptsHeaderLength = 2;
        private const int MessageIdHeaderLength = 16;

        /// <summary>
        /// Creates a new instance
        /// </summary>
        public Message() { }

        /// <summary>
        /// Creates a new instance with given data
        /// </summary>
        /// <param name="data">The data</param>
        public Message(string data)
        {
            Timestamp = DateTime.UtcNow.ToUnixTimestamp();
            Id = Encoding.UTF8.GetBytes(data.Md5().Substring(0, 16));
            Body = Encoding.UTF8.GetBytes(data);
        }

        /// <summary>
        /// Creates a new intance with given data
        /// </summary>
        /// <param name="data">The data</param>
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

        /// <summary>
        /// Gets the timestamp of the message as provided by NSQ
        /// </summary>
        public long Timestamp { get; set; }

        /// <summary>
        /// Gets the total number of attempts
        /// </summary>
        public short Attempts { get; set; }

        /// <summary>
        /// Gets the ID of the message
        /// </summary>
        public byte[] Id { get; set; }

        /// <summary>
        /// Gets the ID of the message as a UTF8 string
        /// </summary>
        public string IdString
        {
            get
            {
                return Encoding.ASCII.GetString(Id);
            }
        }

        /// <summary>
        /// Gets the body of the message
        /// </summary>
        public byte[] Body { get; set; }

        /// <summary>
        /// Converts the message to a byte array
        /// </summary>
        /// <returns></returns>
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

        /// <summary>
        /// Converts the body of the message to a UTF8 string
        /// </summary>
        /// <returns></returns>
        public string ToUtf8String()
        {
            EnsureNonEmptyBody();
            return Encoding.UTF8.GetString(Body);
        }

        /// <summary>
        /// Deserializes the message into an instance of TResult
        /// </summary>
        /// <typeparam name="TResult">The type of object to be used in deserialization</typeparam>
        /// <returns>The deserialized message</returns>
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

        /// <summary>
        /// Writes the message to the provided stream
        /// </summary>
        /// <param name="stream"></param>
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
            byte[] buffer = new byte[AttemptsHeaderLength];
            
            ms.Read(buffer, 0, AttemptsHeaderLength);

            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(buffer);
            }

            short result = BitConverter.ToInt16(buffer, 0);

            return result;
        }

        private static long ReadTimestamp(Stream ms)
        {
            byte[] buffer = new byte[TimestampHeaderLength];
            
            ms.Read(buffer, 0, TimestampHeaderLength);

            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(buffer);
            }

            long result = BitConverter.ToInt64(buffer, 0);

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
