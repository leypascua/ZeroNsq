using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace ZeroNsq
{
    public static class IPublisherExtensions
    {
        /// <summary>
        /// Publishes the message to the specified topic
        /// </summary>
        /// <param name="publisher">The publisher instance</param>
        /// <param name="topic">The target topic</param>
        /// <param name="message">The message to be published</param>
        public static void Publish(this IPublisher publisher, string topic, byte[] message)
        {
            if (message == null || message.Length == 0)
            {
                throw new RequestException("message cannot be empty.");
            }

            Task.Run(() => publisher.PublishAsync(topic, message))
                .GetAwaiter()
                .GetResult();
        }

        /// <summary>
        /// Publishes the message to the specified topic
        /// </summary>
        /// <param name="publisher">The publisher instance</param>
        /// <param name="topic">The target topic</param>
        /// <param name="utf8String">The message to be published</param>
        public static void Publish(this IPublisher publisher, string topic, string utf8String)
        {
            Task.Run(() => PublishAsync(publisher, topic, utf8String))
                .GetAwaiter()
                .GetResult();
        }

        /// <summary>
        /// Publishes the message to the specified topic
        /// </summary>
        /// <param name="publisher">The publisher instance</param>
        /// <param name="topic">The target topic</param>
        /// <param name="utf8String">The message to be published</param>
        /// <returns>An async Task</returns>
        public static async Task PublishAsync(this IPublisher publisher, string topic, string utf8String)
        {
            byte[] data = GetUtf8StringBytes(utf8String);
            await publisher.PublishAsync(topic, data);
        }

        /// <summary>
        /// Publishes the message to the specified topic
        /// </summary>
        /// <typeparam name="TMessage">The type of message to be serialized into UTF8 JSON</typeparam>
        /// <param name="topic">The target topic</param>
        /// <param name="message">The message to be published</param>
        [Obsolete]
        public static void PublishJson<TMessage>(this IPublisher publisher, string topic, TMessage message) where TMessage : class, new()
        {
            try
            {
                Task.Run(() => PublishJsonAsync<TMessage>(publisher, topic, message)).Wait();
            }
            catch (AggregateException ex)
            {
                throw ex.InnerException;
            }
        }

        /// <summary>
        /// Publishes the message to the specified topic
        /// </summary>
        /// <typeparam name="TMessage">The type of message to be serialized into UTF8 JSON</typeparam>
        /// <param name="publisher"></param>
        /// <param name="topic">The target topic</param>
        /// <param name="message">The message to be published</param>
        /// <returns>An async Task</returns>
        [Obsolete]
        public static async Task PublishJsonAsync<TMessage>(this IPublisher publisher, string topic, TMessage message) where TMessage : class, new()
        {
            if (message == default(TMessage))
            {
                throw new InvalidOperationException("Message cannot be empty.");
            }

            string json = Jil.JSON.Serialize<TMessage>(message, Jil.Options.ExcludeNulls);

            await PublishAsync(publisher, topic, json);
        }

        private static byte[] GetUtf8StringBytes(string utf8String)
        {
            string msg = (utf8String ?? string.Empty).Trim();
            if (string.IsNullOrEmpty(msg))
            {
                throw new RequestException("utf8String cannot be empty.");
            }

            return Encoding.UTF8.GetBytes(utf8String);
        }
    }
}
