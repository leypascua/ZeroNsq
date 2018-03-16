﻿using System;
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

            try
            {
                Task.Run(() => publisher.PublishAsync(topic, message)).Wait();
            }
            catch (AggregateException ex)
            {
                throw ex.InnerException;
            }
        }

        /// <summary>
        /// Publishes the message to the specified topic
        /// </summary>
        /// <param name="publisher">The publisher instance</param>
        /// <param name="topic">The target topic</param>
        /// <param name="utf8String">The message to be published</param>
        public static void Publish(this IPublisher publisher, string topic, string utf8String)
        {
            string msg = (utf8String ?? string.Empty).Trim();
            if (string.IsNullOrEmpty(msg))
            {
                throw new RequestException("utf8String cannot be empty.");
            }

            try
            {
                byte[] data = Encoding.UTF8.GetBytes(utf8String);
                Task.Run(() => publisher.PublishAsync(topic, data)).Wait();
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
        /// <param name="topic">The target topic</param>
        /// <param name="message">The message to be published</param>
        public static void PublishJson<TMessage>(this IPublisher publisher, string topic, TMessage message) where TMessage : class, new()
        {
            if (message == default(TMessage))
            {
                throw new InvalidOperationException("Message cannot be empty.");
            }

            string json = Jil.JSON.Serialize<TMessage>(message, Jil.Options.ExcludeNulls);
            publisher.Publish(topic, json);
        }
    }
}
