using System;

namespace ZeroNsq
{
    /// <summary>
    /// An abstraction for publishing messages to a topic.
    /// </summary>
    public interface IPublisher : IDisposable
    {
        /// <summary>
        /// Publishes the message to the specified topic
        /// </summary>
        /// <param name="topic">The target topic</param>
        /// <param name="message">The message to be published</param>
        void Publish(string topic, byte[] message);

        /// <summary>
        /// Publishes the message to the specified topic
        /// </summary>
        /// <param name="topic">The target topic</param>
        /// <param name="utf8String">The message to be published</param>
        void Publish(string topic, string utf8String);

        /// <summary>
        /// Publishes the message to the specified topic
        /// </summary>
        /// <typeparam name="TMessage">The type of message to be serialized into UTF8 JSON</typeparam>
        /// <param name="topic">The target topic</param>
        /// <param name="message">The message to be published</param>
        void PublishJson<TMessage>(string topic, TMessage message) where TMessage : class, new();
    }
}