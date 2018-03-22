using System;
using System.Threading.Tasks;

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
        Task PublishAsync(string topic, byte[] message);
    }
}