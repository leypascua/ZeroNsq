using System;
using System.Threading.Tasks;

namespace ZeroNsq
{
    /// <summary>
    /// An abstraction for NSQ's subscriber (consumer)
    /// </summary>
    public interface ISubscriber : IDisposable
    {
        /// <summary>
        /// Gets if the subscriber is active
        /// </summary>
        bool IsActive { get; }

        /// <summary>
        /// Sets the connection error callback
        /// </summary>
        /// <param name="callback">The callback</param>
        /// <returns>The current instance</returns>
        ISubscriber OnConnectionError(Action<ConnectionErrorContext> callback);

        /// <summary>
        /// Sets the message received callback.
        /// </summary>
        /// <param name="callback">The callback</param>
        /// <returns>The current instance</returns>
        ISubscriber OnMessageReceived(Action<IMessageContext> callback);

        /// <summary>
        /// Sets the asynchronous message received callback.
        /// </summary>
        /// <param name="callback">The callback</param>
        /// <returns>The current instance</returns>
        ISubscriber OnMessageReceivedAsync(Func<IMessageContext, Task> callback);

        /// <summary>
        /// Starts the subscriber
        /// </summary>
        void Start();

        /// <summary>
        /// Stops the subscriber.
        /// </summary>
        void Stop();
    }
}