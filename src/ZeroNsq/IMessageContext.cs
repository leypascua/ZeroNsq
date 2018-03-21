using System.Threading.Tasks;

namespace ZeroNsq
{
    /// <summary>
    /// An abstraction of NSQ's incoming message and available actions
    /// </summary>
    public interface IMessageContext
    {
        /// <summary>
        /// Gets the message
        /// </summary>
        IMessage Message { get; }

        /// <summary>
        /// Gets the topic name
        /// </summary>
        string TopicName { get; }

        /// <summary>
        /// Gets the channel name
        /// </summary>
        string ChannelName { get; }

        /// <summary>
        /// Advises NSQD that the handler has completed processing the message
        /// </summary>
        /// <exception cref="ZeroNsq.ConnectionException">Throws on a connection error.</exception>
        /// <exception cref="ZeroNsq.RequestException">Throws on a request error</exception>
        void Finish();

        /// <summary>
        /// Advises NSQD that the handler has completed processing the message
        /// </summary>
        /// <exception cref="ZeroNsq.ConnectionException">Throws on a connection error.</exception>
        /// <exception cref="ZeroNsq.RequestException">Throws on a request error</exception>
        Task FinishAsync();

        /// <summary>
        /// Advises NSQD to requeue the message due to a failure
        /// </summary>
        /// <exception cref="ZeroNsq.ConnectionException">Throws on a connection error.</exception>
        /// <exception cref="ZeroNsq.RequestException">Throws on a request error</exception>
        /// <exception cref="ZeroNsq.MessageRequeueException">Thrown when requeue limit is breached.</exception>
        void Requeue();

        /// <summary>
        /// Advises NSQD to requeue the message due to a failure
        /// </summary>
        /// <exception cref="ZeroNsq.ConnectionException">Throws on a connection error.</exception>
        /// <exception cref="ZeroNsq.RequestException">Throws on a request error</exception>
        /// <exception cref="ZeroNsq.MessageRequeueException">Thrown when requeue limit is breached.</exception>
        Task RequeueAsync();

        /// <summary>
        /// Advises NSQD that the handler has not completed processing the message to
        /// prevent a timeout.
        /// </summary>
        /// <exception cref="ZeroNsq.ConnectionException">Throws on a connection error.</exception>
        /// <exception cref="ZeroNsq.RequestException">Throws on a request error</exception>
        void Touch();

        /// <summary>
        /// Advises NSQD that the handler has not completed processing the message to
        /// prevent a timeout.
        /// </summary>
        /// <exception cref="ZeroNsq.ConnectionException">Throws on a connection error.</exception>
        /// <exception cref="ZeroNsq.RequestException">Throws on a request error</exception>
        Task TouchAsync();
    }
}
