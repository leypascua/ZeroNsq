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
        void Finish();

        /// <summary>
        /// Advises NSQD to requeue the message due to a failure
        /// </summary>
        void Requeue();

        /// <summary>
        /// Advises NSQD that the handler has not completed processing the message to
        /// prevent a timeout.
        /// </summary>
        void Touch();
    }
}
