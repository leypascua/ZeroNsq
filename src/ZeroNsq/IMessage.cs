namespace ZeroNsq
{
    /// <summary>
    /// An abstraction of NSQ's incoming message
    /// </summary>
    public interface IMessage
    {
        /// <summary>
        /// Gets the total number of attempts
        /// </summary>
        short Attempts { get; }

        /// <summary>
        /// Gets the body of the message
        /// </summary>
        byte[] Body { get; }

        /// <summary>
        /// Gets the ID of the message as a UTF8 string
        /// </summary>
        string IdString { get; }

        /// <summary>
        /// Gets the timestamp of the message as provided by NSQ
        /// </summary>
        long Timestamp { get; }

        /// <summary>
        /// Deserializes the message into an instance of TResult
        /// </summary>
        /// <typeparam name="TResult">The type of object to be used in deserialization</typeparam>
        /// <returns>The deserialized message</returns>
        TResult Deserialize<TResult>() where TResult : class, new();

        /// <summary>
        /// Converts the body of the message to a UTF8 string
        /// </summary>
        /// <returns>The UTF8 encoded string value of the body</returns>
        string ToUtf8String();
    }
}