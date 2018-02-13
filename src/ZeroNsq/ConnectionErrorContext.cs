using System;

namespace ZeroNsq
{
    /// <summary>
    /// Encapsulates a connection-based error
    /// </summary>
    public class ConnectionErrorContext
    {
        /// <summary>
        /// Creates an instance
        /// </summary>
        /// <param name="connection">The connection</param>
        /// <param name="error">The exception</param>
        public ConnectionErrorContext(INsqConnection connection, Exception error, string channelName)
        {
            Connection = connection;
            Error = error;
            ChannelName = channelName;
        }

        /// <summary>
        /// Gets the connection instance
        /// </summary>
        public INsqConnection Connection { get; private set; }

        /// <summary>
        /// Gets the caught exception
        /// </summary>
        public Exception Error { get; private set; }

        /// <summary>
        /// Gets the name of the channel which owns the connection that caused the error.
        /// </summary>
        public string ChannelName { get; private set; }

        /// <summary>
        /// Returns the string equivalent of the object instance
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return Error.ToString();
        }
    }
}
