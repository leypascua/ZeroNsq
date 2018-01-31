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
        public ConnectionErrorContext(INsqConnection connection, Exception error)
        {
            Connection = connection;
            Error = error;
        }

        /// <summary>
        /// Gets the connection instance
        /// </summary>
        public INsqConnection Connection { get; private set; }

        /// <summary>
        /// Gets the caught exception
        /// </summary>
        public Exception Error { get; private set; }

        public override string ToString()
        {
            return Error.ToString();
        }
    }
}
