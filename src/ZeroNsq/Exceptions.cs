using System;
using System.Collections.Generic;
using System.Text;

namespace ZeroNsq
{
    /// <summary>
    /// Base class for all ZeroNsq known exceptions
    /// </summary>
    public abstract class BaseException : Exception
    {
        public BaseException(string message) : base(message) { }
    }

    /// <summary>
    /// A socket-related exception
    /// </summary>
    public class SocketException : BaseException
    {
        public SocketException(string message) : base(message) { }
    }

    /// <summary>
    /// A connection-related exception
    /// </summary>
    public class ConnectionException : BaseException
    {
        public const string ClosedBeforeResponseReceived = "The connection was forcibly terminated by the host before a valid response was received.";

        public ConnectionException(string message) : base(message) { }
    }

    /// <summary>
    /// An API request related exception
    /// </summary>
    public class RequestException : BaseException
    {
        public RequestException(string message) : base(message) { }
    }

    /// <summary>
    /// An NSQD protocol violation exception
    /// </summary>
    public class ProtocolViolationException : BaseException
    {
        public ProtocolViolationException(string message) : base(message) { }
    }

    /// <summary>
    /// A requeue exception thrown when attempts have exceeded the maximum number as configured
    /// </summary>
    public class MessageRequeueException : BaseException
    {
        public MessageRequeueException(string message) : base(message) { }
    }
}
