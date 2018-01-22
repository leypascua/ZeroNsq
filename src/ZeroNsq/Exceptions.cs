using System;
using System.Collections.Generic;
using System.Text;

namespace ZeroNsq
{
    public abstract class BaseException : Exception
    {
        public BaseException(string message) : base(message) { }
    }

    public class ConnectionException : BaseException
    {
        public const string ClosedBeforeResponseReceived = "The connection was forcibly terminated by the host before a valid response was received.";

        public ConnectionException(string message) : base(message) { }
    }

    public class RequestException : BaseException
    {
        public RequestException(string message) : base(message) { }
    }

    public class ProtocolViolationException : BaseException
    {
        public ProtocolViolationException(string message) : base(message) { }
    }
}
