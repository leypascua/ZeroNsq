using System;
using System.Collections.Generic;
using System.Text;

namespace ZeroNsq
{
    public class ProtocolViolationException : Exception
    {
        public ProtocolViolationException(string message) : base(message) { }
    }
}
