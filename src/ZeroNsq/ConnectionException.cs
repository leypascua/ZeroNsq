using System;
using System.Collections.Generic;
using System.Text;

namespace ZeroNsq
{
    public class ConnectionException : Exception 
    {
        public ConnectionException(string message) : base(message) { }
    }
}
