using System;
using System.Collections.Generic;
using System.Text;

namespace ZeroNsq
{
    public class RequestException : Exception
    {
        public RequestException(string message) : base(message) { }
    }
}
