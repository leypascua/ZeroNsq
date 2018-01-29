using System;
using System.Collections.Generic;
using System.Text;

namespace ZeroNsq
{
    public class ConnectionErrorContext
    {
        public ConnectionErrorContext(INsqConnection connection, Exception error)
        {
            Connection = connection;
            Error = error;
        }

        public INsqConnection Connection { get; private set; }

        public Exception Error { get; private set; }
    }
}
