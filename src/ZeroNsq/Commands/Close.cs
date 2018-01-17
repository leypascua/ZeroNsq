using System;
using System.Collections.Generic;
using System.Text;

namespace ZeroNsq.Commands
{
    public class Close : IProtocolCommandWithResponse
    {
        public static readonly byte[] CommandHeader = Encoding.ASCII.GetBytes("CLS\n");

        public byte[] ToByteArray()
        {
            return CommandHeader;
        }
    }
}
