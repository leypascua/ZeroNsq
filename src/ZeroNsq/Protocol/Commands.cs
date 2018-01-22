using System;
using System.Collections.Generic;
using System.Text;

namespace ZeroNsq.Protocol
{
    public static class Commands
    {
        public static readonly byte[] CLS = Encoding.ASCII.GetBytes("CLS\n");
        public static readonly byte[] IDENTIFY = Encoding.ASCII.GetBytes("IDENTIFY\n");
        public static readonly byte[] MAGIC_V2 = Encoding.ASCII.GetBytes("  V2");
        public static readonly byte[] PUB = Encoding.ASCII.GetBytes("PUB ");
        public static readonly byte[] NOP = Encoding.ASCII.GetBytes("NOP\n");
    }
}
