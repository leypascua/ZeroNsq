using System;
using System.Collections.Generic;
using System.Text;

namespace ZeroNsq.Commands
{
    public class MagicV2 : IProtocolCommand
    {
        public static readonly byte[] CommandHeader = Encoding.ASCII.GetBytes("  V2");

        public byte[] ToByteArray()
        {
            return CommandHeader;
        }
    }
}
