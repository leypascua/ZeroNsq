using System;
using System.Collections.Generic;
using System.Text;

namespace ZeroNsq
{
    public interface IProtocolCommand
    {
        byte[] ToByteArray();
    }

    public interface IProtocolCommandWithResponse : IProtocolCommand { }
}
