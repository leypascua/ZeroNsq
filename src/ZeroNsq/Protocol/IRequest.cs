using System;
using System.Collections.Generic;
using System.Text;

namespace ZeroNsq.Protocol
{
    public interface IRequest
    {
        byte[] ToByteArray();
    }

    public interface IRequestWithResponse : IRequest { }
}
