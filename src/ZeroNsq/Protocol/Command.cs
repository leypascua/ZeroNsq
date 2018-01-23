using System;
using System.Collections.Generic;
using System.Text;

namespace ZeroNsq.Protocol
{
    public class Command : IRequest
    {
        private readonly byte[] _data;

        public Command(byte[] data)
        {
            _data = data;
        }

        public byte[] ToByteArray()
        {
            return _data;
        }
    }
}
