using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using ZeroNsq.Helpers;

namespace ZeroNsq.Protocol
{
    public class Ready : IRequest
    {
        const int DefaultMaxInFlight = 2500;

        public Ready(int count)
        {
            MaxInFlight = count <= 0 ? 1 : Math.Min(count, DefaultMaxInFlight);
        }

        public int MaxInFlight { get; set; }

        public byte[] ToByteArray()
        {
            using (var ms = new MemoryStream())
            {
                ms.WriteBytes(Commands.RDY);
                ms.WriteASCII(MaxInFlight.ToString() + "\n");

                return ms.ToArray();
            }
        }
    }
}
