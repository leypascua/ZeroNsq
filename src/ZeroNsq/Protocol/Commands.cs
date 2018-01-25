using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using ZeroNsq.Helpers;

namespace ZeroNsq.Protocol
{
    public static class Commands
    {
        public static readonly byte[] CLS = Encoding.ASCII.GetBytes("CLS\n");
        public static readonly byte[] IDENTIFY = Encoding.ASCII.GetBytes("IDENTIFY\n");
        public static readonly byte[] MAGIC_V2 = Encoding.ASCII.GetBytes("  V2");
        public static readonly byte[] PUB = Encoding.ASCII.GetBytes("PUB ");
        public static readonly byte[] NOP = Encoding.ASCII.GetBytes("NOP\n");
        public static readonly byte[] SUB = Encoding.ASCII.GetBytes("SUB ");
        public static readonly byte[] RDY = Encoding.ASCII.GetBytes("RDY ");
        public static readonly byte[] FIN = Encoding.ASCII.GetBytes("FIN ");
        public static readonly byte[] REQ = Encoding.ASCII.GetBytes("REQ ");
        public static readonly byte[] TOUCH = Encoding.ASCII.GetBytes("TOUCH ");
        public static readonly byte[] SPACE = Encoding.ASCII.GetBytes(" ");
        public static readonly byte[] LF = Encoding.ASCII.GetBytes("\n");

        public static Command New(byte[] data)
        {
            return new Command(data);
        }

        public static Command Finish(byte[] messageId)
        {
            using (var ms = new MemoryStream())
            {
                ms.WriteBytes(FIN);
                ms.WriteBytes(messageId);
                ms.WriteBytes(LF);

                return new Command(ms.ToArray());
            }
        }

        public static Command Requeue(byte[] messageId, int timeout)
        {
            using (var ms = new MemoryStream())
            {
                ms.WriteBytes(REQ);
                ms.WriteBytes(messageId);
                ms.WriteBytes(SPACE);
                ms.WriteUtf8(timeout.ToString());
                ms.WriteBytes(LF);

                return new Command(ms.ToArray());
            }
        }

        public static Command Touch(byte[] messageId)
        {
            using (var ms = new MemoryStream())
            {
                ms.WriteBytes(TOUCH);
                ms.WriteBytes(messageId);
                ms.WriteBytes(LF);

                return new Command(ms.ToArray());
            }
        }
    }
}
