﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace ZeroNsq.Test.FrameReaderTests
{
    public static class Utils
    {
        public static Stream MessageFrame(string data)
        {
            byte[] dataBuffer = Encoding.UTF8.GetBytes(data);
            byte[] sizeBuffer = BitConverter.GetBytes(dataBuffer.Length);
            byte[] frameTypeBuffer = BitConverter.GetBytes((int)FrameType.Message);

            var stream = new MemoryStream();
            int offset = 0;

            stream.Write(sizeBuffer, offset, sizeBuffer.Length);
            stream.Write(frameTypeBuffer, offset, frameTypeBuffer.Length);
            stream.Write(dataBuffer, offset, dataBuffer.Length);

            stream.Position = 0;

            return stream;
        }
    }
}