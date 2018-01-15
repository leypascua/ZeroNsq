using System;
using System.Collections.Generic;
using System.Text;

namespace ZeroNsq
{
    /// <summary>
    /// Denotes of the type of frame that was received from a socket connection.
    /// </summary>
    public enum FrameType
    {
        Response = 0,
        Error = 1,
        Message = 2
    }
}
