using System;
using System.Collections.Generic;
using System.Text;

namespace ZeroNsq
{
    public interface IClientConnection
    {
        bool IsOpen { get; }
        void Open();
        Frame ReadFrame();
        void Write(byte[] buffer);
        ProtocolResponse SubmitRequest(IProtocolCommand cmd);
        IClientConnection OnMessageReceived(Action<Message> callback);
    }
}
