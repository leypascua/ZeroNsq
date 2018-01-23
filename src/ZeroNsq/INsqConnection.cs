using System;
using System.Collections.Generic;
using System.Text;
using ZeroNsq.Protocol;

namespace ZeroNsq
{
    public interface INsqConnection
    {
        bool IsConnected { get; }

        void Connect();

        void SendRequest(IRequest request);

        void Close();

        Frame ReadFrame();

        //INsqConnection OnMessageReceived(Action<Message> callback);
    }
}
