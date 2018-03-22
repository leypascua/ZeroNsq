using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using ZeroNsq.Protocol;

namespace ZeroNsq
{
    public interface INsqConnection
    {
        bool IsConnected { get; }

        Task ConnectAsync();

        Task SendRequestAsync(IRequest request);

        void Close();

        Frame ReadFrame();

        INsqConnection OnMessageReceived(Action<Message> callback);
    }
}
