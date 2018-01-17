using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using ZeroNsq.Commands;

namespace ZeroNsq
{
    public class TcpClientConnection : IClientConnection, IDisposable
    {
        private bool _disposedValue = false; // To detect redundant calls
        private readonly DnsEndPoint _endpoint;
        private TcpClient _tcpClient;
        private Stream _networkStream;
        private FrameReader _reader;
        private Action<Message> _onMessageReceivedCallback = msg => { };

        public TcpClientConnection(string host, int port) : this(new DnsEndPoint(host, port)) { }

        public TcpClientConnection(DnsEndPoint endpoint)
        {
            _endpoint = endpoint;
        }

        public bool IsOpen
        {
            get
            {
                return _tcpClient != null && 
                       _tcpClient.Connected;
            }
        }

        public void Open()
        {
            if (!IsOpen)
            {
                _networkStream = EstablishConnection(this);
                _reader = new FrameReader(_networkStream);
            }
        }

        public Frame ReadFrame()
        {
            FrameReader reader = OpenReader(this);
            return reader.ReadFrame();
        }

        public void Write(byte[] buffer)
        {
            var writer = OpenWriter(this);
            _networkStream.Write(buffer, 0, buffer.Length);
        }

        public ProtocolResponse SubmitRequest(IProtocolCommand cmd)
        {
            Open();
            byte[] buffer = cmd.ToByteArray();
            _networkStream.Write(buffer, 0, buffer.Length);

            bool isResponseExpected = cmd is IProtocolCommandWithResponse;

            if (!isResponseExpected) return ProtocolResponse.Success;

            Frame frame = _reader.ReadFrame();

            switch (frame.Type)
            {
                case FrameType.Response:                    
                case FrameType.Error:
                    return new ProtocolResponse(frame.Data);
                case FrameType.Message:
                    ///TODO: Just push to a queue so we don't
                    ///      lose any incoming message while
                    ///      we're waiting for a response for the
                    ///      last submitted request.
                    if (_onMessageReceivedCallback != null)
                    {
                        _onMessageReceivedCallback(frame.ToMessage());
                    }
                    return ProtocolResponse.Success;
                default:
                    throw new ProtocolViolationException("Unsupported frame type.");
            }
        }

        public IClientConnection OnMessageReceived(Action<Message> callback)
        {
            if (callback != null)
            {
                _onMessageReceivedCallback = callback;
            }

            return this;
        }

        private static Stream EstablishConnection(TcpClientConnection conn)
        {
            if (conn._tcpClient == null)
            {
                conn._tcpClient = new TcpClient();
            }

            if (!conn._tcpClient.Connected)
            {
                conn._tcpClient.ConnectAsync(conn._endpoint.Host, conn._endpoint.Port).Wait();
            }

            return conn._tcpClient.GetStream();
        }

        private static FrameReader OpenReader(TcpClientConnection connection)
        {
            connection.Open();
            return connection._reader;
        }

        private static Stream OpenWriter(TcpClientConnection connection)
        {
            connection.Open();
            return connection._networkStream;
        }

        private static void CloseConnection(IClientConnection connection)
        {
            try
            {
                connection.Write(Close.CommandHeader);
            }
            catch
            {
                // ignore all errors since we're closing the 
                // connection anyway.
            }
        }

        #region IDisposable Support

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposedValue)
            {
                if (disposing)
                {
                    // TODO: dispose managed state (managed objects).
                    if (IsOpen)
                    {
                        CloseConnection(this);
                        _tcpClient.Dispose();
                        _tcpClient = null;
                    }
                }

                // TODO: free unmanaged resources (unmanaged objects) and override a finalizer below.
                // TODO: set large fields to null.

                _disposedValue = true;
            }
        }

        // TODO: override a finalizer only if Dispose(bool disposing) above has code to free unmanaged resources.
        // ~TcpClientConnection() {
        //   // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
        //   Dispose(false);
        // }

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
            // TODO: uncomment the following line if the finalizer is overridden above.
            // GC.SuppressFinalize(this);
        }

        #endregion
    }
}
