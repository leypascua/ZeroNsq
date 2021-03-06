﻿using System;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using ZeroNsq.Protocol;

namespace ZeroNsq.Internal
{
    public class ConnectionResource : IDisposable
    {
        private DnsEndPoint _endpoint;
        private TcpClient _tcpClient;        
        private NetworkStream _networkStream;
        private FrameReader _frameReader;
        private object _connectionLock = new object();
        private object _readerLock = new object();
        private object _writerLock = new object();

        public ConnectionResource(DnsEndPoint endpoint)
        {
            _endpoint = endpoint;
        }

        public bool IsInitialized
        {
            get
            {
                return _tcpClient != null &&
                       _tcpClient.Connected;
            }
        }

        public bool IsReaderBusy
        {
            get
            {
                return IsInitialized && _frameReader.IsBusy;
            }
        }

        public async Task<ConnectionResource> InitializeAsync(bool isForced = false)
        {
            if (!isForced)
            {
                if (IsInitialized) return this;
            }

            ReleaseResources();

            _tcpClient = new TcpClient();

            try
            {
                LogProvider.Current.Debug(string.Format("Connecting to Host={0}; Port={1};", _endpoint.Host, _endpoint.Port));
                await _tcpClient.ConnectAsync(_endpoint.Host, _endpoint.Port).ConfigureAwait(false);
            }
            catch (System.Net.Sockets.SocketException ex)
            {
                throw new SocketException(ex.Message);
            }

            _networkStream = _tcpClient.GetStream();
            _frameReader = new FrameReader(_networkStream);
            LogProvider.Current.Info(string.Format("Connection established. Host={0}; Port={1};", _endpoint.Host, _endpoint.Port));

            return this;
        }
        
        public async Task<Frame> ReadFrameAsync()
        {
            return await _frameReader.ReadFrameAsync().ConfigureAwait(false);
        }

        public async Task WriteBytesAsync(byte[] payload)
        {
            await _networkStream.WriteAsync(payload, 0, payload.Length).ConfigureAwait(false);
        }

        public void Dispose()
        {
            ReleaseResources();
        }

        private void ReleaseResources()
        {
            if (IsInitialized)
            {
                _frameReader.Dispose();
                _frameReader = null;

                _networkStream.Close();
                _networkStream.Dispose();
                _networkStream = null;                

                _tcpClient.Close();
                (_tcpClient as IDisposable).Dispose();
                _tcpClient = null;
            }
        }
    }
}
