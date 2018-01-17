using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using ZeroNsq.Commands;

namespace ZeroNsq
{
    public class NsqdConnection : IDisposable
    {   
        private static readonly byte[] Nop = Encoding.ASCII.GetBytes("NOP\n");        

        private readonly DnsEndPoint _endpoint;
        private readonly ConnectionOptions _options;
        private bool _disposedValue = false; // To detect redundant calls

        public NsqdConnection(string host, int port, ConnectionOptions options = null) 
            : this(new DnsEndPoint(host, port), options, null) { }

        public NsqdConnection(DnsEndPoint endpoint, ConnectionOptions options = null, IClientConnection clientConnection = null)
        {
            _endpoint = endpoint;
            _options = SetDefaultOptions(options);            
            Client = clientConnection ?? new TcpClientConnection(_endpoint);
        }

        protected virtual IClientConnection Client { get; private set; }

        public void Publish(string topicName, string utf8String)
        {
            this.Publish(topicName, Encoding.UTF8.GetBytes(utf8String));
        }

        public void Publish(string topicName, byte[] rawMessage)
        {
            var client = PrepareConnectedClient(this);
            var response = client.SubmitRequest(new Publish(topicName, rawMessage));

            if (!response.IsSuccessful)
            {
                throw new InvalidOperationException("Error received: " + response.Error);
            }
        }

        private static IClientConnection PrepareConnectedClient(NsqdConnection instance)
        {
            if (!instance.Client.IsOpen)
            {
                instance.Client.Open();
                instance.Client.Write(MagicV2.CommandHeader);
                Identify(instance.Client, SetDefaultOptions(instance._options));
            }

            return instance.Client;
        }

        private static void Identify(IClientConnection conn, ConnectionOptions options)
        {
            var response = conn.SubmitRequest(new Identify
            {
                hostname = options.Hostname,
                client_id = options.ClientId,
                msg_timeout = options.MessageTimeout
            });

            ///TODO: Handle Identify response.
        }

        private static ConnectionOptions SetDefaultOptions(ConnectionOptions options)
        {
            var opt = options ?? new ConnectionOptions();

            if (string.IsNullOrEmpty(opt.Hostname)) opt.Hostname = Dns.GetHostName();
            if (string.IsNullOrEmpty(opt.ClientId)) opt.ClientId = string.Format("{0}@{1}", opt.GetHashCode(), opt.Hostname);

            return opt;
        }

        #region IDisposable Support

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposedValue)
            {
                if (disposing)
                {
                    // TODO: dispose managed state (managed objects).
                    this.Client.Write(Close.CommandHeader);
                    if (this.Client is IDisposable)
                    {
                        var disposableClient = this.Client as IDisposable;
                        disposableClient.Dispose();
                        this.Client = null;
                    }
                }

                // TODO: free unmanaged resources (unmanaged objects) and override a finalizer below.
                // TODO: set large fields to null.

                _disposedValue = true;
            }
        }

        // TODO: override a finalizer only if Dispose(bool disposing) above has code to free unmanaged resources.
        // ~NsqdConnection() {
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
