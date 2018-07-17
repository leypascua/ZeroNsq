using System;
using System.Threading.Tasks;
using ZeroNsq.Protocol;

namespace ZeroNsq.Internal
{
    public class TcpPublisher : IPublisher
    {
        private bool disposedValue = false; // To detect redundant calls
        private INsqConnection _connection;
        private readonly ConnectionOptions _options;

        public TcpPublisher(string host, int port) : this(host, port, ConnectionOptions.Default) { }

        public TcpPublisher(string host, int port, ConnectionOptions options) : this(new NsqConnectionProxy(host, port, options), options) { }

        public TcpPublisher(INsqConnection connection, ConnectionOptions options)
        {
            _connection = connection;
            _options = options;
        }

        public async Task PublishAsync(string topic, byte[] message)
        {
            await _connection.SendRequestAsync(new Publish(topic, message));
        }

        private void Publish(string topic, byte[] message)
        {
            Task.Run(async () => await PublishAsync(topic, message)).Wait();
        }

        #region IDisposable Support

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    try
                    {
                        Task.Run(() => _connection.CloseAsync()).Wait();
                    }
                    catch {}
                    
                    _connection = null;
                }

                disposedValue = true;
            }
        }

        // TODO: override a finalizer only if Dispose(bool disposing) above has code to free unmanaged resources.
        // ~Publisher() {
        //   // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
        //   Dispose(false);
        // }

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);            
            // GC.SuppressFinalize(this);
        }
        #endregion
    }
}
