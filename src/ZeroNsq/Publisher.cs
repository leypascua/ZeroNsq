using System;
using ZeroNsq.Protocol;

namespace ZeroNsq
{
    public class Publisher : IDisposable
    {
        private bool disposedValue = false; // To detect redundant calls
        private INsqConnection _connection;
        private readonly ConnectionOptions _options;

        public Publisher(string host, int port, ConnectionOptions options) : this(new NsqConnectionProxy(host, port, options), options) { }

        public Publisher(INsqConnection connection, ConnectionOptions options)
        {
            _connection = connection;
            _options = options;
        }

        public void Publish(string topic, byte[] message)
        {
            _connection.SendRequest(new Publish(topic, message));
        }

        public void Publish(string topic, string utf8String)
        {
            _connection.SendRequest(new Publish(topic, utf8String));
        }

        #region IDisposable Support

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    // TODO: dispose managed state (managed objects).
                }

                // TODO: free unmanaged resources (unmanaged objects) and override a finalizer below.
                // TODO: set large fields to null.

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
            // TODO: uncomment the following line if the finalizer is overridden above.
            // GC.SuppressFinalize(this);
        }
        #endregion
    }
}
