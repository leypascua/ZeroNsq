using System;
using System.Collections.Generic;
using System.Net;
using System.Text;

namespace ZeroNsq
{
    public class NsqdConnection : IDisposable
    {
        private readonly DnsEndPoint _endpoint;
        private bool _disposedValue = false; // To detect redundant calls

        public NsqdConnection(DnsEndPoint endpoint)
        {
            _endpoint = endpoint;
        }

        public void Connect()
        {
            
        }

        #region IDisposable Support
        
        protected virtual void Dispose(bool disposing)
        {
            if (!_disposedValue)
            {
                if (disposing)
                {
                    // TODO: dispose managed state (managed objects).
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
