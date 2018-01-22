using System;
using System.Collections.Generic;
using System.Text;
using ZeroNsq.Protocol;

namespace ZeroNsq
{
    public class Publisher : IDisposable
    {
        private bool disposedValue = false; // To detect redundant calls
        private NsqdConnection _connection;

        public Publisher(string host, int port, ConnectionOptions options)
        {
            _connection = new NsqdConnection(host, port, options);
        }

        public void Publish(string topic, byte[] message)
        {
            EnsureOpenConnection(_connection);

            _connection.SendRequest(new Publish(topic, message));
            var frame = _connection.ReadFrame();

            if (frame.Type != FrameType.Response)
            {
                throw new ProtocolViolationException(
                    string.Format("Unexpected frame type received when sending a command: {0}", frame.Type.ToString()));
            }

            
        }

        private static void EnsureOpenConnection(NsqdConnection conn)
        {
            if (conn.IsConnected) return;

            conn.Connect();
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
