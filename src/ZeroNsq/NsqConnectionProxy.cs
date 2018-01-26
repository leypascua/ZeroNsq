using System;
using System.Net.Sockets;
using System.Threading;
using ZeroNsq.Protocol;
using ZeroNsq.Helpers;

namespace ZeroNsq
{
    public class NsqConnectionProxy : INsqConnection, IDisposable
    {
        private readonly ConnectionOptions _options;
        private NsqdConnection _rawConnection;        

        public NsqConnectionProxy(string host, int port, ConnectionOptions options)
        {
            _options = ConnectionOptions.SetDefaults(options);
            _rawConnection = new NsqdConnection(host, port, _options);

            Id = GenerateId(host, port);
        }

        public string Id { get; private set; }

        public int ReconnectionAttempts { get; private set; }

        public bool IsConnected
        {
            get
            {
                return _rawConnection != null && _rawConnection.IsConnected;
            }
        }

        public static string GenerateId(string host, int port)
        {
            return string.Format("{0}|{1}", host, port).Md5();
        }

        public Frame ReadFrame()
        {
            Frame result = null;

            Execute(conn =>
            {
                result = conn.ReadFrame();
            });

            return result;
        }

        public void SendRequest(IRequest request)
        {
            Execute(conn =>
            {
                conn.SendRequest(request);
            });
        }

        public INsqConnection OnMessageReceived(Action<Message> callback)
        {
            _rawConnection.OnMessageReceived(callback);
            return this;
        }

        void INsqConnection.Connect()
        {
            if (IsConnected) return;

            int backoffTime = _options.InitialBackoffTimeInSeconds * ReconnectionAttempts;
            Thread.Sleep(TimeSpan.FromSeconds(backoffTime));

            _rawConnection.Connect();
        }

        void INsqConnection.Close()
        {
            if (_rawConnection != null)
            {
                _rawConnection.Close();
                _rawConnection.Dispose();
                _rawConnection = null;
            }
        }

        private void Execute(Action<INsqConnection> callback)
        {
            try
            {
                (this as INsqConnection).Connect();
                callback(_rawConnection);
                ReconnectionAttempts = 0;
            }
            catch (SocketException ex)
            {
                if (ReconnectionAttempts == 0)
                {
                    // Immediately terminate if there's a socket
                    // error. It is likely due to an unreachable host.
                    throw;
                }

                AttemptRetry(ex, callback);
            }
            catch (ConnectionException ex)
            {
                AttemptRetry(ex, callback);
            }
        }

        private void AttemptRetry(Exception error, Action<INsqConnection> callback)
        {
            if (ReconnectionAttempts > _options.MaxClientReconnectionAttempts)
            {
                throw error;
            }

            ReconnectionAttempts += 1;
            Execute(callback);
        }

        #region IDisposable members

        public void Dispose()
        {
            (this as INsqConnection).Close();
        }

        #endregion
    }
}
