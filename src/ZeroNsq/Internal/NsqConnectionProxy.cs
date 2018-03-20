using System;
using System.Threading;
using System.Threading.Tasks;
using ZeroNsq.Helpers;
using ZeroNsq.Protocol;

namespace ZeroNsq.Internal
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

        public async Task SendRequestAsync(IRequest request)
        {
            await ExecuteAsync(async conn =>
            {
                await conn.SendRequestAsync(request);
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

            Wait.For(TimeSpan.FromSeconds(backoffTime))
                .Then(() => _rawConnection.Connect())
                .Start();
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
            Func<INsqConnection, Task> asyncCallback = conn => Task.Run(() => callback(conn));
            ExecuteAsync(asyncCallback).Wait();
        }

        private async Task ExecuteAsync(Func<INsqConnection, Task> callback)
        {
            try
            {
                (this as INsqConnection).Connect();
                await callback(_rawConnection);
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

                await AttemptRetry(ex, callback);
            }
            catch (ConnectionException ex)
            {
                await AttemptRetry(ex, callback);
            }
        }

        private async Task AttemptRetry(Exception error, Func<INsqConnection, Task> callback)
        {
            if (_options.MaxClientReconnectionAttempts > 0)
            {
                if (ReconnectionAttempts > _options.MaxClientReconnectionAttempts)
                {
                    throw error;
                }
            }

            ReconnectionAttempts += 1;
            await ExecuteAsync(callback);
        }

        #region IDisposable members

        public void Dispose()
        {
            (this as INsqConnection).Close();
        }

        #endregion
    }
}
