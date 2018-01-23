using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using ZeroNsq.Protocol;

namespace ZeroNsq
{
    public class NsqdConnection : INsqConnection, IDisposable
    {
        private const int DefaultThreadSleepTime = 100;        
        private const int MaxLastResponseFetchCount = 128;
        private readonly DnsEndPoint _endpoint;
        private readonly ConnectionOptions _options;
        private ConnectionResource _connectionResource;
        private bool _isIdentified = false;
        private bool _disposedValue = false; // To detect redundant calls         
        private ConcurrentQueue<Frame> _receivedFramesQueue = new ConcurrentQueue<Frame>();
        private Task _workerTask;
        private CancellationTokenSource _workerCancellationTokenSource;

        public NsqdConnection(string host, int port, ConnectionOptions options = null) 
            : this(new DnsEndPoint(host, port), options) { }

        public NsqdConnection(DnsEndPoint endpoint, ConnectionOptions options = null)
        {
            _endpoint = endpoint;
            _options = ConnectionOptions.SetDefaults(options);                        
        }

        public bool IsConnected
        {
            get
            {
                bool workerIsRunning =
                    _workerTask != null &&                    
                    !_workerTask.IsFaulted && 
                    !_workerTask.IsCompleted; 

                return _connectionResource != null &&
                       _connectionResource.IsInitialized &&
                       _isIdentified &&
                       workerIsRunning &&
                       !_disposedValue;
            }
        }

        public void Connect()
        {
            if (IsConnected) return;

            Initialize(this);
            PerformHandshake(_options);

            // start the worker and wait for incoming messages
            _workerCancellationTokenSource = new CancellationTokenSource();

            _workerTask = Task.Factory.StartNew(
                s => WorkerLoop(), 
                TaskCreationOptions.LongRunning, 
                _workerCancellationTokenSource.Token
            );
        }

        public void SendRequest(IRequest request)
        {
            SendRequest(request, isForced: false);
        }

        public Frame ReadFrame()
        {   
            int attempts = 0;

            do
            {
                Frame nextFrame = null;

                if (_receivedFramesQueue.Count > 0)
                {
                    _receivedFramesQueue.TryDequeue(out nextFrame);
                }

                // return the last retrieved frame.
                if (nextFrame != null) return nextFrame;

                if (!IsConnected)
                {
                    throw new ConnectionException(ConnectionException.ClosedBeforeResponseReceived);
                }

                if (attempts <= MaxLastResponseFetchCount)
                {
                    Thread.Sleep(DefaultThreadSleepTime);

                    if (!_connectionResource.IsReaderBusy)
                    {
                        attempts++;
                    }
                }
            }
            while (attempts <= MaxLastResponseFetchCount);

            // did not receive a frame in a timely manner.
            return null;
        }

        public void Close()
        {
            if (!IsConnected) return;
            DispatchCls();

            if (_workerCancellationTokenSource != null)
            {
                _workerCancellationTokenSource.Cancel();
                _workerCancellationTokenSource.Dispose();
                _workerCancellationTokenSource = null;

                try
                {
                    _workerTask.Dispose();
                }
                catch { }

                _workerTask = null;
            }

            if (_connectionResource != null)
            {
                _connectionResource.Dispose();
                _connectionResource = null;
            }

            _isIdentified = false;
        }

        internal void SendRequest(byte[] request)
        {
            SendRequest(request, false);
        }

        private void DispatchCls()
        {
            try
            {
                // ignore all errors for this command.
                _connectionResource.WriteBytes(Commands.CLS);
                Thread.Sleep(TimeSpan.FromSeconds(2));
            }
            catch { }
        }

        private void SendRequest(IRequest request, bool isForced)
        {
            SendRequest(request.ToByteArray(), isForced);

            bool isResponseExpected = request is IRequestWithResponse;
            if (!isResponseExpected) return;

            HandleResponse();
        }

        private void HandleResponse()
        {
            Response response = FetchLastResponse();

            if (response == null)
            {
                if (!IsConnected)
                {
                    throw new ConnectionException(ConnectionException.ClosedBeforeResponseReceived);
                }

                throw new ProtocolViolationException("A valid response was not received for the last sent request.");
            }

            if (!response.IsSuccessful)
            {
                throw new RequestException(response.Error);
            }
        }

        private Response FetchLastResponse()
        {   
            var frame = ReadFrame();
            if (frame == null)
            {
                return null;
            }

            if (frame.Type == FrameType.Response)
            {
                string ascii = frame.ToASCII();
                string error = ascii.StartsWith("E_") ? ascii : null;
                
                return new Response(frame.Data, error);
            }

            if (frame.Type == FrameType.Message)
            {
                Thread.Sleep(DefaultThreadSleepTime);
                _receivedFramesQueue.Enqueue(frame);
                return FetchLastResponse();
            }

            if (frame.Type == FrameType.Error)
            {
                return new Response(frame.Data, frame.ToASCII());
            }

            throw new NotSupportedException("Unsupported frame type: " + frame.Type.ToString());
        }

        private void WorkerLoop()
        {
            while (IsConnected)
            {
                Frame frame = null;

                try
                {
                    frame = _connectionResource.ReadFrame();
                }
                catch (ConnectionException)
                {
                    Close();                    
                    throw;
                }

                if (!IsConnected) return;
                if (frame == null) break;

                OnFrameReceived(frame);

                Thread.Sleep(DefaultThreadSleepTime);
            }
        }

        private void OnFrameReceived(Frame frame)
        {
            switch (frame.Type)
            {
                case FrameType.Response:
                    if (frame.Data.SequenceEqual(Response.Heartbeat))
                    {
                        SendRequest(Commands.NOP);
                        return;
                    }
                    break;
            }

            _receivedFramesQueue.Enqueue(frame);
        }

        private void PerformHandshake(ConnectionOptions options)
        {
            SendRequest(Commands.MAGIC_V2, isForced: true);
            SendRequest(new Identify
            {
                hostname = options.Hostname,
                client_id = options.ClientId,
                msg_timeout = options.MessageTimeout,
                heartbeat_interval = (int)TimeSpan.FromSeconds(options.HeartbeatIntervalInSeconds.Value).TotalMilliseconds
            }, isForced: true);

            var frame = _connectionResource.ReadFrame();
            if (frame.Type != FrameType.Response)
            {
                throw new ProtocolViolationException("Unexpected handshake response received: " + frame.ToASCII());
            }

            _isIdentified = true;
        }

        private void SendRequest(byte[] payload, bool isForced = false)
        {
            if (!isForced)
            {
                EnsureOpenConnection(this);
            }

            _connectionResource.WriteBytes(payload);
        }

        private static void Initialize(NsqdConnection conn, bool isForced = false)
        {
            if (!isForced)
            {
                if (conn.IsConnected) return;
            }

            if (conn._workerTask != null)
            {
                conn._workerTask.Dispose();
                conn._workerTask = null;
            }
            
            conn.Close();

            conn._connectionResource = new ConnectionResource(conn._endpoint)
                .Initialize(isForced);
        }

        private static void EnsureOpenConnection(NsqdConnection instance)
        {
            if (!instance.IsConnected)
            {
                if (instance._workerTask != null && instance._workerTask.IsFaulted)
                {
                    var flatException = instance._workerTask.Exception.Flatten();

                    var knownErrors = flatException.InnerExceptions
                        .Where(error => error is BaseException)
                        .Select(x => x.Message)
                        .ToArray();

                    if (knownErrors.Any())
                    {
                        string message = string.Join(";", knownErrors);
                        throw new ConnectionException("One or more errors occurred. " + message);
                    }

                    throw flatException;
                }

                throw new ConnectionException("Unable to perform request with a closed connection.");
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
                    Close();
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
