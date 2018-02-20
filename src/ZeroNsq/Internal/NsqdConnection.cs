using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using ZeroNsq.Helpers;
using ZeroNsq.Protocol;

namespace ZeroNsq.Internal
{
    public class NsqdConnection : INsqConnection, IDisposable
    {
        private const int DefaultThreadSleepTime = 100;        
        private const int MaxLastResponseFetchCount = 32;        
        private readonly DnsEndPoint _endpoint;
        private readonly ConnectionOptions _options;
        private readonly ManualResetEventSlim _frameReceivedResetEvent = new ManualResetEventSlim();
        private ConnectionResource _connectionResource;        
        private ConcurrentQueue<Frame> _receivedFramesQueue = new ConcurrentQueue<Frame>();
        private ConcurrentQueue<Message> _receivedMessagesQueue = new ConcurrentQueue<Message>();
        private Thread _workerThread;
        private bool _isWorkerThreadRunning = false;
        private CancellationTokenSource _workerCancellationTokenSource;        
        private Action<Message> _onMessageReceivedCallback = msg => { };
        private Action _onHeartbeatRespondedCallback;
        private bool _isIdentified = false;
        private bool _disposedValue = false; // To detect redundant calls         
        private BaseException _workerLoopException;        

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
                return _connectionResource != null &&
                       _connectionResource.IsInitialized &&
                       _isIdentified &&
                       _isWorkerThreadRunning &&
                       _workerThread != null &&
                       !_disposedValue;
            }
        }

        public async Task ConnectAsync()
        {
            if (IsConnected) return;

            Initialize(this);
            await PerformHandshakeAsync(_options);

            // start the worker and wait for incoming messages
            _workerCancellationTokenSource = new CancellationTokenSource();

            _isWorkerThreadRunning = true;
            _workerThread = new Thread(WorkerLoop);
            _workerThread.Start();
        }

        public async Task SendRequestAsync(IRequest request)
        {
            await SendRequestAsync(request, isForced: false);
        }

        public Frame ReadFrame()
        {   
            int attempts = 0;

            do
            {
                Frame nextFrame = null;

                if (_receivedFramesQueue.IsEmpty)
                {
                    if (!_frameReceivedResetEvent.IsSet)
                    {
                        _frameReceivedResetEvent.Reset();
                    }
                    
                    _frameReceivedResetEvent.Wait();
                }

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
                    Wait.For(TimeSpan.FromMilliseconds(DefaultThreadSleepTime))
                        .Start();

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

        public INsqConnection OnMessageReceived(Action<Message> callback)
        {
            if (callback != null)
            {
                _onMessageReceivedCallback = callback;
            }

            return this;
        }

        public INsqConnection OnHeartbeatResponded(Action callback)
        {
            if (callback != null)
            {
                _onHeartbeatRespondedCallback = callback;
            }

            return this;
        }

        public void Close()
        {
            if (!IsConnected) return;
            
            DispatchCls();

            _isWorkerThreadRunning = false;

            if (_workerCancellationTokenSource != null)
            {
                _workerCancellationTokenSource.Cancel();
                _workerCancellationTokenSource.Dispose();
                _workerCancellationTokenSource = null;
            }

            try
            {
                if (!_workerThread.Join(TimeSpan.FromSeconds(3)))
                {
                    _workerThread.Abort();
                }
            }
            catch { }

            _workerThread = null;

            if (_connectionResource != null)
            {
                _connectionResource.Dispose();
                _connectionResource = null;
            }

            _isIdentified = false;
            LogProvider.Current.Info("NsqdConnection is closed.");
        }

        internal async Task SendRequestAsync(IRequest request, bool isForced = false)
        {
            await SendRequestAsync(request.ToByteArray(), isForced);

            bool isResponseExpected = request is IRequestWithResponse;
            if (!isResponseExpected) return;

            HandleResponse();
        }

        private void DispatchCls()
        {
            try
            {
                LogProvider.Current.Debug("NsqdConnection: Advise CLS to daemon host");
                
                // ignore all errors for this command.
                _connectionResource.WriteBytes(Commands.CLS);

                // give enough time for the server to respond
                Wait.For(TimeSpan.FromSeconds(1))
                    .Start();
            }
            catch { }
        }

        private void HandleResponse()
        {
            Response response = FetchLastResponse();

            if (response == null)
            {
                if (!IsConnected)
                {
                    LogProvider.Current.Error(ConnectionException.ClosedBeforeResponseReceived);
                    throw new ConnectionException(ConnectionException.ClosedBeforeResponseReceived);
                }

                string protocolError = "A valid response was not received for the last sent request.";
                LogProvider.Current.Error(protocolError);
                throw new ProtocolViolationException(protocolError);
            }

            if (!response.IsSuccessful)
            {
                LogProvider.Current.Error(response.Error);
                throw new RequestException(response.Error);
            }
        }

        private Response FetchLastResponse()
        {   
            var frame = ReadFrame();
            if (frame == null)
            {
                LogProvider.Current.Warn("Frame not received in a timely manner.");
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
                _receivedFramesQueue.Enqueue(frame);
                return FetchLastResponse();
            }

            if (frame.Type == FrameType.Error)
            {
                return new Response(frame.Data, frame.ToASCII());
            }

            string unsupportedError = "Unsupported frame type: " + frame.Type.ToString();
            LogProvider.Current.Fatal(unsupportedError);
            throw new NotSupportedException(unsupportedError);
        }

        private void WorkerLoop()
        {
            while (IsConnected)
            {
                Frame frame = null;

                try
                {
                    LogProvider.Current.Debug("NsqdConnection.WorkerLoop is waiting for a frame");
                    frame = _connectionResource.ReadFrame();
                }
                catch (ObjectDisposedException)
                {   
                    break;
                }
                catch (SocketException ex)
                {
                    _workerLoopException = ex;
                    break;
                }
                catch (ConnectionException ex)
                {
                    Close();
                    _workerLoopException = ex;
                    break;
                }

                if (!IsConnected) break;
                if (frame == null) break;

                try
                {
                    Task.Run(() => OnFrameReceived(frame))
                        .Wait();
                }
                catch (AggregateException ex)
                {
                    LogProvider.Current.Debug("WorkerLoop Exception caught: " + ex.InnerException.Message);
                    throw ex.InnerException;
                }
            }

            _isWorkerThreadRunning = false;
            _isIdentified = false;
            LogProvider.Current.Warn("NsqdConnection worker loop terminated. Connection is idle.");

            if (_workerLoopException != null)
            {
                LogProvider.Current.Debug("NsqdConnection.WorkerLoop Exception: " + _workerLoopException.Message);
            }
        }

        private async Task OnFrameReceived(Frame frame)
        {
            LogProvider.Current.Debug("NsqdConnection.OnFrameReceived: " + frame.Type.ToString());

            switch (frame.Type)
            {
                case FrameType.Response:
                case FrameType.Error:
                    if (frame.Data.SequenceEqual(Response.Heartbeat))
                    {
                        LogProvider.Current.Debug("Heartbeat request received. Responding with NOP");
                        await SendRequestAsync(Commands.NOP);

                        if (_onHeartbeatRespondedCallback != null)
                        {
                            _onHeartbeatRespondedCallback();
                        }

                        return;
                    }                

                    if (frame.Type == FrameType.Error)
                    {
                        LogProvider.Current.Warn("Error frame received: " + frame.ToErrorCode());
                    }

                    _receivedFramesQueue.Enqueue(frame);
                    _frameReceivedResetEvent.Set();
                    break;

                case FrameType.Message:
                    EnqueueMessage(frame);
                    break;
            }
        }

        private void EnqueueMessage(Frame frame)
        {
            if (frame.Type != FrameType.Message)
            {
                throw new InvalidOperationException("Unable to get message from frame type " + frame.Type.ToString());
            }

            _receivedMessagesQueue.Enqueue(frame.ToMessage());

            if (_onMessageReceivedCallback != null)
            {
                while (_receivedMessagesQueue.Count > 0)
                {
                    Message msg = null;
                    bool isMessageAvailable = _receivedMessagesQueue.TryDequeue(out msg);

                    if (isMessageAvailable)
                    {
                        LogProvider.Current.Debug("Message is available, invoking _onMessageReceivedCallback for message");
                        _onMessageReceivedCallback(msg);
                    }
                }
            }
        }

        private void PerformHandshake(ConnectionOptions options)
        {
            Task.Run(() => PerformHandshakeAsync(options))
                .Wait();
        }

        private async Task PerformHandshakeAsync(ConnectionOptions options)
        {
            LogProvider.Current.Debug(string.Format("Performing handshake"));
            await SendRequestAsync(Commands.MAGIC_V2, isForced: true);
            await SendRequestAsync(new Identify
            {
                hostname = options.Hostname,
                client_id = options.ClientId,
                msg_timeout = (int)TimeSpan.FromSeconds(options.MessageTimeout.Value).TotalMilliseconds,
                heartbeat_interval = (int)TimeSpan.FromSeconds(options.HeartbeatIntervalInSeconds.Value).TotalMilliseconds
            }, isForced: true);

            var frame = _connectionResource.ReadFrame();
            if (frame.Type != FrameType.Response)
            {
                throw new ProtocolViolationException("Unexpected handshake response received: " + frame.ToASCII());
            }

            _isIdentified = true;
            LogProvider.Current.Debug(string.Format("Handshake completed."));
        }

        private async Task SendRequestAsync(byte[] payload, bool isForced = false)
        {
            if (!isForced)
            {
                EnsureOpenConnection(this);
            }

            await _connectionResource.WriteBytesAsync(payload);
        }

        private static void Initialize(NsqdConnection conn, bool isForced = false)
        {
            if (!isForced)
            {
                if (conn.IsConnected) return;
            }
            
            conn.Close();

            conn._connectionResource = new ConnectionResource(conn._endpoint)
                .Initialize(isForced);
        }

        private static void EnsureOpenConnection(NsqdConnection instance)
        {
            if (!instance.IsConnected)
            {
                if (instance._workerLoopException != null)
                {
                    throw instance._workerLoopException;
                }

                string connectionErrorMessage = "Unable to perform request with a closed connection.";
                LogProvider.Current.Error(connectionErrorMessage);
                throw new ConnectionException(connectionErrorMessage);
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
