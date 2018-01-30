using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
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
        private Task _workerTask;        
        private CancellationTokenSource _workerCancellationTokenSource;        
        private Action<Message> _onMessageReceivedCallback = msg => { };
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
                bool workerIsRunning =
                    _workerTask != null && !_workerTask.IsCompleted; 

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
                WorkerLoop, 
                _workerCancellationTokenSource.Token, 
                TaskCreationOptions.LongRunning, 
                TaskScheduler.Current);
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

                if (_receivedFramesQueue.IsEmpty)
                {
                    _frameReceivedResetEvent.Wait(TimeSpan.FromSeconds(10));
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

        public INsqConnection OnMessageReceived(Action<Message> callback)
        {
            if (callback != null)
            {
                _onMessageReceivedCallback = callback;
            }

            return this;
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
                    _workerTask.Wait(TimeSpan.FromSeconds(3));                    
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
            LogProvider.Current.Info("NsqdConnection is closed.");
        }

        internal void SendRequest(byte[] request)
        {
            SendRequest(request, false);
        }

        private void DispatchCls()
        {
            try
            {
                LogProvider.Current.Debug("NsqdConnection: Advise CLS to daemon host");
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
                Thread.Sleep(DefaultThreadSleepTime);
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
                    frame = _connectionResource.ReadFrame();
                }
                catch (ObjectDisposedException)
                {
                    break;
                }
                catch (ConnectionException ex)
                {
                    Close();
                    //throw;
                    _workerLoopException = ex;
                    break;
                }

                if (!IsConnected) break;
                if (frame == null) break;

                OnFrameReceived(frame);

                LogProvider.Current.Debug("NsqdConnection.WorkerLoop is sleeping.");
                Thread.Sleep(DefaultThreadSleepTime);
            }

            LogProvider.Current.Info("NsqdConnection worker loop terminated.");
        }

        private void OnFrameReceived(Frame frame)
        {
            LogProvider.Current.Debug("NsqdConnection.OnFrameReceived: " + frame.Type.ToString());

            switch (frame.Type)
            {
                case FrameType.Response:
                case FrameType.Error:
                    if (frame.Data.SequenceEqual(Response.Heartbeat))
                    {
                        LogProvider.Current.Debug("Heartbeat request received. Responding with NOP");
                        SendRequest(Commands.NOP);
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
            LogProvider.Current.Debug(string.Format("Performing handshake"));
            SendRequest(Commands.MAGIC_V2, isForced: true);
            SendRequest(new Identify
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
                    var knownErrors = instance._workerTask.Exception.InnerExceptions
                        .Where(error => error is BaseException)
                        .Select(x => x.Message)
                        .ToArray();

                    if (knownErrors.Any())
                    {
                        string message = string.Join(";", knownErrors);
                        string errorMessage = "One or more errors occurred. " + message;
                        LogProvider.Current.Error(errorMessage);
                        throw new ConnectionException(errorMessage);
                    }

                    throw instance._workerTask.Exception.Flatten();
                }

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
