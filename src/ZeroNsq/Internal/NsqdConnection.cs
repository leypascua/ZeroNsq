using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
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
        private readonly List<Task> _runningMessageHandlers = new List<Task>();
        private ConnectionResource _connectionResource;        
        private ConcurrentQueue<Frame> _receivedFramesQueue = new ConcurrentQueue<Frame>();                
        private Task _workerLoopTask;
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

            if (_options.OnHeartBeatRespondedCallback != null)
            {
                _onHeartbeatRespondedCallback = options.OnHeartBeatRespondedCallback;
            }
        }

        public bool IsConnected
        {
            get
            {   
                return _connectionResource != null &&
                       _connectionResource.IsInitialized &&
                       _isIdentified &&
                       (_workerLoopTask != null && !_workerLoopTask.IsCompleted) &&                       
                       !_disposedValue;
            }
        }

        public async Task ConnectAsync()
        {
            if (IsConnected) return;

            await InitializeAsync(this);
            await PerformHandshakeAsync(_options);

            // start the worker and wait for incoming messages
            _workerLoopTask = Task.Factory.StartNew(() => WorkerLoop(), _workerCancellationTokenSource.Token, TaskCreationOptions.LongRunning, TaskScheduler.Current);
        }

        public async Task SendRequestAsync(IRequest request)
        {
            await SendRequestAsync(request, isForced: false);
        }

        public async Task<Frame> ReadFrameAsync()
        {   
            int attempts = 0;

            do
            {
                Frame nextFrame = null;

                if (_receivedFramesQueue.IsEmpty)
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(DefaultThreadSleepTime));
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
                    await Task.Delay(TimeSpan.FromMilliseconds(DefaultThreadSleepTime));

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

        public async Task CloseAsync()
        {
            if (!IsConnected) return;

            await DispatchCls();
                        
            if (_workerCancellationTokenSource != null)
            {
                _workerCancellationTokenSource.Cancel();
                _workerCancellationTokenSource.Dispose();
                _workerCancellationTokenSource = null;
            }

            try
            {
                _workerLoopTask.Dispose();
            }
            catch { }

            _workerLoopTask = null;

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
            await SendRequestAsync(request.ToByteArray(), isForced).ConfigureAwait(false);

            bool isResponseExpected = request is IRequestWithResponse;
            if (!isResponseExpected) return;

            await HandleResponseAsync();
        }

        private async Task DispatchCls()
        {
            try
            {
                LogProvider.Current.Debug("NsqdConnection: Advise CLS to daemon host");
                
                // ignore all errors for this command.
                await _connectionResource.WriteBytesAsync(Commands.CLS);

                // give enough time for the server to respond
                await Task.Delay(TimeSpan.FromSeconds(2));
            }
            catch { }
        }

        private async Task HandleResponseAsync()
        {
            Response response = await FetchLastReceivedResponseAsync();

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

        private async Task<Response> FetchLastReceivedResponseAsync()
        {   
            var frame = await ReadFrameAsync();
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
                try
                {   
                    bool isOk = Task.Run(() => ReceiveFramesAsync()).Result;

                    if (!isOk) break;
                }
                catch(AggregateException ex)
                {
                    if (_workerLoopException != null)
                    {
                        _workerLoopException = new ConnectionException(ex.InnerException.Message);
                    }

                    break;
                }

                CleanupMessageHandlers();
            }
                        
            _isIdentified = false;
            LogProvider.Current.Warn("NsqdConnection worker loop terminated. Connection is idle.");

            if (_workerLoopException != null)
            {
                LogProvider.Current.Debug("NsqdConnection.WorkerLoop Exception: " + _workerLoopException.Message);
            }

            CleanupMessageHandlers();
        }

        private async Task<bool> ReceiveFramesAsync()
        {
            try
            {
                LogProvider.Current.Debug("NsqdConnection.WorkerLoop is waiting for a frame");
                Frame frame = await _connectionResource.ReadFrameAsync().ConfigureAwait(false);
                await OnFrameReceivedAsync(frame);
            }
            catch (ObjectDisposedException)
            {
                return false;
            }
            catch (BaseException ex)
            {
                _workerLoopException = ex;
                await CloseAsync();
                return false;
            }

            return true;
        }

        private async Task OnFrameReceivedAsync(Frame frame)
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

            if (_onMessageReceivedCallback != null)
            {
                _runningMessageHandlers.Add(Task.Run(() => _onMessageReceivedCallback(frame.ToMessage())));
            }
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

            var frame = await _connectionResource.ReadFrameAsync().ConfigureAwait(false);
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

            await _connectionResource.WriteBytesAsync(payload).ConfigureAwait(false);
        }

        private static async Task InitializeAsync(NsqdConnection conn, bool isForced = false)
        {
            if (!isForced)
            {
                if (conn.IsConnected) return;
            }

            await conn.CloseAsync();
            
            conn._connectionResource = await new ConnectionResource(conn._endpoint)
                .InitializeAsync(isForced).ConfigureAwait(false);

            conn._workerCancellationTokenSource = new CancellationTokenSource();
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

        private void CleanupMessageHandlers()
        {
            var completedTasks = _runningMessageHandlers.Where(t => t.IsCompleted).ToList();
            foreach (var task in completedTasks)
            {
                task.Dispose();
                _runningMessageHandlers.Remove(task);
            }
        }

        #region IDisposable Support

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposedValue)
            {
                if (disposing)
                {                    
                    Task.Run(() => CloseAsync());
                }
                                
                _disposedValue = true;
            }
        }
                
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
