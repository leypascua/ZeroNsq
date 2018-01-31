using System.Net;

namespace ZeroNsq.Internal
{
    public class ConnectionOptions
    {
        public const int DefaultTcpPort = 4150;
        private readonly static int DefaultMaxClientReconnectionAttempts = 3;
        const int DefaultInitialBackoffTimeInSeconds = 8;
        const int DefaultHeartbeatIntervalInSeconds = 30;
        const int DefaultMessageTimeoutInSeconds = 120;

        public static readonly ConnectionOptions Default = SetDefaults(null);

        public ConnectionOptions()
        {
            MaxClientReconnectionAttempts = DefaultMaxClientReconnectionAttempts;
            InitialBackoffTimeInSeconds = DefaultInitialBackoffTimeInSeconds;
        }

        public string Hostname { get; set; }
        public string ClientId { get; set; }

        /// <summary>
        /// Gets or sets the in-flight message timeout period in seconds.
        /// </summary>
        public int? MessageTimeout { get; set; }
        
        public int? HeartbeatIntervalInSeconds { get; set; }

        public int MaxClientReconnectionAttempts { get; set; }
        public int InitialBackoffTimeInSeconds { get; set; }

        public static ConnectionOptions SetDefaults(ConnectionOptions options)
        {
            var opt = options ?? new ConnectionOptions();

            if (string.IsNullOrEmpty(opt.Hostname)) opt.Hostname = Dns.GetHostName();
            if (string.IsNullOrEmpty(opt.ClientId)) opt.ClientId = string.Format("{0}@{1}", opt.GetHashCode(), opt.Hostname);

            if (!opt.HeartbeatIntervalInSeconds.HasValue)
            {
                opt.HeartbeatIntervalInSeconds = DefaultHeartbeatIntervalInSeconds;
            }

            if (!opt.MessageTimeout.HasValue)
            {
                opt.MessageTimeout = DefaultMessageTimeoutInSeconds;
            }

            if (opt.MaxClientReconnectionAttempts == 0) opt.MaxClientReconnectionAttempts = DefaultMaxClientReconnectionAttempts;
            if (opt.InitialBackoffTimeInSeconds == 0) opt.InitialBackoffTimeInSeconds = DefaultInitialBackoffTimeInSeconds;

            return opt;
        }
    }
}
