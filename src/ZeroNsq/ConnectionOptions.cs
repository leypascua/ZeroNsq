using System;
using System.Collections.Generic;
using System.Text;

namespace ZeroNsq
{
    public class ConnectionOptions
    {
        public ConnectionOptions() {}

        public string Hostname { get; set; }
        public string ClientId { get; set; }
        public int? MessageTimeout { get; set; }
        public int? HeartbeatIntervalInSeconds { get; set; }
    }
}
