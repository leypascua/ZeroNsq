using System;
using System.Collections.Generic;
using System.Text;

namespace ZeroNsq.Lookup
{
    public class ProducerEndpointData
    {
        public string remote_address { get; set; }
        public string hostname { get; set; }
        public string broadcast_address { get; set; }        
        public int tcp_port { get; set; }
        public int http_port { get; set; }
        public string version { get; set; }        
    }
}
