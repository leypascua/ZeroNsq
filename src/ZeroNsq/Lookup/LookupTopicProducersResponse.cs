using System;
using System.Collections.Generic;
using System.Text;

namespace ZeroNsq.Lookup
{
    public class LookupTopicProducersResponse
    {
        public LookupTopicProducersResponse()
        {
            data = new LookupTopicProducersResponse.Result();
        }

        public int status_code { get; set; }
        public string status_txt { get; set; }
        public Result data { get; set; }

        public class Result
        {
            public Result()
            {
                channels = new string[0];
                producers = new ProducerEndpointData[0];
            }

            public string[] channels { get; set; }
            public ProducerEndpointData[] producers { get; set; }
        }
    }
}
