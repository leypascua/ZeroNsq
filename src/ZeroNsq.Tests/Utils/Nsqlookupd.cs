using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using ZeroNsq.Helpers;

namespace ZeroNsq.Tests.Utils
{
    public class Nsqlookupd : IDisposable
    {
        public Nsqlookupd(int tcp = 4160, int http = 4161)
        {
            this.TcpPort = tcp;
            this.HttpPort = http;

            string args = $"-tcp-address 0.0.0.0:{tcp} -http-address 0.0.0.0:{http}";

            var process = new Process();
            process.StartInfo.FileName = "nsqlookupd.exe";
            process.StartInfo.Arguments = args;
            process.StartInfo.RedirectStandardOutput = true;
            process.StartInfo.RedirectStandardError = true;

            if (!process.Start())
            {
                Wait.For(TimeSpan.FromSeconds(3)).Start();
                process.Start();
            }

            if (process.HasExited)
            {
                throw new Exception($"Failed to start nsqlookupd.exe on ports tcp={tcp}; http={http}");
            }

            this.Process = process;
        }

        public int TcpPort { get; }
        public int HttpPort { get; }
        public Process Process { get; private set; }        

        public void Dispose()
        {
            this.Process.Kill();
            this.Process.Dispose();
            Process = null;
        }
    }
}
