﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ZeroNsq.Helpers;

namespace ZeroNsq.Tests.Utils
{
    public static class Nsqd
    {
        public const string Local = "127.0.0.1";
        public const int TcpPort = 4150;
        public const string DefaultTopicName = "ZeroNsq_Utils.DefaultTopic-TEST";
        public static readonly string DefaultChannelName = DefaultChannelName + ".Channel";
        public static int NextAvailablePort = 8149;
        public readonly static object SyncLock = new object();

        public static NsqdInstance StartLocal(int port = 0)
        {
            return Start(Local, port);
        }

        public static NsqdInstance Start(string host, int port = 0)
        {
            if (port == 0)
            {
                port = Nsqd.NextPort();
            }

            return new NsqdInstance(host, port);
        }

        public static int NextPort()
        {
            lock (SyncLock)
            {
                NextAvailablePort += 1;
                return NextAvailablePort;
            }
        }
    }

    public class NsqdInstance : IDisposable
    {
        public NsqdInstance(string host, int port)
        {
            var dinfo = new DirectoryInfo(Path.Combine(Path.GetTempPath(), "nsqd", port.ToString()));

            if (!dinfo.Exists)
            {
                dinfo.Create();
            }

            int httpPort = port + 500;

            string args = string.Format("-tcp-address {0}:{1} -http-address {0}:{2} -data-path {3}",
                host, port, httpPort, dinfo.FullName);

            Process = Process.Start("nsqd.exe", args);

            if (Process.HasExited)
            {
                Wait.For(TimeSpan.FromSeconds(3)).Start();                
                Process = Process.Start("nsqd.exe", args);
            }

            if (Process.HasExited)
            {
                throw new Exception(Process.StandardError.ReadToEnd());
            }

            Host = host;
            Port = port;
            HttpPort = httpPort;
        }

        public Process Process { get; private set; }

        public string Host { get; private set; }
        public int Port { get; private set; }
        public int HttpPort { get; private set; }

        public void Kill()
        {
            Task.Run(() => KillAsync()).Wait();
        }

        public async Task KillAsync()
        {
            await Task.Delay(TimeSpan.FromSeconds(1)).ConfigureAwait(false);

            Dispose();
        }
        
        public void Dispose()
        {
            if (Process != null && !Process.HasExited)
            {
                Process.Kill();
                Process.Dispose();
                Process = null;
            }
        }
    }
}
