using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;

namespace ZeroNsq.Tests.Utils
{
    public static class Nsqd
    {
        public const string Local = "127.0.0.1";
        public const int TcpPort = 4150;
        public const string DefaultTopicName = "ZeroNsq_Utils.DefaultTopic-TEST";
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

            string args = string.Format("-tcp-address {0}:{1} -http-address {0}:{2} -data-path {3}",
                host, port, port + 500, dinfo.FullName);

            Process = Process.Start("nsqd.exe", args);

            if (Process.HasExited)
            {
                Thread.Sleep(TimeSpan.FromSeconds(5));
                Process = Process.Start("nsqd.exe", args);
            }

            if (Process.HasExited)
            {
                throw new Exception(Process.StandardError.ReadToEnd());
            }

            Host = host;
            Port = port;
        }

        public Process Process { get; private set; }

        public string Host { get; private set; }
        public int Port { get; private set; }

        public void Kill()
        {
            Thread.Sleep(TimeSpan.FromSeconds(2));
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
