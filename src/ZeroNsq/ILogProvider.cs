using System;

namespace ZeroNsq
{
    /// <summary>
    /// Provides access to logging functions.
    /// </summary>
    public interface ILogProvider
    {
        /// <summary>
        /// Writes a debug message
        /// </summary>
        /// <param name="message">The message</param>
        void Debug(string message);

        /// <summary>
        /// Writes an info message
        /// </summary>
        /// <param name="message">The message</param>
        void Info(string message);

        /// <summary>
        /// Writes a warning message
        /// </summary>
        /// <param name="message">The message</param>
        void Warn(string message);

        /// <summary>
        /// Writes an error message
        /// </summary>
        /// <param name="message">The message</param>
        void Error(string message);

        /// <summary>
        /// Writes a fatal message
        /// </summary>
        /// <param name="message">The message</param>
        void Fatal(string message);
    }

    public class LogProvider : ILogProvider
    {
        private static Func<ILogProvider> Factory = () => Default;

        public readonly static ILogProvider Default = NullLogProvider.Instance;

        public static ILogProvider Current
        {
            get
            {
                if (Factory == null) return Default;

                var instance = Factory() ?? Default;

                return instance;
            }
        }

        public static LogProvider.Configuration Configure()
        {
            return LogProvider.Configuration.Instance;
        }

        public void Debug(string message)
        {
            System.Diagnostics.Debug.WriteLine(BuildText("DEBUG", message));
        }

        public void Error(string message)
        {
            WriteTrace("ERROR", message);
        }

        public void Fatal(string message)
        {
            WriteTrace("FATAL", message);
        }

        public void Info(string message)
        {
            WriteTrace("INFO", message);
        }

        public void Warn(string message)
        {
            WriteTrace("WARN", message);
        }

        private void WriteTrace(string level, string message)
        {
            System.Diagnostics.Trace.WriteLine(BuildText(level, message));
        }

        private string BuildText(string level, string message)
        {
            string messageFormat = string.Format("{0} [{1}] {2}", DateTime.Now.ToString("yyyy-MM-ddThh:mm:ss"), level, message);
            return messageFormat;
        }

        class NullLogProvider : ILogProvider
        {
            public static readonly ILogProvider Instance = new NullLogProvider();

            public void Debug(string message) { }

            public void Error(string message) { }

            public void Fatal(string message) { }

            public void Info(string message) { }

            public void Warn(string message) { }
        }

        public class Configuration
        {
            private static readonly object syncLock = new object();

            internal Configuration() { }

            public static readonly Configuration Instance = new Configuration();

            public Configuration UseDefault()
            {
                Use(() => LogProvider.Default);
                return this;
            }

            public Configuration UseNull()
            {
                Use(() => NullLogProvider.Instance);
                return this;
            }

            public Configuration Use(ILogProvider instance)
            {
                Use(() => instance);
                return this;
            }

            public Configuration Use(Func<ILogProvider> factory)
            {
                lock (syncLock)
                {
                    LogProvider.Factory = factory;
                    return this;
                }
            }

            public ILogProvider GetInstance()
            {
                return LogProvider.Current;
            }
        }
    }
}
