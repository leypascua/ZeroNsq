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
        static LogProvider()
        {
            Default = new LogProvider();            
        }

        public static ILogProvider Default { get; private set; }

        public static Func<ILogProvider> Factory = () => Default;

        public static ILogProvider Current
        {
            get
            {
                if (Factory == null) return Default;

                var instance = Factory() ?? Default;

                return instance;
            }
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
            System.Diagnostics.Trace.WriteLine(BuildText(level, message), "ZeroNsq.LogProvider.Trace");
        }

        private string BuildText(string level, string message)
        {
            string messageFormat = string.Format("{0} [{1}] {2}", DateTime.Now.ToString("yyyy-MM-ddThh:mm:ss"), level, message);
            return messageFormat;
        }
    }
}
