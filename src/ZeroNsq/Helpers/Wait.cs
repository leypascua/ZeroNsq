using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace ZeroNsq.Helpers
{
    using System.Timers;

    public class Wait
    {
        private readonly TimeSpan _timespan;
        private Action _callback;

        private Wait(TimeSpan timespan)
        {
            _timespan = timespan;
            _callback = () => { };
        }

        public static Wait For(TimeSpan timespan)
        {
            return new Wait(timespan);
        }

        public virtual Wait Then(Action callback)
        {
            if (_callback == null)
            {
                throw new ArgumentNullException("callback");
            }

            _callback = callback;
            return this;
        }

        public virtual void Start()
        {
            using (var resetEvent = new ManualResetEventSlim())
            using (var timer = new Timer(_timespan.TotalMilliseconds))
            {
                timer.Elapsed += (sender, args) =>
                {
                    timer.Stop();                    
                    resetEvent.Set();
                };

                timer.Start();
                resetEvent.Wait();
                _callback();
            }
        }
    }
}
