using System;

namespace ZeroNsq
{
    public interface IPublisher : IDisposable
    {
        void Publish(string topic, byte[] message);
        void Publish(string topic, string utf8String);
    }
}