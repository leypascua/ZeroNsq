using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using Xunit;
using ZeroNsq.Protocol;
using ZeroNsq.Internal;

namespace ZeroNsq.Tests
{   
    public class FrameReaderTests
    {
        [Fact]
        public void ReadMessageFrameTest()
        {
            var msg = new Message("Hello world");
            var expectedFrame = Frame.Message(msg);
            var reader = new FrameReader(expectedFrame.ToStream());

            var result = reader.ReadFrame();

            Assert.Equal(expectedFrame.Type, result.Type);
            Assert.Equal(expectedFrame.Data.Length, result.Data.Length);
            Assert.True(expectedFrame.Data.SequenceEqual(result.Data));

            Message received = result.ToMessage();
            Assert.True(msg.Body.SequenceEqual(received.Body));
            Assert.True(msg.Id.SequenceEqual(received.Id));
        }

        [Fact]
        public void ReadResponseFrameTest()
        {
            var expectedFrame = Frame.Response("OK");
            var reader = new FrameReader(expectedFrame.ToStream());

            var result = reader.ReadFrame();

            Assert.Equal(expectedFrame.Type, result.Type);
            Assert.Equal(expectedFrame.Data.Length, result.Data.Length);
            Assert.True(expectedFrame.Data.SequenceEqual(result.Data));
        }

        [Fact]
        public void ReadErrorFrameTest()
        {
            var expectedFrame = Frame.Error("E_INVALID");
            var reader = new FrameReader(expectedFrame.ToStream());

            var result = reader.ReadFrame();

            Assert.Equal(expectedFrame.Type, result.Type);
            Assert.Equal(expectedFrame.Data.Length, result.Data.Length);
            Assert.True(expectedFrame.Data.SequenceEqual(result.Data));
        }
    }
}
