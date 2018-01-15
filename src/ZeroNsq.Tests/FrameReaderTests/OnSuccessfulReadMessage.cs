using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Xunit;

namespace ZeroNsq.Test.FrameReaderTests
{
    public class OnSuccessfulReadMessage
    {
        private string _data = "hello world";
        private FrameReader _subject;
        private Frame _result;

        public OnSuccessfulReadMessage()
        {
            _subject = new FrameReader(MessageFrame(_data));
            _result = _subject.ReadFrame();
        }

        [Fact]
        public void SizeIsCorrect()
        {
            Assert.Equal(_data.Length, _result.MessageSize);
        } 

        [Fact]
        public void FrameTypeIsCorrect()
        {
            Assert.Equal(_result.Type, FrameType.Message);
        }

        [Fact]
        public void DataIsCorrect()
        {
            Assert.Equal(_data, _result.ToUtf8String());
        }
    }
}
