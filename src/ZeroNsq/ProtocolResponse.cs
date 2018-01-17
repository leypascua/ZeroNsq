using Jil;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Linq;

namespace ZeroNsq
{
    public class ProtocolResponse
    {
        public static readonly string[] SuccessResponses = { "OK", "CLOSE_WAIT" };
        public static readonly string[] ErrorResponses = { "E_INVALID", "E_" };
        public static readonly ProtocolResponse Success = new ProtocolResponse();
        private string _utf8StringData = null;

        public ProtocolResponse() { }

        public ProtocolResponse(byte[] data) : this(data, null) { }

        public ProtocolResponse(byte[] data, string error)
        {
            Data = Data;
            Error = error;
        }

        public byte[] Data { get; private set; }

        public string Error { get; private set; }

        public bool IsSuccessful
        {
            get
            {

                if (!string.IsNullOrEmpty(Error)) return false;

                string val = ToString();

                if (string.IsNullOrEmpty(val)) return true;
                if (SuccessResponses.Contains(val)) return true;
                if (val.StartsWith("E_")) return false;

                return true;
            }
        }

        public TResp Deserialize<TResp>() where TResp : class, new()
        {
            if (Data == null || Data.Length == 0) return default(TResp);

            using (var ms = new MemoryStream(Data))
            using (var reader = new StreamReader(ms))
            {
                return JSON.Deserialize<TResp>(reader);
            }
        }

        public override string ToString()
        {
            if (Data == null || Data.Length == 0) return string.Empty;

            if (string.IsNullOrEmpty(_utf8StringData))
            {
                _utf8StringData = Encoding.UTF8.GetString(Data);
            }

            return _utf8StringData;
        }
    }
}
