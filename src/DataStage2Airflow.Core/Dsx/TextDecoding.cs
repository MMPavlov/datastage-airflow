using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace DataStage2Airflow.Dsx
{
    /// <summary>
    /// Picks the character set of an export file: a byte-order mark wins, then strict UTF-8,
    /// then Windows-1252 (what DataStage writes as <c>CharacterSet "CP1252"</c> or <c>"ENGLISH"</c>).
    /// </summary>
    public static class TextDecoding
    {
        public static Encoding DetectFileEncoding(string path)
        {
            using (var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 1 << 16))
            {
                return DetectEncoding(stream);
            }
        }

        public static Encoding DetectEncoding(Stream stream)
        {
            var head = new byte[3];
            int n = stream.Read(head, 0, 3);
            if (n >= 3 && head[0] == 0xEF && head[1] == 0xBB && head[2] == 0xBF) return new UTF8Encoding(false);
            if (n >= 2 && head[0] == 0xFF && head[1] == 0xFE) return Encoding.Unicode;
            if (n >= 2 && head[0] == 0xFE && head[1] == 0xFF) return Encoding.BigEndianUnicode;

            var decoder = new UTF8Encoding(false, true).GetDecoder();
            var buffer = new byte[1 << 16];
            try
            {
                if (n > 0) decoder.GetCharCount(head, 0, n, false);
                int read;
                while ((read = stream.Read(buffer, 0, buffer.Length)) > 0)
                {
                    decoder.GetCharCount(buffer, 0, read, false);
                }

                decoder.GetCharCount(buffer, 0, 0, true);
                return new UTF8Encoding(false);
            }
            catch (DecoderFallbackException)
            {
                return Windows1252Encoding.Instance;
            }
        }

        public static TextReader OpenText(string path, out Encoding encoding)
        {
            encoding = DetectFileEncoding(path);
            return new StreamReader(path, encoding, detectEncodingFromByteOrderMarks: true);
        }

        public static string Decode(byte[] bytes)
        {
            using (var stream = new MemoryStream(bytes, false))
            {
                var encoding = DetectEncoding(stream);
                stream.Position = 0;
                using (var reader = new StreamReader(stream, encoding, detectEncodingFromByteOrderMarks: true))
                {
                    return reader.ReadToEnd();
                }
            }
        }
    }

    /// <summary>
    /// Windows-1252 without System.Text.Encoding.CodePages, which .NET Core only offers as a separate provider.
    /// </summary>
    public sealed class Windows1252Encoding : Encoding
    {
        public static readonly Windows1252Encoding Instance = new Windows1252Encoding();

        // Code points for bytes 0x80..0x9F; the five unassigned positions map to the C1 control of the same value.
        private static readonly ushort[] HighCodePoints =
        {
            0x20AC, 0x0081, 0x201A, 0x0192, 0x201E, 0x2026, 0x2020, 0x2021,
            0x02C6, 0x2030, 0x0160, 0x2039, 0x0152, 0x008D, 0x017D, 0x008F,
            0x0090, 0x2018, 0x2019, 0x201C, 0x201D, 0x2022, 0x2013, 0x2014,
            0x02DC, 0x2122, 0x0161, 0x203A, 0x0153, 0x009D, 0x017E, 0x0178,
        };

        private static readonly Dictionary<char, byte> Reverse = BuildReverse();

        private Windows1252Encoding()
        {
        }

        public override string WebName => "windows-1252";

        public override string EncodingName => "Western European (Windows)";

        public override bool IsSingleByte => true;

        public override int GetByteCount(char[] chars, int index, int count) => count;

        public override int GetBytes(char[] chars, int charIndex, int charCount, byte[] bytes, int byteIndex)
        {
            for (int i = 0; i < charCount; i++) bytes[byteIndex + i] = ToByte(chars[charIndex + i]);
            return charCount;
        }

        public override int GetCharCount(byte[] bytes, int index, int count) => count;

        public override int GetChars(byte[] bytes, int byteIndex, int byteCount, char[] chars, int charIndex)
        {
            for (int i = 0; i < byteCount; i++) chars[charIndex + i] = ToChar(bytes[byteIndex + i]);
            return byteCount;
        }

        public override int GetMaxByteCount(int charCount) => charCount;

        public override int GetMaxCharCount(int byteCount) => byteCount;

        private static char ToChar(byte b) => b >= 0x80 && b <= 0x9F ? (char)HighCodePoints[b - 0x80] : (char)b;

        private static byte ToByte(char c)
        {
            if (c < 0x80 || (c >= 0xA0 && c <= 0xFF)) return (byte)c;
            return Reverse.TryGetValue(c, out var b) ? b : (byte)'?';
        }

        private static Dictionary<char, byte> BuildReverse()
        {
            var map = new Dictionary<char, byte>();
            for (int i = 0; i < HighCodePoints.Length; i++) map[(char)HighCodePoints[i]] = (byte)(0x80 + i);
            return map;
        }
    }
}
