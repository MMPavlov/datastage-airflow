using System;
using System.Text;

namespace DataStage2Airflow.Dsx
{
    /// <summary>Lexical helpers for the DSX string syntax.</summary>
    public static class DsxText
    {
        /// <summary>Delimits values that span several lines (job control code, schemas, long expressions).</summary>
        public const string MultiLineMarker = "=+=+=+=";

        /// <summary>
        /// Reads a quoted DSX string whose opening quote is at <paramref name="start"/>.
        /// Escapes: <c>\"</c>, <c>\\</c>, and <c>\(1B)</c> for a character given as hex code point.
        /// Returns false when the closing quote is missing; <paramref name="value"/> then holds the rest of the text.
        /// </summary>
        public static bool TryReadQuoted(string text, int start, out string value, out int next)
        {
            var sb = new StringBuilder(Math.Max(0, text.Length - start));
            int i = start + 1;
            while (i < text.Length)
            {
                char c = text[i];
                if (c == '\\' && i + 1 < text.Length)
                {
                    char n = text[i + 1];
                    if (n == '"' || n == '\\')
                    {
                        sb.Append(n);
                        i += 2;
                        continue;
                    }

                    if (n == '(' && TryReadHexEscape(text, i, out char decoded, out int after))
                    {
                        sb.Append(decoded);
                        i = after;
                        continue;
                    }

                    sb.Append(c);
                    i++;
                    continue;
                }

                if (c == '"')
                {
                    value = sb.ToString();
                    next = i + 1;
                    return true;
                }

                sb.Append(c);
                i++;
            }

            value = sb.ToString();
            next = text.Length;
            return false;
        }

        /// <summary>Decodes only the <c>\(hex)</c> escapes. XML exports use them, but leave backslashes alone.</summary>
        public static string DecodeControlEscapes(string text)
        {
            if (text.IndexOf("\\(", StringComparison.Ordinal) < 0) return text;
            var sb = new StringBuilder(text.Length);
            int i = 0;
            while (i < text.Length)
            {
                if (text[i] == '\\' && i + 1 < text.Length && text[i + 1] == '(' &&
                    TryReadHexEscape(text, i, out char decoded, out int after))
                {
                    sb.Append(decoded);
                    i = after;
                    continue;
                }

                sb.Append(text[i]);
                i++;
            }

            return sb.ToString();
        }

        /// <summary>Encodes a value as a quoted DSX string (used by tests and sample writers).</summary>
        public static string Quote(string value)
        {
            var sb = new StringBuilder(value.Length + 2);
            sb.Append('"');
            foreach (char c in value)
            {
                if (c == '"' || c == '\\')
                {
                    sb.Append('\\').Append(c);
                }
                else if (c < 0x20)
                {
                    sb.Append("\\(").Append(((int)c).ToString("X", System.Globalization.CultureInfo.InvariantCulture)).Append(')');
                }
                else
                {
                    sb.Append(c);
                }
            }

            sb.Append('"');
            return sb.ToString();
        }

        private static bool TryReadHexEscape(string text, int backslash, out char decoded, out int after)
        {
            decoded = '\0';
            after = backslash;
            int digitsStart = backslash + 2;
            int close = text.IndexOf(')', digitsStart);
            if (close <= digitsStart || close - digitsStart > 4) return false;

            int code = 0;
            for (int j = digitsStart; j < close; j++)
            {
                int h = HexValue(text[j]);
                if (h < 0) return false;
                code = (code * 16) + h;
            }

            decoded = (char)code;
            after = close + 1;
            return true;
        }

        private static int HexValue(char c)
        {
            if (c >= '0' && c <= '9') return c - '0';
            if (c >= 'a' && c <= 'f') return c - 'a' + 10;
            if (c >= 'A' && c <= 'F') return c - 'A' + 10;
            return -1;
        }
    }
}
