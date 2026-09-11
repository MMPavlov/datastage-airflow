using System;
using System.Collections.Generic;
using System.Text;

namespace DataStage2Airflow.Dsx
{
    /// <summary>
    /// Parses the record-format properties of the parallel engine (OSH schema syntax), e.g. the
    /// <c>APT/SchemaFormat</c> meta property of a Sequential File link:
    /// <c>final_delim=end, delim=',', null_field="", quote=none</c>.
    /// </summary>
    public static class OshFormat
    {
        public static IReadOnlyDictionary<string, string> Parse(string text)
        {
            var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            int i = 0;
            while (i < text.Length)
            {
                SkipSeparators(text, ref i);
                if (i >= text.Length) break;

                int keyStart = i;
                while (i < text.Length && text[i] != '=' && text[i] != ',' && text[i] != '}' && !char.IsWhiteSpace(text[i])) i++;
                var key = text.Substring(keyStart, i - keyStart);
                while (i < text.Length && char.IsWhiteSpace(text[i])) i++;

                string value = "true";
                if (i < text.Length && text[i] == '=')
                {
                    i++;
                    while (i < text.Length && char.IsWhiteSpace(text[i])) i++;
                    value = ReadValue(text, ref i);
                }

                if (key.Length > 0) result[key] = value;
                else i++;
            }

            return result;
        }

        /// <summary>Turns a delimiter or quote setting into the literal character(s), or null for "none".</summary>
        public static string? Symbol(string? setting)
        {
            if (setting == null) return null;
            switch (setting.Trim().ToLowerInvariant())
            {
                case "none":
                case "":
                    return null;
                case "tab":
                    return "\t";
                case "space":
                    return " ";
                case "comma":
                    return ",";
                case "double":
                    return "\"";
                case "single":
                    return "'";
                case "null":
                    return "\0";
                case "end":
                    return null;
                default:
                    return setting;
            }
        }

        private static void SkipSeparators(string text, ref int i)
        {
            while (i < text.Length && (char.IsWhiteSpace(text[i]) || text[i] == ',' || text[i] == '{' || text[i] == '}' || text[i] == ';')) i++;
        }

        private static string ReadValue(string text, ref int i)
        {
            if (i >= text.Length) return string.Empty;
            char q = text[i];
            if (q == '\'' || q == '"')
            {
                var sb = new StringBuilder();
                i++;
                while (i < text.Length && text[i] != q)
                {
                    if (text[i] == '\\' && i + 1 < text.Length)
                    {
                        sb.Append(Unescape(text[i + 1]));
                        i += 2;
                        continue;
                    }

                    sb.Append(text[i]);
                    i++;
                }

                i++;
                return sb.ToString();
            }

            int start = i;
            while (i < text.Length && text[i] != ',' && text[i] != '}' && text[i] != ';') i++;
            return text.Substring(start, i - start).Trim();
        }

        private static char Unescape(char c)
        {
            switch (c)
            {
                case 'n': return '\n';
                case 'r': return '\r';
                case 't': return '\t';
                case '0': return '\0';
                default: return c;
            }
        }
    }
}
