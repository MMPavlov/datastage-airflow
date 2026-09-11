using System;
using System.Collections.Generic;
using System.Globalization;

namespace DataStage2Airflow.Internal
{
    internal static class Text
    {
        public static string? NullIfBlank(string? value) => string.IsNullOrWhiteSpace(value) ? null : value;

        /// <summary>Splits DataStage "a|b|c" lists (pin lists, partner references), dropping blanks.</summary>
        public static List<string> SplitList(string? value, char separator = '|')
        {
            var result = new List<string>();
            if (string.IsNullOrEmpty(value)) return result;
            foreach (var part in value!.Split(separator))
            {
                var trimmed = part.Trim();
                if (trimmed.Length > 0) result.Add(trimmed);
            }

            return result;
        }

        public static int ParseInt(string? value, int fallback = 0) =>
            int.TryParse((value ?? string.Empty).Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var n) ? n : fallback;

        /// <summary>Interprets "1"/"0", "true"/"false", "yes"/"no"; null when absent or unrecognised.</summary>
        public static bool? ParseBool(string? value)
        {
            if (value == null) return null;
            switch (value.Trim().ToLowerInvariant())
            {
                case "1":
                case "true":
                case "yes":
                case "y":
                    return true;
                case "0":
                case "false":
                case "no":
                case "n":
                    return false;
                default:
                    return null;
            }
        }

        public static bool ContainsIgnoreCase(string text, string fragment) =>
            text.IndexOf(fragment, StringComparison.OrdinalIgnoreCase) >= 0;
    }
}
