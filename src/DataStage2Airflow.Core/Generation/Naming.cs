using System;
using System.Collections.Generic;
using System.Text;

namespace DataStage2Airflow.Generation
{
    /// <summary>Turns DataStage names into Python identifiers, module names and Airflow ids.</summary>
    public static class Naming
    {
        private static readonly HashSet<string> Reserved = new HashSet<string>(StringComparer.Ordinal)
        {
            "False", "None", "True", "and", "as", "assert", "async", "await", "break", "class", "continue",
            "def", "del", "elif", "else", "except", "finally", "for", "from", "global", "if", "import", "in",
            "is", "lambda", "nonlocal", "not", "or", "pass", "raise", "return", "try", "while", "with", "yield",
            "match", "case", "type", "print", "input", "list", "dict", "str", "int", "len", "id", "object",
            "F", "dsio", "pd", "ctx", "p", "row", "Decimal", "seq", "dag", "task", "context",
            "RowError", "JobContext", "Param", "routines", "run", "exc", "written", "in_row_num", "out_row_num",
            "datetime", "DAG", "EmptyOperator", "s", "r", "targets", "result", "values",
        };

        /// <summary>snake_case: "PX_Load_Customers" becomes "px_load_customers", "lnkIn" becomes "lnk_in".</summary>
        public static string Snake(string name)
        {
            var sb = new StringBuilder(name.Length + 8);
            char previous = '\0';
            for (int i = 0; i < name.Length; i++)
            {
                char c = name[i];
                if (char.IsLetterOrDigit(c) && c < 128)
                {
                    bool boundary = char.IsUpper(c) && i > 0 &&
                        (char.IsLower(previous) || char.IsDigit(previous) ||
                         (char.IsUpper(previous) && i + 1 < name.Length && char.IsLower(name[i + 1])));
                    if (boundary && sb.Length > 0 && sb[sb.Length - 1] != '_') sb.Append('_');
                    sb.Append(char.ToLowerInvariant(c));
                }
                else if (sb.Length > 0 && sb[sb.Length - 1] != '_')
                {
                    sb.Append('_');
                }

                previous = c;
            }

            var result = sb.ToString().Trim('_');
            if (result.Length == 0) result = "x";
            if (char.IsDigit(result[0])) result = "n" + result;
            return result;
        }

        /// <summary>A Python identifier with an optional prefix, clear of keywords and runtime names.</summary>
        public static string Identifier(string name, string prefix = "")
        {
            var id = prefix + Snake(name);
            if (Reserved.Contains(id)) id += "_";
            return id;
        }

        /// <summary>snake_case name with the prefix, unless the name already starts with it ("svName" stays "sv_name").</summary>
        public static string Prefixed(string prefix, string name)
        {
            var snake = Snake(name);
            return snake.StartsWith(prefix, StringComparison.Ordinal) && snake.Length > prefix.Length ? snake : prefix + snake;
        }

        /// <summary>Airflow task/DAG id: letters, digits, '_', '-' and '.', original case kept.</summary>
        public static string AirflowId(string name)
        {
            var sb = new StringBuilder(name.Length);
            foreach (char c in name)
            {
                sb.Append((char.IsLetterOrDigit(c) && c < 128) || c == '_' || c == '-' || c == '.' ? c : '_');
            }

            var result = sb.ToString().Trim('_');
            return result.Length == 0 ? "task" : result;
        }

        /// <summary>Appends _2, _3... until the name is not in <paramref name="used"/>, then records it.</summary>
        public static string Unique(string candidate, ISet<string> used)
        {
            var name = candidate;
            int n = 2;
            while (used.Contains(name))
            {
                name = candidate + "_" + n.ToString(System.Globalization.CultureInfo.InvariantCulture);
                n++;
            }

            used.Add(name);
            return name;
        }
    }
}
