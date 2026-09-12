using System;
using System.Collections.Generic;
using System.Text;

namespace DataStage2Airflow.Generation
{
    /// <summary>Builds Python source text with four-space indentation.</summary>
    public sealed class PythonWriter
    {
        private readonly StringBuilder _text = new StringBuilder();
        private int _indent;

        public int IndentLevel => _indent;

        public PythonWriter Line(string text = "")
        {
            if (text.Length == 0)
            {
                _text.Append('\n');
                return this;
            }

            _text.Append(' ', _indent * 4).Append(text).Append('\n');
            return this;
        }

        public PythonWriter Lines(IEnumerable<string> lines)
        {
            foreach (var line in lines) Line(line);
            return this;
        }

        /// <summary>Writes the header line and indents until the returned scope is disposed.</summary>
        public Scope Block(string header)
        {
            Line(header);
            _indent++;
            return new Scope(this);
        }

        public void Indent() => _indent++;

        public void Dedent()
        {
            if (_indent == 0) throw new InvalidOperationException("dedent below column 0");
            _indent--;
        }

        /// <summary>Writes <c>target = function(args)</c>, one argument per line when it gets long.</summary>
        public PythonWriter Call(string? target, string function, IList<string> args)
        {
            var prefix = target == null ? string.Empty : target + " = ";
            var single = $"{prefix}{function}({string.Join(", ", args)})";
            if (single.Length + (_indent * 4) <= 100 && single.IndexOf('\n') < 0) return Line(single);

            using (Block($"{prefix}{function}("))
            {
                foreach (var arg in args)
                {
                    var lines = arg.Split('\n');
                    for (int i = 0; i < lines.Length; i++) Line(lines[i] + (i == lines.Length - 1 ? "," : string.Empty));
                }
            }

            return Line(")");
        }

        /// <summary>Strips trailing blanks, collapses runs of more than two blank lines and ends the text with one newline.</summary>
        public static string Tidy(string code)
        {
            var sb = new StringBuilder(code.Length);
            int blanks = 0;
            foreach (var raw in code.Replace("\r\n", "\n").Split('\n'))
            {
                var line = raw.TrimEnd();
                if (line.Length == 0)
                {
                    if (++blanks > 2) continue;
                }
                else
                {
                    blanks = 0;
                }

                sb.Append(line).Append('\n');
            }

            return sb.ToString().TrimEnd('\n') + "\n";
        }

        /// <summary>A comment, one "#" line per input line.</summary>
        public PythonWriter Comment(string text)
        {
            foreach (var line in SplitLines(text))
            {
                Line(line.Length == 0 ? "#" : "# " + line);
            }

            return this;
        }

        public PythonWriter Docstring(string text)
        {
            var trimmed = text.TrimEnd().Replace("\r", string.Empty);
            // Raw docstrings keep DataStage paths (\Jobs\Loads) readable; they cannot end in a backslash or quote.
            bool raw = trimmed.IndexOf('\\') >= 0 && trimmed.IndexOf("\"\"\"", StringComparison.Ordinal) < 0
                && !trimmed.EndsWith("\\", StringComparison.Ordinal) && !trimmed.EndsWith("\"", StringComparison.Ordinal);
            var body = raw ? trimmed : EscapeDocstring(trimmed);
            if (body.EndsWith("\"", StringComparison.Ordinal)) body += " ";
            var open = raw ? "r\"\"\"" : "\"\"\"";
            if (body.IndexOf('\n') < 0)
            {
                Line(open + body + "\"\"\"");
                return this;
            }

            var lines = SplitLines(body);
            Line(open + lines[0]);
            for (int i = 1; i < lines.Count; i++) Line(lines[i]);
            Line("\"\"\"");
            return this;
        }

        public override string ToString() => _text.ToString();

        public static string EscapeDocstring(string text) =>
            text.Replace("\\", "\\\\").Replace("\"\"\"", "\\\"\\\"\\\"").Replace("\r", string.Empty);

        private static List<string> SplitLines(string text) =>
            new List<string>(text.Replace("\r\n", "\n").Replace('\r', '\n').Split('\n'));

        public readonly struct Scope : IDisposable
        {
            private readonly PythonWriter _writer;

            public Scope(PythonWriter writer)
            {
                _writer = writer;
            }

            public void Dispose() => _writer.Dedent();
        }
    }
}
