using System.Collections.Generic;
using System.Globalization;
using System.Text;

namespace DataStage2Airflow.Reporting
{
    /// <summary>Minimal indented JSON writer (netstandard2.0 has no System.Text.Json in the box).</summary>
    public sealed class JsonWriter
    {
        private readonly StringBuilder _sb = new StringBuilder();
        private readonly Stack<bool> _hasItems = new Stack<bool>();
        private bool _afterName;

        public JsonWriter BeginObject() => Open('{');

        public JsonWriter EndObject() => Close('}');

        public JsonWriter BeginArray() => Open('[');

        public JsonWriter EndArray() => Close(']');

        public JsonWriter Name(string name)
        {
            Separator();
            _sb.Append(Quote(name)).Append(": ");
            _afterName = true;
            return this;
        }

        public JsonWriter Value(string? value) => Raw(value == null ? "null" : Quote(value));

        public JsonWriter Value(bool value) => Raw(value ? "true" : "false");

        public JsonWriter Value(int value) => Raw(value.ToString(CultureInfo.InvariantCulture));

        public JsonWriter Property(string name, string? value) => Name(name).Value(value);

        public JsonWriter Property(string name, bool value) => Name(name).Value(value);

        public JsonWriter Property(string name, int value) => Name(name).Value(value);

        public JsonWriter StringArray(string name, IEnumerable<string> values)
        {
            Name(name).BeginArray();
            foreach (var value in values) Value(value);
            return EndArray();
        }

        public override string ToString() => _sb.ToString();

        public static string Quote(string value)
        {
            var sb = new StringBuilder(value.Length + 2);
            sb.Append('"');
            foreach (char c in value)
            {
                switch (c)
                {
                    case '"': sb.Append("\\\""); break;
                    case '\\': sb.Append("\\\\"); break;
                    case '\n': sb.Append("\\n"); break;
                    case '\r': sb.Append("\\r"); break;
                    case '\t': sb.Append("\\t"); break;
                    default:
                        if (c < 0x20) sb.Append("\\u").Append(((int)c).ToString("x4", CultureInfo.InvariantCulture));
                        else sb.Append(c);
                        break;
                }
            }

            sb.Append('"');
            return sb.ToString();
        }

        private JsonWriter Open(char bracket)
        {
            Separator();
            _sb.Append(bracket);
            _hasItems.Push(false);
            return this;
        }

        private JsonWriter Close(char bracket)
        {
            bool hadItems = _hasItems.Pop();
            if (hadItems)
            {
                _sb.Append('\n');
                Indent();
            }

            _sb.Append(bracket);
            return this;
        }

        private JsonWriter Raw(string text)
        {
            Separator();
            _sb.Append(text);
            return this;
        }

        private void Separator()
        {
            if (_afterName)
            {
                _afterName = false;
                return;
            }

            if (_hasItems.Count == 0) return;
            bool hadItems = _hasItems.Pop();
            if (hadItems) _sb.Append(',');
            _sb.Append('\n');
            _hasItems.Push(true);
            Indent();
        }

        private void Indent() => _sb.Append(' ', _hasItems.Count * 2);
    }
}
