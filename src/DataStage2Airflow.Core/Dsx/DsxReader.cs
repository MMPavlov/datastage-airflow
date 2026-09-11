using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace DataStage2Airflow.Dsx
{
    /// <summary>
    /// Parser for the DataStage text export format (.dsx), as written by Ascential DataStage 7.x
    /// and IBM InfoSphere DataStage 8.x-11.x:
    /// <code>
    /// BEGIN HEADER ... END HEADER
    /// BEGIN DSJOB
    ///    Identifier "JobName"
    ///    BEGIN DSRECORD
    ///       Identifier "V0S1"
    ///       OLEType "CTransformerStage"
    ///       StageVars "CStageVar"          &lt;- a property line followed by subrecords names a collection
    ///       BEGIN DSSUBRECORD ... END DSSUBRECORD
    ///       Expression =+=+=+=
    ///       multi-line text
    ///       =+=+=+=
    ///    END DSRECORD
    /// END DSJOB
    /// </code>
    /// The parser is lenient: structural problems become diagnostics, not exceptions.
    /// </summary>
    public static class DsxReader
    {
        public static DsxDocument Parse(string text, string sourceName = "<text>", DiagnosticBag? diagnostics = null)
        {
            using (var reader = new StringReader(text))
            {
                return Parse(reader, sourceName, diagnostics);
            }
        }

        public static DsxDocument Parse(TextReader reader, string sourceName, DiagnosticBag? diagnostics = null)
        {
            return new Parser(reader, sourceName, diagnostics ?? new DiagnosticBag()).Run();
        }

        private sealed class Frame
        {
            public Frame(string section, object target)
            {
                Section = section;
                Target = target;
            }

            public string Section { get; }

            public object Target { get; }

            public DsxCollection? CurrentCollection { get; set; }

            public string LastPropertyName { get; set; } = string.Empty;

            public string LastPropertyValue { get; set; } = string.Empty;

            public bool LastWasProperty { get; set; }
        }

        private sealed class Parser
        {
            private readonly TextReader _reader;
            private readonly string _source;
            private readonly DiagnosticBag _diagnostics;
            private readonly Stack<Frame> _stack = new Stack<Frame>();
            private readonly DsxDocument _document;
            private int _line;

            public Parser(TextReader reader, string source, DiagnosticBag diagnostics)
            {
                _reader = reader;
                _source = source;
                _diagnostics = diagnostics;
                _document = new DsxDocument(source, ExportFormat.Dsx);
            }

            public DsxDocument Run()
            {
                string? raw;
                while ((raw = _reader.ReadLine()) != null)
                {
                    _line++;
                    var line = raw.Trim();
                    if (line.Length == 0) continue;

                    if (IsKeywordLine(line, "BEGIN"))
                    {
                        Begin(line.Substring(5).Trim());
                    }
                    else if (IsKeywordLine(line, "END"))
                    {
                        End(line.Substring(3).Trim());
                    }
                    else
                    {
                        Property(line);
                    }
                }

                if (_stack.Count > 0)
                {
                    Warn("DSX003", $"file ends inside an open '{_stack.Peek().Section}' block");
                }

                if (_document.Header.Count == 0 && _document.Objects.Count == 0)
                {
                    throw new DsxFormatException($"{_source}: not a DataStage export (no BEGIN HEADER or BEGIN DSJOB found)");
                }

                return _document;
            }

            private static bool IsKeywordLine(string line, string keyword) =>
                line.Length > keyword.Length + 1 &&
                line.StartsWith(keyword, StringComparison.Ordinal) &&
                line[keyword.Length] == ' ';

            private Frame? Top => _stack.Count == 0 ? null : _stack.Peek();

            private void Begin(string section)
            {
                switch (section)
                {
                    case "HEADER":
                        _stack.Push(new Frame(section, _document.Header));
                        break;

                    case "DSRECORD":
                        BeginRecord(section);
                        break;

                    case "DSSUBRECORD":
                        BeginSubRecord(section);
                        break;

                    default:
                        var obj = new DsxObject(section) { Line = _line };
                        if (Top?.Target is DsxObject parent)
                        {
                            parent.Children.Add(obj);
                        }
                        else
                        {
                            if (_stack.Count > 0) Warn("DSX004", $"block '{section}' opened inside '{Top!.Section}'");
                            _document.Objects.Add(obj);
                        }

                        _stack.Push(new Frame(section, obj));
                        break;
                }
            }

            private void BeginRecord(string section)
            {
                DsxObject? owner = null;
                foreach (var frame in _stack)
                {
                    if (frame.Target is DsxObject o)
                    {
                        owner = o;
                        break;
                    }
                }

                if (owner == null)
                {
                    Warn("DSX005", "DSRECORD outside of any job or container block");
                    owner = new DsxObject("DSJOB") { Identifier = "<orphan records>", Line = _line };
                    _document.Objects.Add(owner);
                }

                var record = new DsxRecord(string.Empty) { Line = _line };
                owner.Records.Add(record);
                _stack.Push(new Frame(section, record));
            }

            private void BeginSubRecord(string section)
            {
                var top = Top;
                if (top == null || !(top.Target is DsxRecord record))
                {
                    Warn("DSX006", "DSSUBRECORD outside of a DSRECORD");
                    _stack.Push(new Frame(section, new DsxPropertyBag()));
                    return;
                }

                var collection = top.CurrentCollection;
                if (top.LastWasProperty || collection == null)
                {
                    var name = top.LastWasProperty ? top.LastPropertyName : string.Empty;
                    var type = top.LastWasProperty ? top.LastPropertyValue : string.Empty;
                    collection = record.Collection(name);
                    if (collection == null)
                    {
                        collection = new DsxCollection(name, type);
                        record.Collections.Add(collection);
                    }

                    top.CurrentCollection = collection;
                }

                var bag = new DsxPropertyBag();
                collection.Items.Add(bag);
                top.LastWasProperty = false;
                _stack.Push(new Frame(section, bag));
            }

            private void End(string section)
            {
                if (_stack.Count == 0)
                {
                    Warn("DSX007", $"END {section} without a matching BEGIN");
                    return;
                }

                if (_stack.Peek().Section == section)
                {
                    _stack.Pop();
                    return;
                }

                bool open = false;
                foreach (var frame in _stack)
                {
                    if (frame.Section == section)
                    {
                        open = true;
                        break;
                    }
                }

                if (!open)
                {
                    Warn("DSX007", $"END {section} without a matching BEGIN");
                    return;
                }

                Warn("DSX008", $"END {section} closes unterminated '{_stack.Peek().Section}' block(s)");
                while (_stack.Pop().Section != section)
                {
                }
            }

            private void Property(string line)
            {
                int space = IndexOfWhitespace(line);
                string name = space < 0 ? line : line.Substring(0, space);
                string rest = space < 0 ? string.Empty : line.Substring(space).TrimStart();
                string value = ReadValue(rest, name);

                var top = Top;
                if (top == null)
                {
                    Warn("DSX009", $"property '{name}' outside of any block");
                    return;
                }

                switch (top.Target)
                {
                    case DsxPropertyBag bag:
                        bag.Add(name, value);
                        break;

                    case DsxRecord record:
                        if (name == "Identifier" && record.Identifier.Length == 0)
                        {
                            record.Identifier = value;
                        }
                        else
                        {
                            record.Properties.Add(name, value);
                        }

                        top.LastWasProperty = true;
                        top.LastPropertyName = name;
                        top.LastPropertyValue = value;
                        break;

                    case DsxObject obj:
                        if (name == "Identifier" && obj.Identifier == null)
                        {
                            obj.Identifier = value;
                        }
                        else
                        {
                            obj.Properties.Add(name, value);
                        }

                        break;
                }
            }

            private string ReadValue(string rest, string name)
            {
                if (rest.Length == 0) return string.Empty;

                if (rest[0] == '"')
                {
                    if (!DsxText.TryReadQuoted(rest, 0, out var quoted, out _))
                    {
                        Warn("DSX010", $"unterminated string in property '{name}'");
                    }

                    return quoted;
                }

                if (rest.StartsWith(DsxText.MultiLineMarker, StringComparison.Ordinal))
                {
                    return ReadMultiLine(rest.Substring(DsxText.MultiLineMarker.Length), name);
                }

                return rest;
            }

            // The opening marker ends its line; the text runs until the next marker,
            // which normally sits alone at the start of a line.
            private string ReadMultiLine(string afterMarker, string name)
            {
                int inline = afterMarker.IndexOf(DsxText.MultiLineMarker, StringComparison.Ordinal);
                if (inline >= 0) return afterMarker.Substring(0, inline);

                var sb = new StringBuilder();
                bool first = true;
                if (afterMarker.Trim().Length > 0)
                {
                    sb.Append(afterMarker);
                    first = false;
                }

                int startLine = _line;
                string? raw;
                while ((raw = _reader.ReadLine()) != null)
                {
                    _line++;
                    int close = raw.IndexOf(DsxText.MultiLineMarker, StringComparison.Ordinal);
                    if (close >= 0)
                    {
                        if (close > 0)
                        {
                            if (!first) sb.Append('\n');
                            sb.Append(raw, 0, close);
                        }

                        return sb.ToString();
                    }

                    if (!first) sb.Append('\n');
                    sb.Append(raw);
                    first = false;
                }

                Warn("DSX011", $"multi-line value of '{name}' starting at line {startLine} is not terminated");
                return sb.ToString();
            }

            private static int IndexOfWhitespace(string s)
            {
                for (int i = 0; i < s.Length; i++)
                {
                    if (char.IsWhiteSpace(s[i])) return i;
                }

                return -1;
            }

            private void Warn(string code, string message) =>
                _diagnostics.Warning(code, message, $"{_source}:{_line}");
        }
    }
}
