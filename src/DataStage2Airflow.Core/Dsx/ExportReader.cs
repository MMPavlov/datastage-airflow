using System;
using System.IO;

namespace DataStage2Airflow.Dsx
{
    /// <summary>Reads an export file in either format, detected from its content.</summary>
    public static class ExportReader
    {
        public static DsxDocument ReadFile(string path, DiagnosticBag? diagnostics = null)
        {
            if (!File.Exists(path)) throw new FileNotFoundException("Export file not found", path);
            using (var reader = TextDecoding.OpenText(path, out _))
            {
                return Read(reader, path, diagnostics);
            }
        }

        public static DsxDocument ReadText(string text, string sourceName = "<text>", DiagnosticBag? diagnostics = null)
        {
            using (var reader = new StringReader(text))
            {
                return Read(reader, sourceName, diagnostics);
            }
        }

        private static DsxDocument Read(TextReader reader, string sourceName, DiagnosticBag? diagnostics)
        {
            // Peek the first significant character without consuming the stream.
            var buffered = new PeekableReader(reader);
            int first = buffered.PeekSignificant();
            if (first == '<') return XmlExportReader.Parse(buffered, sourceName, diagnostics);
            return DsxReader.Parse(buffered, sourceName, diagnostics);
        }

        /// <summary>A TextReader that can look past leading whitespace and a BOM before parsing starts.</summary>
        private sealed class PeekableReader : TextReader
        {
            private readonly TextReader _inner;
            private string _pending = string.Empty;
            private int _pendingPos;

            public PeekableReader(TextReader inner)
            {
                _inner = inner;
            }

            public int PeekSignificant()
            {
                var sb = new System.Text.StringBuilder();
                int c;
                while ((c = _inner.Read()) >= 0)
                {
                    sb.Append((char)c);
                    if (!char.IsWhiteSpace((char)c) && c != 0xFEFF) break;
                }

                _pending = sb.ToString();
                _pendingPos = 0;
                return c;
            }

            public override int Peek()
            {
                if (_pendingPos < _pending.Length) return _pending[_pendingPos];
                return _inner.Peek();
            }

            public override int Read()
            {
                if (_pendingPos < _pending.Length) return _pending[_pendingPos++];
                return _inner.Read();
            }

            public override int Read(char[] buffer, int index, int count)
            {
                if (_pendingPos < _pending.Length)
                {
                    int n = Math.Min(count, _pending.Length - _pendingPos);
                    _pending.CopyTo(_pendingPos, buffer, index, n);
                    _pendingPos += n;
                    return n;
                }

                return _inner.Read(buffer, index, count);
            }

            public override string? ReadLine()
            {
                if (_pendingPos < _pending.Length)
                {
                    var sb = new System.Text.StringBuilder();
                    while (_pendingPos < _pending.Length)
                    {
                        char ch = _pending[_pendingPos++];
                        if (ch == '\n') return sb.ToString();
                        if (ch == '\r')
                        {
                            if (_pendingPos < _pending.Length)
                            {
                                if (_pending[_pendingPos] == '\n') _pendingPos++;
                            }
                            else if (_inner.Peek() == '\n')
                            {
                                _inner.Read();
                            }

                            return sb.ToString();
                        }

                        sb.Append(ch);
                    }

                    var rest = _inner.ReadLine();
                    return rest == null ? sb.ToString() : sb.Append(rest).ToString();
                }

                return _inner.ReadLine();
            }

            protected override void Dispose(bool disposing)
            {
                if (disposing) _inner.Dispose();
                base.Dispose(disposing);
            }
        }
    }
}
