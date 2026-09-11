using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace DataStage2Airflow
{
    public enum Severity
    {
        Info,
        Warning,
        Error,
    }

    /// <summary>A message about the input or the conversion, surfaced in the migration report.</summary>
    public sealed class Diagnostic
    {
        public Diagnostic(Severity severity, string code, string message, string? location = null)
        {
            Severity = severity;
            Code = code;
            Message = message;
            Location = location;
        }

        public Severity Severity { get; }

        /// <summary>Stable identifier such as <c>DSX002</c>, usable for filtering.</summary>
        public string Code { get; }

        public string Message { get; }

        /// <summary>Where the problem is: a file and line, or a job / stage path.</summary>
        public string? Location { get; }

        public override string ToString()
        {
            var where = Location == null ? string.Empty : Location + ": ";
            return $"{where}{Severity.ToString().ToLowerInvariant()} {Code}: {Message}";
        }
    }

    public sealed class DiagnosticBag : IEnumerable<Diagnostic>
    {
        private readonly List<Diagnostic> _items = new List<Diagnostic>();

        public int Count => _items.Count;

        public int ErrorCount => _items.Count(d => d.Severity == Severity.Error);

        public int WarningCount => _items.Count(d => d.Severity == Severity.Warning);

        public void Add(Diagnostic diagnostic)
        {
            if (diagnostic == null) throw new ArgumentNullException(nameof(diagnostic));
            _items.Add(diagnostic);
        }

        public void AddRange(IEnumerable<Diagnostic> diagnostics)
        {
            foreach (var d in diagnostics) Add(d);
        }

        public void Info(string code, string message, string? location = null) =>
            Add(new Diagnostic(Severity.Info, code, message, location));

        public void Warning(string code, string message, string? location = null) =>
            Add(new Diagnostic(Severity.Warning, code, message, location));

        public void Error(string code, string message, string? location = null) =>
            Add(new Diagnostic(Severity.Error, code, message, location));

        public IEnumerator<Diagnostic> GetEnumerator() => _items.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
