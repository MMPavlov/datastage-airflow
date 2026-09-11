using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace DataStage2Airflow.Dsx
{
    public enum ExportFormat
    {
        Dsx,
        Xml,
    }

    /// <summary>
    /// Ordered name/value list with case-insensitive lookup. DSX allows a name to repeat,
    /// so this is deliberately not a dictionary; lookups return the first occurrence.
    /// </summary>
    public sealed class DsxPropertyBag : IEnumerable<KeyValuePair<string, string>>
    {
        private readonly List<KeyValuePair<string, string>> _items = new List<KeyValuePair<string, string>>();

        public int Count => _items.Count;

        public string? this[string name] => Get(name);

        public void Add(string name, string value) => _items.Add(new KeyValuePair<string, string>(name, value));

        public string? Get(string name)
        {
            for (int i = 0; i < _items.Count; i++)
            {
                if (string.Equals(_items[i].Key, name, StringComparison.OrdinalIgnoreCase)) return _items[i].Value;
            }

            return null;
        }

        /// <summary>First value found under any of the names. Property names drift between DataStage releases.</summary>
        public string? GetAny(params string[] names)
        {
            foreach (var name in names)
            {
                var value = Get(name);
                if (value != null) return value;
            }

            return null;
        }

        public bool Has(string name) => Get(name) != null;

        public IEnumerable<string> GetAll(string name) =>
            _items.Where(kv => string.Equals(kv.Key, name, StringComparison.OrdinalIgnoreCase)).Select(kv => kv.Value);

        public IEnumerator<KeyValuePair<string, string>> GetEnumerator() => _items.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }

    /// <summary>
    /// A run of DSSUBRECORD blocks introduced by a property line such as <c>Columns "COutputColumn"</c>.
    /// </summary>
    public sealed class DsxCollection
    {
        public DsxCollection(string name, string typeName)
        {
            Name = name;
            TypeName = typeName;
        }

        /// <summary>The introducing property name, e.g. <c>Columns</c>, <c>Properties</c>, <c>MetaBag</c>.</summary>
        public string Name { get; }

        /// <summary>The class of the items, e.g. <c>COutputColumn</c>.</summary>
        public string TypeName { get; }

        public List<DsxPropertyBag> Items { get; } = new List<DsxPropertyBag>();
    }

    /// <summary>One BEGIN DSRECORD ... END DSRECORD block: a job, container view, stage, link pin, routine...</summary>
    public sealed class DsxRecord
    {
        public DsxRecord(string identifier)
        {
            Identifier = identifier;
        }

        public string Identifier { get; set; }

        /// <summary>1-based line of the BEGIN DSRECORD in the source file (0 when unknown).</summary>
        public int Line { get; set; }

        public DsxPropertyBag Properties { get; } = new DsxPropertyBag();

        public List<DsxCollection> Collections { get; } = new List<DsxCollection>();

        public string OleType => Properties.Get("OLEType") ?? string.Empty;

        public string Name => Properties.Get("Name") ?? Identifier;

        public string? this[string property] => Properties.Get(property);

        public DsxCollection? Collection(string name) =>
            Collections.FirstOrDefault(c => string.Equals(c.Name, name, StringComparison.OrdinalIgnoreCase));

        public IReadOnlyList<DsxPropertyBag> SubRecords(string collectionName)
        {
            var collection = Collection(collectionName);
            return collection == null ? (IReadOnlyList<DsxPropertyBag>)Array.Empty<DsxPropertyBag>() : collection.Items;
        }

        public override string ToString() => $"{Identifier} {OleType} '{Name}'";
    }

    /// <summary>A top-level block such as DSJOB, DSSHAREDCONTAINER, DSROUTINES or DSTABLEDEFS.</summary>
    public sealed class DsxObject
    {
        public DsxObject(string section)
        {
            Section = section;
        }

        public string Section { get; }

        public string? Identifier { get; set; }

        public int Line { get; set; }

        public DsxPropertyBag Properties { get; } = new DsxPropertyBag();

        public List<DsxRecord> Records { get; } = new List<DsxRecord>();

        /// <summary>Nested non-record blocks, e.g. DSUBINARY inside DSROUTINES.</summary>
        public List<DsxObject> Children { get; } = new List<DsxObject>();

        public DsxRecord? FindRecord(string identifier) =>
            Records.FirstOrDefault(r => string.Equals(r.Identifier, identifier, StringComparison.Ordinal));

        public DsxRecord? Root => FindRecord("ROOT") ?? Records.FirstOrDefault(r => r.OleType == "CJobDefn");

        public override string ToString() => $"{Section} {Identifier}";
    }

    /// <summary>A whole export file.</summary>
    public sealed class DsxDocument
    {
        public DsxDocument(string sourceName, ExportFormat format)
        {
            SourceName = sourceName;
            Format = format;
        }

        public string SourceName { get; }

        public ExportFormat Format { get; }

        public DsxPropertyBag Header { get; } = new DsxPropertyBag();

        public List<DsxObject> Objects { get; } = new List<DsxObject>();

        /// <summary>The DataStage project the export was taken from.</summary>
        public string? ProjectName => Header.Get("ToolInstanceID");

        public string? ServerVersion => Header.Get("ServerVersion");

        public string? ExportingTool => Header.Get("ExportingTool");

        public IEnumerable<DsxObject> Jobs => Objects.Where(o => o.Section == "DSJOB");

        public IEnumerable<DsxObject> SharedContainers => Objects.Where(o => o.Section == "DSSHAREDCONTAINER");

        public IEnumerable<DsxObject> OfSection(string section) => Objects.Where(o => o.Section == section);
    }

    public sealed class DsxFormatException : Exception
    {
        public DsxFormatException(string message)
            : base(message)
        {
        }
    }
}
