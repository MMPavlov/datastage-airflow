using System;
using System.Collections.Generic;
using System.Linq;
using DataStage2Airflow.Dsx;

namespace DataStage2Airflow.Model
{
    public enum LinkKind
    {
        Stream,
        Reference,
        Reject,
    }

    public sealed class Column
    {
        public Column(string name)
        {
            Name = name;
        }

        public string Name { get; }

        public int SqlType { get; set; }

        public int Precision { get; set; }

        public int Scale { get; set; }

        public bool Nullable { get; set; }

        /// <summary>Position in the key (1-based); 0 when the column is not a key.</summary>
        public int KeyPosition { get; set; }

        /// <summary>Transformer (or lookup key) expression, as written by the developer.</summary>
        public string? Derivation { get; set; }

        /// <summary>DataStage's normalised copy of <see cref="Derivation"/>.</summary>
        public string? ParsedDerivation { get; set; }

        public string? SourceColumn { get; set; }

        public string Description { get; set; } = string.Empty;

        /// <summary>All properties of the column subrecord (stage-specific ones such as the server aggregator's).</summary>
        public DsxPropertyBag? Raw { get; set; }

        public bool IsKey => KeyPosition > 0;

        public DsLogicalType LogicalType => SqlTypes.Logical(SqlType);

        public string TypeName => SqlTypes.Describe(SqlType, Precision, Scale);

        /// <summary>The derivation to translate: the original text, falling back to the parsed form.</summary>
        public string? EffectiveDerivation =>
            !string.IsNullOrWhiteSpace(Derivation) ? Derivation : (!string.IsNullOrWhiteSpace(ParsedDerivation) ? ParsedDerivation : null);

        public override string ToString() => $"{Name} {TypeName}{(Nullable ? " NULL" : string.Empty)}";
    }

    public sealed class StageVariable
    {
        public StageVariable(string name, string expression)
        {
            Name = name;
            Expression = expression;
        }

        public string Name { get; }

        public string Expression { get; }

        public string? InitialValue { get; set; }

        public int SqlType { get; set; }

        public int Precision { get; set; }

        public int Scale { get; set; }

        public DsLogicalType LogicalType => SqlTypes.Logical(SqlType);
    }

    /// <summary>Decoded parallel-stage properties (the <c>Properties "CCustomProperty"</c> collection).</summary>
    public sealed class PropertySet
    {
        public static readonly PropertySet Empty = new PropertySet(new List<PropertyNode>());

        private readonly List<PropertyNode> _nodes;

        public PropertySet(List<PropertyNode> nodes)
        {
            _nodes = nodes;
        }

        public IReadOnlyList<PropertyNode> Nodes => _nodes;

        public PropertyNode? Get(string name) =>
            _nodes.FirstOrDefault(n => string.Equals(n.Name, name, StringComparison.OrdinalIgnoreCase));

        public IEnumerable<PropertyNode> GetAll(string name) =>
            _nodes.Where(n => string.Equals(n.Name, name, StringComparison.OrdinalIgnoreCase));

        /// <summary>The value, or null when missing or blank (DataStage writes " " for unset options).</summary>
        public string? Value(string name)
        {
            var node = Get(name);
            if (node == null) return null;
            return string.IsNullOrWhiteSpace(node.Value) ? null : node.Value;
        }

        public string? ValueAny(params string[] names)
        {
            foreach (var name in names)
            {
                var v = Value(name);
                if (v != null) return v;
            }

            return null;
        }

        public bool IsTrue(string name)
        {
            var v = Value(name)?.Trim();
            if (string.IsNullOrEmpty(v)) return false;
            return string.Equals(v, "true", StringComparison.OrdinalIgnoreCase)
                || v == "1"
                || string.Equals(v, "yes", StringComparison.OrdinalIgnoreCase)
                || string.Equals(v, name, StringComparison.OrdinalIgnoreCase);
        }

        public static PropertySet FromRecord(DsxRecord? record, string collection = "Properties")
        {
            if (record == null) return Empty;
            var nodes = new List<PropertyNode>();
            foreach (var sub in record.SubRecords(collection))
            {
                var name = (sub.Get("Name") ?? string.Empty).Trim();
                if (name.Length == 0) continue;
                nodes.AddRange(PropertyTree.Parse(name, sub.Get("Value") ?? string.Empty));
            }

            return new PropertySet(nodes);
        }
    }

    /// <summary>Engine meta properties (<c>MetaBag "CMetaProperty"</c>), e.g. APT/SchemaFormat.</summary>
    public sealed class MetaBag
    {
        public static readonly MetaBag Empty = new MetaBag(new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase));

        private readonly Dictionary<string, string> _values;

        private MetaBag(Dictionary<string, string> values)
        {
            _values = values;
        }

        public IEnumerable<KeyValuePair<string, string>> Values => _values;

        public string? Get(string name) => _values.TryGetValue(name, out var v) ? v : null;

        public static MetaBag FromRecord(DsxRecord? record)
        {
            if (record == null) return Empty;
            var values = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var sub in record.SubRecords("MetaBag"))
            {
                var name = sub.Get("Name");
                if (name == null || values.ContainsKey(name)) continue;
                values[name] = sub.Get("Value") ?? string.Empty;
            }

            return new MetaBag(values);
        }
    }

    public sealed class Stage
    {
        public Stage(string id, string name, string oleType, string stageType, DsxRecord record)
        {
            Id = id;
            Name = name;
            OleType = oleType;
            StageType = stageType;
            Record = record;
            Properties = PropertySet.FromRecord(record);
            Meta = MetaBag.FromRecord(record);
        }

        /// <summary>Record identifier inside the job, e.g. <c>V0S3</c>.</summary>
        public string Id { get; }

        public string Name { get; }

        public string OleType { get; }

        /// <summary><c>StageType</c> of the record (e.g. PxSequentialFile), or the OLE type for built-in stages.</summary>
        public string StageType { get; }

        public DsxRecord Record { get; }

        /// <summary>The container view (V0 = job canvas, V1.. = local containers) the stage sits in.</summary>
        public string ViewId { get; set; } = "V0";

        public PropertySet Properties { get; }

        public MetaBag Meta { get; }

        public List<Link> Inputs { get; } = new List<Link>();

        public List<Link> Outputs { get; } = new List<Link>();

        public List<StageVariable> StageVariables { get; } = new List<StageVariable>();

        public bool IsTransformer => OleType == "CTransformerStage" || StageType == "CTransformerStage";

        public string Description => Record["Description"] ?? string.Empty;

        public override string ToString() => $"{Name} ({StageType})";
    }

    /// <summary>
    /// A link, stored in DSX as two pins: the source stage's output pin (which normally owns the columns
    /// and derivations) and the target stage's input pin, cross-referenced through <c>Partner</c>.
    /// </summary>
    public sealed class Link
    {
        public Link(string name, Stage source, Stage? target, DsxRecord sourcePin, DsxRecord? targetPin)
        {
            Name = name;
            Source = source;
            Target = target;
            SourcePin = sourcePin;
            TargetPin = targetPin;
            SourceProperties = PropertySet.FromRecord(sourcePin);
            TargetProperties = PropertySet.FromRecord(targetPin);
            SourceMeta = MetaBag.FromRecord(sourcePin);
            TargetMeta = MetaBag.FromRecord(targetPin);
        }

        public string Name { get; }

        public Stage Source { get; }

        public Stage? Target { get; }

        public DsxRecord SourcePin { get; }

        public DsxRecord? TargetPin { get; }

        public string Id => SourcePin.Identifier;

        public List<Column> Columns { get; } = new List<Column>();

        /// <summary>Columns stored on the target stage's input pin (transformer and lookup key expressions live there).</summary>
        public List<Column> TargetColumns { get; } = new List<Column>();

        public LinkKind Kind { get; set; } = LinkKind.Stream;

        public PropertySet SourceProperties { get; }

        public PropertySet TargetProperties { get; }

        public MetaBag SourceMeta { get; }

        public MetaBag TargetMeta { get; }

        /// <summary>Transformer output constraint (the WHERE of an output link).</summary>
        public string? Constraint { get; set; }

        /// <summary>Transformer "Otherwise" link: gets rows no other output accepted.</summary>
        public bool IsOtherwise { get; set; }

        /// <summary>Transformer or stage error/reject link.</summary>
        public bool IsError { get; set; }

        public int RowLimit { get; set; }

        /// <summary>Numeric LinkType of the input pin: 1 = stream, 2 = reference.</summary>
        public int PinLinkType { get; set; }

        /// <summary>Lookup behaviour when no match is found: continue, drop, fail or reject.</summary>
        public string? LookupFailure { get; set; }

        public string? ConditionNotMet { get; set; }

        public string? LookupCondition { get; set; }

        public Column? FindColumn(string name) =>
            Columns.FirstOrDefault(c => string.Equals(c.Name, name, StringComparison.OrdinalIgnoreCase));

        public override string ToString() => $"{Source.Name} --{Name}--> {Target?.Name ?? "?"}";
    }
}
