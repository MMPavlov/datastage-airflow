using System;
using System.Collections.Generic;
using System.Linq;

namespace DataStage2Airflow.Dsx
{
    /// <summary>One node of a parallel-stage property value.</summary>
    public sealed class PropertyNode
    {
        public PropertyNode(string name, string value)
        {
            Name = name;
            Value = value;
        }

        public string Name { get; }

        public string Value { get; }

        public List<PropertyNode> Children { get; } = new List<PropertyNode>();

        public PropertyNode? Child(string name) =>
            Children.FirstOrDefault(c => string.Equals(c.Name, name, StringComparison.OrdinalIgnoreCase));

        public string? ChildValue(string name) => Child(name)?.Value;

        public bool HasChild(string name) => Child(name) != null;

        public override string ToString() =>
            Children.Count == 0 ? $"{Name}={Value}" : $"{Name}={Value} {{{string.Join(", ", Children)}}}";
    }

    /// <summary>
    /// Decodes the tree encoding DataStage uses for parallel-stage properties. After DSX unescaping,
    /// a value such as <c>\(2)\(2)0\(1)\(3)key\(2)CUST_ID\(2)0\(1)\(3)\(3)asc_desc\(2)desc\(2)0</c>
    /// is a list of entries separated by U+0001; each entry starts with one U+0003 per nesting level,
    /// followed by <c>name U+0002 value U+0002 flag</c>. The example is <c>key=CUST_ID { asc_desc=desc }</c>.
    /// Plain values (no control characters) become a single node.
    /// </summary>
    public static class PropertyTree
    {
        private const char EntrySeparator = (char)0x01;
        private const char FieldSeparator = (char)0x02;
        private const char LevelMarker = (char)0x03;

        public static bool IsEncoded(string value)
        {
            foreach (char c in value)
            {
                if (c == EntrySeparator || c == FieldSeparator || c == LevelMarker) return true;
            }

            return false;
        }

        public static IReadOnlyList<PropertyNode> Parse(string name, string value)
        {
            var roots = new List<PropertyNode>();
            if (!IsEncoded(value))
            {
                roots.Add(new PropertyNode(name.Trim(), value));
                return roots;
            }

            var path = new List<PropertyNode>();
            var entries = value.Split(EntrySeparator);
            for (int e = 1; e < entries.Length; e++)
            {
                var entry = entries[e];
                int depth = 0;
                while (depth < entry.Length && entry[depth] == LevelMarker) depth++;
                if (depth == 0) continue;

                var fields = entry.Substring(depth).Split(FieldSeparator);
                var node = new PropertyNode(fields[0].Trim(), fields.Length > 1 ? fields[1] : string.Empty);

                if (depth == 1 || path.Count == 0)
                {
                    roots.Add(node);
                    path.Clear();
                    path.Add(node);
                }
                else
                {
                    int parentIndex = Math.Min(depth - 2, path.Count - 1);
                    path[parentIndex].Children.Add(node);
                    path.RemoveRange(parentIndex + 1, path.Count - parentIndex - 1);
                    path.Add(node);
                }
            }

            return roots;
        }
    }
}
