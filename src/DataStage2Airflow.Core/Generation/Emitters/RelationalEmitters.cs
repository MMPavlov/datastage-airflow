using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using DataStage2Airflow.Expressions;
using DataStage2Airflow.Internal;
using DataStage2Airflow.Model;

namespace DataStage2Airflow.Generation.Emitters
{
    /// <summary>Resolves bare column names of one link (Filter where clauses, lookup conditions).</summary>
    internal sealed class ColumnResolver : INameResolver
    {
        private readonly StageContext _context;
        private readonly Link _link;
        private readonly string _row;

        public ColumnResolver(StageContext context, Link link, string row)
        {
            _context = context;
            _link = link;
            _row = row;
        }

        public ResolvedName? Resolve(string name)
        {
            var column = name;
            int dot = name.IndexOf('.');
            if (dot > 0 && string.Equals(name.Substring(0, dot), _link.Name, StringComparison.OrdinalIgnoreCase))
            {
                column = name.Substring(dot + 1);
            }

            var metadata = _link.FindColumn(column);
            if (metadata != null)
            {
                return new ResolvedName($"{_row}[{Py.Str(metadata.Name)}]", _context.Typed ? metadata.LogicalType : DsLogicalType.String, metadata.Nullable);
            }

            return TransformerResolver.ResolveShared(_context, name);
        }

        public ResolvedName? ResolveParameterReference(string name) => TransformerResolver.ResolveShared(_context, name);

        public string? ResolveRoutine(string name) => TransformerResolver.ResolveRoutine(_context, name);
    }

    internal sealed class LookupEmitter : StageEmitter
    {
        public override string Implementation => "dsio.lookup (hash lookup)";

        public override bool Handles(Stage stage, JobKind kind) => TypeIs(stage, "PxLookup");

        public override void Emit(StageContext c)
        {
            var stream = c.Stage.Inputs.FirstOrDefault(l => l.Kind != LinkKind.Reference);
            var references = c.Stage.Inputs.Where(l => l.Kind == LinkKind.Reference).ToList();
            if (stream == null || references.Count == 0 || c.Stage.Outputs.Count == 0)
            {
                c.Blocking("a lookup needs a stream input, at least one reference input and an output");
                return;
            }

            var output = c.Stage.Outputs[0];
            Link? reject = c.Stage.Outputs.Count > 1 ? c.Stage.Outputs[1] : null;
            var inputs = new List<Link> { stream };
            inputs.AddRange(references);

            var entries = new List<string>();
            foreach (var reference in references)
            {
                var keys = LookupKeys(reference, stream);
                if (keys.Count == 0)
                {
                    c.Blocking($"no key columns found for reference link {reference.Name}");
                    continue;
                }

                var onFail = Mode(reference.LookupFailure, "fail");
                var parts = new List<string>
                {
                    "\"name\": " + Py.Str(reference.Name),
                    "\"frame\": " + c.Var(reference),
                    "\"keys\": [" + string.Join(", ", keys.Select(k => $"({Py.Str(k.Stream)}, {Py.Str(k.Reference)})")) + "]",
                    "\"on_fail\": " + Py.Str(onFail),
                };
                if (reference.TargetProperties.IsTrue("allow_dups") || reference.TargetProperties.IsTrue("allowDups")) parts.Add("\"multiple\": True");
                if (reference.LookupCondition != null)
                {
                    var condition = c.Translate(reference.LookupCondition, new ColumnResolver(c, stream, "row"), $"lookup condition of {reference.Name}", TranslationUse.Condition);
                    parts.Add("\"condition\": lambda row: " + condition.Code);
                    parts.Add("\"condition_not_met\": " + Py.Str(Mode(reference.ConditionNotMet, "fail")));
                }

                if (onFail == "reject" && reject == null)
                {
                    c.Approximation($"lookup failures on {reference.Name} are rejected but there is no reject link; rows are dropped with a warning");
                }

                entries.Add("{" + string.Join(", ", parts) + "}");
            }

            if (c.Failed) return;
            var mapping = c.MappingConstant(output, c.Mapping(output, inputs));
            var args = new List<string> { "ctx", Py.Str(c.Stage.Name), c.Var(stream), "[\n    " + string.Join(",\n    ", entries) + ",\n]", mapping, c.Columns(output) };
            if (reject != null)
            {
                args.Add("reject=True");
                args.Add("reject_columns=" + c.Columns(reject));
            }

            c.Call(reject != null ? c.Assign(output, reject) : c.Assign(output), "dsio.lookup", args);
        }

        internal static string Mode(string? value, string fallback)
        {
            var v = (value ?? string.Empty).Trim().ToLowerInvariant();
            return v == "continue" || v == "drop" || v == "fail" || v == "reject" ? v : fallback;
        }

        private static List<(string Stream, string Reference)> LookupKeys(Link reference, Link stream)
        {
            var keys = new List<(string, string)>();
            foreach (var columns in new[] { reference.TargetColumns, reference.Columns })
            {
                foreach (var column in columns.Where(col => col.IsKey))
                {
                    var streamColumn = StreamColumn(column.EffectiveDerivation, stream) ?? stream.FindColumn(column.Name)?.Name;
                    if (streamColumn != null) keys.Add((streamColumn, column.Name));
                }

                if (keys.Count > 0) break;
            }

            return keys;
        }

        private static string? StreamColumn(string? derivation, Link stream)
        {
            if (derivation == null) return null;
            var m = Regex.Match(derivation.Trim(), "^(?:([A-Za-z_][\\w$]*)\\.)?([A-Za-z_][\\w$]*)$");
            if (!m.Success) return null;
            if (m.Groups[1].Success && !string.Equals(m.Groups[1].Value, stream.Name, StringComparison.OrdinalIgnoreCase)) return null;
            return stream.FindColumn(m.Groups[2].Value)?.Name;
        }
    }

    internal sealed class JoinEmitter : StageEmitter
    {
        public override string Implementation => "dsio.join (hash join)";

        public override bool Handles(Stage stage, JobKind kind) => TypeIs(stage, "PxJoin");

        public override void Emit(StageContext c)
        {
            if (c.Stage.Inputs.Count < 2 || c.Stage.Outputs.Count == 0)
            {
                c.Blocking("a join needs at least two inputs and an output");
                return;
            }

            var how = (c.Stage.Properties.ValueAny("operator", "joinType", "join_type") ?? "innerjoin").Trim().ToLowerInvariant();
            var keys = Keys(c.Stage.Properties);
            if (keys.Count == 0)
            {
                c.Blocking("the join has no key columns");
                return;
            }

            var output = c.Stage.Outputs[0];
            var mapping = c.MappingConstant(output, c.Mapping(output, c.Stage.Inputs));
            var inputs = "[" + string.Join(", ", c.Stage.Inputs.Select(c.Var)) + "]";
            c.Call(c.Assign(output), "dsio.join", new List<string> { "ctx", Py.Str(c.Stage.Name), Py.Str(how), inputs, Py.List(keys), mapping, c.Columns(output) });
            if (how.Contains("full")) c.Note("full outer join: each key column is filled from the input named in its derivation");
        }
    }

    internal sealed class MergeEmitter : StageEmitter
    {
        public override string Implementation => "dsio.merge";

        public override bool Handles(Stage stage, JobKind kind) => TypeIs(stage, "PxMerge");

        public override void Emit(StageContext c)
        {
            if (c.Stage.Inputs.Count < 2 || c.Stage.Outputs.Count == 0)
            {
                c.Blocking("a merge needs a master, at least one update and an output");
                return;
            }

            var keys = Keys(c.Stage.Properties);
            if (keys.Count == 0)
            {
                c.Blocking("the merge has no key columns");
                return;
            }

            var master = c.Stage.Inputs[0];
            var updates = c.Stage.Inputs.Skip(1).ToList();
            var output = c.Stage.Outputs[0];
            var rejects = c.Stage.Outputs.Skip(1).ToList();
            var unmatched = (c.Stage.Properties.ValueAny("unmatchedMasters", "unmatched_masters", "dropBadMasters") ?? "keep").Trim().ToLowerInvariant();
            var mode = unmatched.Contains("drop") || unmatched == "true" ? "drop" : "keep";
            var mapping = c.MappingConstant(output, c.Mapping(output, c.Stage.Inputs));
            var rejectColumns = updates.Select((u, i) => i < rejects.Count ? c.Columns(rejects[i]) : "None");
            var temp = c.State.LocalName("merged_" + c.Stage.Name);
            c.Call(temp, "dsio.merge", new List<string>
            {
                "ctx", Py.Str(c.Stage.Name), c.Var(master), "[" + string.Join(", ", updates.Select(c.Var)) + "]",
                Py.List(keys), mapping, c.Columns(output), "unmatched_masters=" + Py.Str(mode),
                "reject_columns=[" + string.Join(", ", rejectColumns) + "]",
            });
            c.Run.Line($"{c.Assign(output)} = {temp}[0]");
            for (int i = 0; i < rejects.Count && i < updates.Count; i++) c.Run.Line($"{c.Assign(rejects[i])} = {temp}[{i + 1}]");
        }
    }

    internal sealed class AggregatorEmitter : StageEmitter
    {
        private static readonly HashSet<string> Functions = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "max", "min", "sum", "mean", "count", "nvalues", "missing", "nmissing", "nrecs", "range", "std", "ste", "var", "css", "uss", "cv", "first", "last",
        };

        public override string Implementation => "dsio.aggregate";

        public override bool Handles(Stage stage, JobKind kind) => TypeIs(stage, "PxAggregator");

        public override void Emit(StageContext c)
        {
            if (c.Stage.Inputs.Count == 0 || c.Stage.Outputs.Count == 0)
            {
                c.Blocking("an aggregator needs an input and an output");
                return;
            }

            var props = c.Stage.Properties;
            var input = c.Stage.Inputs[0];
            var output = c.Stage.Outputs[0];
            var keys = Keys(props);
            var calculations = new List<string>();
            foreach (var node in props.GetAll("reduce").Concat(props.GetAll("rereduce")))
            {
                foreach (var child in node.Children.Where(ch => Functions.Contains(ch.Name)))
                {
                    calculations.Add($"({Py.Str(node.Value.Trim())}, {Py.Str(child.Name.ToLowerInvariant())}, {Py.Str(child.Value.Trim())})");
                }

                foreach (var child in node.Children.Where(ch => !Functions.Contains(ch.Name) && ch.Name.Length > 0))
                {
                    var option = child.Name.ToLowerInvariant();
                    if (option != "preservetype" && option != "decimaloutput") c.Note($"aggregator option {child.Name} on {node.Value} is ignored");
                }
            }

            var countField = Text.NullIfBlank(props.ValueAny("countField", "count_field"));
            if (calculations.Count == 0 && countField == null)
            {
                c.Blocking("no aggregate calculations found");
                return;
            }

            var args = new List<string> { "ctx", Py.Str(c.Stage.Name), c.Var(input), Py.List(keys), "[" + string.Join(", ", calculations) + "]", c.Columns(output) };
            if (countField != null) args.Add("count_field=" + Py.Str(countField.Trim()));
            if (string.Equals(props.Value("method"), "sort", StringComparison.OrdinalIgnoreCase)) args.Add("sort_output=True");
            c.Call(c.Assign(output), "dsio.aggregate", args);
        }
    }

    /// <summary>Server Aggregator: output columns are either group keys or carry an aggregate function.</summary>
    internal sealed class ServerAggregatorEmitter : StageEmitter
    {
        private static readonly Dictionary<string, string> Functions = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["sum"] = "sum", ["max"] = "max", ["maximum"] = "max", ["min"] = "min", ["minimum"] = "min", ["count"] = "count",
            ["average"] = "mean", ["avg"] = "mean", ["mean"] = "mean", ["first"] = "first", ["last"] = "last",
        };

        public override string Implementation => "dsio.aggregate";

        public override bool Handles(Stage stage, JobKind kind) =>
            TypeIs(stage, "CAggregatorStage", "AGGREGATOR") || (kind == JobKind.Server && TypeContains(stage, "aggregat"));

        public override void Emit(StageContext c)
        {
            if (c.Stage.Inputs.Count == 0 || c.Stage.Outputs.Count == 0)
            {
                c.Blocking("an aggregator needs an input and an output");
                return;
            }

            var input = c.Stage.Inputs[0];
            var output = c.Stage.Outputs[0];
            var keys = new List<string>();
            var calculations = new List<string>();
            foreach (var column in output.Columns)
            {
                var derivation = column.EffectiveDerivation ?? column.Name;
                var function = column.Raw?.GetAny("AggregateFunction", "Aggregation", "AggFunction", "AggFunc", "Function");
                var source = derivation;
                var call = Regex.Match(derivation.Trim(), "^(\\w+)\\s*\\(\\s*(?:[\\w$]+\\.)?([\\w$]+)\\s*\\)$");
                if (call.Success)
                {
                    function = function ?? call.Groups[1].Value;
                    source = call.Groups[2].Value;
                }
                else
                {
                    var m = Regex.Match(derivation.Trim(), "^(?:[\\w$]+\\.)?([\\w$]+)$");
                    if (m.Success) source = m.Groups[1].Value;
                }

                bool group = column.Raw?["Group"] == "1" || string.Equals(function, "Group", StringComparison.OrdinalIgnoreCase)
                    || string.Equals(function, "Group By", StringComparison.OrdinalIgnoreCase);
                if (group)
                {
                    keys.Add(string.Equals(source, column.Name, StringComparison.Ordinal) ? Py.Str(source) : $"({Py.Str(source)}, {Py.Str(column.Name)})");
                    continue;
                }

                if (function != null && Functions.TryGetValue(function.Trim(), out var mapped))
                {
                    calculations.Add($"({Py.Str(source)}, {Py.Str(mapped)}, {Py.Str(column.Name)})");
                    continue;
                }

                c.Blocking($"output column {column.Name} is neither a group key nor a known aggregation ({function ?? derivation})");
            }

            if (c.Failed) return;
            c.Call(c.Assign(output), "dsio.aggregate", new List<string>
            {
                "ctx", Py.Str(c.Stage.Name), c.Var(input), "[" + string.Join(", ", keys) + "]", "[" + string.Join(", ", calculations) + "]", c.Columns(output),
            });
        }
    }

    internal sealed class ChangeCaptureEmitter : StageEmitter
    {
        public override string Implementation => "dsio.change_capture";

        public override bool Handles(Stage stage, JobKind kind) => TypeIs(stage, "PxChangeCapture");

        public override void Emit(StageContext c)
        {
            if (c.Stage.Inputs.Count < 2 || c.Stage.Outputs.Count == 0)
            {
                c.Blocking("change capture needs before and after inputs and an output");
                return;
            }

            var props = c.Stage.Properties;
            var before = c.Stage.Inputs[0];
            var after = c.Stage.Inputs[1];
            var keys = Keys(props);
            if (keys.Count == 0)
            {
                c.Blocking("change capture has no key columns");
                return;
            }

            var values = Keys(props, "value");
            if (values.Count == 0)
            {
                values = after.Columns.Select(col => col.Name).Where(n => !keys.Contains(n, StringComparer.OrdinalIgnoreCase)).ToList();
                c.Note("no value columns listed: every non-key column is compared");
            }

            var codeColumn = Text.NullIfBlank(props.ValueAny("codeField", "code_field", "changeCodeField")) ?? "change_code";
            var drops = new List<string>();
            var codes = new[] { ("dropCopy", "copyCode", 0), ("dropInsert", "insertCode", 1), ("dropDelete", "deleteCode", 2), ("dropEdit", "editCode", 3) }
                .Select(t => (Drop: props.IsTrue(t.Item1), Code: Text.ParseInt(props.Value(t.Item2), t.Item3))).ToList();
            foreach (var code in codes.Where(x => x.Drop)) drops.Add(Py.Int(code.Code));
            var output = c.Stage.Outputs[0];
            c.Call(c.Assign(output), "dsio.change_capture", new List<string>
            {
                "ctx", Py.Str(c.Stage.Name), c.Var(before), c.Var(after), Py.List(keys), Py.List(values), c.Columns(output),
                "code_column=" + Py.Str(codeColumn.Trim()),
                "drop=[" + string.Join(", ", drops) + "]",
                "codes=(" + string.Join(", ", codes.Select(x => Py.Int(x.Code))) + ")",
            });
        }
    }
}
