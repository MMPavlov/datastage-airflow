using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using DataStage2Airflow.Dsx;
using DataStage2Airflow.Internal;
using DataStage2Airflow.Model;

namespace DataStage2Airflow.Generation.Emitters
{
    internal static class RowHelpers
    {
        public static bool Require(StageContext c, int inputs, int outputs)
        {
            if (c.Stage.Inputs.Count >= inputs && c.Stage.Outputs.Count >= outputs) return true;
            c.Blocking($"the stage needs {inputs} input(s) and {outputs} output(s)");
            return false;
        }

        /// <summary>Writes <c>output = dsio.project(ctx, input, [(out, in), ...], COLUMNS)</c>.</summary>
        public static void Project(StageContext c, Link input, Link output)
        {
            var mapping = c.Mapping(output, new[] { input });
            var sb = new StringBuilder("[\n");
            foreach (var m in mapping) sb.Append($"    ({Py.Str(m.Output)}, {Py.Str(m.Column)}),\n");
            sb.Append(']');
            var constant = c.State.Constant("MAP_" + output.Name, sb.ToString());
            var typed = c.Typed ? string.Empty : ", typed=False";
            c.Run.Line($"{c.Assign(output)} = dsio.project(ctx, {c.Var(input)}, {constant}, {c.Columns(output)}{typed})");
        }

        public static string SortKey(PropertyNode node)
        {
            var column = node.Value.Trim();
            var direction = (node.ChildValue("asc_desc") ?? node.ChildValue("order") ?? "asc").Trim().ToLowerInvariant();
            bool ascending = !direction.StartsWith("desc", StringComparison.Ordinal);
            var nulls = (node.ChildValue("nulls_position") ?? node.ChildValue("nullsPosition") ?? "first").Trim().ToLowerInvariant();
            bool caseSensitive = !string.Equals((node.ChildValue("ci_cs") ?? "cs").Trim(), "ci", StringComparison.OrdinalIgnoreCase);
            var args = new List<string> { Py.Str(column) };
            if (!ascending) args.Add("ascending=False");
            if (nulls.StartsWith("last", StringComparison.Ordinal)) args.Add("nulls_first=False");
            if (!caseSensitive) args.Add("case_sensitive=False");
            return $"dsio.SortKey({string.Join(", ", args)})";
        }
    }

    internal sealed class SortEmitter : StageEmitter
    {
        public override string Implementation => "dsio.sort (stable multi-key sort)";

        public override bool Handles(Stage stage, JobKind kind) => TypeIs(stage, "PxSort");

        public override void Emit(StageContext c)
        {
            if (!RowHelpers.Require(c, 1, 1)) return;
            var props = c.Stage.Properties;
            var keys = props.GetAll("key").Where(n => n.Value.Trim().Length > 0).Select(RowHelpers.SortKey).ToList();
            if (keys.Count == 0)
            {
                c.Blocking("the sort has no key columns");
                return;
            }

            var output = c.Stage.Outputs[0];
            var args = new List<string> { "ctx", Py.Str(c.Stage.Name), c.Var(c.Stage.Inputs[0]), "[" + string.Join(", ", keys) + "]", c.Columns(output) };
            var allowDuplicates = props.Value("allowDups") ?? props.Value("allow_dups");
            if (props.IsTrue("unique") || string.Equals(allowDuplicates, "false", StringComparison.OrdinalIgnoreCase)) args.Add("unique=True");
            if (props.IsTrue("createKeyChangeColumn") || props.IsTrue("keyChangeColumn")) args.Add("key_change_column=\"keyChange\"");
            if (props.IsTrue("createClusterKeyChangeColumn")) c.Approximation("cluster key change column is not generated");
            c.Call(c.Assign(output), "dsio.sort", args);
        }
    }

    /// <summary>Server Sort plugin: a sort specification such as "COL1 asc, COL2 desc".</summary>
    internal sealed class ServerSortEmitter : StageEmitter
    {
        private static readonly Regex Specification = new Regex(
            "^\\s*[\\w$]+(\\s+(asc|desc|ascending|descending|a|d))?(\\s*,\\s*[\\w$]+(\\s+(asc|desc|ascending|descending|a|d))?)*\\s*$",
            RegexOptions.IgnoreCase);

        public override string Implementation => "dsio.sort";

        public override bool Handles(Stage stage, JobKind kind) => kind == JobKind.Server && (TypeIs(stage, "SORT") || TypeContains(stage, "sort"));

        public override void Emit(StageContext c)
        {
            if (!RowHelpers.Require(c, 1, 1)) return;
            var input = c.Stage.Inputs[0];
            string? spec = null;
            var candidates = c.Stage.Record.Properties.Concat(input.TargetPin?.Properties ?? new DsxPropertyBag())
                .Concat(c.Stage.Properties.Nodes.Select(n => new KeyValuePair<string, string>(n.Name, n.Value)));
            foreach (var pair in candidates)
            {
                if (pair.Key.IndexOf("sort", StringComparison.OrdinalIgnoreCase) < 0 && pair.Key.IndexOf("spec", StringComparison.OrdinalIgnoreCase) < 0) continue;
                if (Specification.IsMatch(pair.Value) && input.Columns.Any(col => pair.Value.IndexOf(col.Name, StringComparison.OrdinalIgnoreCase) >= 0))
                {
                    spec = pair.Value;
                    break;
                }
            }

            if (spec == null)
            {
                c.Blocking("no sort specification found");
                return;
            }

            var keys = new List<string>();
            foreach (var part in spec.Split(','))
            {
                var words = part.Trim().Split(new[] { ' ', '\t' }, StringSplitOptions.RemoveEmptyEntries);
                bool descending = words.Length > 1 && words[1].StartsWith("d", StringComparison.OrdinalIgnoreCase);
                keys.Add($"dsio.SortKey({Py.Str(words[0])}{(descending ? ", ascending=False" : string.Empty)})");
            }

            var output = c.Stage.Outputs[0];
            c.Call(c.Assign(output), "dsio.sort", new List<string> { "ctx", Py.Str(c.Stage.Name), c.Var(input), "[" + string.Join(", ", keys) + "]", c.Columns(output) });
        }
    }

    internal sealed class RemoveDuplicatesEmitter : StageEmitter
    {
        public override string Implementation => "dsio.remove_duplicates (adjacent duplicates, input assumed sorted)";

        public override bool Handles(Stage stage, JobKind kind) => TypeIs(stage, "PxRemDup");

        public override void Emit(StageContext c)
        {
            if (!RowHelpers.Require(c, 1, 1)) return;
            var nodes = c.Stage.Properties.GetAll("key").Where(n => n.Value.Trim().Length > 0).ToList();
            if (nodes.Count == 0)
            {
                c.Blocking("remove duplicates has no key columns");
                return;
            }

            var keep = (c.Stage.Properties.Value("keep") ?? "first").Trim().ToLowerInvariant();
            var output = c.Stage.Outputs[0];
            var args = new List<string>
            {
                "ctx", Py.Str(c.Stage.Name), c.Var(c.Stage.Inputs[0]), Py.List(nodes.Select(n => n.Value.Trim())), c.Columns(output),
                "keep=" + Py.Str(keep == "last" ? "last" : "first"),
            };
            if (nodes.Any(n => string.Equals((n.ChildValue("ci_cs") ?? "cs").Trim(), "ci", StringComparison.OrdinalIgnoreCase)))
            {
                args.Add("case_sensitive=[" + string.Join(", ", nodes.Select(n => Py.Bool(!string.Equals((n.ChildValue("ci_cs") ?? "cs").Trim(), "ci", StringComparison.OrdinalIgnoreCase)))) + "]");
            }

            c.Call(c.Assign(output), "dsio.remove_duplicates", args);
        }
    }

    internal sealed class FunnelEmitter : StageEmitter
    {
        public override string Implementation => "dsio.funnel";

        public override bool Handles(Stage stage, JobKind kind) => TypeIs(stage, "PxFunnel");

        public override void Emit(StageContext c)
        {
            if (!RowHelpers.Require(c, 1, 1)) return;
            var type = (c.Stage.Properties.ValueAny("funneltype", "funnel_type", "type") ?? "continuous").Trim().ToLowerInvariant();
            var output = c.Stage.Outputs[0];
            var args = new List<string> { "ctx", Py.Str(c.Stage.Name), "[" + string.Join(", ", c.Stage.Inputs.Select(c.Var)) + "]", c.Columns(output) };
            if (type.Contains("sort"))
            {
                var keys = c.Stage.Properties.GetAll("key").Where(n => n.Value.Trim().Length > 0).Select(RowHelpers.SortKey).ToList();
                if (keys.Count == 0) c.Approximation("sort funnel without keys: inputs are concatenated");
                else args.Add("sort_keys=[" + string.Join(", ", keys) + "]");
            }
            else if (!type.Contains("sequence"))
            {
                c.Note("continuous funnel: inputs are concatenated in link order (DataStage interleaves them)");
            }

            c.Call(c.Assign(output), "dsio.funnel", args);
        }
    }

    internal sealed class CopyEmitter : StageEmitter
    {
        public override string Implementation => "dsio.project";

        public override bool Handles(Stage stage, JobKind kind) => TypeIs(stage, "PxCopy");

        public override void Emit(StageContext c)
        {
            if (c.Stage.Inputs.Count == 0)
            {
                c.Blocking("the copy stage has no input");
                return;
            }

            foreach (var output in c.Stage.Outputs) RowHelpers.Project(c, c.Stage.Inputs[0], output);
        }
    }

    /// <summary>Filter: SQL-like where clauses, each routed to an output link.</summary>
    internal sealed class FilterEmitter : StageEmitter
    {
        private static readonly Regex SqlLiteral = new Regex("'[^']*'|\"[^\"]*\"");
        private static readonly Regex LiteralMark = new Regex("\\u0001(\\d+)\\u0001");

        public override string Implementation => "dsio.filter_rows";

        public override bool Handles(Stage stage, JobKind kind) => TypeIs(stage, "PxFilter");

        public override void Emit(StageContext c)
        {
            if (!RowHelpers.Require(c, 1, 1)) return;
            var props = c.Stage.Properties;
            var input = c.Stage.Inputs[0];
            var outputs = c.Stage.Outputs.ToList();
            Link? reject = props.IsTrue("reject") && outputs.Count > 1 ? outputs[outputs.Count - 1] : null;
            var streams = outputs.Where(o => !ReferenceEquals(o, reject)).ToList();
            var clauses = new List<string>[streams.Count];
            for (int i = 0; i < clauses.Length; i++) clauses[i] = new List<string>();

            var wheres = props.GetAll("where").Where(n => n.Value.Trim().Length > 0).ToList();
            if (wheres.Count == 0)
            {
                c.Blocking("the filter has no where clauses");
                return;
            }

            var resolver = new ColumnResolver(c, input, "row");
            for (int i = 0; i < wheres.Count; i++)
            {
                var node = wheres[i];
                var target = node.ChildValue("outputlink") ?? node.ChildValue("output_link") ?? node.ChildValue("outputLink") ?? node.ChildValue("link") ?? node.ChildValue("target");
                int index = target != null ? Text.ParseInt(target, i) : i;
                if (index < 0 || index >= streams.Count)
                {
                    c.Approximation($"where clause '{Py.Short(node.Value, 60)}' names output {index}, which does not exist; it goes to the last output");
                    index = streams.Count - 1;
                }

                var translated = c.Translate(SqlToExpression(node.Value), resolver, "where clause", TranslationUse.Condition);
                clauses[index].Add(translated.Code);
            }

            var predicates = clauses.Select(list => list.Count == 0 ? "lambda row: False" : "lambda row: " + (list.Count == 1 ? list[0] : string.Join(" or ", list.Select(x => "(" + x + ")")))).ToList();
            var args = new List<string>
            {
                "ctx", Py.Str(c.Stage.Name), c.Var(input),
                "[\n    " + string.Join(",\n    ", predicates) + ",\n]",
                "[" + string.Join(", ", streams.Select(c.Columns)) + "]",
            };
            if (props.IsTrue("first") || props.IsTrue("outputRowsOnlyOnce")) args.Add("first_only=True");
            var assigned = new List<Link>(streams);
            if (reject != null)
            {
                args.Add("reject_columns=" + c.Columns(reject));
                assigned.Add(reject);
            }

            c.Call(c.Assign(assigned.ToArray()) + (assigned.Count == 1 ? ", " : string.Empty), "dsio.filter_rows", args);
        }

        /// <summary>SQL predicates to DataStage expression syntax: IS [NOT] NULL, [NOT] LIKE, BETWEEN, TRUE, FALSE.
        /// String literals are set aside first, so that the rewrites never change their text.</summary>
        internal static string SqlToExpression(string clause)
        {
            const string Literal = "\u0001\\d+\u0001";
            var literals = new List<string>();
            var text = SqlLiteral.Replace(clause, m =>
            {
                literals.Add(m.Value);
                return "\u0001" + Py.Int(literals.Count - 1) + "\u0001";
            });
            text = Regex.Replace(text, "([\\w.$]+)\\s+IS\\s+NOT\\s+NULL", "IsNotNull($1)", RegexOptions.IgnoreCase);
            text = Regex.Replace(text, "([\\w.$]+)\\s+IS\\s+NULL", "IsNull($1)", RegexOptions.IgnoreCase);
            text = Regex.Replace(text, "([\\w.$]+)\\s+NOT\\s+LIKE\\s+(" + Literal + ")", "Not(Like($1, $2))", RegexOptions.IgnoreCase);
            text = Regex.Replace(text, "([\\w.$]+)\\s+LIKE\\s+(" + Literal + ")", "Like($1, $2)", RegexOptions.IgnoreCase);
            text = Regex.Replace(text, "([\\w.$]+)\\s+BETWEEN\\s+(" + Literal + "|[\\w.+-]+)\\s+AND\\s+(" + Literal + "|[\\w.+-]+)", "($1 >= $2 AND $1 <= $3)", RegexOptions.IgnoreCase);
            text = Regex.Replace(text, "\\bTRUE\\b", "1", RegexOptions.IgnoreCase);
            text = Regex.Replace(text, "\\bFALSE\\b", "0", RegexOptions.IgnoreCase);
            return LiteralMark.Replace(text, m => literals[Text.ParseInt(m.Groups[1].Value)]);
        }
    }

    internal sealed class SwitchEmitter : StageEmitter
    {
        public override string Implementation => "dsio.switch_rows";

        public override bool Handles(Stage stage, JobKind kind) => TypeIs(stage, "PxSwitch");

        public override void Emit(StageContext c)
        {
            if (!RowHelpers.Require(c, 1, 1)) return;
            var props = c.Stage.Properties;
            var selector = Text.NullIfBlank(props.ValueAny("selector", "selectorColumn", "selector_column"));
            if (selector == null)
            {
                c.Blocking("the switch has no selector column");
                return;
            }

            var cases = new List<string>();
            int next = 0;
            foreach (var node in props.GetAll("case"))
            {
                var text = node.Value.Trim();
                var m = Regex.Match(text, "^(.*?)\\s*=\\s*(\\d+)\\s*$");
                var value = (m.Success ? m.Groups[1].Value : text).Trim().Trim('\'', '"');
                int index = m.Success ? Text.ParseInt(m.Groups[2].Value, next) : next;
                next = index + 1;
                cases.Add($"{Py.Str(value)}: {Py.Int(index)}");
            }

            if (cases.Count == 0)
            {
                c.Blocking("the switch has no cases");
                return;
            }

            var ifNotFound = (props.ValueAny("ifNotFound", "if_not_found", "discard") ?? "drop").Trim().ToLowerInvariant();
            c.Approximation("switch cases were read from the stage properties; check the case-to-link mapping");
            var outputs = c.Stage.Outputs.ToArray();
            c.Call(c.Assign(outputs) + (outputs.Length == 1 ? ", " : string.Empty), "dsio.switch_rows", new List<string>
            {
                "ctx", Py.Str(c.Stage.Name), c.Var(c.Stage.Inputs[0]), Py.Str(selector.Trim()), "{" + string.Join(", ", cases) + "}",
                "[" + string.Join(", ", outputs.Select(c.Columns)) + "]",
                "if_not_found=" + Py.Str(ifNotFound.Contains("fail") ? "fail" : "drop"),
            });
        }
    }

    internal sealed class HeadTailEmitter : StageEmitter
    {
        public override string Implementation => "dsio.head / dsio.tail";

        public override bool Handles(Stage stage, JobKind kind) => TypeIs(stage, "PxHead", "PxTail");

        public override void Emit(StageContext c)
        {
            if (!RowHelpers.Require(c, 1, 1)) return;
            var props = c.Stage.Properties;
            int count = Text.ParseInt(props.ValueAny("nrecs", "numRows", "rows"), 10);
            var output = c.Stage.Outputs[0];
            bool head = TypeIs(c.Stage, "PxHead");
            var args = new List<string> { "ctx", Py.Str(c.Stage.Name), c.Var(c.Stage.Inputs[0]), Py.Int(count), c.Columns(output) };
            if (head)
            {
                int skip = Text.ParseInt(props.ValueAny("skip", "skipRows"), 0);
                if (skip > 0) args.Add("skip=" + Py.Int(skip));
                if (props.Value("period") != null) c.Approximation("the head period option is ignored");
            }

            c.Call(c.Assign(output), head ? "dsio.head" : "dsio.tail", args);
        }
    }

    internal sealed class SampleEmitter : StageEmitter
    {
        public override string Implementation => "dsio.sample";

        public override bool Handles(Stage stage, JobKind kind) => TypeIs(stage, "PxSample");

        public override void Emit(StageContext c)
        {
            if (!RowHelpers.Require(c, 1, 1)) return;
            var props = c.Stage.Properties;
            var output = c.Stage.Outputs[0];
            var args = new List<string> { "ctx", Py.Str(c.Stage.Name), c.Var(c.Stage.Inputs[0]), c.Columns(output) };
            var period = props.ValueAny("period", "sample_period");
            var percent = props.ValueAny("percent", "percentage");
            if (period != null) args.Add("period=" + Py.Int(Text.ParseInt(period, 1)));
            else if (percent != null) args.Add("percent=" + Py.Int(Text.ParseInt(percent, 100)));
            var max = props.ValueAny("maxoutputrows", "max_rows", "maxRows");
            if (max != null) args.Add("max_rows=" + Py.Int(Text.ParseInt(max, 0)));
            if (c.Stage.Outputs.Count > 1) c.Approximation("only the first sample output is produced");
            c.Note("sampling is deterministic (every n-th row), not random");
            c.Call(c.Assign(output), "dsio.sample", args);
        }
    }

    internal sealed class PeekEmitter : StageEmitter
    {
        public override string Implementation => "dsio.peek (logs rows)";

        public override bool Handles(Stage stage, JobKind kind) => TypeIs(stage, "PxPeek");

        public override void Emit(StageContext c)
        {
            if (c.Stage.Inputs.Count == 0)
            {
                c.Blocking("the peek has no input");
                return;
            }

            int count = Text.ParseInt(c.Stage.Properties.ValueAny("nrecs", "numRows", "all"), 10);
            var call = $"dsio.peek(ctx, {Py.Str(c.Stage.Name)}, {c.Var(c.Stage.Inputs[0])}, {Py.Int(count)})";
            if (c.Stage.Outputs.Count > 0) c.Run.Line($"{c.Assign(c.Stage.Outputs[0])} = {call}");
            else c.Run.Line(call);
        }
    }

    internal sealed class RowGeneratorEmitter : StageEmitter
    {
        public override string Implementation => "dsio.generate_rows";

        public override bool Handles(Stage stage, JobKind kind) => TypeIs(stage, "PxRowGenerator");

        public override void Emit(StageContext c)
        {
            if (!RowHelpers.Require(c, 0, 1)) return;
            int count = Text.ParseInt(c.Stage.Properties.ValueAny("records", "nrecs", "rows"), 10);
            var output = c.Stage.Outputs[0];
            c.Approximation("generated values follow DataStage's default cycle only approximately (0, 1, 2... and empty strings)");
            c.Run.Line($"{c.Assign(output)} = dsio.generate_rows(ctx, {Py.Str(c.Stage.Name)}, {Py.Int(count)}, {c.Columns(output)})");
        }
    }

    internal sealed class SurrogateKeyEmitter : StageEmitter
    {
        public override string Implementation => "dsio.surrogate_key";

        public override bool Handles(Stage stage, JobKind kind) => TypeIs(stage, "PxSurrogateKey", "PxSurrogateKeyGenerator", "SurrogateKeyGenerator");

        public override void Emit(StageContext c)
        {
            if (!RowHelpers.Require(c, 1, 1)) return;
            var props = c.Stage.Properties;
            var input = c.Stage.Inputs[0];
            var output = c.Stage.Outputs[0];
            var column = Text.NullIfBlank(props.ValueAny("keyColumn", "outputColumn", "newColumn", "surrogateKeyName", "surrogate_key_name"))
                ?? output.Columns.Select(col => col.Name).FirstOrDefault(n => input.FindColumn(n) == null);
            if (column == null)
            {
                c.Blocking("the surrogate key column could not be determined");
                return;
            }

            int start = Text.ParseInt(props.ValueAny("startValue", "start_value", "start"), 1);
            c.Note("keys restart at the start value on every run (no key state file)");
            c.Run.Line($"{c.Assign(output)} = dsio.surrogate_key(ctx, {Py.Str(c.Stage.Name)}, {c.Var(input)}, {Py.Str(column.Trim())}, {c.Columns(output)}, start={Py.Int(start)})");
        }
    }

    /// <summary>Modify: DROP / KEEP / renames and type conversions, expressed as a projection.</summary>
    internal sealed class ModifyEmitter : StageEmitter
    {
        public override string Implementation => "dsio.project";

        public override bool Handles(Stage stage, JobKind kind) => TypeIs(stage, "PxModify");

        public override void Emit(StageContext c)
        {
            if (!RowHelpers.Require(c, 1, 1)) return;
            var input = c.Stage.Inputs[0];
            var output = c.Stage.Outputs[0];
            var renames = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var node in c.Stage.Properties.GetAll("specification"))
            {
                foreach (var spec in node.Value.Split(';').Select(s => s.Trim()).Where(s => s.Length > 0))
                {
                    if (Regex.IsMatch(spec, "^(DROP|KEEP)\\s", RegexOptions.IgnoreCase)) continue;
                    var rename = Regex.Match(spec, "^([\\w$]+)\\s*(?::[^=]+)?=\\s*([\\w$]+)\\s*$");
                    if (rename.Success)
                    {
                        renames[rename.Groups[1].Value] = rename.Groups[2].Value;
                        continue;
                    }

                    var conversion = Regex.Match(spec, "^([\\w$]+)\\s*(?::[^=]+)?=\\s*(\\w+)\\s*\\(\\s*([\\w$]+)");
                    if (conversion.Success)
                    {
                        renames[conversion.Groups[1].Value] = conversion.Groups[3].Value;
                        c.Approximation($"modify function {conversion.Groups[2].Value} on {conversion.Groups[3].Value} becomes a plain conversion to the output type");
                        continue;
                    }

                    c.Blocking($"modify specification '{Py.Short(spec, 60)}' is not understood");
                }
            }

            if (c.Failed) return;
            var sb = new StringBuilder("[\n");
            foreach (var column in output.Columns)
            {
                var source = renames.TryGetValue(column.Name, out var from) ? from : column.Name;
                if (input.FindColumn(source) == null) c.Approximation($"column {column.Name} has no source in {input.Name}; it will be null");
                sb.Append($"    ({Py.Str(column.Name)}, {Py.Str(source)}),\n");
            }

            sb.Append(']');
            var constant = c.State.Constant("MAP_" + output.Name, sb.ToString());
            c.Run.Line($"{c.Assign(output)} = dsio.project(ctx, {c.Var(input)}, {constant}, {c.Columns(output)})");
        }
    }

    /// <summary>Server IPC, Link Partitioner and Link Collector: plumbing for parallelism in server jobs.</summary>
    internal sealed class PassThroughEmitter : StageEmitter
    {
        public override string Implementation => "pass-through";

        public override bool Handles(Stage stage, JobKind kind) =>
            TypeIs(stage, "IPC", "LinkPartitioner", "LinkCollector") || TypeContains(stage, "partitioner", "collector")
            || (kind == JobKind.Server && TypeContains(stage, "ipc"));

        public override void Emit(StageContext c)
        {
            if (!RowHelpers.Require(c, 1, 1)) return;
            if (TypeContains(c.Stage, "collector"))
            {
                var output = c.Stage.Outputs[0];
                c.Run.Line($"{c.Assign(output)} = dsio.funnel(ctx, {Py.Str(c.Stage.Name)}, [{string.Join(", ", c.Stage.Inputs.Select(c.Var))}], {c.Columns(output)})");
                return;
            }

            if (TypeContains(c.Stage, "partitioner"))
            {
                var outputs = c.Stage.Outputs.ToArray();
                var temp = c.State.LocalName("parts_" + c.Stage.Name);
                c.Run.Line($"{temp} = dsio.partition(ctx, {Py.Str(c.Stage.Name)}, {c.Var(c.Stage.Inputs[0])}, {Py.Int(outputs.Length)})");
                for (int i = 0; i < outputs.Length; i++) c.Run.Line($"{c.Assign(outputs[i])} = {temp}[{i}]");
                c.Note("rows are dealt round-robin");
                return;
            }

            foreach (var output in c.Stage.Outputs) RowHelpers.Project(c, c.Stage.Inputs[0], output);
        }
    }
}
