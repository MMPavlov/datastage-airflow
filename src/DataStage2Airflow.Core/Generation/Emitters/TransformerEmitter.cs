using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using DataStage2Airflow.Expressions;
using DataStage2Airflow.Model;

namespace DataStage2Airflow.Generation.Emitters
{
    /// <summary>
    /// Server and parallel transformers become a function that walks the input rows once: reference
    /// lookups (server), stage variables in order (they keep their value between rows), then each output
    /// link in order with its constraint; the Otherwise link takes rows no other link wrote; rows whose
    /// evaluation raises RowError go to the error link or are dropped with a warning.
    /// </summary>
    internal sealed class TransformerEmitter : StageEmitter
    {
        public override string Implementation => "row-by-row Python function";

        public override bool Handles(Stage stage, JobKind kind) => stage.IsTransformer || TypeIs(stage, "PxBASICTransformer");

        public override void Emit(StageContext c)
        {
            var stage = c.Stage;
            var primary = stage.Inputs.FirstOrDefault(l => l.Kind != LinkKind.Reference) ?? stage.Inputs.FirstOrDefault();
            if (primary == null)
            {
                c.Blocking("the transformer has no input link");
                return;
            }

            var references = stage.Inputs.Where(l => !ReferenceEquals(l, primary)).ToList();
            var outputs = stage.Outputs.ToList();
            var resolver = new TransformerResolver(c, primary);
            var used = new HashSet<string>(StringComparer.Ordinal);

            var refInfos = new List<RefInfo>();
            foreach (var reference in references)
            {
                var snake = Naming.Snake(reference.Name);
                var info = new RefInfo(
                    reference,
                    Naming.Unique("idx_" + snake, used),
                    Naming.Unique("r_" + snake, used),
                    Naming.Unique("nf_" + snake, used),
                    Naming.Unique("empty_" + snake, used));
                resolver.AddReference(reference, info.Row, info.NotFound);
                refInfos.Add(info);
            }

            var variables = new List<(StageVariable Variable, string Name)>();
            foreach (var variable in stage.StageVariables)
            {
                var name = Naming.Unique(Naming.Prefixed("sv_", variable.Name), used);
                resolver.AddStageVariable(variable.Name, name, c.Typed ? variable.LogicalType : DsLogicalType.String);
                variables.Add((variable, name));
            }

            foreach (var info in refInfos)
            {
                var keyColumns = info.Link.Columns.Where(col => col.IsKey).ToList();
                if (keyColumns.Count == 0)
                {
                    c.Blocking($"reference link {info.Link.Name} has no key columns");
                    continue;
                }

                foreach (var key in keyColumns)
                {
                    var expression = KeyExpression(info.Link, key, primary);
                    if (expression == null)
                    {
                        c.Blocking($"no key expression for reference column {info.Link.Name}.{key.Name}");
                        continue;
                    }

                    var result = c.Translate(expression, resolver, $"key {info.Link.Name}.{key.Name}", TranslationUse.Raw);
                    info.Keys.Add((key.Name, result.Code));
                }
            }

            var initial = variables.Select(v => (v.Name, Code: InitialValue(c, v.Variable, resolver))).ToList();
            var assignments = new List<(string Name, string Code, string Original)>();
            foreach (var (variable, name) in variables)
            {
                if (string.IsNullOrWhiteSpace(variable.Expression)) continue;
                var result = c.Translate(variable.Expression, resolver, $"stage variable {variable.Name}");
                assignments.Add((name, result.Code, variable.Expression));
            }

            var outInfos = new List<OutInfo>();
            foreach (var output in outputs)
            {
                var info = new OutInfo(output, Naming.Unique("out_" + Naming.Snake(output.Name), used));
                resolver.UsesOutRowNumber = false;
                if (output.Constraint != null && !output.IsError)
                {
                    info.Condition = c.Translate(output.Constraint, resolver, $"constraint of {output.Name}", TranslationUse.Condition).Code;
                }

                foreach (var column in output.Columns)
                {
                    if (output.IsError)
                    {
                        var source = SimpleSource(column.EffectiveDerivation, primary) ?? column.Name;
                        info.Columns.Add((column.Name, $"row.get({Py.Str(source)})", string.Empty));
                        continue;
                    }

                    var derivation = column.EffectiveDerivation;
                    if (derivation == null)
                    {
                        c.Approximation($"column {output.Name}.{column.Name} has no derivation; it is set to null");
                        info.Columns.Add((column.Name, "None", string.Empty));
                        continue;
                    }

                    var result = c.Translate(derivation, resolver, $"derivation of {output.Name}.{column.Name}");
                    var plainCopy = string.Equals(Regex.Replace(derivation, "\\s+", string.Empty), primary.Name + "." + column.Name, StringComparison.OrdinalIgnoreCase);
                    info.Columns.Add((column.Name, result.Code, plainCopy ? string.Empty : derivation));
                }

                info.UsesOutRowNumber = resolver.UsesOutRowNumber;
                outInfos.Add(info);
            }

            if (outInfos.Count(o => o.Link.IsError) > 1) c.Approximation("several error links: only the first receives rejected rows");

            var function = c.State.NewFunction(stage.Name, "xfm_");
            var parameters = new List<string> { "ctx", c.Var(primary) };
            parameters.AddRange(references.Select(c.Var));
            WriteFunction(c, function.Name, function.Writer, parameters, primary, refInfos, initial, assignments, outInfos);

            var call = $"{function.Name}({string.Join(", ", parameters)})";
            if (outputs.Count == 0) c.Run.Line(call);
            else c.Run.Line($"{c.Assign(outputs.ToArray())} = {call}");
        }

        private static void WriteFunction(
            StageContext c,
            string name,
            PythonWriter w,
            List<string> parameters,
            Link primary,
            List<RefInfo> refInfos,
            List<(string Name, string Code)> initial,
            List<(string Name, string Code, string Original)> assignments,
            List<OutInfo> outInfos)
        {
            using (w.Block($"def {name}({string.Join(", ", parameters)}):"))
            {
                var doc = $"Transformer {c.Stage.Name}: {primary.Name}";
                if (refInfos.Count > 0) doc += " with lookups on " + string.Join(", ", refInfos.Select(r => r.Link.Name));
                if (outInfos.Count > 0) doc += " -> " + string.Join(", ", outInfos.Select(o => o.Link.Name + (o.Link.IsOtherwise ? " (otherwise)" : o.Link.IsError ? " (errors)" : string.Empty)));
                w.Docstring(doc + ".");
                foreach (var info in refInfos)
                {
                    w.Line($"{info.Index} = dsio.index({c.Var(info.Link)}, {Py.List(info.Keys.Select(k => k.Column))})");
                    w.Line($"{info.Empty} = dict.fromkeys({Py.List(info.Link.Columns.Select(col => col.Name))})");
                }

                foreach (var output in outInfos) w.Line($"{output.List} = []");
                foreach (var (variable, code) in initial) w.Line($"{variable} = {code}");
                w.Line("in_row_num = 0");
                using (w.Block($"for row in dsio.rows({c.Var(primary)}):"))
                {
                    w.Line("in_row_num += 1");
                    using (w.Block("try:"))
                    {
                        foreach (var info in refInfos)
                        {
                            w.Line($"{info.Row} = {info.Index}.get(dsio.lookup_key({string.Join(", ", info.Keys.Select(k => k.Code))}))");
                            w.Line($"{info.NotFound} = {info.Row} is None");
                            using (w.Block($"if {info.NotFound}:"))
                            {
                                w.Line($"{info.Row} = {info.Empty}");
                            }
                        }

                        foreach (var (variable, code, original) in assignments)
                        {
                            if (!string.Equals(Regex.Replace(original, "\\s+", string.Empty), code, StringComparison.Ordinal))
                            {
                                w.Comment(Py.Short(original));
                            }

                            w.Line($"{variable} = {code}");
                        }

                        bool needWritten = outInfos.Any(o => o.Link.IsOtherwise);
                        if (needWritten) w.Line("written = False");
                        foreach (var output in outInfos.Where(o => !o.Link.IsError))
                        {
                            WriteOutput(c, w, output, needWritten, outInfos.Count(o => !o.Link.IsError));
                        }
                    }

                    using (w.Block("except RowError as exc:"))
                    {
                        var error = outInfos.FirstOrDefault(o => o.Link.IsError);
                        if (error != null)
                        {
                            w.Line($"{error.List}.append({{{string.Join(", ", error.Columns.Select(col => $"{Py.Str(col.Name)}: {col.Code}"))}}})");
                        }
                        else
                        {
                            w.Line($"ctx.reject({Py.Str(c.Stage.Name)}, in_row_num, exc)");
                        }
                    }
                }

                var typed = c.Typed ? string.Empty : ", typed=False";
                var frames = outInfos.Select(o => $"dsio.frame({o.List}, {c.Columns(o.Link)}{typed})").ToList();
                if (frames.Count == 0)
                {
                    w.Line("return None");
                }
                else if (frames.Count == 1)
                {
                    w.Line("return " + frames[0]);
                }
                else
                {
                    using (w.Block("return ("))
                    {
                        foreach (var frame in frames) w.Line(frame + ",");
                    }

                    w.Line(")");
                }
            }
        }

        private static void WriteOutput(StageContext c, PythonWriter w, OutInfo output, bool needWritten, int streamOutputs)
        {
            var link = output.Link;
            var conditions = new List<string>();
            if (link.IsOtherwise) conditions.Add("not written");
            if (output.Condition != null) conditions.Add(output.Condition);
            if (link.RowLimit > 0) conditions.Add($"len({output.List}) < {link.RowLimit}");

            // The constraint can hold a top-level "or" or "if ... else", so it is parenthesized when joined with "and".
            if (output.Condition != null && conditions.Count > 1) conditions[link.IsOtherwise ? 1 : 0] = "(" + output.Condition + ")";

            var label = link.Name + (link.Constraint != null ? ": " + Py.Short(link.Constraint, 80) : string.Empty) + (link.IsOtherwise ? " (otherwise)" : string.Empty);
            if (streamOutputs > 1 || link.Constraint != null) w.Comment(label);
            if (conditions.Count > 0)
            {
                w.Line($"if {string.Join(" and ", conditions)}:");
                w.Indent();
            }

            if (output.UsesOutRowNumber) w.Line($"out_row_num = len({output.List}) + 1");
            w.Line($"{output.List}.append({{");
            w.Indent();
            foreach (var (column, code, original) in output.Columns)
            {
                var comment = original.Length == 0 ? string.Empty : "  # " + Py.Short(original, 70);
                w.Line($"{Py.Str(column)}: {code},{comment}");
            }

            w.Dedent();
            w.Line("})");
            if (needWritten && !link.IsOtherwise) w.Line("written = True");
            if (conditions.Count > 0) w.Dedent();
        }

        private static string InitialValue(StageContext c, StageVariable variable, TransformerResolver resolver)
        {
            var text = variable.InitialValue;
            if (!string.IsNullOrWhiteSpace(text))
            {
                // An initial value that does not translate (plain text, an unknown name) is used as a string.
                var result = new PythonTranslator(resolver, c.Dialect).TranslateValue(text!);
                return result.Ok ? result.Code : Py.Str(text);
            }

            switch (c.Typed ? variable.LogicalType : DsLogicalType.String)
            {
                case DsLogicalType.Integer:
                case DsLogicalType.Decimal:
                case DsLogicalType.Float:
                    return "0";
                case DsLogicalType.String:
                case DsLogicalType.Unknown:
                    return "\"\"";
                default:
                    return "None";
            }
        }

        /// <summary>The key expression of a reference column: the derivation on the transformer's input pin,
        /// then on the reference link, then the same-named column of the stream input.</summary>
        private static string? KeyExpression(Link reference, Column key, Link primary)
        {
            var targetColumn = reference.TargetColumns.FirstOrDefault(col => string.Equals(col.Name, key.Name, StringComparison.OrdinalIgnoreCase));
            var expression = targetColumn?.EffectiveDerivation ?? key.EffectiveDerivation;
            if (expression != null) return expression;
            var same = primary.FindColumn(key.Name);
            return same == null ? null : primary.Name + "." + same.Name;
        }

        private static string? SimpleSource(string? derivation, Link primary)
        {
            if (derivation == null) return null;
            var m = Regex.Match(derivation.Trim(), "^([A-Za-z_][\\w$]*)\\.([A-Za-z_@$][\\w$]*)$");
            return m.Success && string.Equals(m.Groups[1].Value, primary.Name, StringComparison.OrdinalIgnoreCase) ? m.Groups[2].Value : null;
        }

        private sealed class RefInfo
        {
            public RefInfo(Link link, string index, string row, string notFound, string empty)
            {
                Link = link;
                Index = index;
                Row = row;
                NotFound = notFound;
                Empty = empty;
            }

            public Link Link { get; }

            public string Index { get; }

            public string Row { get; }

            public string NotFound { get; }

            public string Empty { get; }

            public List<(string Column, string Code)> Keys { get; } = new List<(string, string)>();
        }

        private sealed class OutInfo
        {
            public OutInfo(Link link, string list)
            {
                Link = link;
                List = list;
            }

            public Link Link { get; }

            public string List { get; }

            public string? Condition { get; set; }

            public bool UsesOutRowNumber { get; set; }

            public List<(string Name, string Code, string Original)> Columns { get; } = new List<(string, string, string)>();
        }
    }
}
