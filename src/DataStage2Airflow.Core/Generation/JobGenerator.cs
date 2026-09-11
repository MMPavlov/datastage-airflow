using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using DataStage2Airflow.Expressions;
using DataStage2Airflow.Generation.Emitters;
using DataStage2Airflow.Model;
using DataStage2Airflow.Reporting;

namespace DataStage2Airflow.Generation
{
    public sealed class JobConversion
    {
        internal JobConversion(DsJob job, JobReport report, string moduleName)
        {
            Job = job;
            Report = report;
            ModuleName = moduleName;
        }

        public DsJob Job { get; }

        public JobReport Report { get; }

        /// <summary>Python module name, without package.</summary>
        public string ModuleName { get; }

        /// <summary>The generated module, or null when the job could not be converted at all.</summary>
        public string? Code { get; internal set; }

        /// <summary>Code was produced and nothing blocks running it.</summary>
        public bool Convertible => Code != null && Report.BlockingCount == 0;
    }

    /// <summary>
    /// Generates a Python module that does the work of a parallel or server job. Stages run in
    /// dependency order inside <c>run()</c>; each link is a frame; transformers become row-by-row functions.
    /// </summary>
    public sealed class JobGenerator
    {
        private readonly DsProject _project;
        private readonly MigrationOptions _options;

        public JobGenerator(DsProject project, MigrationOptions options)
        {
            _project = project;
            _options = options;
        }

        public JobConversion Generate(DsJob job, string moduleName)
        {
            var report = new JobReport(job.Name, job.Kind) { Category = job.Category, Description = job.Description };
            var conversion = new JobConversion(job, report, moduleName);
            var location = "job " + job.Name;

            if (job.Kind != JobKind.Parallel && job.Kind != JobKind.Server)
            {
                report.Issues.Add(new Issue(Severity.Error, "GEN001", $"{job.Kind} jobs are not converted to Python", location, true));
                return conversion;
            }

            if (job.Stages.Count == 0)
            {
                var message = job.JobControlCode != null
                    ? "batch job: its job control code (DataStage BASIC) has to be ported by hand"
                    : "the job has no stages";
                report.Issues.Add(new Issue(Severity.Error, "GEN002", message, location, true));
                return conversion;
            }

            var order = ExecutionOrder(job);
            if (order == null)
            {
                report.Issues.Add(new Issue(Severity.Error, "GEN003", "the stages form a cycle", location, true));
                return conversion;
            }

            var state = new JobGenerationState(job, _project, _options, report, order);
            EmitSubroutine(state, job.BeforeSubroutine, job.BeforeSubroutineInput, "before-job subroutine");
            foreach (var stage in order)
            {
                EmitStage(state, stage);
            }

            EmitSubroutine(state, job.AfterSubroutine, job.AfterSubroutineInput, "after-job subroutine");
            conversion.Code = Render(state, moduleName);
            return conversion;
        }

        /// <summary>Stages in dependency order; ties keep canvas order. Null when the graph has a cycle.</summary>
        public static List<Stage>? ExecutionOrder(DsJob job)
        {
            var position = new Dictionary<Stage, int>();
            for (int i = 0; i < job.Stages.Count; i++) position[job.Stages[i]] = i;
            var pending = job.Stages.ToDictionary(s => s, s => s.Inputs.Count(l => position.ContainsKey(l.Source)));
            var ready = job.Stages.Where(s => pending[s] == 0).ToList();
            var order = new List<Stage>();
            while (ready.Count > 0)
            {
                var next = ready.OrderBy(s => position[s]).First();
                ready.Remove(next);
                order.Add(next);
                foreach (var link in next.Outputs)
                {
                    if (link.Target == null || !pending.ContainsKey(link.Target)) continue;
                    pending[link.Target]--;
                    if (pending[link.Target] == 0) ready.Add(link.Target);
                }
            }

            return order.Count == job.Stages.Count ? order : null;
        }

        private static void EmitStage(JobGenerationState state, Stage stage)
        {
            var stageReport = new StageReport(stage.Name, stage.StageType);
            state.Report.Stages.Add(stageReport);
            var context = new StageContext(state, stage, stageReport);

            state.Run.Line();
            state.Run.Comment($"{stage.Name} ({StageLabel(stage)})" + Arrows(stage));

            foreach (var link in stage.Outputs.Where(l => l.Columns.Count == 0))
            {
                context.Blocking($"link {link.Name} has no column metadata (runtime column propagation is not converted)");
            }

            var emitter = StageEmitterRegistry.Find(stage, state.Job.Kind);
            if (emitter == null)
            {
                context.Blocking($"stage type {stage.StageType} is not supported");
            }
            else
            {
                stageReport.Implementation = emitter.Implementation;
                emitter.Emit(context);
            }

            var unassigned = stage.Outputs.Where(l => !state.IsAssigned(l)).ToList();
            if (unassigned.Count > 0 || (emitter == null && stage.Outputs.Count == 0) || (context.Failed && stage.Outputs.Count == 0))
            {
                var reason = context.FirstProblem ?? "not converted";
                var call = $"dsio.not_converted({Py.Str(stage.Name)}, {Py.Str(reason)})";
                if (unassigned.Count > 0) state.Run.Line($"{context.Assign(unassigned.ToArray())} = {call}");
                else state.Run.Line(call);
            }

            foreach (var link in stage.Outputs)
            {
                if (state.IsAssigned(link)) state.Run.Line($"ctx.count({Py.Str(link.Name)}, {state.Var(link)})");
            }
        }

        private static string Arrows(Stage stage)
        {
            var inputs = stage.Inputs.Select(l => l.Name).ToList();
            var outputs = stage.Outputs.Select(l => l.Name).ToList();
            var text = string.Empty;
            if (inputs.Count > 0) text += ": " + string.Join(", ", inputs);
            if (outputs.Count > 0) text += (inputs.Count > 0 ? " -> " : ": -> ") + string.Join(", ", outputs);
            return text;
        }

        internal static string StageLabel(Stage stage)
        {
            var type = stage.StageType;
            if (type.StartsWith("Px", StringComparison.Ordinal)) return type.Substring(2);
            if (type.StartsWith("C", StringComparison.Ordinal) && type.EndsWith("Stage", StringComparison.Ordinal) && type.Length > 6)
            {
                return type.Substring(1, type.Length - 6);
            }

            return type;
        }

        private static void EmitSubroutine(JobGenerationState state, string? routine, string? input, string what)
        {
            if (string.IsNullOrWhiteSpace(routine)) return;
            var name = routine!.Trim();
            state.Run.Line();
            state.Run.Comment($"{what}: {name} {input}");
            var upper = name.Split('.').Last().ToUpperInvariant();
            if (upper.StartsWith("EXECSH", StringComparison.Ordinal) || upper.StartsWith("EXECDOS", StringComparison.Ordinal) || upper == "EXECTCL")
            {
                state.Run.Line($"dsio.exec_subroutine(ctx, {Py.Str(name)}, {Py.Expand(input ?? string.Empty)})");
                return;
            }

            state.AddIssue(null, Severity.Error, "GEN004", $"{what} {name} is not converted", true);
            state.Run.Line($"dsio.not_converted({Py.Str(what)}, {Py.Str(name + " has no Python implementation")})");
        }

        // ------------------------------------------------------------------ module rendering

        private string Render(JobGenerationState state, string moduleName)
        {
            var job = state.Job;
            var w = new PythonWriter();
            var doc = new StringBuilder();
            doc.Append(job.Name).Append("\n\n");
            doc.Append($"Generated by {new MigrationReport(string.Empty).Tool} from DataStage {job.Kind.ToString().ToLowerInvariant()} job {job.Name}");
            doc.Append($" (project {_project.Name}).\n");
            doc.Append($"Source: {System.IO.Path.GetFileName(job.SourceFile)}");
            if (job.Category.Length > 0) doc.Append($", category {job.Category}");
            doc.Append(".\n");
            var description = job.FullDescription.Length > 0 ? job.FullDescription : job.Description;
            if (description.Trim().Length > 0) doc.Append('\n').Append(description.Trim()).Append('\n');
            doc.Append("\nStages, in execution order:\n");
            foreach (var stage in state.Order) doc.Append($"    {stage.Name} ({StageLabel(stage)})\n");
            if (state.Report.BlockingCount > 0)
            {
                doc.Append($"\nNOT FULLY CONVERTED: {state.Report.BlockingCount} blocking issue(s); see the migration report.\n");
            }

            w.Docstring(doc.ToString());
            w.Line();
            w.Line("from __future__ import annotations");
            w.Line();

            var body = state.Run.ToString() + string.Join("\n", state.Functions.Select(f => f.ToString()));
            if (body.Contains("Decimal(")) w.Line("from decimal import Decimal").Line();
            w.Line("from ds2af_runtime import dsfunc as F");
            w.Line("from ds2af_runtime import dsio");
            if (body.Contains("RowError")) w.Line("from ds2af_runtime.dsfunc import RowError");
            w.Line("from ds2af_runtime.job import JobContext, Param");
            if (state.UsesRoutines) w.Line("from . import routines");
            w.Line();
            w.Line($"JOB_NAME = {Py.Str(job.Name)}");
            w.Line();
            RenderParameters(w, job);
            if (state.Connections.Count > 0)
            {
                w.Line();
                w.Comment("Airflow connection ids used by database stages; adjust to your environment.");
                using (w.Block("CONNECTIONS = {"))
                {
                    foreach (var pair in state.Connections) w.Line($"{Py.Str(pair.Key)}: {Py.Str(pair.Value)},");
                }

                w.Line("}");
            }

            foreach (var constant in state.Constants)
            {
                w.Line();
                w.Lines(constant.Split('\n'));
            }

            w.Line();
            w.Line();
            using (w.Block("def run(params=None, context=None):"))
            {
                w.Docstring("Runs the job; returns its DataStage status (1 = finished OK, 2 = finished with warnings).");
                w.Line("ctx = context or JobContext(JOB_NAME, PARAMETERS, params)");
                foreach (var line in state.Run.ToString().TrimEnd('\n').Split('\n'))
                {
                    if (line.Length == 0) w.Line();
                    else w.Line(line);
                }

                w.Line();
                w.Line("return ctx.finish()");
            }

            foreach (var function in state.Functions)
            {
                w.Line();
                w.Line();
                foreach (var line in function.ToString().TrimEnd('\n').Split('\n')) w.Line(line.Length == 0 ? string.Empty : line);
            }

            return PythonWriter.Tidy(w.ToString());
        }

        private static void RenderParameters(PythonWriter w, DsJob job)
        {
            if (job.Parameters.Count == 0)
            {
                w.Line("PARAMETERS = []");
                return;
            }

            using (w.Block("PARAMETERS = ["))
            {
                foreach (var p in job.Parameters)
                {
                    var type = p.Type == ParameterType.Unknown || p.Type == ParameterType.ParameterSet ? "string" : p.Type.ToString().ToLowerInvariant();
                    var value = p.Type == ParameterType.Encrypted ? string.Empty : p.DefaultValue;
                    var prompt = p.Prompt.Length > 0 ? ", " + Py.Str(p.Prompt) : string.Empty;
                    var comment = p.Type == ParameterType.Encrypted ? "  # encrypted in DataStage: supply the value at run time" : string.Empty;
                    w.Line($"Param({Py.Str(p.Name)}, {Py.Str(type)}, {Py.Str(value)}{prompt}),{comment}");
                }
            }

            w.Line("]");
        }
    }

    /// <summary>Python literal helpers shared by the generators.</summary>
    internal static class Py
    {
        public static string Str(string? text) => text == null ? "None" : PythonTranslator.PyString(text);

        /// <summary>A string property that may contain #Param# references.</summary>
        public static string Expand(string text) => text.IndexOf('#') >= 0 ? $"ctx.expand({Str(text)})" : Str(text);

        public static string List(IEnumerable<string> items) => "[" + string.Join(", ", items.Select(Str)) + "]";

        public static string Bool(bool value) => value ? "True" : "False";

        public static string Int(int value) => value.ToString(CultureInfo.InvariantCulture);

        /// <summary>One-line summary of an expression for comments.</summary>
        public static string Short(string expression, int max = 100)
        {
            var collapsed = Regex.Replace(expression, "\\s+", " ").Trim();
            return collapsed.Length <= max ? collapsed : collapsed.Substring(0, max - 3) + "...";
        }
    }

    /// <summary>State shared by the stage emitters while one job module is generated.</summary>
    internal sealed class JobGenerationState
    {
        private readonly Dictionary<Link, string> _linkVars = new Dictionary<Link, string>();
        private readonly HashSet<Link> _assigned = new HashSet<Link>();
        private readonly Dictionary<Link, string> _columnConstants = new Dictionary<Link, string>();
        private readonly HashSet<string> _used = new HashSet<string>(StringComparer.Ordinal);
        private readonly HashSet<string> _constantNames = new HashSet<string>(StringComparer.Ordinal) { "JOB_NAME", "PARAMETERS", "CONNECTIONS" };

        public JobGenerationState(DsJob job, DsProject project, MigrationOptions options, JobReport report, List<Stage> order)
        {
            Job = job;
            Project = project;
            Options = options;
            Report = report;
            Order = order;
            foreach (var stage in order)
            {
                foreach (var link in stage.Outputs) _linkVars[link] = Naming.Unique(Naming.Identifier(link.Name), _used);
            }
        }

        public DsJob Job { get; }

        public DsProject Project { get; }

        public MigrationOptions Options { get; }

        public JobReport Report { get; }

        public List<Stage> Order { get; }

        public PythonWriter Run { get; } = new PythonWriter();

        public List<PythonWriter> Functions { get; } = new List<PythonWriter>();

        /// <summary>Module-level constants (column lists, mappings), rendered in creation order.</summary>
        public List<string> Constants { get; } = new List<string>();

        public SortedDictionary<string, string> Connections { get; } = new SortedDictionary<string, string>(StringComparer.Ordinal);

        public bool UsesRoutines { get; set; }

        public string Var(Link link) => _linkVars.TryGetValue(link, out var name) ? name : "None";

        public bool IsAssigned(Link link) => _assigned.Contains(link);

        public void MarkAssigned(Link link) => _assigned.Add(link);

        public string ColumnConstant(Link link)
        {
            if (_columnConstants.TryGetValue(link, out var existing)) return existing;
            var name = Naming.Unique(Naming.Snake(link.Name).ToUpperInvariant(), _constantNames);
            _columnConstants[link] = name;
            var sb = new StringBuilder();
            sb.Append(name).Append(" = [");
            if (link.Columns.Count == 0)
            {
                sb.Append(']');
            }
            else
            {
                sb.Append('\n');
                foreach (var column in link.Columns) sb.Append("    ").Append(ColumnSpec(column)).Append(",\n");
                sb.Append(']');
            }

            Constants.Add(sb.ToString());
            return name;
        }

        public string Constant(string baseName, string value)
        {
            var name = Naming.Unique(Naming.Snake(baseName).ToUpperInvariant(), _constantNames);
            Constants.Add(name + " = " + value);
            return name;
        }

        public (string Name, PythonWriter Writer) NewFunction(string stageName, string prefix)
        {
            var name = Naming.Unique(Naming.Identifier(Naming.Prefixed(prefix, stageName)), _used);
            var writer = new PythonWriter();
            Functions.Add(writer);
            return (name, writer);
        }

        public string LocalName(string baseName) => Naming.Unique(Naming.Identifier(baseName), _used);

        public void AddIssue(Stage? stage, Severity severity, string code, string message, bool blocking)
        {
            var location = stage == null ? "job " + Job.Name : $"job {Job.Name}, stage {stage.Name}";
            Report.Issues.Add(new Issue(severity, code, message, location, blocking));
        }

        public static string ColumnSpec(Column column)
        {
            var type = LogicalName(column.LogicalType);
            var args = new List<string> { Py.Str(column.Name), Py.Str(type) };
            bool fixedLength = column.SqlType == 1 || column.SqlType == -8;
            if (column.Precision > 0)
            {
                args.Add(Py.Int(column.Precision));
                if (type == "decimal" && column.Scale > 0) args.Add(Py.Int(column.Scale));
            }

            if (!column.Nullable) args.Add("nullable=False");
            if (column.IsKey) args.Add("key=True");
            if (fixedLength && column.Precision > 0) args.Add("fixed=True");
            return $"dsio.Col({string.Join(", ", args)})";
        }

        public static string LogicalName(DsLogicalType type) => type switch
        {
            DsLogicalType.Integer => "integer",
            DsLogicalType.Decimal => "decimal",
            DsLogicalType.Float => "float",
            DsLogicalType.Date => "date",
            DsLogicalType.Time => "time",
            DsLogicalType.Timestamp => "timestamp",
            DsLogicalType.Binary => "binary",
            _ => "string",
        };
    }

    /// <summary>What an emitter needs: its stage, the job state, and helpers for common code shapes.</summary>
    internal sealed class StageContext
    {
        private readonly JobGenerationState _state;

        public StageContext(JobGenerationState state, Stage stage, StageReport report)
        {
            _state = state;
            Stage = stage;
            Report = report;
        }

        public Stage Stage { get; }

        public StageReport Report { get; }

        public JobGenerationState State => _state;

        public DsJob Job => _state.Job;

        public DsProject Project => _state.Project;

        public MigrationOptions Options => _state.Options;

        public PythonWriter Run => _state.Run;

        /// <summary>Parallel jobs carry typed values; server jobs work on strings as the BASIC engine does.</summary>
        public bool Typed => Job.Kind != JobKind.Server;

        public ExpressionDialect Dialect => Job.Kind == JobKind.Server ? ExpressionDialect.Basic : ExpressionDialect.Parallel;

        public bool Failed { get; private set; }

        public string? FirstProblem { get; private set; }

        public string Var(Link link) => _state.Var(link);

        public string Columns(Link link) => _state.ColumnConstant(link);

        /// <summary>Marks the links as produced and returns the assignment target ("a" or "a, b").</summary>
        public string Assign(params Link[] links)
        {
            foreach (var link in links) _state.MarkAssigned(link);
            return links.Length == 0 ? "_" : string.Join(", ", links.Select(Var));
        }

        public void Blocking(string message)
        {
            Failed = true;
            if (FirstProblem == null) FirstProblem = message;
            Report.Status = StageStatus.Unsupported;
            Report.Notes.Add(message);
            _state.AddIssue(Stage, Severity.Error, "STG001", message, true);
        }

        /// <summary>Converted, but DataStage would behave differently in the situation described.</summary>
        public void Approximation(string message)
        {
            if (Report.Status == StageStatus.Converted) Report.Status = StageStatus.Approximated;
            Report.Notes.Add(message);
            _state.AddIssue(Stage, Severity.Warning, "STG002", message, false);
        }

        public void Note(string message) => Report.Notes.Add(message);

        /// <summary>Writes <c>target = function(args)</c>, one argument per line when it gets long.</summary>
        public void Call(string? target, string function, IList<string> args) => Run.Call(target, function, args);

        public TranslationResult Translate(string text, INameResolver resolver, string what, TranslationUse use = TranslationUse.Value)
        {
            var translator = new PythonTranslator(resolver, Dialect);
            TranslationResult result;
            switch (use)
            {
                case TranslationUse.Condition:
                    result = translator.TranslateCondition(text);
                    break;
                case TranslationUse.Raw:
                    result = translator.Translate(text);
                    break;
                default:
                    result = translator.TranslateValue(text);
                    break;
            }

            foreach (var problem in result.Problems) Blocking($"{what}: {problem} in '{Py.Short(text, 80)}'");
            foreach (var note in result.Notes) Note($"{what}: {note}");
            return result;
        }

        /// <summary>(output column, input position, input column) for stages that only move columns.</summary>
        public List<(string Output, int Position, string Column)> Mapping(Link output, IReadOnlyList<Link> inputs)
        {
            var result = new List<(string, int, string)>();
            foreach (var column in output.Columns)
            {
                int position = -1;
                string source = column.Name;
                var reference = column.EffectiveDerivation ?? column.SourceColumn;
                if (reference != null)
                {
                    var m = Regex.Match(reference.Trim(), "^([A-Za-z_][\\w$]*)\\.([A-Za-z_@$][\\w$]*)$");
                    if (m.Success)
                    {
                        int index = FindLink(inputs, m.Groups[1].Value);
                        if (index >= 0)
                        {
                            position = index;
                            source = m.Groups[2].Value;
                        }
                    }
                    else if (Regex.IsMatch(reference.Trim(), "^[A-Za-z_][\\w$]*$"))
                    {
                        source = reference.Trim();
                    }
                    else
                    {
                        Approximation($"derivation '{Py.Short(reference, 60)}' of {output.Name}.{column.Name} is not a plain column; mapped by name");
                    }
                }

                if (position < 0)
                {
                    for (int i = 0; i < inputs.Count && position < 0; i++)
                    {
                        if (inputs[i].FindColumn(source) != null) position = i;
                    }
                }

                if (position < 0)
                {
                    Approximation($"column {output.Name}.{column.Name} has no source column in the inputs; it will be null");
                    position = 0;
                }

                result.Add((column.Name, position, source));
            }

            return result;
        }

        public string MappingConstant(Link output, IEnumerable<(string Output, int Position, string Column)> mapping)
        {
            var sb = new StringBuilder("[\n");
            foreach (var m in mapping) sb.Append($"    ({Py.Str(m.Output)}, {Py.Int(m.Position)}, {Py.Str(m.Column)}),\n");
            sb.Append(']');
            return _state.Constant("MAP_" + output.Name, sb.ToString());
        }

        public static int FindLink(IReadOnlyList<Link> links, string name)
        {
            for (int i = 0; i < links.Count; i++)
            {
                if (string.Equals(links[i].Name, name, StringComparison.OrdinalIgnoreCase)) return i;
            }

            return -1;
        }
    }

    internal enum TranslationUse
    {
        Value,
        Condition,
        Raw,
    }
}
