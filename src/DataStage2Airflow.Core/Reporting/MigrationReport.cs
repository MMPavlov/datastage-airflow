using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using DataStage2Airflow.Model;

namespace DataStage2Airflow.Reporting
{
    public enum StageStatus
    {
        Converted,
        Approximated,
        Unsupported,
    }

    public sealed class Issue
    {
        public Issue(Severity severity, string code, string message, string location, bool blocking)
        {
            Severity = severity;
            Code = code;
            Message = message;
            Location = location;
            Blocking = blocking;
        }

        public Severity Severity { get; }

        public string Code { get; }

        public string Message { get; }

        public string Location { get; }

        /// <summary>Prevents automatic conversion: in auto mode the job keeps running in DataStage.</summary>
        public bool Blocking { get; }
    }

    public sealed class StageReport
    {
        public StageReport(string name, string stageType)
        {
            Name = name;
            StageType = stageType;
        }

        public string Name { get; }

        public string StageType { get; }

        public StageStatus Status { get; set; } = StageStatus.Converted;

        public string Implementation { get; set; } = string.Empty;

        public List<string> Notes { get; } = new List<string>();
    }

    public sealed class JobReport
    {
        public JobReport(string name, JobKind kind)
        {
            Name = name;
            Kind = kind;
        }

        public string Name { get; }

        public JobKind Kind { get; }

        public string Category { get; set; } = string.Empty;

        public string Description { get; set; } = string.Empty;

        /// <summary>"python", "dsjob" or "not converted".</summary>
        public string Outcome { get; set; } = "not converted";

        public string? Module { get; set; }

        public string? ModulePath { get; set; }

        /// <summary>Partial conversion kept for manual completion when the job was not converted.</summary>
        public string? DraftPath { get; set; }

        public string? DagId { get; set; }

        public string? DagPath { get; set; }

        public List<StageReport> Stages { get; } = new List<StageReport>();

        public List<Issue> Issues { get; } = new List<Issue>();

        /// <summary>Sequences with a job activity that runs this job.</summary>
        public List<string> RunBy { get; } = new List<string>();

        public int BlockingCount => Issues.Count(i => i.Blocking);
    }

    public sealed class ActivityReport
    {
        public ActivityReport(string name, ActivityKind kind, string taskId)
        {
            Name = name;
            Kind = kind;
            TaskId = taskId;
        }

        public string Name { get; }

        public ActivityKind Kind { get; }

        public string TaskId { get; }

        /// <summary>What the activity does: the job it runs, the command, the file...</summary>
        public string Detail { get; set; } = string.Empty;

        public string Implementation { get; set; } = string.Empty;

        public List<string> Notes { get; } = new List<string>();

        public List<string> UnmappedProperties { get; } = new List<string>();
    }

    public sealed class TriggerReport
    {
        public TriggerReport(string name, string from, string to, TriggerKind kind)
        {
            Name = name;
            From = from;
            To = to;
            Kind = kind;
        }

        public string Name { get; }

        public string From { get; }

        public string To { get; }

        public TriggerKind Kind { get; }

        public string RawType { get; set; } = string.Empty;

        public string? RawExpression { get; set; }

        /// <summary>The Python condition the DAG evaluates (empty for plain dependencies).</summary>
        public string Condition { get; set; } = string.Empty;

        /// <summary>True when the trigger type was inferred from its numeric code alone.</summary>
        public bool Inferred { get; set; }
    }

    public sealed class SequenceReport
    {
        public SequenceReport(string name)
        {
            Name = name;
        }

        public string Name { get; }

        public string Category { get; set; } = string.Empty;

        public string? DagId { get; set; }

        public string? DagPath { get; set; }

        public List<ActivityReport> Activities { get; } = new List<ActivityReport>();

        public List<TriggerReport> Triggers { get; } = new List<TriggerReport>();

        public List<Issue> Issues { get; } = new List<Issue>();
    }

    public sealed class MigrationReport
    {
        public const string Version = "0.1.0";

        public MigrationReport(string projectName)
        {
            ProjectName = projectName;
        }

        public string Tool => "ds2af " + Version;

        public DateTime Generated { get; set; } = DateTime.Now;

        public string ProjectName { get; }

        public string? ServerVersion { get; set; }

        public string? ExportingTool { get; set; }

        public MigrationOptions? Options { get; set; }

        public List<string> SourceFiles { get; } = new List<string>();

        public List<JobReport> Jobs { get; } = new List<JobReport>();

        public List<SequenceReport> Sequences { get; } = new List<SequenceReport>();

        public List<Diagnostic> Diagnostics { get; } = new List<Diagnostic>();

        public List<string> Files { get; } = new List<string>();

        public int CountOutcome(string outcome) => Jobs.Count(j => j.Outcome == outcome);

        public IEnumerable<Issue> AllIssues => Jobs.SelectMany(j => j.Issues).Concat(Sequences.SelectMany(s => s.Issues));

        // ------------------------------------------------------------------ JSON

        public string ToJson()
        {
            var w = new JsonWriter();
            w.BeginObject();
            w.Property("tool", Tool);
            w.Property("generated", Generated.ToString("s", CultureInfo.InvariantCulture));
            w.Name("project").BeginObject()
                .Property("name", ProjectName)
                .Property("serverVersion", ServerVersion)
                .Property("exportingTool", ExportingTool)
                .StringArray("sourceFiles", SourceFiles)
                .EndObject();
            if (Options != null)
            {
                w.Name("options").BeginObject()
                    .Property("mode", Options.Mode.ToString().ToLowerInvariant())
                    .Property("airflow", (int)Options.Airflow)
                    .Property("schedule", Options.Schedule)
                    .Property("dsjobPath", Options.DsjobPath)
                    .Property("dsjobSshConnId", Options.DsjobSshConnId)
                    .Property("failOnWarning", Options.FailOnWarning)
                    .Property("standaloneJobDags", Options.StandaloneJobDags)
                    .EndObject();
            }

            w.Name("summary").BeginObject()
                .Property("jobs", Jobs.Count)
                .Property("convertedToPython", CountOutcome("python"))
                .Property("runThroughDsjob", CountOutcome("dsjob"))
                .Property("notConverted", CountOutcome("not converted"))
                .Property("sequences", Sequences.Count)
                .Property("blockingIssues", AllIssues.Count(i => i.Blocking))
                .Property("warnings", AllIssues.Count(i => !i.Blocking && i.Severity == Severity.Warning))
                .EndObject();

            w.Name("jobs").BeginArray();
            foreach (var job in Jobs)
            {
                w.BeginObject()
                    .Property("name", job.Name)
                    .Property("kind", job.Kind.ToString())
                    .Property("category", job.Category)
                    .Property("outcome", job.Outcome)
                    .Property("module", job.Module)
                    .Property("modulePath", job.ModulePath)
                    .Property("draftPath", job.DraftPath)
                    .Property("dagId", job.DagId)
                    .Property("dagPath", job.DagPath)
                    .StringArray("runBy", job.RunBy);
                w.Name("stages").BeginArray();
                foreach (var stage in job.Stages)
                {
                    w.BeginObject()
                        .Property("name", stage.Name)
                        .Property("type", stage.StageType)
                        .Property("status", stage.Status.ToString().ToLowerInvariant())
                        .Property("implementation", stage.Implementation)
                        .StringArray("notes", stage.Notes)
                        .EndObject();
                }

                w.EndArray();
                WriteIssues(w, job.Issues);
                w.EndObject();
            }

            w.EndArray();

            w.Name("sequences").BeginArray();
            foreach (var sequence in Sequences)
            {
                w.BeginObject()
                    .Property("name", sequence.Name)
                    .Property("category", sequence.Category)
                    .Property("dagId", sequence.DagId)
                    .Property("dagPath", sequence.DagPath);
                w.Name("activities").BeginArray();
                foreach (var activity in sequence.Activities)
                {
                    w.BeginObject()
                        .Property("name", activity.Name)
                        .Property("kind", activity.Kind.ToString())
                        .Property("taskId", activity.TaskId)
                        .Property("detail", activity.Detail)
                        .Property("implementation", activity.Implementation)
                        .StringArray("notes", activity.Notes)
                        .StringArray("unmappedProperties", activity.UnmappedProperties)
                        .EndObject();
                }

                w.EndArray();
                w.Name("triggers").BeginArray();
                foreach (var trigger in sequence.Triggers)
                {
                    w.BeginObject()
                        .Property("name", trigger.Name)
                        .Property("from", trigger.From)
                        .Property("to", trigger.To)
                        .Property("kind", trigger.Kind.ToString())
                        .Property("rawType", trigger.RawType)
                        .Property("rawExpression", trigger.RawExpression)
                        .Property("condition", trigger.Condition)
                        .Property("inferred", trigger.Inferred)
                        .EndObject();
                }

                w.EndArray();
                WriteIssues(w, sequence.Issues);
                w.EndObject();
            }

            w.EndArray();

            w.Name("diagnostics").BeginArray();
            foreach (var d in Diagnostics)
            {
                w.BeginObject()
                    .Property("severity", d.Severity.ToString().ToLowerInvariant())
                    .Property("code", d.Code)
                    .Property("message", d.Message)
                    .Property("location", d.Location)
                    .EndObject();
            }

            w.EndArray();
            w.StringArray("files", Files);
            w.EndObject();
            return w + "\n";
        }

        private static void WriteIssues(JsonWriter w, IEnumerable<Issue> issues)
        {
            w.Name("issues").BeginArray();
            foreach (var issue in issues)
            {
                w.BeginObject()
                    .Property("severity", issue.Severity.ToString().ToLowerInvariant())
                    .Property("code", issue.Code)
                    .Property("message", issue.Message)
                    .Property("location", issue.Location)
                    .Property("blocking", issue.Blocking)
                    .EndObject();
            }

            w.EndArray();
        }

        // ------------------------------------------------------------------ Markdown

        public string ToMarkdown()
        {
            var md = new StringBuilder();
            md.Append("# DataStage to Airflow migration report\n\n");
            md.Append($"Project **{Escape(ProjectName)}**");
            if (!string.IsNullOrEmpty(ServerVersion)) md.Append($", DataStage {Escape(ServerVersion!)}");
            if (!string.IsNullOrEmpty(ExportingTool)) md.Append($" ({Escape(ExportingTool!)})");
            md.Append($". Generated by {Tool} on {Generated.ToString("yyyy-MM-dd HH:mm", CultureInfo.InvariantCulture)}.\n\n");
            if (Options != null)
            {
                md.Append($"Mode `{Options.Mode.ToString().ToLowerInvariant()}`, Airflow {(int)Options.Airflow}, ");
                md.Append($"schedule `{Options.Schedule ?? "None"}`, dsjob `{Options.DsjobPath}`");
                md.Append(Options.DsjobSshConnId == null ? " (local)" : $" over SSH connection `{Options.DsjobSshConnId}`");
                md.Append(".\n\n");
            }

            md.Append("Source files: ").Append(string.Join(", ", SourceFiles.Select(f => "`" + f + "`"))).Append("\n\n");

            md.Append("## Summary\n\n| | Count |\n|---|---:|\n");
            md.Append($"| Jobs | {Jobs.Count} |\n");
            md.Append($"| Converted to Python | {CountOutcome("python")} |\n");
            md.Append($"| Still run in DataStage (dsjob) | {CountOutcome("dsjob")} |\n");
            md.Append($"| Not converted | {CountOutcome("not converted")} |\n");
            md.Append($"| Sequences converted to DAGs | {Sequences.Count(s => s.DagId != null)} |\n");
            md.Append($"| Standalone job DAGs | {Jobs.Count(j => j.DagId != null)} |\n");
            md.Append($"| Blocking issues | {AllIssues.Count(i => i.Blocking)} |\n");
            md.Append($"| Warnings | {AllIssues.Count(i => !i.Blocking && i.Severity == Severity.Warning)} |\n\n");

            if (Sequences.Count > 0)
            {
                md.Append("## Sequences\n\n");
                foreach (var sequence in Sequences)
                {
                    md.Append($"### {Escape(sequence.Name)}\n\n");
                    if (sequence.DagId != null) md.Append($"DAG `{sequence.DagId}` in `{sequence.DagPath}`.\n\n");
                    md.Append("| Activity | Kind | Task | Does | Implementation |\n|---|---|---|---|---|\n");
                    foreach (var a in sequence.Activities)
                    {
                        md.Append($"| {Cell(a.Name)} | {a.Kind} | `{a.TaskId}` | {Cell(a.Detail)} | {Cell(a.Implementation)} |\n");
                    }

                    md.Append("\n| Trigger | From | To | Type | Condition in the DAG |\n|---|---|---|---|---|\n");
                    foreach (var t in sequence.Triggers)
                    {
                        var kind = t.Kind + (t.Inferred ? " (inferred from code " + t.RawType + ")" : string.Empty);
                        var condition = t.Condition.Length == 0 ? "dependency"
                            : t.Condition.StartsWith("inside loop task", StringComparison.Ordinal) ? Cell(t.Condition)
                            : "`" + Cell(t.Condition) + "`";
                        md.Append($"| {Cell(t.Name)} | {Cell(t.From)} | {Cell(t.To)} | {Cell(kind)} | {condition} |\n");
                    }

                    md.Append('\n');
                    var notes = sequence.Activities.Where(a => a.Notes.Count > 0 || a.UnmappedProperties.Count > 0).ToList();
                    foreach (var a in notes)
                    {
                        foreach (var note in a.Notes) md.Append($"- {Escape(a.Name)}: {Escape(note)}\n");
                        if (a.UnmappedProperties.Count > 0)
                        {
                            md.Append($"- {Escape(a.Name)}: properties not interpreted: {Escape(string.Join("; ", a.UnmappedProperties))}\n");
                        }
                    }

                    AppendIssues(md, sequence.Issues);
                    md.Append('\n');
                }
            }

            md.Append("## Jobs\n\n| Job | Kind | Outcome | Module / DAG | Blocking | Warnings |\n|---|---|---|---|---:|---:|\n");
            foreach (var job in Jobs)
            {
                var where = job.Module ?? (job.DraftPath != null ? "draft: " + job.DraftPath : string.Empty);
                if (job.DagId != null) where += (where.Length > 0 ? ", " : string.Empty) + "DAG " + job.DagId;
                md.Append($"| {Cell(job.Name)} | {job.Kind} | {job.Outcome} | {Cell(where)} | {job.BlockingCount} | {job.Issues.Count(i => !i.Blocking && i.Severity == Severity.Warning)} |\n");
            }

            md.Append('\n');
            foreach (var job in Jobs)
            {
                md.Append($"### {Escape(job.Name)}\n\n");
                md.Append($"{job.Kind} job, **{job.Outcome}**");
                if (job.RunBy.Count > 0) md.Append("; run by " + string.Join(", ", job.RunBy.Select(Escape)));
                md.Append(".\n\n");
                if (job.Stages.Count > 0)
                {
                    md.Append("| Stage | Type | Status | Notes |\n|---|---|---|---|\n");
                    foreach (var stage in job.Stages)
                    {
                        md.Append($"| {Cell(stage.Name)} | {Cell(stage.StageType)} | {stage.Status.ToString().ToLowerInvariant()} | {Cell(string.Join("; ", stage.Notes))} |\n");
                    }

                    md.Append('\n');
                }

                AppendIssues(md, job.Issues);
                md.Append('\n');
            }

            if (Diagnostics.Count > 0)
            {
                md.Append("## Export diagnostics\n\n");
                foreach (var d in Diagnostics) md.Append("- ").Append(Escape(d.ToString())).Append('\n');
                md.Append('\n');
            }

            return md.ToString();
        }

        private static void AppendIssues(StringBuilder md, IEnumerable<Issue> issues)
        {
            foreach (var issue in issues.OrderByDescending(i => i.Blocking).ThenByDescending(i => i.Severity))
            {
                var tag = issue.Blocking ? "**blocking**" : issue.Severity.ToString().ToLowerInvariant();
                md.Append($"- {tag} `{issue.Code}` {Escape(issue.Location)}: {Escape(issue.Message)}\n");
            }
        }

        private static string Escape(string text) => text.Replace("\r", " ").Replace("\n", " ");

        private static string Cell(string text) => Escape(text).Replace("|", "\\|");
    }
}
