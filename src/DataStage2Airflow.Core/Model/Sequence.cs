using System;
using System.Collections.Generic;
using System.Linq;
using DataStage2Airflow.Dsx;

namespace DataStage2Airflow.Model
{
    public enum ActivityKind
    {
        Job,
        Routine,
        ExecCommand,
        Notification,
        WaitForFile,
        Sequencer,
        Condition,
        Terminator,
        ExceptionHandler,
        StartLoop,
        EndLoop,
        UserVariables,
        Unknown,
    }

    public enum TriggerKind
    {
        Unconditional,
        Otherwise,
        Ok,
        Failed,
        Warning,
        Custom,
        UserStatus,
        ReturnValue,
    }

    public enum ExecutionAction
    {
        Run,
        ResetIfRequiredThenRun,
        ValidateOnly,
        ResetOnly,
    }

    public sealed class NamedExpression
    {
        public NamedExpression(string name, string expression)
        {
            Name = name;
            Expression = expression;
        }

        public string Name { get; }

        /// <summary>A DataStage expression: a quoted literal, a parameter, or any BASIC expression.</summary>
        public string Expression { get; }

        public override string ToString() => $"{Name} = {Expression}";
    }

    public sealed class Activity
    {
        public Activity(string id, string name, ActivityKind kind, DsxRecord record)
        {
            Id = id;
            Name = name;
            Kind = kind;
            Record = record;
        }

        public string Id { get; }

        public string Name { get; }

        public ActivityKind Kind { get; }

        public DsxRecord Record { get; }

        public string OleType => Record.OleType;

        public string Description { get; set; } = string.Empty;

        public List<Trigger> Outgoing { get; } = new List<Trigger>();

        public List<Trigger> Incoming { get; } = new List<Trigger>();

        // Job activity
        public string? JobName { get; set; }

        public string? InvocationId { get; set; }

        public ExecutionAction ExecutionAction { get; set; } = ExecutionAction.Run;

        public bool DoNotCheckpoint { get; set; }

        /// <summary>Job parameter values (job activity) or routine arguments (routine activity).</summary>
        public List<NamedExpression> Parameters { get; } = new List<NamedExpression>();

        // Routine activity
        public string? RoutineName { get; set; }

        // Execute command activity
        public string? Command { get; set; }

        public string? CommandArguments { get; set; }

        // Notification activity
        public string? SmtpServer { get; set; }

        public string? Sender { get; set; }

        public string? Recipients { get; set; }

        public string? Subject { get; set; }

        public string? Body { get; set; }

        public string? Attachments { get; set; }

        public bool IncludeJobStatus { get; set; }

        // Wait-for-file activity
        public string? FileName { get; set; }

        public bool WaitForDisappearance { get; set; }

        /// <summary>Raw timeout as entered (usually hh:mm:ss); null when the activity never times out.</summary>
        public string? Timeout { get; set; }

        // Sequencer
        public bool SequencerAny { get; set; }

        // Start loop
        public bool IsListLoop { get; set; }

        public string? LoopFrom { get; set; }

        public string? LoopStep { get; set; }

        public string? LoopTo { get; set; }

        public string? LoopValues { get; set; }

        public string? LoopDelimiter { get; set; }

        // User variables
        public List<NamedExpression> Variables { get; } = new List<NamedExpression>();

        // Terminator
        public bool SendStopRequests { get; set; } = true;

        /// <summary>Properties of the record the builder did not interpret; listed in the report.</summary>
        public List<KeyValuePair<string, string>> UnmappedProperties { get; } = new List<KeyValuePair<string, string>>();

        public override string ToString() => $"{Name} ({Kind})";
    }

    public sealed class Trigger
    {
        public Trigger(string name, Activity source)
        {
            Name = name;
            Source = source;
        }

        public string Name { get; }

        public Activity Source { get; }

        public Activity? Target { get; set; }

        public TriggerKind Kind { get; set; } = TriggerKind.Unconditional;

        /// <summary>Custom condition, user status value or return value, depending on <see cref="Kind"/>.</summary>
        public string? Expression { get; set; }

        /// <summary>The trigger type as stored in the export, for the report.</summary>
        public string RawType { get; set; } = string.Empty;

        public string? RawExpression { get; set; }

        /// <summary>True when the kind came only from the numeric type code (the mapping is inferred).</summary>
        public bool KindFromCodeOnly { get; set; }

        public override string ToString() =>
            $"{Source.Name} -[{Kind}{(Expression == null ? string.Empty : ": " + Expression)}]-> {Target?.Name ?? "?"}";
    }

    public sealed class SequenceDefinition
    {
        public List<Activity> Activities { get; } = new List<Activity>();

        public List<Trigger> Triggers { get; } = new List<Trigger>();

        /// <summary>"Automatically handle activities that fail".</summary>
        public bool? AutoHandleFailures { get; set; }

        /// <summary>"Add checkpoints so sequence is restartable on failure".</summary>
        public bool? Restartable { get; set; }

        public Activity? Find(string name) =>
            Activities.FirstOrDefault(a => string.Equals(a.Name, name, StringComparison.OrdinalIgnoreCase));

        public IEnumerable<Activity> OfKind(ActivityKind kind) => Activities.Where(a => a.Kind == kind);
    }
}
