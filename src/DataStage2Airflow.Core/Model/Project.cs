using System;
using System.Collections.Generic;
using System.Linq;

namespace DataStage2Airflow.Model
{
    public enum JobKind
    {
        Server,
        Parallel,
        Sequence,
        Mainframe,
        Unknown,
    }

    public enum ParameterType
    {
        String,
        Encrypted,
        Integer,
        Float,
        Pathname,
        List,
        Date,
        Time,
        ParameterSet,
        Unknown,
    }

    public sealed class JobParameter
    {
        public JobParameter(string name)
        {
            Name = name;
        }

        public string Name { get; }

        public string Prompt { get; set; } = string.Empty;

        public string DefaultValue { get; set; } = string.Empty;

        public ParameterType Type { get; set; } = ParameterType.String;

        public string RawType { get; set; } = string.Empty;

        public string HelpText { get; set; } = string.Empty;

        public List<string> ListValues { get; } = new List<string>();

        /// <summary>Parameters named <c>$NAME</c> map onto environment variables.</summary>
        public bool IsEnvironmentVariable => Name.StartsWith("$", StringComparison.Ordinal);

        public static ParameterType ParseType(string? code) => (code ?? "0").Trim() switch
        {
            "0" => ParameterType.String,
            "1" => ParameterType.Encrypted,
            "2" => ParameterType.Integer,
            "3" => ParameterType.Float,
            "4" => ParameterType.Pathname,
            "5" => ParameterType.List,
            "6" => ParameterType.Date,
            "7" => ParameterType.Time,
            "13" => ParameterType.ParameterSet,
            "" => ParameterType.String,
            _ => ParameterType.Unknown,
        };

        public override string ToString() => $"{Name}={DefaultValue} ({Type})";
    }

    /// <summary>A server routine, transform function or before/after subroutine exported with the project.</summary>
    public sealed class DsRoutine
    {
        public DsRoutine(string name, string identifier)
        {
            Name = name;
            Identifier = identifier;
        }

        public string Name { get; }

        /// <summary>Repository path, e.g. <c>DSU.MyRoutine</c> or <c>\Routines\Utils\MyRoutine</c>.</summary>
        public string Identifier { get; }

        public string Category { get; set; } = string.Empty;

        public string Description { get; set; } = string.Empty;

        public string RoutineType { get; set; } = string.Empty;

        /// <summary>DataStage BASIC source, when the export contains it.</summary>
        public string Source { get; set; } = string.Empty;

        public List<string> Arguments { get; } = new List<string>();
    }

    public sealed class DsJob
    {
        public DsJob(string name)
        {
            Name = name;
        }

        public string Name { get; }

        public JobKind Kind { get; set; } = JobKind.Unknown;

        public string RawJobType { get; set; } = string.Empty;

        /// <summary>Repository folder, e.g. <c>\Jobs\Warehouse\Loads</c>.</summary>
        public string Category { get; set; } = string.Empty;

        public string Description { get; set; } = string.Empty;

        public string FullDescription { get; set; } = string.Empty;

        public string? DateModified { get; set; }

        /// <summary>BASIC job control: written by hand for batch jobs, generated for sequences.</summary>
        public string? JobControlCode { get; set; }

        public bool AllowMultipleInvocations { get; set; }

        public string? BeforeSubroutine { get; set; }

        public string? BeforeSubroutineInput { get; set; }

        public string? AfterSubroutine { get; set; }

        public string? AfterSubroutineInput { get; set; }

        public bool IsSharedContainer { get; set; }

        public string SourceFile { get; set; } = string.Empty;

        public List<JobParameter> Parameters { get; } = new List<JobParameter>();

        public List<Stage> Stages { get; } = new List<Stage>();

        public List<Link> Links { get; } = new List<Link>();

        /// <summary>Activities and triggers, for sequence jobs only.</summary>
        public SequenceDefinition? Sequence { get; set; }

        public JobParameter? FindParameter(string name) =>
            Parameters.FirstOrDefault(p => string.Equals(p.Name, name, StringComparison.OrdinalIgnoreCase));

        public Stage? FindStage(string name) =>
            Stages.FirstOrDefault(s => string.Equals(s.Name, name, StringComparison.OrdinalIgnoreCase));

        public Link? FindLink(string name) =>
            Links.FirstOrDefault(l => string.Equals(l.Name, name, StringComparison.OrdinalIgnoreCase));

        public override string ToString() => $"{Name} ({Kind})";
    }

    public sealed class DsProject
    {
        public DsProject(string name)
        {
            Name = name;
        }

        public string Name { get; set; }

        public string? ServerVersion { get; set; }

        public string? ExportingTool { get; set; }

        public List<string> SourceFiles { get; } = new List<string>();

        public List<DsJob> Jobs { get; } = new List<DsJob>();

        public List<DsJob> SharedContainers { get; } = new List<DsJob>();

        public List<DsRoutine> Routines { get; } = new List<DsRoutine>();

        public DsJob? FindJob(string name)
        {
            var trimmed = name.Trim();
            return Jobs.FirstOrDefault(j => string.Equals(j.Name, trimmed, StringComparison.OrdinalIgnoreCase));
        }

        public DsJob? FindSharedContainer(string name) =>
            SharedContainers.FirstOrDefault(j => string.Equals(j.Name, name.Trim(), StringComparison.OrdinalIgnoreCase));

        public DsRoutine? FindRoutine(string name)
        {
            var trimmed = name.Trim();
            return Routines.FirstOrDefault(r => string.Equals(r.Name, trimmed, StringComparison.OrdinalIgnoreCase))
                ?? Routines.FirstOrDefault(r => r.Identifier.EndsWith("." + trimmed, StringComparison.OrdinalIgnoreCase)
                                             || r.Identifier.EndsWith("\\" + trimmed, StringComparison.OrdinalIgnoreCase));
        }
    }
}
