using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using DataStage2Airflow.Dsx;
using DataStage2Airflow.Generation;
using DataStage2Airflow.Model;
using DataStage2Airflow.Reporting;

namespace DataStage2Airflow
{
    public sealed class MigrationResult
    {
        public MigrationResult(DsProject project, MigrationReport report, DiagnosticBag diagnostics)
        {
            Project = project;
            Report = report;
            Diagnostics = diagnostics;
        }

        public DsProject Project { get; }

        public MigrationReport Report { get; }

        public DiagnosticBag Diagnostics { get; }
    }

    /// <summary>
    /// Reads DataStage exports and writes an Airflow DAGs folder:
    /// <code>
    /// dags/&lt;project&gt;/*.py              one DAG per job sequence, plus one per job no sequence runs
    /// dags/ds2af_jobs/&lt;project&gt;/*.py   converted jobs and routines.py
    /// dags/ds2af_runtime/              runtime support package
    /// drafts/&lt;project&gt;/*.py            partial conversions of jobs that still run in DataStage
    /// reports/migration_report.md|json
    /// </code>
    /// </summary>
    public sealed class Migrator
    {
        private readonly MigrationOptions _options;

        public Migrator(MigrationOptions options)
        {
            _options = options;
        }

        public static List<string> ExpandInputs(IEnumerable<string> inputs, DiagnosticBag diagnostics)
        {
            var files = new List<string>();
            foreach (var input in inputs)
            {
                if (Directory.Exists(input))
                {
                    files.AddRange(Directory.EnumerateFiles(input, "*.*", SearchOption.AllDirectories)
                        .Where(f => f.EndsWith(".dsx", StringComparison.OrdinalIgnoreCase) || f.EndsWith(".xml", StringComparison.OrdinalIgnoreCase))
                        .OrderBy(f => f, StringComparer.OrdinalIgnoreCase));
                }
                else if (File.Exists(input))
                {
                    files.Add(input);
                }
                else
                {
                    diagnostics.Error("IN001", "input not found", input);
                }
            }

            return files;
        }

        public DsProject Load(IEnumerable<string> inputs, DiagnosticBag diagnostics)
        {
            var documents = new List<DsxDocument>();
            foreach (var file in ExpandInputs(inputs, diagnostics))
            {
                try
                {
                    documents.Add(ExportReader.ReadFile(file, diagnostics));
                }
                catch (DsxFormatException ex)
                {
                    diagnostics.Error("IN002", ex.Message, file);
                }
                catch (IOException ex)
                {
                    diagnostics.Error("IN003", ex.Message, file);
                }
            }

            return new ModelBuilder(diagnostics).Build(documents, _options.ProjectName);
        }

        public MigrationResult Run(IEnumerable<string> inputs, IOutputSink sink)
        {
            var diagnostics = new DiagnosticBag();
            var project = Load(inputs, diagnostics);
            return Run(project, sink, diagnostics);
        }

        public MigrationResult Run(DsProject project, IOutputSink sink, DiagnosticBag? diagnostics = null)
        {
            diagnostics ??= new DiagnosticBag();
            var report = new MigrationReport(project.Name)
            {
                ServerVersion = project.ServerVersion,
                ExportingTool = project.ExportingTool,
                Options = _options,
            };
            report.SourceFiles.AddRange(project.SourceFiles.Select(f => Path.GetFileName(f) ?? f));

            var projectPackage = Naming.Identifier(project.Name);
            var jobsPackage = _options.JobsPackage + "." + projectPackage;
            var jobsDirectory = $"dags/{_options.JobsPackage}/{projectPackage}";
            var dagsDirectory = $"dags/{projectPackage}";

            void Write(string path, string content)
            {
                sink.Write(path, content);
                report.Files.Add(path);
            }

            var runBy = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
            foreach (var sequence in project.Jobs.Where(j => j.Kind == JobKind.Sequence && j.Sequence != null))
            {
                foreach (var activity in sequence.Sequence!.Activities.Where(a => a.Kind == ActivityKind.Job && a.JobName != null))
                {
                    var name = activity.JobName!.Trim();
                    if (!runBy.TryGetValue(name, out var list)) runBy[name] = list = new List<string>();
                    if (!list.Contains(sequence.Name)) list.Add(sequence.Name);
                }
            }

            // Jobs: convert to Python, or keep in DataStage.
            var modules = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);
            var moduleNames = new HashSet<string>(StringComparer.Ordinal) { "routines", "__init__" };
            var reports = new Dictionary<DsJob, JobReport>();
            var generator = new JobGenerator(project, _options);
            bool jobsUseRoutines = false;
            foreach (var job in project.Jobs.Where(j => j.Kind != JobKind.Sequence))
            {
                var moduleName = Naming.Unique(Naming.Identifier(job.Name), moduleNames);
                JobReport jobReport;
                if (_options.Mode == ConversionMode.Dsjob || job.Kind == JobKind.Mainframe)
                {
                    jobReport = new JobReport(job.Name, job.Kind)
                    {
                        Category = job.Category,
                        Description = job.Description,
                        Outcome = job.Kind == JobKind.Mainframe ? "not converted" : "dsjob",
                    };
                    modules[job.Name] = null;
                }
                else
                {
                    var conversion = generator.Generate(job, moduleName);
                    jobReport = conversion.Report;
                    bool python = conversion.Code != null && (_options.Mode == ConversionMode.Python || conversion.Convertible);
                    if (python)
                    {
                        var path = $"{jobsDirectory}/{moduleName}.py";
                        Write(path, conversion.Code!);
                        jobReport.Outcome = "python";
                        jobReport.Module = jobsPackage + "." + moduleName;
                        jobReport.ModulePath = path;
                        modules[job.Name] = jobReport.Module;
                        if (conversion.Code!.Contains("from . import routines")) jobsUseRoutines = true;
                    }
                    else
                    {
                        if (conversion.Code != null)
                        {
                            var draft = $"drafts/{projectPackage}/{moduleName}.py";
                            Write(draft, conversion.Code);
                            jobReport.DraftPath = draft;
                        }

                        jobReport.Outcome = job.Kind == JobKind.Parallel || job.Kind == JobKind.Server ? "dsjob" : "not converted";
                        modules[job.Name] = null;
                    }
                }

                if (runBy.TryGetValue(job.Name, out var sequences)) jobReport.RunBy.AddRange(sequences);
                report.Jobs.Add(jobReport);
                reports[job] = jobReport;
            }

            // Sequences: one DAG each.
            var dagFiles = new HashSet<string>(StringComparer.Ordinal);
            var sequenceGenerator = new SequenceGenerator(project, _options, modules, jobsPackage);
            bool sequencesUseRoutines = false;
            foreach (var job in project.Jobs.Where(j => j.Kind == JobKind.Sequence))
            {
                var dagId = SequenceGenerator.DagId(_options, job.Name);
                var sequenceReport = sequenceGenerator.Generate(job, dagId, out var code);
                var file = $"{dagsDirectory}/{Naming.Unique(Naming.Identifier(job.Name), dagFiles)}.py";
                Write(file, code);
                sequenceReport.DagPath = file;
                report.Sequences.Add(sequenceReport);
                if (code.Contains("ROUTINES")) sequencesUseRoutines = true;
            }

            // Jobs that no sequence runs get a DAG of their own.
            if (_options.StandaloneJobDags)
            {
                foreach (var job in project.Jobs.Where(j => j.Kind == JobKind.Parallel || j.Kind == JobKind.Server))
                {
                    var jobReport = reports[job];
                    if (jobReport.RunBy.Count > 0) continue;
                    var dagId = SequenceGenerator.DagId(_options, job.Name);
                    var module = modules.TryGetValue(job.Name, out var m) ? m : null;
                    var file = $"{dagsDirectory}/{Naming.Unique(Naming.Identifier(job.Name), dagFiles)}.py";
                    Write(file, JobDagGenerator.Generate(job, project, _options, dagId, module));
                    jobReport.DagId = dagId;
                    jobReport.DagPath = file;
                }
            }

            if (project.Routines.Count > 0 || jobsUseRoutines || sequencesUseRoutines)
            {
                var referenced = project.Jobs.Where(j => j.Sequence != null)
                    .SelectMany(j => j.Sequence!.Activities)
                    .Where(a => a.Kind == ActivityKind.Routine && a.RoutineName != null)
                    .Select(a => a.RoutineName!)
                    .Distinct(StringComparer.OrdinalIgnoreCase)
                    .ToList();
                Write($"{jobsDirectory}/routines.py", RoutinesGenerator.Generate(project, referenced, out _));
            }

            Write($"dags/{_options.JobsPackage}/__init__.py", "\"\"\"DataStage jobs converted to Python by ds2af.\"\"\"\n");
            Write($"{jobsDirectory}/__init__.py", $"\"\"\"Jobs of DataStage project {project.Name}.\"\"\"\n");
            if (_options.IncludeRuntime)
            {
                foreach (var file in RuntimeFiles.Load()) Write("dags/" + file.Key, file.Value);
            }

            Write("dags/.airflowignore", "ds2af_runtime/\n" + _options.JobsPackage + "/\n");
            report.Diagnostics.AddRange(diagnostics);
            report.Files.Add("reports/migration_report.json");
            report.Files.Add("reports/migration_report.md");
            sink.Write("reports/migration_report.json", report.ToJson());
            sink.Write("reports/migration_report.md", report.ToMarkdown());
            return new MigrationResult(project, report, diagnostics);
        }
    }
}
