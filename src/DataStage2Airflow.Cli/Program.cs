using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using DataStage2Airflow.Generation;
using DataStage2Airflow.Model;
using DataStage2Airflow.Reporting;

namespace DataStage2Airflow.Cli
{
    internal static class Program
    {
        private const string Help = @"ds2af - migrate Ascential/IBM DataStage exports to Apache Airflow

Usage:
  ds2af convert <export.dsx|export.xml|folder>... -o <output folder> [options]
  ds2af inspect <export.dsx|export.xml|folder>... [options]
  ds2af --version

convert writes an Airflow DAGs folder (DAGs, converted jobs, runtime) and a migration report.
inspect does the same conversion in memory and prints what would happen.

Options:
  -o, --output <dir>         output folder (convert)
  --mode <auto|python|dsjob> auto: convert jobs whose stages and expressions all translate and run
                             the rest through dsjob (default); python: always generate Python;
                             dsjob: only orchestrate, every job keeps running in DataStage
  --airflow <2|3>            target Airflow version (default 3)
  --project <name>           project name (default: from the export header)
  --dag-prefix <prefix>      prefix for every DAG id
  --owner <name>             DAG owner (default datastage)
  --schedule <cron>          schedule of the generated DAGs (default: none, triggered manually)
  --start-date <yyyy-mm-dd>  DAG start date (default 2024-01-01)
  --dsjob <path>             dsjob executable on the DataStage server (default dsjob)
  --dsjob-server <host>      -server argument for dsjob
  --dsjob-ssh-conn <id>      Airflow SSH connection used to run dsjob on the DataStage server
  --connection <src=id>      map a DataStage data source (DSN, server) to an Airflow connection id;
                             repeatable
  --warnings-ok              OK triggers also fire when a job finishes with warnings
  --fail-on-warning          a job that finishes with warnings fails its task
  --no-job-dags              do not generate DAGs for jobs that no sequence runs
  --no-runtime               do not copy the ds2af_runtime package
  --strict                   exit with code 3 when any issue blocks a conversion
";

        private static int Main(string[] args)
        {
            try
            {
                return Run(args);
            }
            catch (UsageException ex)
            {
                Console.Error.WriteLine("error: " + ex.Message);
                Console.Error.WriteLine("Run 'ds2af --help' for usage.");
                return 2;
            }
            catch (Exception ex) when (!(ex is OutOfMemoryException))
            {
                Console.Error.WriteLine("error: " + ex.Message);
                return 1;
            }
        }

        private static int Run(string[] args)
        {
            if (args.Length == 0)
            {
                Console.Write(Help);
                return 2;
            }

            switch (args[0])
            {
                case "convert":
                    return Convert(Parse(args.Skip(1)), write: true);
                case "inspect":
                    return Convert(Parse(args.Skip(1)), write: false);
                case "--version":
                case "version":
                    Console.WriteLine("ds2af " + MigrationReport.Version);
                    return 0;
                case "-h":
                case "--help":
                case "help":
                    Console.Write(Help);
                    return 0;
                default:
                    throw new UsageException($"unknown command '{args[0]}'");
            }
        }

        private sealed class Arguments
        {
            public List<string> Inputs { get; } = new List<string>();

            public string? Output { get; set; }

            public bool Strict { get; set; }

            public MigrationOptions Options { get; } = new MigrationOptions();
        }

        private static Arguments Parse(IEnumerable<string> args)
        {
            var result = new Arguments();
            var queue = new Queue<string>(args);
            while (queue.Count > 0)
            {
                var arg = queue.Dequeue();
                string Value()
                {
                    if (queue.Count == 0) throw new UsageException($"{arg} needs a value");
                    return queue.Dequeue();
                }

                switch (arg)
                {
                    case "-o":
                    case "--output":
                        result.Output = Value();
                        break;
                    case "--mode":
                        result.Options.Mode = Value().ToLowerInvariant() switch
                        {
                            "auto" => ConversionMode.Auto,
                            "python" => ConversionMode.Python,
                            "dsjob" => ConversionMode.Dsjob,
                            _ => throw new UsageException("--mode must be auto, python or dsjob"),
                        };
                        break;
                    case "--airflow":
                        result.Options.Airflow = Value() switch
                        {
                            "2" => AirflowVersion.V2,
                            "3" => AirflowVersion.V3,
                            _ => throw new UsageException("--airflow must be 2 or 3"),
                        };
                        break;
                    case "--project":
                        result.Options.ProjectName = Value();
                        break;
                    case "--dag-prefix":
                        result.Options.DagPrefix = Value();
                        break;
                    case "--owner":
                        result.Options.Owner = Value();
                        break;
                    case "--schedule":
                        result.Options.Schedule = Value();
                        break;
                    case "--start-date":
                        var text = Value();
                        if (!DateTime.TryParseExact(text, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var date))
                        {
                            throw new UsageException("--start-date must be yyyy-mm-dd");
                        }

                        result.Options.StartDate = date;
                        break;
                    case "--dsjob":
                        result.Options.DsjobPath = Value();
                        break;
                    case "--dsjob-server":
                        result.Options.DsjobServer = Value();
                        break;
                    case "--dsjob-ssh-conn":
                        result.Options.DsjobSshConnId = Value();
                        break;
                    case "--connection":
                        var mapping = Value();
                        int eq = mapping.IndexOf('=');
                        if (eq <= 0 || eq == mapping.Length - 1) throw new UsageException("--connection must look like SOURCE=conn_id");
                        result.Options.Connections[mapping.Substring(0, eq)] = mapping.Substring(eq + 1);
                        break;
                    case "--warnings-ok":
                        result.Options.WarningsAreOk = true;
                        break;
                    case "--fail-on-warning":
                        result.Options.FailOnWarning = true;
                        break;
                    case "--no-job-dags":
                        result.Options.StandaloneJobDags = false;
                        break;
                    case "--no-runtime":
                        result.Options.IncludeRuntime = false;
                        break;
                    case "--strict":
                        result.Strict = true;
                        break;
                    default:
                        if (arg.StartsWith("-", StringComparison.Ordinal)) throw new UsageException($"unknown option {arg}");
                        result.Inputs.Add(arg);
                        break;
                }
            }

            if (result.Inputs.Count == 0) throw new UsageException("no input files");
            return result;
        }

        private static int Convert(Arguments arguments, bool write)
        {
            if (write && arguments.Output == null) throw new UsageException("convert needs --output");
            IOutputSink sink = write ? (IOutputSink)new DirectorySink(arguments.Output!) : new MemorySink();
            var result = new Migrator(arguments.Options).Run(arguments.Inputs, sink);
            var report = result.Report;

            foreach (var diagnostic in result.Diagnostics.Where(d => d.Severity != Severity.Info))
            {
                Console.Error.WriteLine(diagnostic.ToString());
            }

            if (result.Project.Jobs.Count == 0)
            {
                Console.Error.WriteLine("error: no jobs found in the input");
                return 1;
            }

            Console.WriteLine($"Project {report.ProjectName}" + (report.ServerVersion != null ? $" (DataStage {report.ServerVersion})" : string.Empty));
            Console.WriteLine();
            Console.WriteLine("Jobs:");
            foreach (var job in report.Jobs)
            {
                var detail = job.Module ?? job.DraftPath ?? string.Empty;
                var blocking = job.BlockingCount > 0 ? $", {job.BlockingCount} blocking issue(s)" : string.Empty;
                Console.WriteLine($"  {job.Name,-40} {job.Kind,-9} {job.Outcome,-14} {detail}{blocking}");
                if (!write)
                {
                    foreach (var issue in job.Issues.Where(i => i.Blocking)) Console.WriteLine($"      - {issue.Message}");
                }
            }

            if (report.Sequences.Count > 0)
            {
                Console.WriteLine();
                Console.WriteLine("Sequences:");
                foreach (var sequence in report.Sequences)
                {
                    var errors = sequence.Issues.Count(i => i.Blocking);
                    Console.WriteLine($"  {sequence.Name,-40} DAG {sequence.DagId}, {sequence.Activities.Count} activities, {sequence.Triggers.Count} triggers" + (errors > 0 ? $", {errors} error(s)" : string.Empty));
                    if (!write)
                    {
                        foreach (var issue in sequence.Issues) Console.WriteLine($"      - {issue.Severity.ToString().ToLowerInvariant()}: {issue.Message}");
                    }
                }
            }

            Console.WriteLine();
            Console.WriteLine($"{report.CountOutcome("python")} job(s) converted to Python, {report.CountOutcome("dsjob")} left in DataStage (dsjob), {report.CountOutcome("not converted")} not converted.");
            if (write)
            {
                var root = ((DirectorySink)sink).Root;
                Console.WriteLine($"Wrote {report.Files.Count} files to {root}");
                Console.WriteLine($"Report: {Path.Combine(root, "reports", "migration_report.md")}");
            }

            bool blockingIssues = report.AllIssues.Any(i => i.Blocking);
            return arguments.Strict && blockingIssues ? 3 : 0;
        }
    }

    internal sealed class UsageException : Exception
    {
        public UsageException(string message)
            : base(message)
        {
        }
    }
}
