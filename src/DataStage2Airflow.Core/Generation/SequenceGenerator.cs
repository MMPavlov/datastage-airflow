using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using DataStage2Airflow.Expressions;
using DataStage2Airflow.Model;
using DataStage2Airflow.Reporting;

namespace DataStage2Airflow.Generation
{
    /// <summary>
    /// Turns a job sequence into an Airflow DAG. Activities become tasks. An activity whose triggers are all
    /// OK (or all unconditional) is a plain task: it fails on job failure and, for OK triggers, skips its
    /// downstream when the job finishes with warnings, as DataStage does. Any other trigger mix makes the
    /// activity a branch task that runs it and returns the task ids of the triggers that fired.
    /// Sequencers become EmptyOperators with all_success / one_success; a Start Loop ... End Loop section
    /// becomes one task that runs the loop body for each value; a job activity that runs another sequence
    /// triggers that sequence's DAG.
    /// </summary>
    public sealed class SequenceGenerator
    {
        private readonly DsProject _project;
        private readonly MigrationOptions _options;
        private readonly IReadOnlyDictionary<string, string?> _jobModules;
        private readonly string _jobsPackage;

        /// <param name="jobModules">Job name to converted Python module, or null when the job runs through dsjob.</param>
        public SequenceGenerator(DsProject project, MigrationOptions options, IReadOnlyDictionary<string, string?> jobModules, string jobsPackage)
        {
            _project = project;
            _options = options;
            _jobModules = jobModules;
            _jobsPackage = jobsPackage;
        }

        public SequenceReport Generate(DsJob job, string dagId, out string code)
        {
            var builder = new Builder(this, job, dagId);
            code = builder.Build();
            return builder.Report;
        }

        internal static string DagId(MigrationOptions options, string jobName) => options.DagPrefix + Naming.AirflowId(jobName);

        internal static IEnumerable<string> AirflowImports(AirflowVersion version, bool empty, bool triggerDag)
        {
            if (version == AirflowVersion.V2)
            {
                yield return "from airflow import DAG";
                yield return "from airflow.decorators import task";
                yield return "from airflow.models.param import Param";
                if (empty) yield return "from airflow.operators.empty import EmptyOperator";
                if (triggerDag) yield return "from airflow.operators.trigger_dagrun import TriggerDagRunOperator";
            }
            else
            {
                if (empty) yield return "from airflow.providers.standard.operators.empty import EmptyOperator";
                if (triggerDag) yield return "from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator";
                yield return "from airflow.sdk import DAG, Param, task";
            }
        }

        /// <summary>The <c>with DAG(...) as dag:</c> header shared by sequence and job DAGs.</summary>
        internal static void DagHeader(PythonWriter w, MigrationOptions options, DsProject project, DsJob job, string dagId, string kindTag, IEnumerable<JobParameter> parameters)
        {
            w.Line("with DAG(");
            w.Indent();
            w.Line($"dag_id={Py.Str(dagId)},");
            var description = (job.Description.Length > 0 ? job.Description : $"DataStage {kindTag} {job.Name}").Replace("\r", " ").Replace("\n", " ");
            w.Line($"description={Py.Str(description)},");
            w.Line($"schedule={(options.Schedule == null ? "None" : Py.Str(options.Schedule))},");
            var start = options.StartDate;
            w.Line($"start_date=datetime.datetime({start.Year}, {start.Month}, {start.Day}, tzinfo=datetime.timezone.utc),");
            w.Line("catchup=False,");
            w.Line("max_active_runs=1,");
            var dagParams = parameters.Where(p => !p.IsEnvironmentVariable).ToList();
            if (dagParams.Count == 0)
            {
                w.Line("params={},");
            }
            else
            {
                w.Line("params={");
                w.Indent();
                foreach (var p in dagParams)
                {
                    var args = new List<string> { Py.Str(p.Type == ParameterType.Encrypted ? string.Empty : p.DefaultValue), "type=\"string\"" };
                    var description2 = p.Prompt.Length > 0 ? p.Prompt : p.HelpText;
                    if (p.Type == ParameterType.Encrypted) description2 = (description2 + " (encrypted in DataStage)").Trim();
                    if (description2.Length > 0) args.Add("description=" + Py.Str(description2));
                    if (p.Type == ParameterType.List && p.ListValues.Count > 0) args.Add("enum=" + Py.List(p.ListValues));
                    w.Line($"{Py.Str(p.Name)}: Param({string.Join(", ", args)}),");
                }

                w.Dedent();
                w.Line("},");
            }

            w.Line($"tags=[\"datastage\", {Py.Str(project.Name)}, {Py.Str(kindTag)}],");
            w.Line($"default_args={{\"owner\": {Py.Str(options.Owner)}, \"retries\": 0}},");
            w.Line("doc_md=__doc__,");
            w.Dedent();
            w.Line(") as dag:");
        }

        internal static int TimeoutSeconds(string? timeout)
        {
            if (string.IsNullOrWhiteSpace(timeout)) return 7 * 24 * 3600;
            var parts = timeout!.Trim().Split(':');
            int total = 0;
            foreach (var part in parts)
            {
                if (!int.TryParse(part.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var n)) return 7 * 24 * 3600;
                total = (total * 60) + n;
            }

            return total <= 0 ? 7 * 24 * 3600 : total;
        }

        private enum NodeKind
        {
            Task,
            Branch,
            Sensor,
            Empty,
            Loop,
            ChildDag,
        }

        private sealed class Node
        {
            public Node(string taskId, string function, NodeKind kind, Activity activity)
            {
                TaskId = taskId;
                Function = function;
                Kind = kind;
                Activity = activity;
                Var = "t_" + function;
                EntryVar = Var;
            }

            public string TaskId { get; }

            public string Function { get; }

            public NodeKind Kind { get; set; }

            /// <summary>The activity (the Start Loop for a loop node).</summary>
            public Activity Activity { get; }

            public string Var { get; set; }

            public string EntryVar { get; set; }

            public string? TriggerRule { get; set; }

            public Activity? LoopEnd { get; set; }

            public List<Activity> Body { get; } = new List<Activity>();

            public string? ChildDag { get; set; }
        }

        private sealed class Builder
        {
            private readonly SequenceGenerator _owner;
            private readonly DsJob _job;
            private readonly SequenceDefinition _sequence;
            private readonly string _dagId;
            private readonly Dictionary<Activity, Node> _nodeOf = new Dictionary<Activity, Node>();
            private readonly List<Node> _nodes = new List<Node>();
            private readonly HashSet<string> _functions = new HashSet<string>(StringComparer.Ordinal);
            private readonly HashSet<string> _taskIds = new HashSet<string>(StringComparer.Ordinal);
            private readonly SequenceResolver _resolver;
            private readonly Dictionary<Activity, ActivityReport> _activityReports = new Dictionary<Activity, ActivityReport>();

            // (branch, target) -> join task id, for targets that several tasks can trigger.
            private readonly Dictionary<(Node From, Node To), string> _junctions = new Dictionary<(Node, Node), string>();

            // exception handler -> the tasks whose failure it handles.
            private readonly Dictionary<Node, List<Node>> _watchers = new Dictionary<Node, List<Node>>();

            // (trigger, result variable) -> the trigger's Python condition, translated once.
            private readonly Dictionary<(Trigger Trigger, string Result), string> _conditions = new Dictionary<(Trigger, string), string>();
            private bool _usesRoutines;

            public Builder(SequenceGenerator owner, DsJob job, string dagId)
            {
                _owner = owner;
                _job = job;
                _sequence = job.Sequence ?? new SequenceDefinition();
                _dagId = dagId;
                Report = new SequenceReport(job.Name) { Category = job.Category, DagId = dagId };
                _resolver = new SequenceResolver(this);
            }

            public SequenceReport Report { get; }

            private MigrationOptions Options => _owner._options;

            // ------------------------------------------------------------------ structure

            public string Build()
            {
                CreateLoopNodes();
                foreach (var activity in _sequence.Activities)
                {
                    if (!_nodeOf.ContainsKey(activity)) CreateNode(activity);
                }

                // Tasks appear in the DAG file in the order of the sequence's activities.
                var position = _sequence.Activities.Select((a, i) => (a, i)).ToDictionary(x => x.a, x => x.i);
                _nodes.Sort((x, y) => position[x.Activity].CompareTo(position[y.Activity]));

                var edges = new List<(Node From, Node To, Trigger Trigger)>();
                foreach (var trigger in _sequence.Triggers)
                {
                    if (trigger.Target == null) continue;
                    var from = _nodeOf[trigger.Source];
                    var to = _nodeOf[trigger.Target];
                    if (!ReferenceEquals(from, to)) edges.Add((from, to, trigger));
                }

                AssignTriggerRules(edges);
                PlanJunctions(edges);
                PlanWatchers(edges);
                foreach (var node in _nodes) ReportNode(node);
                foreach (var trigger in _sequence.Triggers) ReportTrigger(trigger);
                return Render(edges);
            }

            private void CreateLoopNodes()
            {
                foreach (var start in _sequence.OfKind(ActivityKind.StartLoop).ToList())
                {
                    var reachable = Reachable(start, a => a.Kind == ActivityKind.EndLoop);
                    var end = reachable.FirstOrDefault(a => a.Kind == ActivityKind.EndLoop);
                    if (end == null)
                    {
                        Issue(start, "LOOP001", $"start loop {start.Name} has no end loop; it is treated as a plain step", false);
                        continue;
                    }

                    var body = reachable.Where(a => !ReferenceEquals(a, start) && !ReferenceEquals(a, end) && Reachable(a, x => x.Kind == ActivityKind.EndLoop).Contains(end)).ToList();
                    if (body.Any(a => a.Kind == ActivityKind.StartLoop))
                    {
                        Issue(start, "LOOP002", $"loop {start.Name} contains another loop; nested loops are not converted", true);
                    }

                    var node = NewNode(start, NodeKind.Loop);
                    node.LoopEnd = end;
                    node.Body.AddRange(TopologicalBody(body, start));
                    _nodeOf[end] = node;
                    foreach (var member in node.Body)
                    {
                        if (!_nodeOf.ContainsKey(member)) _nodeOf[member] = node;
                    }
                }
            }

            private void CreateNode(Activity activity)
            {
                NodeKind kind;
                switch (activity.Kind)
                {
                    case ActivityKind.Sequencer:
                    case ActivityKind.ExceptionHandler:
                    case ActivityKind.StartLoop:
                    case ActivityKind.EndLoop:
                        kind = NeedsBranch(activity) ? NodeKind.Branch : NodeKind.Empty;
                        break;
                    case ActivityKind.Condition:
                        kind = NodeKind.Branch;
                        break;
                    case ActivityKind.WaitForFile:
                        kind = NeedsBranch(activity) ? NodeKind.Branch : NodeKind.Sensor;
                        break;
                    case ActivityKind.Job when ChildSequence(activity) != null:
                        kind = NodeKind.ChildDag;
                        break;
                    default:
                        kind = NeedsBranch(activity) ? NodeKind.Branch : NodeKind.Task;
                        break;
                }

                var node = NewNode(activity, kind);
                if (kind == NodeKind.ChildDag)
                {
                    node.ChildDag = DagId(Options, ChildSequence(activity)!.Name);
                    node.EntryVar = "t_" + node.Function + "_params";
                    if (NeedsBranch(activity))
                    {
                        Issue(activity, "SEQ010", "triggers of an activity that runs another sequence are simplified: OK follows success of the child DAG, Failed its failure", false);
                    }
                }
            }

            private Node NewNode(Activity activity, NodeKind kind)
            {
                var taskId = Naming.Unique(Naming.AirflowId(activity.Name), _taskIds);
                var function = Naming.Unique(Naming.Identifier(activity.Name), _functions);
                var node = new Node(taskId, function, kind, activity);
                _nodes.Add(node);
                _nodeOf[activity] = node;
                return node;
            }

            private static bool NeedsBranch(Activity activity)
            {
                if (activity.Kind == ActivityKind.Condition) return true;
                var kinds = activity.Outgoing.Where(t => t.Target != null).Select(t => t.Kind).Distinct().ToList();
                if (kinds.Any(k => k != TriggerKind.Ok && k != TriggerKind.Unconditional)) return true;
                return kinds.Contains(TriggerKind.Ok) && kinds.Contains(TriggerKind.Unconditional);
            }

            private DsJob? ChildSequence(Activity activity)
            {
                if (activity.JobName == null) return null;
                var target = _owner._project.FindJob(activity.JobName);
                return target != null && target.Kind == JobKind.Sequence ? target : null;
            }

            private static List<Activity> Reachable(Activity start, Func<Activity, bool> stopAt)
            {
                var seen = new List<Activity> { start };
                var queue = new Queue<Activity>();
                queue.Enqueue(start);
                while (queue.Count > 0)
                {
                    var current = queue.Dequeue();
                    if (!ReferenceEquals(current, start) && stopAt(current)) continue;
                    foreach (var trigger in current.Outgoing)
                    {
                        if (trigger.Target != null && !seen.Contains(trigger.Target))
                        {
                            seen.Add(trigger.Target);
                            queue.Enqueue(trigger.Target);
                        }
                    }
                }

                return seen;
            }

            private static List<Activity> TopologicalBody(List<Activity> body, Activity start)
            {
                var pending = body.ToDictionary(a => a, a => a.Incoming.Count(t => body.Contains(t.Source)));
                var ready = body.Where(a => pending[a] == 0).ToList();
                var order = new List<Activity>();
                while (ready.Count > 0)
                {
                    var next = ready[0];
                    ready.RemoveAt(0);
                    order.Add(next);
                    foreach (var trigger in next.Outgoing)
                    {
                        if (trigger.Target == null || !pending.ContainsKey(trigger.Target)) continue;
                        pending[trigger.Target]--;
                        if (pending[trigger.Target] == 0) ready.Add(trigger.Target);
                    }
                }

                order.AddRange(body.Where(a => !order.Contains(a)));
                return order;
            }

            private void AssignTriggerRules(List<(Node From, Node To, Trigger Trigger)> edges)
            {
                foreach (var node in _nodes)
                {
                    var incoming = edges.Where(e => ReferenceEquals(e.To, node)).ToList();
                    if (node.Activity.Kind == ActivityKind.Sequencer && node.Kind != NodeKind.Loop)
                    {
                        node.TriggerRule = node.Activity.SequencerAny ? "one_success" : "all_success";
                        continue;
                    }

                    if (node.Activity.Kind == ActivityKind.ExceptionHandler)
                    {
                        node.TriggerRule = "one_failed";
                        continue;
                    }

                    if (incoming.Count == 0) continue;
                    if (incoming.Count == 1)
                    {
                        var edge = incoming[0];
                        if (edge.From.Kind == NodeKind.Branch) continue;
                        if (edge.From.Kind == NodeKind.ChildDag && edge.Trigger.Kind == TriggerKind.Failed) node.TriggerRule = "all_failed";
                        else if (edge.Trigger.Kind == TriggerKind.Unconditional) node.TriggerRule = "none_skipped";
                        continue;
                    }

                    node.TriggerRule = "one_success";
                    Issue(node.Activity, "SEQ011", $"{node.Activity.Name} has {incoming.Count} input triggers; it runs when any of them fires", false);
                }
            }

            /// <summary>
            /// Airflow skips every direct downstream task a branch does not select. A task that several
            /// branches (or a branch and another task) can trigger would be skipped by the first branch that
            /// passes it over, so each such branch selects a join task of its own instead.
            /// </summary>
            private void PlanJunctions(List<(Node From, Node To, Trigger Trigger)> edges)
            {
                foreach (var group in edges.GroupBy(e => e.To))
                {
                    var target = group.Key;
                    var sources = group.Select(e => e.From).Distinct().ToList();
                    if (sources.Count < 2) continue;
                    if (target.Activity.Kind == ActivityKind.Sequencer && !target.Activity.SequencerAny) continue;
                    foreach (var source in sources.Where(s => s.Kind == NodeKind.Branch))
                    {
                        _junctions[(source, target)] = Naming.Unique(Naming.AirflowId(source.TaskId + "__to__" + target.TaskId), _taskIds);
                    }
                }
            }

            private void PlanWatchers(List<(Node From, Node To, Trigger Trigger)> edges)
            {
                foreach (var handler in _nodes.Where(n => n.Activity.Kind == ActivityKind.ExceptionHandler))
                {
                    var downstream = new HashSet<Node>();
                    var queue = new Queue<Node>();
                    queue.Enqueue(handler);
                    while (queue.Count > 0)
                    {
                        var current = queue.Dequeue();
                        foreach (var edge in edges.Where(e => ReferenceEquals(e.From, current)))
                        {
                            if (downstream.Add(edge.To)) queue.Enqueue(edge.To);
                        }
                    }

                    _watchers[handler] = _nodes
                        .Where(n => !ReferenceEquals(n, handler) && !downstream.Contains(n) && n.Kind != NodeKind.Empty)
                        .ToList();
                }
            }

            private static string JunctionVar(string taskId) => "j_" + Naming.Snake(taskId);

            // ------------------------------------------------------------------ rendering

            private string Render(List<(Node From, Node To, Trigger Trigger)> edges)
            {
                var body = new PythonWriter();
                body.Indent();
                foreach (var node in _nodes)
                {
                    body.Line();
                    EmitNode(body, node);
                }

                if (_junctions.Count > 0)
                {
                    body.Line();
                    body.Comment("A branch task skips the tasks it does not select, so a task that several tasks can");
                    body.Comment("trigger is reached through one join per branch.");
                    foreach (var junction in _junctions.Values) body.Line($"{JunctionVar(junction)} = EmptyOperator(task_id={Py.Str(junction)})");
                }

                body.Line();
                EmitDependencies(body, edges);

                var w = new PythonWriter();
                w.Docstring(Docstring());
                w.Line();
                w.Line("from __future__ import annotations");
                w.Line();
                w.Line("import datetime");
                w.Line();
                var text = body.ToString();
                bool empty = _nodes.Any(n => n.Kind == NodeKind.Empty) || _junctions.Count > 0;
                bool child = _nodes.Any(n => n.Kind == NodeKind.ChildDag);
                w.Lines(AirflowImports(Options.Airflow, empty, child));
                w.Line();
                if (text.Contains("F.")) w.Line("from ds2af_runtime import dsfunc as F");
                w.Line("from ds2af_runtime import sequence as seq");
                w.Line();
                w.Line($"SEQUENCE = {Py.Str(_job.Name)}");
                w.Line($"PROJECT = {Py.Str(_owner._project.Name)}");
                w.Line($"JOBS_PACKAGE = {Py.Str(_owner._jobsPackage)}");
                if (_usesRoutines) w.Line("ROUTINES = JOBS_PACKAGE + \".routines\"");
                w.Line();
                w.Comment("Sequence parameters and their DataStage defaults.");
                w.Line("PARAMETERS = {");
                w.Indent();
                foreach (var p in _job.Parameters.Where(p => !p.IsEnvironmentVariable))
                {
                    w.Line($"{Py.Str(p.Name)}: {Py.Str(p.Type == ParameterType.Encrypted ? string.Empty : p.DefaultValue)},");
                }

                w.Dedent();
                w.Line("}");
                w.Line();
                w.Comment("DataStage activity -> Airflow task holding its result.");
                w.Line("TASKS = {");
                w.Indent();
                foreach (var activity in _sequence.Activities)
                {
                    w.Line($"{Py.Str(activity.Name)}: {Py.Str(_nodeOf[activity].TaskId)},");
                }

                w.Dedent();
                w.Line("}");
                if (text.Contains("DSJOB"))
                {
                    w.Line();
                    w.Comment("Jobs that were not converted still run in DataStage through dsjob.");
                    var args = new List<string> { "PROJECT", $"dsjob={Py.Str(Options.DsjobPath)}" };
                    if (Options.DsjobServer != null) args.Add($"server={Py.Str(Options.DsjobServer)}");
                    if (Options.DsjobSshConnId != null) args.Add($"ssh_conn_id={Py.Str(Options.DsjobSshConnId)}");
                    w.Line($"DSJOB = seq.DsJobConfig({string.Join(", ", args)})");
                }

                w.Line();
                w.Line();
                using (w.Block("def _state(context):"))
                {
                    w.Line("return seq.state(context, SEQUENCE, PARAMETERS, TASKS, PROJECT)");
                }

                w.Line();
                w.Line();
                DagHeader(w, Options, _owner._project, _job, _dagId, "sequence", _job.Parameters);
                foreach (var line in text.TrimEnd('\n').Split('\n')) w.Line(line);
                return PythonWriter.Tidy(w.ToString());
            }

            private string Docstring()
            {
                var sb = new StringBuilder();
                sb.Append(_job.Name).Append("\n\n");
                sb.Append($"Generated by {new MigrationReport(string.Empty).Tool} from DataStage job sequence {_job.Name} (project {_owner._project.Name}).\n");
                sb.Append($"Source: {System.IO.Path.GetFileName(_job.SourceFile)}");
                if (_job.Category.Length > 0) sb.Append($", category {_job.Category}");
                sb.Append(".\n");
                var description = _job.FullDescription.Length > 0 ? _job.FullDescription : _job.Description;
                if (description.Trim().Length > 0) sb.Append('\n').Append(description.Trim()).Append('\n');
                sb.Append("\nActivities:\n");
                foreach (var activity in _sequence.Activities)
                {
                    sb.Append($"    {activity.Name}: {Describe(activity)}\n");
                }

                return sb.ToString();
            }

            private string Describe(Activity a)
            {
                switch (a.Kind)
                {
                    case ActivityKind.Job:
                        var child = ChildSequence(a);
                        if (child != null) return $"runs sequence {child.Name} (DAG {DagId(Options, child.Name)})";
                        return $"runs job {a.JobName} ({(ModuleOf(a.JobName) != null ? "converted Python job" : "in DataStage via dsjob")})";
                    case ActivityKind.Routine:
                        return $"calls routine {a.RoutineName}";
                    case ActivityKind.ExecCommand:
                        return $"runs command {a.Command} {a.CommandArguments}".TrimEnd();
                    case ActivityKind.Notification:
                        return $"e-mails {a.Recipients}";
                    case ActivityKind.WaitForFile:
                        return $"waits for {a.FileName} to {(a.WaitForDisappearance ? "disappear" : "appear")}";
                    case ActivityKind.Sequencer:
                        return a.SequencerAny ? "sequencer (any)" : "sequencer (all)";
                    case ActivityKind.Condition:
                        return "nested condition";
                    case ActivityKind.Terminator:
                        return "terminates the sequence";
                    case ActivityKind.ExceptionHandler:
                        return "exception handler";
                    case ActivityKind.StartLoop:
                        return a.IsListLoop ? $"loops over {a.LoopValues}" : $"loops from {a.LoopFrom} to {a.LoopTo} step {a.LoopStep ?? "1"}";
                    case ActivityKind.EndLoop:
                        return "end of loop";
                    case ActivityKind.UserVariables:
                        return "sets " + string.Join(", ", a.Variables.Select(v => v.Name));
                    default:
                        return "unknown activity type " + a.OleType;
                }
            }

            private string? ModuleOf(string? jobName)
            {
                if (jobName == null) return null;
                return _owner._jobModules.TryGetValue(jobName.Trim(), out var module) ? module : null;
            }

            private void EmitNode(PythonWriter w, Node node)
            {
                switch (node.Kind)
                {
                    case NodeKind.Empty:
                        w.Line($"{node.Var} = EmptyOperator(task_id={Py.Str(node.TaskId)}{RuleArgument(node)})");
                        break;
                    case NodeKind.Sensor:
                        EmitSensor(w, node);
                        break;
                    case NodeKind.Loop:
                        EmitLoop(w, node);
                        break;
                    case NodeKind.ChildDag:
                        EmitChildDag(w, node);
                        break;
                    default:
                        EmitTask(w, node);
                        break;
                }
            }

            private static string RuleArgument(Node node) => node.TriggerRule == null ? string.Empty : $", trigger_rule={Py.Str(node.TriggerRule)}";

            private void EmitTask(PythonWriter w, Node node)
            {
                var a = node.Activity;
                bool branch = node.Kind == NodeKind.Branch;
                w.Line($"@task{(branch ? ".branch" : string.Empty)}(task_id={Py.Str(node.TaskId)}{RuleArgument(node)})");
                using (w.Block($"def {node.Function}(**context):"))
                {
                    w.Docstring($"{a.Name}: {Describe(a)}.");
                    w.Line("s = _state(context)");
                    var onWarning = branch ? "ok" : WarningPolicy(a);
                    EmitRun(w, a, "r", !branch, onWarning);
                    if (branch)
                    {
                        EmitRouting(w, a, "r");
                    }
                    else
                    {
                        w.Line(HasResult(a) ? "return r.as_dict()" : "return None");
                    }
                }

                w.Line();
                w.Line($"{node.Var} = {node.Function}()");
            }

            private string WarningPolicy(Activity a)
            {
                if (a.Kind != ActivityKind.Job) return "ok";
                if (Options.FailOnWarning) return "fail";
                bool okTriggers = a.Outgoing.Any(t => t.Target != null && t.Kind == TriggerKind.Ok);
                return okTriggers ? "skip" : "ok";
            }

            private void EmitSensor(PythonWriter w, Node node)
            {
                var a = node.Activity;
                int timeout = TimeoutSeconds(a.Timeout);
                w.Line($"@task.sensor(task_id={Py.Str(node.TaskId)}, poke_interval=60, timeout={timeout}, mode=\"reschedule\"{RuleArgument(node)})");
                using (w.Block($"def {node.Function}(**context):"))
                {
                    w.Docstring($"{a.Name}: {Describe(a)}.");
                    w.Line("s = _state(context)");
                    w.Line($"return seq.check_file(s, {Py.Str(a.Name)}, {Text(a.FileName, a, "file name")}, appear={Py.Bool(!a.WaitForDisappearance)})");
                }

                w.Line();
                w.Line($"{node.Var} = {node.Function}()");
            }

            private void EmitChildDag(PythonWriter w, Node node)
            {
                var a = node.Activity;
                w.Line($"@task(task_id={Py.Str(node.TaskId + "__params")}{RuleArgument(node)})");
                using (w.Block($"def {node.Function}_params(**context):"))
                {
                    w.Docstring($"Parameters for sequence {a.JobName}, run by {a.Name}.");
                    w.Line("s = _state(context)");
                    w.Line("return " + ParameterDict(a));
                }

                w.Line();
                w.Line($"{node.EntryVar} = {node.Function}_params()");
                w.Line($"{node.Var} = TriggerDagRunOperator(");
                w.Indent();
                w.Line($"task_id={Py.Str(node.TaskId)},");
                w.Line($"trigger_dag_id={Py.Str(node.ChildDag!)},");
                w.Line($"conf={node.EntryVar},");
                w.Line("wait_for_completion=True,");
                w.Line("poke_interval=30,");
                w.Dedent();
                w.Line(")");
            }

            private void EmitLoop(PythonWriter w, Node node)
            {
                var start = node.Activity;
                w.Line($"@task(task_id={Py.Str(node.TaskId)}{RuleArgument(node)})");
                using (w.Block($"def {node.Function}(**context):"))
                {
                    w.Docstring($"{start.Name} ... {node.LoopEnd!.Name}: {Describe(start)}; runs {string.Join(", ", node.Body.Select(b => b.Name))} for each value.");
                    w.Line("s = _state(context)");
                    w.Line("iterations = 0");
                    string values;
                    if (start.IsListLoop)
                    {
                        values = $"seq.loop_values(numeric=False, values={Text(start.LoopValues ?? string.Empty, start, "loop values")}, delimiter={Py.Str(start.LoopDelimiter ?? ",")})";
                    }
                    else
                    {
                        values = $"seq.loop_values(start={LoopBound(start.LoopFrom, "1", start)}, step={LoopBound(start.LoopStep, "1", start)}, end={LoopBound(start.LoopTo, "1", start)})";
                    }

                    using (w.Block($"for counter in {values}:"))
                    {
                        w.Line($"seq.set_counter(s, {Py.Str(start.Name)}, counter)");
                        w.Line("iterations += 1");
                        var entry = start.Outgoing.Where(t => t.Target != null && node.Body.Contains(t.Target)).Select(t => t.Target!).Distinct().ToList();
                        w.Line("fired = {" + string.Join(", ", entry.Select(e => $"{Py.Str(e.Name)}: 1")) + "}");
                        foreach (var activity in node.Body)
                        {
                            var fromBody = activity.Incoming.Count(t => node.Body.Contains(t.Source));
                            var activation = activity.Kind == ActivityKind.Sequencer && !activity.SequencerAny && fromBody > 1
                                ? $"fired.get({Py.Str(activity.Name)}, 0) >= {fromBody}"
                                : $"fired.get({Py.Str(activity.Name)})";
                            using (w.Block($"if {activation}:"))
                            {
                                bool handlesFailure = activity.Outgoing.Any(t => t.Kind == TriggerKind.Failed || t.Kind == TriggerKind.Otherwise || t.Kind == TriggerKind.Custom || t.Kind == TriggerKind.ReturnValue || t.Kind == TriggerKind.Unconditional);
                                EmitRun(w, activity, "r", !handlesFailure, "ok");
                                var inside = activity.Outgoing.Where(t => t.Target != null && node.Body.Contains(t.Target)).ToList();
                                var otherwise = inside.Where(t => t.Kind == TriggerKind.Otherwise).ToList();
                                if (otherwise.Count > 0) w.Line("hit = False");
                                foreach (var trigger in inside.Where(t => t.Kind != TriggerKind.Otherwise))
                                {
                                    var condition = Condition(trigger, activity, "r");
                                    var fire = $"fired[{Py.Str(trigger.Target!.Name)}] = fired.get({Py.Str(trigger.Target.Name)}, 0) + 1";
                                    if (condition == "True")
                                    {
                                        w.Line(fire);
                                        if (otherwise.Count > 0 && trigger.Kind != TriggerKind.Unconditional) w.Line("hit = True");
                                        continue;
                                    }

                                    using (w.Block($"if {condition}:"))
                                    {
                                        w.Line(fire);
                                        if (otherwise.Count > 0 && trigger.Kind != TriggerKind.Unconditional) w.Line("hit = True");
                                    }
                                }

                                if (otherwise.Count > 0)
                                {
                                    using (w.Block("if not hit:"))
                                    {
                                        foreach (var trigger in otherwise) w.Line($"fired[{Py.Str(trigger.Target!.Name)}] = fired.get({Py.Str(trigger.Target.Name)}, 0) + 1");
                                    }
                                }
                            }
                        }
                    }

                    w.Line($"return seq.record_loop(s, {Py.Str(start.Name)}, iterations).as_dict()");
                }

                w.Line();
                w.Line($"{node.Var} = {node.Function}()");
            }

            private string LoopBound(string? text, string fallback, Activity loop)
            {
                if (string.IsNullOrWhiteSpace(text)) return fallback;
                return Expression(text!, loop, "loop bound", TranslationUse.Raw, true);
            }

            private static bool HasResult(Activity a)
            {
                switch (a.Kind)
                {
                    case ActivityKind.Job:
                    case ActivityKind.Routine:
                    case ActivityKind.ExecCommand:
                    case ActivityKind.Notification:
                    case ActivityKind.WaitForFile:
                    case ActivityKind.UserVariables:
                        return true;
                    default:
                        return false;
                }
            }

            /// <summary>Statements that run the activity and bind its ActivityResult to <paramref name="r"/>.</summary>
            private void EmitRun(PythonWriter w, Activity a, string r, bool failOnError, string onWarning)
            {
                switch (a.Kind)
                {
                    case ActivityKind.Job:
                        EmitJob(w, a, r, failOnError, onWarning);
                        break;

                    case ActivityKind.Routine:
                        if (a.RoutineName == null)
                        {
                            Issue(a, "SEQ020", "routine activity without a routine name", true);
                            w.Line($"{r} = seq.not_converted(s, {Py.Str(a.Name)}, \"no routine name\")");
                            break;
                        }

                        _usesRoutines = true;
                        if (_owner._project.FindRoutine(a.RoutineName.StartsWith("DSU.", StringComparison.OrdinalIgnoreCase) ? a.RoutineName.Substring(4) : a.RoutineName) == null)
                        {
                            Note(a, $"routine {a.RoutineName} is not in the export; a stub is generated in routines.py");
                        }

                        var args = a.Parameters.Select(p => Expression(p.Expression, a, "argument " + p.Name, TranslationUse.Raw, true));
                        w.Line($"{r} = seq.run_routine(s, {Py.Str(a.Name)}, {Py.Str(RoutineNames.Function(a.RoutineName))}, [{string.Join(", ", args)}], ROUTINES{(failOnError ? string.Empty : ", fail_on_error=False")})");
                        break;

                    case ActivityKind.ExecCommand:
                        var arguments = a.CommandArguments == null ? "\"\"" : Text(a.CommandArguments, a, "parameters");
                        w.Line($"{r} = seq.run_command(s, {Py.Str(a.Name)}, {Text(a.Command, a, "command")}, {arguments}{(failOnError ? string.Empty : ", fail_on_error=False")})");
                        break;

                    case ActivityKind.Notification:
                        var mail = new List<string> { "s", Py.Str(a.Name), Text(a.Recipients, a, "recipients") };
                        if (a.Subject != null) mail.Add("subject=" + Text(a.Subject, a, "subject"));
                        if (a.Body != null) mail.Add("body=" + Text(a.Body, a, "body"));
                        if (a.Sender != null) mail.Add("sender=" + Text(a.Sender, a, "sender"));
                        if (a.SmtpServer != null) mail.Add("smtp_server=" + Text(a.SmtpServer, a, "SMTP server"));
                        if (a.Attachments != null) mail.Add("attachments=" + Text(a.Attachments, a, "attachments"));
                        if (a.IncludeJobStatus) mail.Add("include_status=True");
                        w.Call(r, "seq.send_notification", mail);
                        break;

                    case ActivityKind.WaitForFile:
                        w.Line($"{r} = seq.wait_for_file(s, {Py.Str(a.Name)}, {Text(a.FileName, a, "file name")}, appear={Py.Bool(!a.WaitForDisappearance)}, timeout={(a.Timeout == null ? "None" : TimeoutSeconds(a.Timeout).ToString(CultureInfo.InvariantCulture))})");
                        break;

                    case ActivityKind.UserVariables:
                        w.Line("values = {}");
                        _resolver.CurrentUserVariables = a;
                        _resolver.LocalVariables.Clear();
                        foreach (var variable in a.Variables)
                        {
                            w.Line($"values[{Py.Str(variable.Name)}] = {Expression(variable.Expression, a, "user variable " + variable.Name, TranslationUse.Value, true)}");
                            _resolver.LocalVariables.Add(variable.Name);
                        }

                        _resolver.CurrentUserVariables = null;
                        w.Line($"{r} = seq.record_variables(s, {Py.Str(a.Name)}, values)");
                        break;

                    case ActivityKind.Terminator:
                        w.Line($"seq.terminate(s, {Py.Str(a.Name)})");
                        w.Line($"{r} = None");
                        break;

                    case ActivityKind.Unknown:
                        Issue(a, "SEQ021", $"activity type {a.OleType} is not converted", true);
                        w.Line($"{r} = seq.not_converted(s, {Py.Str(a.Name)}, {Py.Str("activity type " + a.OleType)})");
                        break;

                    default:
                        w.Line($"{r} = None");
                        break;
                }
            }

            private void EmitJob(PythonWriter w, Activity a, string r, bool failOnError, string onWarning)
            {
                if (a.JobName == null)
                {
                    Issue(a, "SEQ022", "job activity without a job name", true);
                    w.Line($"{r} = seq.not_converted(s, {Py.Str(a.Name)}, \"no job name\")");
                    return;
                }

                var args = new List<string> { "s", Py.Str(a.Name), Py.Str(a.JobName), "params=" + ParameterDict(a) };
                var module = ModuleOf(a.JobName);
                if (module != null)
                {
                    args.Add("module=" + Py.Str(module));
                }
                else
                {
                    args.Add("dsjob=DSJOB");
                    if (_owner._project.FindJob(a.JobName) == null) Note(a, $"job {a.JobName} is not in the export; it runs in DataStage");
                    else if (ChildSequence(a) != null) Note(a, $"sequence {a.JobName} runs in DataStage from inside a loop");
                }

                if (a.InvocationId != null) args.Add("invocation_id=" + Expression(a.InvocationId, a, "invocation id", TranslationUse.Raw, true));
                switch (a.ExecutionAction)
                {
                    case ExecutionAction.ResetIfRequiredThenRun:
                        args.Add("action=\"reset_then_run\"");
                        break;
                    case ExecutionAction.ValidateOnly:
                        args.Add("action=\"validate\"");
                        break;
                    case ExecutionAction.ResetOnly:
                        args.Add("action=\"reset\"");
                        break;
                }

                if (!failOnError) args.Add("fail_on_error=False");
                if (onWarning != "ok") args.Add("on_warning=" + Py.Str(onWarning));
                if (NeedsUserStatus(a)) args.Add("need_user_status=True");
                w.Call(r, "seq.run_job", args);
            }

            private string ParameterDict(Activity a)
            {
                var entries = new List<string>();
                foreach (var p in a.Parameters)
                {
                    if (string.IsNullOrWhiteSpace(p.Expression)) continue;
                    entries.Add($"{Py.Str(p.Name)}: {Expression(p.Expression, a, "parameter " + p.Name, TranslationUse.Value, true)}");
                }

                if (entries.Count == 0) return "{}";
                return "{\n    " + string.Join(",\n    ", entries) + ",\n}";
            }

            private bool NeedsUserStatus(Activity a)
            {
                if (a.Outgoing.Any(t => t.Kind == TriggerKind.UserStatus)) return true;
                var reference = a.Name + ".$UserStatus";
                return _sequence.Triggers.Any(t => (t.Expression ?? string.Empty).IndexOf(reference, StringComparison.OrdinalIgnoreCase) >= 0)
                    || _sequence.Activities.SelectMany(x => x.Parameters.Concat(x.Variables)).Any(p => p.Expression.IndexOf(reference, StringComparison.OrdinalIgnoreCase) >= 0);
            }

            private void EmitRouting(PythonWriter w, Activity a, string r)
            {
                w.Line("targets = []");
                var triggers = a.Outgoing.Where(t => t.Target != null).ToList();
                foreach (var trigger in triggers.Where(t => t.Kind != TriggerKind.Otherwise && t.Kind != TriggerKind.Unconditional))
                {
                    w.Comment($"trigger {trigger.Name}: {TriggerLabel(trigger)}");
                    using (w.Block($"if {Condition(trigger, a, r)}:"))
                    {
                        w.Line($"targets.append({Py.Str(BranchTarget(a, trigger.Target!))})");
                    }
                }

                var otherwise = triggers.Where(t => t.Kind == TriggerKind.Otherwise).ToList();
                if (otherwise.Count > 0)
                {
                    using (w.Block("if not targets:"))
                    {
                        foreach (var trigger in otherwise)
                        {
                            w.Comment($"trigger {trigger.Name}: otherwise");
                            w.Line($"targets.append({Py.Str(BranchTarget(a, trigger.Target!))})");
                        }
                    }
                }

                foreach (var trigger in triggers.Where(t => t.Kind == TriggerKind.Unconditional))
                {
                    w.Comment($"trigger {trigger.Name}: unconditional");
                    w.Line($"targets.append({Py.Str(BranchTarget(a, trigger.Target!))})");
                }

                // The exception handler stays selected so that only its trigger rule decides whether it runs.
                var node = _nodeOf[a];
                var always = _watchers.Where(pair => pair.Value.Contains(node)).Select(pair => pair.Key.TaskId).ToList();
                var alwaysArgument = always.Count > 0 ? ", always=" + Py.List(always) : string.Empty;
                w.Line($"return seq.route(s, {(HasResult(a) ? r : "None")}, targets{alwaysArgument})");
            }

            private string BranchTarget(Activity source, Activity target)
            {
                var from = _nodeOf[source];
                var to = _nodeOf[target];
                return _junctions.TryGetValue((from, to), out var junction) ? junction : EntryTaskId(target);
            }

            private string EntryTaskId(Activity activity)
            {
                var node = _nodeOf[activity];
                return node.Kind == NodeKind.ChildDag ? node.TaskId + "__params" : node.TaskId;
            }

            private static string TriggerLabel(Trigger trigger) =>
                trigger.Kind + (trigger.Expression == null ? string.Empty : " " + Py.Short(trigger.Expression, 70));

            /// <summary>Python condition for a trigger evaluated against the activity result <paramref name="r"/>.</summary>
            private string Condition(Trigger trigger, Activity a, string r)
            {
                // The report previews branch conditions before the DAG is rendered; translating a trigger
                // twice would record its issues and notes twice.
                if (_conditions.TryGetValue((trigger, r), out var cached)) return cached;
                var condition = TranslateTrigger(trigger, a, r);
                _conditions[(trigger, r)] = condition;
                return condition;
            }

            private string TranslateTrigger(Trigger trigger, Activity a, string r)
            {
                bool result = HasResult(a);
                switch (trigger.Kind)
                {
                    case TriggerKind.Unconditional:
                        return "True";
                    case TriggerKind.Ok:
                        if (!result) return "True";
                        return Options.WarningsAreOk && a.Kind == ActivityKind.Job ? $"({r}.ok or {r}.warning)" : r + ".ok";
                    case TriggerKind.Failed:
                        return result ? r + ".failed" : "False";
                    case TriggerKind.Warning:
                        return result ? r + ".warning" : "False";
                    case TriggerKind.UserStatus:
                        return $"F.basic_eq({r}.user_status, {Expression(trigger.Expression ?? "\"\"", a, $"trigger {trigger.Name}", TranslationUse.Raw, true)})";
                    case TriggerKind.ReturnValue:
                        return $"F.basic_eq({r}.return_value, {Expression(trigger.Expression ?? "0", a, $"trigger {trigger.Name}", TranslationUse.Raw, true)})";
                    case TriggerKind.Custom:
                        return Expression(trigger.Expression ?? "0", a, $"trigger {trigger.Name}", TranslationUse.Condition, false);
                    default:
                        return "False";
                }
            }

            private void EmitDependencies(PythonWriter w, List<(Node From, Node To, Trigger Trigger)> edges)
            {
                var lines = new List<(string From, string To)>();
                void Add(string from, string to)
                {
                    if (!lines.Contains((from, to))) lines.Add((from, to));
                }

                foreach (var edge in edges)
                {
                    if (_junctions.TryGetValue((edge.From, edge.To), out var junction))
                    {
                        Add(edge.From.Var, JunctionVar(junction));
                        Add(JunctionVar(junction), edge.To.EntryVar);
                    }
                    else
                    {
                        Add(edge.From.Var, edge.To.EntryVar);
                    }
                }

                foreach (var group in lines.GroupBy(l => l.From))
                {
                    var targets = group.Select(l => l.To).ToList();
                    w.Line($"{group.Key} >> {(targets.Count == 1 ? targets[0] : "[" + string.Join(", ", targets) + "]")}");
                }

                foreach (var pair in _watchers)
                {
                    if (pair.Value.Count == 0) continue;
                    w.Comment($"{pair.Key.Activity.Name} runs when any of these tasks fails.");
                    w.Line($"[{string.Join(", ", pair.Value.Select(n => n.Var))}] >> {pair.Key.EntryVar}");
                }
            }

            // ------------------------------------------------------------------ expressions and text

            /// <summary>A text property (file name, command, e-mail fields): literal text with #Param# references.</summary>
            private static string Text(string? text, Activity a, string what) =>
                text == null ? "\"\"" : text.IndexOf('#') >= 0 ? $"s.expand({Py.Str(text)})" : Py.Str(text);

            private string Expression(string text, Activity a, string what, TranslationUse use, bool textFallback)
            {
                var translator = new PythonTranslator(_resolver, ExpressionDialect.Basic);
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

                if (result.Ok)
                {
                    foreach (var note in result.Notes)
                    {
                        _usesRoutines = true;
                        Note(a, $"{what}: {note}");
                    }

                    return result.Code;
                }

                bool onlyText = result.Problems.All(p => p.StartsWith("syntax error", StringComparison.Ordinal) || p.StartsWith("unknown name", StringComparison.Ordinal));
                if (textFallback && onlyText)
                {
                    Issue(a, "SEQ030", $"{what}: '{Py.Short(text, 60)}' is not a valid expression; it is used as text", false);
                    return $"s.expand({Py.Str(text.Trim())})";
                }

                foreach (var problem in result.Problems) Issue(a, "SEQ031", $"{what}: {problem}", true);
                return result.Code;
            }

            // ------------------------------------------------------------------ report

            private ActivityReport ActivityReportFor(Activity a)
            {
                if (_activityReports.TryGetValue(a, out var existing)) return existing;
                var node = _nodeOf.TryGetValue(a, out var n) ? n : null;
                var report = new ActivityReport(a.Name, a.Kind, node?.TaskId ?? string.Empty) { Detail = Describe(a) };
                foreach (var pair in a.UnmappedProperties) report.UnmappedProperties.Add($"{pair.Key}={Py.Short(pair.Value, 60)}");
                _activityReports[a] = report;
                Report.Activities.Add(report);
                return report;
            }

            private void ReportNode(Node node)
            {
                var implementation = node.Kind switch
                {
                    NodeKind.Branch => "branch task (@task.branch)",
                    NodeKind.Sensor => "sensor (@task.sensor)",
                    NodeKind.Empty => "EmptyOperator" + (node.TriggerRule != null ? $" ({node.TriggerRule})" : string.Empty),
                    NodeKind.Loop => "one task running the loop body",
                    NodeKind.ChildDag => $"TriggerDagRunOperator -> {node.ChildDag}",
                    _ => "task (@task)",
                };
                ActivityReportFor(node.Activity).Implementation = implementation;
                if (node.Kind == NodeKind.Loop)
                {
                    ActivityReportFor(node.LoopEnd!).Implementation = "part of loop task " + node.TaskId;
                    foreach (var member in node.Body) ActivityReportFor(member).Implementation = "inside loop task " + node.TaskId;
                }
            }

            private void ReportTrigger(Trigger trigger)
            {
                var report = new TriggerReport(trigger.Name, trigger.Source.Name, trigger.Target?.Name ?? "?", trigger.Kind)
                {
                    RawType = trigger.RawType,
                    RawExpression = trigger.RawExpression,
                    Inferred = trigger.KindFromCodeOnly,
                };
                if (trigger.Target != null && _nodeOf.TryGetValue(trigger.Source, out var from))
                {
                    if (from.Kind == NodeKind.Loop && ReferenceEquals(from, _nodeOf[trigger.Target]))
                    {
                        report.Condition = "inside loop task " + from.TaskId;
                    }
                    else if (from.Kind == NodeKind.Branch)
                    {
                        report.Condition = trigger.Kind == TriggerKind.Otherwise ? "not targets" : trigger.Kind == TriggerKind.Unconditional ? "True" : ConditionPreview(trigger);
                    }
                }

                if (trigger.KindFromCodeOnly)
                {
                    Report.Issues.Add(new Issue(Severity.Warning, "SEQ004", $"trigger {trigger.Name}: type {trigger.Kind} inferred from code {trigger.RawType}; verify it", $"sequence {_job.Name}", false));
                }

                Report.Triggers.Add(report);
            }

            private string ConditionPreview(Trigger trigger)
            {
                try
                {
                    return Condition(trigger, trigger.Source, "r");
                }
                catch (Exception)
                {
                    return trigger.Expression ?? string.Empty;
                }
            }

            private void Issue(Activity a, string code, string message, bool blocking) =>
                Report.Issues.Add(new Issue(blocking ? Severity.Error : Severity.Warning, code, message, $"sequence {_job.Name}, activity {a.Name}", blocking));

            private void Note(Activity a, string message) => ActivityReportFor(a).Notes.Add(message);

            // ------------------------------------------------------------------ resolver

            private sealed class SequenceResolver : INameResolver
            {
                private static readonly HashSet<string> Macros = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
                {
                    "DSJobName", "DSProjectName", "DSJobInvocationId", "DSJobController", "DSHostName",
                    "DSJobStartDate", "DSJobStartTime", "DSJobStartTimestamp", "DSJobWaveNo",
                };

                private readonly Builder _builder;

                public SequenceResolver(Builder builder)
                {
                    _builder = builder;
                }

                public Activity? CurrentUserVariables { get; set; }

                public HashSet<string> LocalVariables { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                public ResolvedName? Resolve(string name)
                {
                    int dot = name.IndexOf('.');
                    if (dot > 0)
                    {
                        var activity = _builder._sequence.Find(name.Substring(0, dot));
                        if (activity != null) return ActivityValue(activity, name.Substring(dot + 1));
                    }

                    return ResolveParameterReference(name);
                }

                public ResolvedName? ResolveParameterReference(string name)
                {
                    var parameter = _builder._job.FindParameter(name);
                    if (parameter != null) return new ResolvedName($"s.param({Py.Str(parameter.Name)})", DsLogicalType.String, false);
                    if (name.StartsWith("$", StringComparison.Ordinal)) return new ResolvedName($"s.param({Py.Str(name)})", DsLogicalType.String, false);
                    if (Macros.Contains(name)) return new ResolvedName($"s.macro({Py.Str(name)})", DsLogicalType.String, false);
                    return null;
                }

                public string? ResolveRoutine(string name)
                {
                    var bare = name.StartsWith("DSU.", StringComparison.OrdinalIgnoreCase) ? name.Substring(4) : name;
                    var routine = _builder._owner._project.FindRoutine(bare);
                    if (routine == null) return null;
                    return $"seq.routine(ROUTINES, {Py.Str(RoutineNames.Function(routine.Name))})";
                }

                private ResolvedName? ActivityValue(Activity activity, string member)
                {
                    var act = $"s.act({Py.Str(activity.Name)})";
                    switch (member.ToUpperInvariant())
                    {
                        case "$JOBSTATUS":
                            return new ResolvedName(act + ".job_status", DsLogicalType.Integer, true);
                        case "$USERSTATUS":
                            return new ResolvedName(act + ".user_status", DsLogicalType.String, true);
                        case "$RETURNVALUE":
                            return new ResolvedName(act + ".return_value", DsLogicalType.Unknown, true);
                        case "$COMMANDOUTPUT":
                            return new ResolvedName(act + ".command_output", DsLogicalType.String, true);
                        case "$JOBNAME":
                            return new ResolvedName(act + ".job_name", DsLogicalType.String, true);
                        case "$COUNTER":
                            return new ResolvedName($"s.counter({Py.Str(activity.Name)})", DsLogicalType.Unknown, true);
                        case "$ERRSOURCE":
                        case "$ERRNUMBER":
                        case "$ERRMESSAGE":
                            return new ResolvedName($"s.error({Py.Str(member.Substring(1))})", DsLogicalType.String, true);
                    }

                    if (activity.Kind == ActivityKind.UserVariables)
                    {
                        if (ReferenceEquals(activity, CurrentUserVariables) && LocalVariables.Contains(member))
                        {
                            return new ResolvedName($"values[{Py.Str(member)}]", DsLogicalType.String, true);
                        }

                        return new ResolvedName($"s.user_var({Py.Str(activity.Name)}, {Py.Str(member)})", DsLogicalType.String, true);
                    }

                    return null;
                }
            }
        }
    }
}
