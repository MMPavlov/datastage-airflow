using System;
using System.Collections.Generic;
using System.Linq;
using DataStage2Airflow.Dsx;
using DataStage2Airflow.Internal;

namespace DataStage2Airflow.Model
{
    /// <summary>Turns raw export records into the project / job / stage / link / sequence model.</summary>
    public sealed class ModelBuilder
    {
        // Canvas layout and bookkeeping properties that carry no behaviour.
        private static readonly HashSet<string> LayoutProperties = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "Identifier", "OLEType", "Readonly", "Name", "NextID", "InputPins", "OutputPins", "StageType",
            "AllowColumnMapping", "NextRecordID", "LeftTextPos", "TopTextPos", "ValidationStatus", "Description",
            "FullDescription", "ShowBorder?", "Partner",
        };

        private readonly DiagnosticBag _diagnostics;

        public ModelBuilder(DiagnosticBag diagnostics)
        {
            _diagnostics = diagnostics;
        }

        public DsProject Build(IEnumerable<DsxDocument> documents, string? projectName = null)
        {
            DsProject? project = null;
            foreach (var document in documents)
            {
                if (project == null) project = new DsProject(projectName ?? document.ProjectName ?? "datastage");
                if (project.ServerVersion == null) project.ServerVersion = document.ServerVersion;
                if (project.ExportingTool == null) project.ExportingTool = document.ExportingTool;
                project.SourceFiles.Add(document.SourceName);

                foreach (var obj in document.Objects)
                {
                    AddObject(project, obj, document.SourceName);
                }
            }

            return project ?? new DsProject(projectName ?? "datastage");
        }

        private void AddObject(DsProject project, DsxObject obj, string source)
        {
            switch (obj.Section)
            {
                case "DSJOB":
                    AddUnique(project.Jobs, BuildJob(obj, source, false));
                    break;
                case "DSSHAREDCONTAINER":
                    AddUnique(project.SharedContainers, BuildJob(obj, source, true));
                    break;
                case "DSROUTINES":
                    foreach (var record in obj.Records)
                    {
                        var routine = BuildRoutine(record);
                        if (routine != null) project.Routines.Add(routine);
                    }

                    break;
                case "DSTABLEDEFS":
                case "DSSTAGETYPES":
                case "DSDATAELEMENTS":
                case "DSTRANSFORMS":
                case "DSEXECJOB":
                    break;
                default:
                    _diagnostics.Info("MOD001", $"export section {obj.Section} is not used by the migrator", $"{source}:{obj.Line}");
                    break;
            }
        }

        private void AddUnique(List<DsJob> list, DsJob job)
        {
            int existing = list.FindIndex(j => string.Equals(j.Name, job.Name, StringComparison.OrdinalIgnoreCase));
            if (existing < 0)
            {
                list.Add(job);
                return;
            }

            _diagnostics.Warning("MOD002", $"job '{job.Name}' appears more than once; the copy from {job.SourceFile} is used", job.SourceFile);
            list[existing] = job;
        }

        public DsJob BuildJob(DsxObject obj, string sourceFile, bool isContainer)
        {
            var root = obj.Root;
            var name = root?.Properties.Get("Name") ?? obj.Identifier ?? "<unnamed>";
            var job = new DsJob(name) { SourceFile = sourceFile, IsSharedContainer = isContainer };
            var location = $"job {name}";
            job.DateModified = obj.Properties.Get("DateModified");

            if (root == null)
            {
                _diagnostics.Warning("MOD003", "no ROOT (CJobDefn) record; job properties and parameters are missing", location);
            }
            else
            {
                job.RawJobType = root["JobType"] ?? string.Empty;
                job.Kind = ParseJobKind(job.RawJobType);
                job.Category = root["Category"] ?? string.Empty;
                job.Description = root["Description"] ?? string.Empty;
                job.FullDescription = root["FullDescription"] ?? string.Empty;
                job.JobControlCode = Text.NullIfBlank(root["JobControlCode"]);
                job.AllowMultipleInvocations = root["AllowMultipleInvocations"] == "1";
                job.BeforeSubroutine = Text.NullIfBlank(root.Properties.GetAny("BeforeSubr", "BeforeSubroutine"));
                job.BeforeSubroutineInput = Text.NullIfBlank(root.Properties.GetAny("BeforeSubrArg", "BeforeSubrArgs", "BeforeSubrInput", "BeforeSubroutineInput"));
                job.AfterSubroutine = Text.NullIfBlank(root.Properties.GetAny("AfterSubr", "AfterSubroutine"));
                job.AfterSubroutineInput = Text.NullIfBlank(root.Properties.GetAny("AfterSubrArg", "AfterSubrArgs", "AfterSubrInput", "AfterSubroutineInput"));

                foreach (var sub in root.SubRecords("Parameters"))
                {
                    job.Parameters.Add(BuildParameter(sub));
                }
            }

            var records = IndexRecords(obj, location);
            var stageRecords = FindStageRecords(obj, records, out var viewOf);

            if (job.Kind == JobKind.Unknown) job.Kind = GuessJobKind(stageRecords, isContainer);

            if (job.Kind == JobKind.Sequence)
            {
                job.Sequence = BuildSequence(root, stageRecords, records, location);
                return job;
            }

            foreach (var record in stageRecords)
            {
                var stageType = Text.NullIfBlank(record["StageType"]) ?? record.OleType;
                var stage = new Stage(record.Identifier, record.Name, record.OleType, stageType, record);
                if (viewOf.TryGetValue(record.Identifier, out var view)) stage.ViewId = view;

                foreach (var sub in record.SubRecords("StageVars"))
                {
                    var variable = new StageVariable(sub.Get("Name") ?? string.Empty, sub.GetAny("Expression", "ParsedExpression") ?? string.Empty)
                    {
                        InitialValue = sub.Get("InitialValue"),
                        SqlType = Text.ParseInt(sub["SqlType"]),
                        Precision = Text.ParseInt(sub["Precision"]),
                        Scale = Text.ParseInt(sub.GetAny("ColScale", "Scale")),
                    };
                    stage.StageVariables.Add(variable);
                }

                if (record.Collection("LoopVars") != null && record.SubRecords("LoopVars").Count > 0)
                {
                    _diagnostics.Warning("MOD004", $"transformer loop variables in stage '{stage.Name}' are not converted", location);
                }

                job.Stages.Add(stage);
            }

            BuildLinks(job, records, location);
            return job;
        }

        private Dictionary<string, DsxRecord> IndexRecords(DsxObject obj, string location)
        {
            var records = new Dictionary<string, DsxRecord>(StringComparer.Ordinal);
            foreach (var record in obj.Records)
            {
                if (record.Identifier.Length == 0) continue;
                if (records.ContainsKey(record.Identifier))
                {
                    _diagnostics.Warning("MOD005", $"duplicate record identifier '{record.Identifier}'", location);
                    continue;
                }

                records[record.Identifier] = record;
            }

            return records;
        }

        /// <summary>Stage (or activity) records, in canvas order: pins, views, annotations and the job record are excluded.</summary>
        private static List<DsxRecord> FindStageRecords(DsxObject obj, Dictionary<string, DsxRecord> records, out Dictionary<string, string> viewOf)
        {
            viewOf = new Dictionary<string, string>(StringComparer.Ordinal);
            var pins = new HashSet<string>(StringComparer.Ordinal);
            foreach (var record in obj.Records)
            {
                foreach (var id in Text.SplitList(record["InputPins"])) pins.Add(id);
                foreach (var id in Text.SplitList(record["OutputPins"])) pins.Add(id);
            }

            var ordered = new List<DsxRecord>();
            var seen = new HashSet<string>(StringComparer.Ordinal);
            foreach (var view in obj.Records.Where(r => r.OleType == "CContainerView"))
            {
                foreach (var id in Text.SplitList(view["StageList"]))
                {
                    if (!viewOf.ContainsKey(id)) viewOf[id] = view.Identifier;
                    if (records.TryGetValue(id, out var record) && IsStageRecord(record, pins) && seen.Add(id)) ordered.Add(record);
                }
            }

            foreach (var record in obj.Records)
            {
                if (IsStageRecord(record, pins) && seen.Add(record.Identifier)) ordered.Add(record);
            }

            return ordered;
        }

        private static bool IsStageRecord(DsxRecord record, HashSet<string> pins)
        {
            var ole = record.OleType;
            if (ole == "CJobDefn" || ole == "CContainerView") return false;
            if (Text.ContainsIgnoreCase(ole, "Annotation") || ole.StartsWith("ID_PALETTE", StringComparison.Ordinal)) return false;
            if (pins.Contains(record.Identifier)) return false;
            return record.Properties.Has("InputPins") || record.Properties.Has("OutputPins") || record.Properties.Has("StageType")
                || ole.StartsWith("CJS", StringComparison.Ordinal) || ole.EndsWith("Stage", StringComparison.Ordinal);
        }

        private static JobKind ParseJobKind(string code)
        {
            switch (code.Trim())
            {
                case "0": return JobKind.Server;
                case "1": return JobKind.Mainframe;
                case "2": return JobKind.Sequence;
                case "3": return JobKind.Parallel;
                default: return JobKind.Unknown;
            }
        }

        private static JobKind GuessJobKind(List<DsxRecord> stages, bool isContainer)
        {
            if (stages.Any(s => s.OleType.StartsWith("CJS", StringComparison.Ordinal))) return JobKind.Sequence;
            if (stages.Any(s => (s["StageType"] ?? string.Empty).StartsWith("Px", StringComparison.Ordinal))) return JobKind.Parallel;
            return isContainer ? JobKind.Parallel : JobKind.Server;
        }

        private static JobParameter BuildParameter(DsxPropertyBag sub)
        {
            var parameter = new JobParameter(sub.Get("Name") ?? string.Empty)
            {
                Prompt = sub["Prompt"] ?? string.Empty,
                DefaultValue = sub.GetAny("Default", "DefaultValue") ?? string.Empty,
                RawType = sub["ParamType"] ?? "0",
                HelpText = sub.GetAny("HelpTxt", "HelpText") ?? string.Empty,
            };
            parameter.Type = JobParameter.ParseType(parameter.RawType);

            var list = sub.GetAny("ListValues", "ParamListValues", "ValidValues");
            if (!string.IsNullOrEmpty(list))
            {
                char separator = list!.IndexOf('|') >= 0 ? '|' : '\n';
                parameter.ListValues.AddRange(Text.SplitList(list, separator));
            }

            return parameter;
        }

        private void BuildLinks(DsJob job, Dictionary<string, DsxRecord> records, string location)
        {
            var stagesById = job.Stages.ToDictionary(s => s.Id, StringComparer.Ordinal);
            var linkedInputPins = new HashSet<string>(StringComparer.Ordinal);

            foreach (var stage in job.Stages)
            {
                foreach (var pinId in Text.SplitList(stage.Record["OutputPins"]))
                {
                    if (!records.TryGetValue(pinId, out var pin))
                    {
                        _diagnostics.Warning("MOD006", $"stage '{stage.Name}' lists output pin {pinId}, which is not in the export", location);
                        continue;
                    }

                    var partner = Text.SplitList(pin["Partner"]);
                    Stage? target = null;
                    DsxRecord? targetPin = null;
                    if (partner.Count > 0) stagesById.TryGetValue(partner[0], out target);
                    if (partner.Count > 1) records.TryGetValue(partner[1], out targetPin);
                    if (target == null)
                    {
                        _diagnostics.Warning("MOD007", $"link '{pin.Name}' from stage '{stage.Name}' has no target stage", location);
                    }

                    var link = new Link(pin.Name, stage, target, pin, targetPin);
                    FillLink(link);
                    job.Links.Add(link);
                    stage.Outputs.Add(link);
                    if (target != null) target.Inputs.Add(link);
                    if (targetPin != null) linkedInputPins.Add(targetPin.Identifier);
                }
            }

            foreach (var stage in job.Stages)
            {
                var order = Text.SplitList(stage.Record["InputPins"]);
                var sorted = stage.Inputs.OrderBy(l => IndexOrLast(order, l.TargetPin?.Identifier)).ToList();
                stage.Inputs.Clear();
                stage.Inputs.AddRange(sorted);

                foreach (var pinId in order)
                {
                    if (!linkedInputPins.Contains(pinId))
                    {
                        _diagnostics.Warning("MOD008", $"input pin {pinId} of stage '{stage.Name}' is not connected to any output", location);
                    }
                }
            }
        }

        private static int IndexOrLast(List<string> order, string? id)
        {
            if (id == null) return int.MaxValue;
            int index = order.IndexOf(id);
            return index < 0 ? int.MaxValue : index;
        }

        private static void FillLink(Link link)
        {
            var pin = link.SourcePin;
            foreach (var bag in ColumnBags(pin)) link.Columns.Add(BuildColumn(bag));
            if (link.Columns.Count == 0 && link.TargetPin != null)
            {
                foreach (var bag in ColumnBags(link.TargetPin)) link.Columns.Add(BuildColumn(bag));
            }

            link.Constraint = Text.NullIfBlank(pin.Properties.GetAny("Constraint", "ParsedConstraint"));
            link.IsOtherwise = pin["Reject"] == "1";
            link.IsError = pin["ErrorPin"] == "1";
            link.RowLimit = Text.ParseInt(pin["RowLimit"]);

            var targetPin = link.TargetPin;
            if (targetPin != null)
            {
                foreach (var bag in ColumnBags(targetPin)) link.TargetColumns.Add(BuildColumn(bag));
                link.PinLinkType = Text.ParseInt(targetPin["LinkType"]);
                link.LookupFailure = Text.NullIfBlank(targetPin.Properties.GetAny("LookupFail", "LookupFailure"));
                link.ConditionNotMet = Text.NullIfBlank(targetPin["ConditionNotMet"]);
                link.LookupCondition = Text.NullIfBlank(targetPin.Properties.GetAny("LookupCondition", "Condition", "ParsedCondition"));
            }

            if (link.PinLinkType == 2) link.Kind = LinkKind.Reference;
            else if (link.IsError) link.Kind = LinkKind.Reject;
            else link.Kind = LinkKind.Stream;
        }

        private static IEnumerable<DsxPropertyBag> ColumnBags(DsxRecord pin)
        {
            var columns = pin.SubRecords("Columns");
            if (columns.Count > 0) return columns;

            // Some exports put column subrecords straight after another collection; recognise them by shape.
            return pin.Collections
                .SelectMany(c => c.Items)
                .Where(item => item.Has("SqlType") && !item.Has("Owner"));
        }

        private static Column BuildColumn(DsxPropertyBag bag)
        {
            return new Column(bag.Get("Name") ?? string.Empty)
            {
                SqlType = Text.ParseInt(bag["SqlType"]),
                Precision = Text.ParseInt(bag["Precision"]),
                Scale = Text.ParseInt(bag.GetAny("Scale", "ColScale")),
                Nullable = bag["Nullable"] == "1",
                KeyPosition = Text.ParseInt(bag["KeyPosition"]),
                Derivation = Text.NullIfBlank(bag["Derivation"]),
                ParsedDerivation = Text.NullIfBlank(bag["ParsedDerivation"]),
                SourceColumn = Text.NullIfBlank(bag["SourceColumn"]),
                Description = bag["Description"] ?? string.Empty,
                Raw = bag,
            };
        }

        private static DsRoutine? BuildRoutine(DsxRecord record)
        {
            var name = record.Properties.Get("Name");
            if (name == null) return null;
            var routine = new DsRoutine(name, record.Identifier)
            {
                Category = record["Category"] ?? string.Empty,
                Description = record["Description"] ?? string.Empty,
                RoutineType = record.Properties.GetAny("RoutineType", "Type") ?? string.Empty,
                Source = record.Properties.GetAny("Source", "Code", "RoutineCode", "SourceCode") ?? string.Empty,
            };

            foreach (var arg in record.SubRecords("Arguments"))
            {
                var argName = arg.Get("Name");
                if (argName != null) routine.Arguments.Add(argName);
            }

            return routine;
        }

        // ---------------------------------------------------------------- sequences

        private SequenceDefinition BuildSequence(DsxRecord? root, List<DsxRecord> stageRecords, Dictionary<string, DsxRecord> records, string location)
        {
            var sequence = new SequenceDefinition();
            if (root != null)
            {
                sequence.AutoHandleFailures = Text.ParseBool(root.Properties.GetAny("AutoHandleFail", "AutoHandleFailures", "HandleActivityFailures", "SeqAutoHandleFailures"));
                sequence.Restartable = Text.ParseBool(root.Properties.GetAny("RestartSequence", "Restartable", "SeqRestartable", "EnableCheckpoints", "CheckpointRestart"));
            }

            var byId = new Dictionary<string, Activity>(StringComparer.Ordinal);
            foreach (var record in stageRecords)
            {
                var activity = new Activity(record.Identifier, record.Name, ClassifyActivity(record), record);
                ReadActivityProperties(activity);
                if (activity.Kind == ActivityKind.Unknown)
                {
                    _diagnostics.Warning("SEQ001", $"activity '{activity.Name}' has unrecognised type {record.OleType}/{record["StageType"]}", location);
                }

                sequence.Activities.Add(activity);
                byId[record.Identifier] = activity;
            }

            foreach (var activity in sequence.Activities)
            {
                foreach (var pinId in Text.SplitList(activity.Record["OutputPins"]))
                {
                    if (!records.TryGetValue(pinId, out var pin))
                    {
                        _diagnostics.Warning("SEQ002", $"activity '{activity.Name}' lists trigger pin {pinId}, which is not in the export", location);
                        continue;
                    }

                    var trigger = new Trigger(pin.Name, activity);
                    var partner = Text.SplitList(pin["Partner"]);
                    if (partner.Count > 0 && byId.TryGetValue(partner[0], out var target))
                    {
                        trigger.Target = target;
                        target.Incoming.Add(trigger);
                    }
                    else
                    {
                        _diagnostics.Warning("SEQ003", $"trigger '{pin.Name}' of activity '{activity.Name}' has no target activity", location);
                    }

                    ResolveTrigger(trigger, pin);
                    if (trigger.KindFromCodeOnly)
                    {
                        _diagnostics.Warning(
                            "SEQ004",
                            $"trigger '{trigger.Name}' of '{activity.Name}': type taken from code {trigger.RawType} as {trigger.Kind}; verify it",
                            location);
                    }

                    activity.Outgoing.Add(trigger);
                    sequence.Triggers.Add(trigger);
                }
            }

            return sequence;
        }

        public static ActivityKind ClassifyActivity(DsxRecord record)
        {
            var type = ((record["StageType"] ?? string.Empty) + " " + record.OleType).ToLowerInvariant();
            if (type.Contains("jobactivity")) return ActivityKind.Job;
            if (type.Contains("routine")) return ActivityKind.Routine;
            if (type.Contains("execcmd") || type.Contains("execcommand") || type.Contains("executecommand")) return ActivityKind.ExecCommand;
            if (type.Contains("mail") || type.Contains("notif")) return ActivityKind.Notification;
            if (type.Contains("waitforfile") || type.Contains("wff")) return ActivityKind.WaitForFile;
            if (type.Contains("startloop")) return ActivityKind.StartLoop;
            if (type.Contains("endloop")) return ActivityKind.EndLoop;
            if (type.Contains("uservar")) return ActivityKind.UserVariables;
            if (type.Contains("sequencer")) return ActivityKind.Sequencer;
            if (type.Contains("condition")) return ActivityKind.Condition;
            if (type.Contains("terminator")) return ActivityKind.Terminator;
            if (type.Contains("exception")) return ActivityKind.ExceptionHandler;
            return ActivityKind.Unknown;
        }

        private static void ReadActivityProperties(Activity activity)
        {
            var record = activity.Record;
            var consumed = new HashSet<string>(LayoutProperties, StringComparer.OrdinalIgnoreCase);

            string? Take(params string[] names)
            {
                foreach (var name in names)
                {
                    if (record.Collection(name) != null) continue;
                    var value = record.Properties.Get(name);
                    if (value == null) continue;
                    consumed.Add(name);
                    return value;
                }

                return null;
            }

            void TakeExpressions(List<NamedExpression> target, params string[] collections)
            {
                foreach (var collectionName in collections)
                {
                    var collection = record.Collection(collectionName);
                    if (collection == null) continue;
                    consumed.Add(collectionName);
                    foreach (var item in collection.Items)
                    {
                        var name = item.GetAny("Name", "ParamName", "Parameter", "VarName", "ArgName");
                        var value = item.GetAny("ValueExpression", "Expression", "DisplayValue", "Value", "ParamValue", "ArgValue", "Default");
                        if (name != null) target.Add(new NamedExpression(name, value ?? string.Empty));
                    }

                    return;
                }
            }

            activity.Description = record["Description"] ?? string.Empty;

            switch (activity.Kind)
            {
                case ActivityKind.Job:
                    activity.JobName = Text.NullIfBlank(Take("Jobname", "JobName", "Job", "JobToRun"));
                    activity.InvocationId = Text.NullIfBlank(Take("InvocationId", "InvocationID", "InvocationIdExpr", "InvocationIdExpression", "JobInvocationId"));
                    activity.ExecutionAction = ParseExecutionAction(Take("ExecutionAction", "ExecAction", "RunMode", "Action"));
                    activity.DoNotCheckpoint = Text.ParseBool(Take("DoNotCheckpoint", "NoCheckpoint", "DontCheckpoint")) == true;
                    TakeExpressions(activity.Parameters, "ParamValues", "Parameters", "ParameterValues", "JobParameters", "Params");
                    break;

                case ActivityKind.Routine:
                    activity.RoutineName = Text.NullIfBlank(Take("RoutineName", "Routinename", "Routine", "RoutineToRun"));
                    TakeExpressions(activity.Parameters, "Arguments", "RoutineArguments", "ArgValues", "ParamValues", "Parameters");
                    break;

                case ActivityKind.ExecCommand:
                    activity.Command = Text.NullIfBlank(Take("Command", "CommandName", "ExecCommand", "CommandLine", "CmdName"));
                    activity.CommandArguments = Text.NullIfBlank(Take("Parameters", "CommandParameters", "CommandArgs", "Arguments", "Params"));
                    break;

                case ActivityKind.Notification:
                    activity.SmtpServer = Text.NullIfBlank(Take("SMTPServer", "SmtpServer", "SMTPMailServer", "MailServer", "Server"));
                    activity.Sender = Text.NullIfBlank(Take("SenderAddress", "Sender", "From", "SenderEmail"));
                    activity.Recipients = Text.NullIfBlank(Take("RecipientAddress", "Recipients", "Recipient", "To", "RecipientEmail"));
                    activity.Subject = Text.NullIfBlank(Take("Subject", "EmailSubject", "MailSubject"));
                    activity.Body = Text.NullIfBlank(Take("Body", "EmailBody", "MailBody", "MessageText", "Text"));
                    activity.Attachments = Text.NullIfBlank(Take("Attachments", "AttachmentFiles", "Attachment"));
                    activity.IncludeJobStatus = Text.ParseBool(Take("IncludeJobStatus", "IncludeStatus", "IncludeJobStatusInEmail")) == true;
                    break;

                case ActivityKind.WaitForFile:
                    activity.FileName = Text.NullIfBlank(Take("FileName", "Filename", "File", "WaitFile", "FilePath"));
                    var mode = (Take("WaitType", "Mode", "WaitMode", "WaitForFileMode", "WaitFor") ?? string.Empty).Trim().ToLowerInvariant();
                    activity.WaitForDisappearance = mode == "1" || mode.Contains("disappear")
                        || Text.ParseBool(Take("WaitForFileToDisappear", "Disappear")) == true;
                    activity.Timeout = Text.NullIfBlank(Take("Timeout", "TimeoutLength", "TimeOut"));
                    if (Text.ParseBool(Take("DoNotTimeout", "NoTimeout", "DontTimeout")) == true) activity.Timeout = null;
                    break;

                case ActivityKind.Sequencer:
                    var seqMode = (Take("SequencerMode", "Mode", "SeqMode", "Type") ?? string.Empty).Trim().ToLowerInvariant();
                    activity.SequencerAny = seqMode == "1" || seqMode == "any";
                    break;

                case ActivityKind.StartLoop:
                    var loopType = (Take("LoopType", "Type", "LoopMode") ?? string.Empty).Trim().ToLowerInvariant();
                    activity.LoopFrom = Text.NullIfBlank(Take("From", "LoopFrom", "Start", "StartValue"));
                    activity.LoopStep = Text.NullIfBlank(Take("Step", "LoopStep", "Increment"));
                    activity.LoopTo = Text.NullIfBlank(Take("To", "LoopTo", "End", "EndValue"));
                    activity.LoopValues = Take("Values", "LoopValues", "Delimited", "DelimitedValues", "List", "ListValues");
                    activity.LoopDelimiter = Take("Delimiter", "LoopDelimiter", "ListDelimiter", "Separator");
                    activity.IsListLoop = loopType == "1" || loopType.Contains("list") || (loopType.Length == 0 && activity.LoopValues != null);
                    break;

                case ActivityKind.UserVariables:
                    TakeExpressions(activity.Variables, "UserVariables", "Variables", "UserVars", "VarList", "ParamValues");
                    break;

                case ActivityKind.Terminator:
                    activity.SendStopRequests = Text.ParseBool(Take("SendStopRequests", "StopJobs", "SendStop")) ?? true;
                    break;
            }

            foreach (var property in record.Properties)
            {
                if (consumed.Contains(property.Key)) continue;
                if (record.Collection(property.Key) != null && record.Collection(property.Key)!.Items.Count > 0)
                {
                    activity.UnmappedProperties.Add(new KeyValuePair<string, string>(property.Key, $"<{record.Collection(property.Key)!.Items.Count} subrecords>"));
                    continue;
                }

                activity.UnmappedProperties.Add(property);
            }
        }

        private static ExecutionAction ParseExecutionAction(string? code)
        {
            var value = (code ?? string.Empty).Trim().ToLowerInvariant();
            if (value == "1" || value.Contains("reset if")) return ExecutionAction.ResetIfRequiredThenRun;
            if (value == "2" || value.Contains("validate")) return ExecutionAction.ValidateOnly;
            if (value == "3" || value == "reset" || value.Contains("reset only")) return ExecutionAction.ResetOnly;
            return ExecutionAction.Run;
        }

        /// <summary>
        /// Works out what a trigger means. The Designer stores a description for fixed trigger types
        /// ("Executed OK"), the expression for custom ones, and a type code whose numbering is not
        /// documented; text evidence therefore wins over the code.
        /// </summary>
        public static void ResolveTrigger(Trigger trigger, DsxRecord pin)
        {
            var rawType = (pin.Properties.GetAny("ConditionType", "TriggerType", "ExpressionType", "ConditionKind") ?? string.Empty).Trim();
            var rawExpression = pin.Properties.GetAny("Expression", "ExpressionText", "Condition", "CustomExpression", "TriggerExpression", "ParsedExpression");
            trigger.RawType = rawType;
            trigger.RawExpression = rawExpression;

            var text = (rawExpression ?? string.Empty).Trim();
            if (text.Length >= 2 && text[0] == '"' && text[text.Length - 1] == '"' && IsFixedDescription(text.Substring(1, text.Length - 2)))
            {
                text = text.Substring(1, text.Length - 2);
            }

            var named = KindFromName(rawType);
            var fromDescription = KindFromDescription(text);
            var fromCode = KindFromCode(rawType);
            bool blank = text.Length == 0 || string.Equals(text, "N/A", StringComparison.OrdinalIgnoreCase);

            if (named != null)
            {
                trigger.Kind = named.Value;
            }
            else if (fromDescription != null)
            {
                trigger.Kind = fromDescription.Value;
            }
            else if (blank)
            {
                if (fromCode == TriggerKind.Otherwise || Text.ContainsIgnoreCase(trigger.Name, "otherwise"))
                {
                    trigger.Kind = TriggerKind.Otherwise;
                }
                else if (fromCode != null && fromCode != TriggerKind.Custom && fromCode != TriggerKind.UserStatus && fromCode != TriggerKind.ReturnValue)
                {
                    trigger.Kind = fromCode.Value;
                    trigger.KindFromCodeOnly = fromCode != TriggerKind.Unconditional;
                }
                else
                {
                    trigger.Kind = TriggerKind.Unconditional;
                }
            }
            else
            {
                trigger.Kind = fromCode == TriggerKind.UserStatus || fromCode == TriggerKind.ReturnValue ? fromCode.Value : TriggerKind.Custom;
            }

            switch (trigger.Kind)
            {
                case TriggerKind.Custom:
                case TriggerKind.UserStatus:
                case TriggerKind.ReturnValue:
                    trigger.Expression = blank ? null : text;
                    break;
                default:
                    trigger.Expression = null;
                    break;
            }
        }

        private static bool IsFixedDescription(string text) => KindFromDescription(text) != null;

        private static TriggerKind? KindFromDescription(string text)
        {
            var t = text.Trim().ToLowerInvariant();
            if (t.Length == 0 || t.Length > 60) return null;
            // Real expressions reference activities or status constants; descriptions do not.
            if (t.Contains("$") || t.Contains("dsjs.") || t.Contains("=") || t.Contains("<") || t.Contains(">")) return null;
            bool execution = t.Contains("execut") || t.Contains("finish") || t.Contains("complet");
            if (!execution) return null;
            if (t.Contains("warn")) return TriggerKind.Warning;
            if (t.Contains("fail") || t.Contains("error") || t.Contains("abort")) return TriggerKind.Failed;
            if (t.Contains("ok") || t.Contains("success")) return TriggerKind.Ok;
            return null;
        }

        private static TriggerKind? KindFromName(string rawType)
        {
            switch (rawType.Trim().ToLowerInvariant().Replace(" ", string.Empty).Replace("-(conditional)", string.Empty))
            {
                case "unconditional": return TriggerKind.Unconditional;
                case "otherwise": return TriggerKind.Otherwise;
                case "ok": return TriggerKind.Ok;
                case "failed":
                case "failure": return TriggerKind.Failed;
                case "warning":
                case "warnings": return TriggerKind.Warning;
                case "custom": return TriggerKind.Custom;
                case "userstatus": return TriggerKind.UserStatus;
                case "returnvalue": return TriggerKind.ReturnValue;
                default: return null;
            }
        }

        // Numbering follows the order of the Designer's "Expression Type" list; unverified, so the
        // builder reports every trigger whose meaning rests on the code alone.
        private static TriggerKind? KindFromCode(string rawType)
        {
            switch (rawType.Trim())
            {
                case "0": return TriggerKind.Unconditional;
                case "1": return TriggerKind.Otherwise;
                case "2": return TriggerKind.Ok;
                case "3": return TriggerKind.Failed;
                case "4": return TriggerKind.Warning;
                case "5": return TriggerKind.Custom;
                case "6": return TriggerKind.UserStatus;
                case "7": return TriggerKind.ReturnValue;
                default: return null;
            }
        }
    }
}
