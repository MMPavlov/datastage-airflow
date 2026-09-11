using System.Linq;
using DataStage2Airflow.Model;
using Xunit;

namespace DataStage2Airflow.Tests
{
    public class ModelBuilderTests
    {
        private static readonly DsProject Project = Load();

        private static DsProject Load()
        {
            var diagnostics = new DiagnosticBag();
            return new Migrator(new MigrationOptions()).Load(new[] { TestPaths.SampleDsx }, diagnostics);
        }

        [Fact]
        public void Loads_every_sample_job_and_routine()
        {
            Assert.Equal("DWPROJ", Project.Name);
            Assert.Equal("7.5.1", Project.ServerVersion);
            Assert.Equal(7, Project.Jobs.Count);
            Assert.Equal(JobKind.Parallel, Project.FindJob("PX_Load_Customers")!.Kind);
            Assert.Equal(JobKind.Server, Project.FindJob("SRV_Load_Orders")!.Kind);
            Assert.Equal(JobKind.Sequence, Project.FindJob("SEQ_Daily_Load")!.Kind);
            Assert.Equal(2, Project.Routines.Count);
            Assert.Equal(new[] { "Prefix", "RunDate" }, Project.FindRoutine("FormatRunTag")!.Arguments);
        }

        [Fact]
        public void Parameters_keep_types_defaults_and_environment_variables()
        {
            var job = Project.FindJob("PX_Load_Customers")!;
            Assert.Equal("2005-06-15", job.FindParameter("RunDate")!.DefaultValue);
            Assert.Equal(ParameterType.Pathname, job.FindParameter("SourceDir")!.Type);
            Assert.True(job.FindParameter("$APT_CONFIG_FILE")!.IsEnvironmentVariable);
            Assert.Equal(ParameterType.Encrypted, Project.FindJob("PX_Extract_Accounts")!.FindParameter("DB_PASSWORD")!.Type);
        }

        [Fact]
        public void Stages_and_links_follow_the_pins()
        {
            var job = Project.FindJob("PX_Load_Customers")!;
            Assert.Equal(7, job.Stages.Count);
            var lookup = job.FindStage("lkpCountry")!;
            Assert.Equal(new[] { "lnkClean", "lnkCountries" }, lookup.Inputs.Select(l => l.Name).ToArray());
            Assert.Equal(new[] { "lnkEnriched", "lnkNoCountry" }, lookup.Outputs.Select(l => l.Name).ToArray());
            var reference = job.FindLink("lnkCountries")!;
            Assert.Equal(LinkKind.Reference, reference.Kind);
            Assert.Equal("reject", reference.LookupFailure);
            Assert.Equal("lnkClean.COUNTRY_CODE", reference.TargetColumns.Single(c => c.IsKey).Derivation);
        }

        [Fact]
        public void Transformer_details_are_read()
        {
            var job = Project.FindJob("PX_Load_Customers")!;
            var transformer = job.FindStage("xfmClean")!;
            Assert.True(transformer.IsTransformer);
            Assert.Equal(new[] { "svName", "svCount", "svValidDate" }, transformer.StageVariables.Select(v => v.Name).ToArray());
            Assert.Equal("0", transformer.StageVariables[1].InitialValue);
            var clean = job.FindLink("lnkClean")!;
            Assert.Equal("lnkCustomers.STATUS = \"A\" And svValidDate", clean.Constraint);
            Assert.Contains("\nElse", clean.FindColumn("BALANCE")!.Derivation);
            Assert.True(job.FindLink("lnkInvalid")!.IsOtherwise);
            Assert.Equal(DsLogicalType.Date, clean.FindColumn("SIGNUP_DATE")!.LogicalType);
        }

        [Fact]
        public void Server_stages_keep_their_pin_properties()
        {
            var job = Project.FindJob("SRV_Load_Orders")!;
            var orders = job.FindLink("lnkOrders")!;
            Assert.Equal("#SourceDir#/orders.txt", orders.SourcePin["FileName"]);
            Assert.Equal("\"", orders.SourcePin["QuoteChar"]);
            var reference = job.FindLink("lnkProductRef")!;
            Assert.Equal(LinkKind.Reference, reference.Kind);
            Assert.Equal("Trim(lnkOrders.PRODUCT_ID)", reference.TargetColumns.Single(c => c.IsKey).Derivation);
        }

        [Fact]
        public void Sequence_activities_are_classified()
        {
            var sequence = Project.FindJob("SEQ_Daily_Load")!.Sequence!;
            Assert.Equal(17, sequence.Activities.Count);
            Assert.Equal(ActivityKind.UserVariables, sequence.Find("UV_Run")!.Kind);
            Assert.Equal(ActivityKind.WaitForFile, sequence.Find("WF_Ready")!.Kind);
            Assert.Equal(ActivityKind.Sequencer, sequence.Find("SEQ_Join")!.Kind);
            Assert.Equal(ActivityKind.Condition, sequence.Find("NC_Check")!.Kind);
            Assert.Equal(ActivityKind.StartLoop, sequence.Find("SL_Regions")!.Kind);
            Assert.Equal(ActivityKind.EndLoop, sequence.Find("EL_Regions")!.Kind);
            Assert.Equal(ActivityKind.Terminator, sequence.Find("TM_Abort")!.Kind);
            Assert.Equal(ActivityKind.ExceptionHandler, sequence.Find("EH_Handler")!.Kind);

            var job = sequence.Find("JA_Sales_Summary")!;
            Assert.Equal("PX_Sales_Summary", job.JobName);
            Assert.Equal(ExecutionAction.ResetIfRequiredThenRun, job.ExecutionAction);
            Assert.Equal(new[] { "SourceDir", "TargetDir" }, job.Parameters.Select(p => p.Name).ToArray());
            Assert.Equal("FormatRunTag", sequence.Find("RA_Tag")!.RoutineName);
            Assert.Equal("echo", sequence.Find("EC_Archive")!.Command);
            Assert.True(sequence.Find("SL_Regions")!.IsListLoop);
            Assert.Equal("EU,US", sequence.Find("SL_Regions")!.LoopValues);
            Assert.Equal(2, sequence.Find("UV_Run")!.Variables.Count);
        }

        [Fact]
        public void Trigger_kinds_come_from_descriptions_expressions_and_codes()
        {
            var sequence = Project.FindJob("SEQ_Daily_Load")!.Sequence!;
            Trigger T(string name) => sequence.Triggers.Single(t => t.Name == name);
            Assert.Equal(TriggerKind.Ok, T("UV_OK").Kind);
            Assert.False(T("UV_OK").KindFromCodeOnly);
            Assert.Equal(TriggerKind.Failed, T("Customers_Failed").Kind);
            Assert.Equal(TriggerKind.Custom, T("Customers_Done").Kind);
            Assert.StartsWith("JA_Load_Customers.$JobStatus", T("Customers_Done").Expression);
            Assert.Equal(TriggerKind.ReturnValue, T("Archive_RC0").Kind);
            Assert.Equal("0", T("Archive_RC0").Expression);
            Assert.Equal(TriggerKind.Otherwise, T("Archive_Else").Kind);
            Assert.Equal(TriggerKind.Unconditional, T("Join_Out").Kind);
            Assert.Equal("SEQ_Join", T("Orders_OK").Target!.Name);
        }
    }
}
