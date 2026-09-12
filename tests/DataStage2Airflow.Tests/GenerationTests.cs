using System.IO;
using System.Linq;
using DataStage2Airflow.Generation;
using Xunit;

namespace DataStage2Airflow.Tests
{
    public class GenerationTests
    {
        private static (MigrationResult Result, MemorySink Sink) Convert(MigrationOptions? options = null)
        {
            var sink = new MemorySink();
            var result = new Migrator(options ?? new MigrationOptions()).Run(new[] { TestPaths.SampleDsx }, sink);
            return (result, sink);
        }

        /// <summary>Converts the sample project with text replaced in one of its files, for cases the samples do not cover.</summary>
        private static (MigrationResult Result, MemorySink Sink) ConvertVariant(string file, params (string Find, string Replace)[] edits)
        {
            var folder = TestPaths.NewTempDirectory("variant");
            foreach (var path in Directory.GetFiles(TestPaths.SampleDsx))
            {
                var text = File.ReadAllText(path);
                if (Path.GetFileName(path) == file)
                {
                    foreach (var (find, replace) in edits)
                    {
                        Assert.Contains(find, text);
                        text = text.Replace(find, replace);
                    }
                }

                File.WriteAllText(Path.Combine(folder, Path.GetFileName(path)), text);
            }

            var sink = new MemorySink();
            var result = new Migrator(new MigrationOptions()).Run(new[] { folder }, sink);
            return (result, sink);
        }

        [Fact]
        public void Auto_mode_converts_supported_jobs_and_keeps_the_rest_in_datastage()
        {
            var (result, sink) = Convert();
            var jobs = result.Report.Jobs.ToDictionary(j => j.Name);
            Assert.Equal("python", jobs["PX_Load_Customers"].Outcome);
            Assert.Equal("python", jobs["SRV_Load_Orders"].Outcome);
            Assert.Equal("python", jobs["PX_Extract_Accounts"].Outcome);
            var apply = jobs["PX_Apply_Changes"];
            Assert.Equal("dsjob", apply.Outcome);
            Assert.Contains(apply.Issues, i => i.Blocking && i.Message.Contains("PxChangeApply"));
            Assert.Contains("dsio.not_converted(", sink.Files[apply.DraftPath!]);
            Assert.Equal(new[] { "SEQ_Daily_Load" }, jobs["PX_Load_Customers"].RunBy);
        }

        [Fact]
        public void Output_follows_the_dags_folder_layout()
        {
            var (_, sink) = Convert();
            foreach (var path in new[]
            {
                "dags/dwproj/seq_daily_load.py",
                "dags/dwproj/px_apply_changes.py",
                "dags/ds2af_jobs/__init__.py",
                "dags/ds2af_jobs/dwproj/__init__.py",
                "dags/ds2af_jobs/dwproj/px_load_customers.py",
                "dags/ds2af_jobs/dwproj/routines.py",
                "dags/ds2af_runtime/dsfunc.py",
                "dags/ds2af_runtime/sequence.py",
                "dags/.airflowignore",
                "reports/migration_report.md",
                "reports/migration_report.json",
            })
            {
                Assert.True(sink.Files.ContainsKey(path), "missing " + path);
            }

            // Jobs that a sequence runs get no DAG of their own.
            Assert.False(sink.Files.ContainsKey("dags/dwproj/px_load_customers.py"));
            Assert.Contains("ds2af_runtime/", sink.Files["dags/.airflowignore"]);
        }

        [Fact]
        public void Sequence_dag_uses_branches_joins_sensors_and_trigger_rules()
        {
            var (_, sink) = Convert();
            var dag = sink.Files["dags/dwproj/seq_daily_load.py"];
            Assert.Contains("from airflow.sdk import DAG, Param, task", dag);
            Assert.Contains("@task.branch(task_id=\"JA_Load_Customers\")", dag);
            Assert.Contains("@task.sensor(task_id=\"WF_Ready\", poke_interval=60, timeout=1800", dag);
            Assert.Contains("EmptyOperator(task_id=\"SEQ_Join\", trigger_rule=\"all_success\")", dag);
            Assert.Contains("EmptyOperator(task_id=\"JA_Load_Customers__to__NT_Failure\")", dag);
            Assert.Contains("always=[\"EH_Handler\"]", dag);
            Assert.Contains("trigger_rule=\"one_failed\"", dag);
            Assert.Contains("seq.loop_values(numeric=False, values=\"EU,US\", delimiter=\",\")", dag);
            Assert.Contains("module=\"ds2af_jobs.dwproj.px_load_customers\"", dag);
            Assert.Contains("on_warning=\"skip\"", dag);
            Assert.Contains("F.basic_eq(r.return_value, 0)", dag);
            Assert.Contains("\"Region\": s.counter(\"SL_Regions\")", dag);
            Assert.Contains("seq.run_routine(s, \"RA_Tag\", \"format_run_tag\", [\"SALES\", s.param(\"RunDate\")], ROUTINES)", dag);
            Assert.True(dag.IndexOf("def uv_run(", System.StringComparison.Ordinal) < dag.IndexOf("def sl_regions(", System.StringComparison.Ordinal));
        }

        [Fact]
        public void Airflow_2_imports_are_used_on_request()
        {
            var (_, sink) = Convert(new MigrationOptions { Airflow = AirflowVersion.V2 });
            var dag = sink.Files["dags/dwproj/seq_daily_load.py"];
            Assert.Contains("from airflow import DAG", dag);
            Assert.Contains("from airflow.decorators import task", dag);
            Assert.Contains("from airflow.operators.empty import EmptyOperator", dag);
            Assert.DoesNotContain("airflow.sdk", dag);
        }

        [Fact]
        public void Dsjob_mode_only_orchestrates()
        {
            var (result, sink) = Convert(new MigrationOptions { Mode = ConversionMode.Dsjob, DsjobSshConnId = "ds_server" });
            Assert.All(result.Report.Jobs, j => Assert.Equal("dsjob", j.Outcome));
            Assert.DoesNotContain(sink.Files.Keys, k => k.StartsWith("dags/ds2af_jobs/dwproj/px_", System.StringComparison.Ordinal));
            var dag = sink.Files["dags/dwproj/seq_daily_load.py"];
            Assert.Contains("DSJOB = seq.DsJobConfig(PROJECT, dsjob=\"dsjob\", ssh_conn_id=\"ds_server\")", dag);
            Assert.Contains("dsjob=DSJOB", dag);
            Assert.DoesNotContain("module=", dag);
        }

        [Fact]
        public void Python_mode_writes_modules_even_with_blocking_issues()
        {
            var (result, sink) = Convert(new MigrationOptions { Mode = ConversionMode.Python });
            Assert.Equal("python", result.Report.Jobs.Single(j => j.Name == "PX_Apply_Changes").Outcome);
            Assert.Contains("dsio.not_converted(\"chaApply\"", sink.Files["dags/ds2af_jobs/dwproj/px_apply_changes.py"]);
        }

        [Fact]
        public void Data_sources_map_to_airflow_connections()
        {
            var options = new MigrationOptions();
            options.Connections["DWHPROD"] = "warehouse";
            var (_, sink) = Convert(options);
            var module = sink.Files["dags/ds2af_jobs/dwproj/px_extract_accounts.py"];
            Assert.Contains("\"SrcAccounts\": \"warehouse\",", module);
            Assert.Contains("ctx.expand(\"SELECT ACCOUNT_ID, OWNER, STATUS FROM ACCOUNTS WHERE REGION = '#Region#' ORDER BY ACCOUNT_ID\")", module);
        }

        [Fact]
        public void Report_describes_outcomes_and_triggers()
        {
            var (result, sink) = Convert();
            var json = sink.Files["reports/migration_report.json"];
            Assert.Contains("\"convertedToPython\": 5", json);
            Assert.Contains("\"runThroughDsjob\": 1", json);
            var markdown = sink.Files["reports/migration_report.md"];
            Assert.Contains("# DataStage to Airflow migration report", markdown);
            Assert.Contains("| Customers_Failed | JA_Load_Customers | NT_Failure | Failed |", markdown);
            var sequence = result.Report.Sequences.Single();
            Assert.Equal("SEQ_Daily_Load", sequence.DagId);
            Assert.Equal(21, sequence.Triggers.Count);
            Assert.DoesNotContain(sequence.Issues, i => i.Blocking);
        }

        [Fact]
        public void Routines_with_one_assignment_are_translated()
        {
            var (_, sink) = Convert();
            var routines = sink.Files["dags/ds2af_jobs/dwproj/routines.py"];
            Assert.Contains("def format_run_tag(prefix, run_date):", routines);
            Assert.Contains("return F.concat(prefix, \"_\", F.convert(\"-\", \"\", run_date))", routines);
            Assert.Contains("raise NotImplementedError(\"routine CleanPhone has not been ported from DataStage BASIC\")", routines);
        }

        [Fact]
        public void Each_trigger_issue_is_reported_once()
        {
            // Branch conditions are translated for the report and for the DAG; the issue must not repeat.
            var (result, _) = ConvertVariant("seq_daily_load.dsx", ("Or JA_Load_Customers.$JobStatus = DSJS.RUNWARN", "Or Bogus.$Thing = 1"));
            var issue = Assert.Single(result.Report.Sequences.Single().Issues, i => i.Code == "SEQ031");
            Assert.Contains("Bogus.$Thing", issue.Message);
        }

        [Fact]
        public void Row_limit_applies_to_a_constraint_with_a_top_level_or()
        {
            var (_, sink) = ConvertVariant(
                "px_load_customers.dsx",
                ("Constraint \"lnkCustomers.STATUS", "Constraint \"(lnkCustomers.STATUS"),
                ("And svValidDate\"", "Or lnkCustomers.STATUS = 'B') And svValidDate Or lnkCustomers.CUST_ID = 1\""),
                ("RowLimit \"0\"", "RowLimit \"5\""));
            var module = sink.Files["dags/ds2af_jobs/dwproj/px_load_customers.py"];
            Assert.Contains(
                "if ((row[\"STATUS\"] == \"A\" or row[\"STATUS\"] == \"B\") and F.truth(sv_valid_date) or row[\"CUST_ID\"] == 1) and len(out_lnk_clean) < 5:",
                module);
        }

        [Fact]
        public void Filter_rewrites_leave_string_literals_alone()
        {
            var (_, sink) = ConvertVariant("px_sales_summary.dsx", ("AMOUNT > 0 AND REGION IS NOT NULL", "AMOUNT > 0 AND REGION <> 'TRUE' AND PRODUCT LIKE 'W%'"));
            var module = sink.Files["dags/ds2af_jobs/dwproj/px_sales_summary.py"];
            Assert.Contains("F.ne(row[\"REGION\"], \"TRUE\")", module);
            Assert.Contains("F.like(row[\"PRODUCT\"], \"W%\")", module);
        }
    }
}
