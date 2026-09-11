using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using DataStage2Airflow.Generation;
using Xunit;

namespace DataStage2Airflow.Tests
{
    /// <summary>Converts the samples once; the tests run the generated Python.</summary>
    public sealed class ConvertedSamples : IDisposable
    {
        public ConvertedSamples()
        {
            Root = TestPaths.NewTempDirectory("e2e");
            Output = Path.Combine(Root, "out");
            Result = new Migrator(new MigrationOptions()).Run(new[] { TestPaths.SampleDsx }, new DirectorySink(Output));
        }

        public string Root { get; }

        public string Output { get; }

        public string Dags => Path.Combine(Output, "dags");

        public MigrationResult Result { get; }

        public void Dispose()
        {
            try
            {
                Directory.Delete(Root, true);
            }
            catch (IOException)
            {
            }
            catch (UnauthorizedAccessException)
            {
            }
        }
    }

    public class EndToEndTests : IClassFixture<ConvertedSamples>
    {
        private readonly ConvertedSamples _samples;

        public EndToEndTests(ConvertedSamples samples)
        {
            _samples = samples;
        }

        private static string Expected(string name) => Path.Combine(TestPaths.SampleData, "expected", name);

        private static string Normalize(string text) => text.Replace("\r\n", "\n");

        private static void AssertSame(string expectedFile, string actualFile)
        {
            Assert.True(File.Exists(actualFile), "missing output " + actualFile);
            Assert.Equal(Normalize(File.ReadAllText(expectedFile)), Normalize(File.ReadAllText(actualFile)));
        }

        private static (int Code, string Output, string Error) Python(params string[] arguments) => PythonRunner.Run(arguments);

        [Fact]
        public void Runtime_unit_tests_pass()
        {
            var run = Python("-m", "unittest", "discover", "-s", Path.Combine(TestPaths.RepositoryRoot, "tests", "python"));
            Assert.True(run.Code == 0, run.Error);
        }

        [Fact]
        public void Every_generated_file_compiles()
        {
            var run = Python("-m", "compileall", "-q", _samples.Output);
            Assert.True(run.Code == 0, run.Output + run.Error);
        }

        [Fact]
        public void Converted_jobs_reproduce_the_expected_outputs()
        {
            var target = TestPaths.NewTempDirectory("jobs");
            var script = string.Join("\n", new[]
            {
                "import sys",
                "sys.path.insert(0, sys.argv[1])",
                "from ds2af_jobs.dwproj import px_load_customers, px_sales_summary, srv_build_product_hash, srv_load_orders",
                "p = {'SourceDir': sys.argv[2], 'TargetDir': sys.argv[3], 'HashDir': sys.argv[3] + '/hash'}",
                "for job in (px_load_customers, px_sales_summary, srv_build_product_hash, srv_load_orders):",
                "    status = job.run(p)",
                "    assert status == 1, (job.JOB_NAME, status)",
            });
            var run = Python("-c", script, _samples.Dags, Path.Combine(TestPaths.SampleData, "in"), target);
            Assert.True(run.Code == 0, run.Error);
            foreach (var name in new[] { "customers_clean.csv", "customers_rejected.csv", "customers_no_country.csv", "sales_by_region.csv", "orders_out.txt", "orders_missing.txt" })
            {
                AssertSame(Expected(name), Path.Combine(target, name));
            }
        }

        private JsonDocument RunSequence(string source, string target, string database)
        {
            var hash = Path.Combine(target, "hash");
            var environment = new Dictionary<string, string> { ["DS2AF_CONN_DS_DWHPROD"] = "sqlite:///" + database };
            var run = PythonRunner.Run(
                new[]
                {
                    Path.Combine(TestPaths.PythonScripts, "run_dag.py"), _samples.Dags, Path.Combine(_samples.Dags, "dwproj", "seq_daily_load.py"), "--run",
                    "--param", "SourceDir=" + source, "--param", "TargetDir=" + target, "--param", "HashDir=" + hash,
                },
                environment);
            Assert.True(run.Code == 0, run.Error);
            return JsonDocument.Parse(run.Output);
        }

        private static (string Source, string Target, string Database) PrepareRun(string name, bool withSales = true)
        {
            var root = TestPaths.NewTempDirectory(name);
            var source = Path.Combine(root, "in");
            var target = Path.Combine(root, "out");
            Directory.CreateDirectory(source);
            Directory.CreateDirectory(target);
            foreach (var file in Directory.GetFiles(Path.Combine(TestPaths.SampleData, "in")))
            {
                if (!withSales && Path.GetFileName(file) == "sales.csv") continue;
                File.Copy(file, Path.Combine(source, Path.GetFileName(file)));
            }

            File.WriteAllText(Path.Combine(source, "ready.flg"), "ok");
            var database = Path.Combine(root, "dwh.db");
            var create = string.Join("\n", new[]
            {
                "import sqlite3, sys",
                "c = sqlite3.connect(sys.argv[1])",
                "c.execute('create table ACCOUNTS (ACCOUNT_ID integer, OWNER text, STATUS text, REGION text)')",
                "c.executemany('insert into ACCOUNTS values (?,?,?,?)', [(1,'Ann','A','EU'),(2,'Bob','I','US'),(3,None,'A','EU')])",
                "c.commit()",
            });
            var run = PythonRunner.Run(new[] { "-c", create, database });
            Assert.True(run.Code == 0, run.Error);
            return (source, target, database);
        }

        private static Dictionary<string, string> States(JsonDocument run) =>
            run.RootElement.GetProperty("states").EnumerateObject().ToDictionary(p => p.Name, p => p.Value.GetString()!);

        [Fact]
        public void Sequence_dag_runs_every_step_on_the_happy_path()
        {
            var (source, target, database) = PrepareRun("seq-ok");
            using (var run = RunSequence(source, target, database))
            {
                var states = States(run);
                foreach (var task in new[] { "UV_Run", "WF_Ready", "JA_Build_Hash", "JA_Load_Orders", "JA_Load_Customers", "SEQ_Join", "JA_Sales_Summary", "EC_Archive", "RA_Tag", "NC_Check", "SL_Regions", "NT_Done" })
                {
                    Assert.True(states[task] == "success", $"{task} is {states[task]}: {run.RootElement.GetProperty("errors")}");
                }

                foreach (var task in new[] { "NT_Failure", "TM_Abort", "EH_Handler", "JA_Load_Customers__to__NT_Failure", "NC_Check__to__NT_Done" })
                {
                    Assert.Equal("skipped", states[task]);
                }

                var email = Assert.Single(run.RootElement.GetProperty("emails").EnumerateArray());
                Assert.Equal("Daily load 2005-06-15 finished", email.GetProperty("subject").GetString());
            }

            AssertSame(Expected("sales_by_region.csv"), Path.Combine(target, "sales_by_region.csv"));
            AssertSame(Expected("orders_out.txt"), Path.Combine(target, "orders_out.txt"));
            Assert.True(File.Exists(Path.Combine(target, "accounts_EU.ds")));
            Assert.True(File.Exists(Path.Combine(target, "accounts_US.ds")));
        }

        [Fact]
        public void Sequence_dag_routes_a_failed_job_to_the_failure_notification()
        {
            var (source, target, database) = PrepareRun("seq-fail", withSales: false);
            using (var run = RunSequence(source, target, database))
            {
                var states = States(run);
                Assert.Equal("success", states["JA_Sales_Summary"]);
                Assert.Equal("success", states["JA_Sales_Summary__to__NT_Failure"]);
                Assert.Equal("skipped", states["EC_Archive"]);
                Assert.Equal("success", states["NT_Failure"]);
                Assert.Equal("failed", states["TM_Abort"]);
                Assert.Equal("skipped", states["NT_Done"]);
                var email = Assert.Single(run.RootElement.GetProperty("emails").EnumerateArray());
                Assert.Equal("Daily load 2005-06-15 FAILED", email.GetProperty("subject").GetString());
                Assert.Contains("JA_Sales_Summary", email.GetProperty("body").GetString());
            }
        }

        [Fact]
        public void Standalone_job_dag_runs_the_job_through_dsjob()
        {
            var run = Python(Path.Combine(TestPaths.PythonScripts, "run_dag.py"), _samples.Dags, Path.Combine(_samples.Dags, "dwproj", "px_apply_changes.py"));
            Assert.True(run.Code == 0, run.Error);
            using (var dag = JsonDocument.Parse(run.Output))
            {
                Assert.Equal("PX_Apply_Changes", dag.RootElement.GetProperty("dag_id").GetString());
                var task = Assert.Single(dag.RootElement.GetProperty("tasks").EnumerateArray());
                Assert.Equal("PX_Apply_Changes", task.GetProperty("task_id").GetString());
            }

            Assert.Contains("dsjob=DSJOB", File.ReadAllText(Path.Combine(_samples.Dags, "dwproj", "px_apply_changes.py")));
        }

        [Fact]
        public void Airflow_2_dags_import_with_the_airflow_2_paths()
        {
            var output = Path.Combine(_samples.Root, "airflow2");
            new Migrator(new MigrationOptions { Airflow = AirflowVersion.V2 }).Run(new[] { TestPaths.SampleDsx }, new DirectorySink(output));
            var dags = Path.Combine(output, "dags");
            var run = Python(Path.Combine(TestPaths.PythonScripts, "run_dag.py"), dags, Path.Combine(dags, "dwproj", "seq_daily_load.py"));
            Assert.True(run.Code == 0, run.Error);
            using (var dag = JsonDocument.Parse(run.Output))
            {
                Assert.Equal(19, dag.RootElement.GetProperty("tasks").GetArrayLength());
            }
        }
    }
}
