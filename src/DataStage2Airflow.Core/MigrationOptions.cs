using System;
using System.Collections.Generic;

namespace DataStage2Airflow
{
    public enum ConversionMode
    {
        /// <summary>Convert each job to Python when every stage and expression translates; otherwise run it through dsjob.</summary>
        Auto,

        /// <summary>Always generate Python jobs; untranslatable parts raise at run time.</summary>
        Python,

        /// <summary>Only orchestrate: every job still runs in DataStage through dsjob.</summary>
        Dsjob,
    }

    public enum AirflowVersion
    {
        V2 = 2,
        V3 = 3,
    }

    public sealed class MigrationOptions
    {
        public ConversionMode Mode { get; set; } = ConversionMode.Auto;

        public AirflowVersion Airflow { get; set; } = AirflowVersion.V3;

        /// <summary>Overrides the project name from the export header.</summary>
        public string? ProjectName { get; set; }

        /// <summary>Prepended to every DAG id.</summary>
        public string DagPrefix { get; set; } = string.Empty;

        public string Owner { get; set; } = "datastage";

        /// <summary>Cron expression or preset for generated DAGs; null means triggered manually.</summary>
        public string? Schedule { get; set; }

        public DateTime StartDate { get; set; } = new DateTime(2024, 1, 1);

        /// <summary>Path of dsjob on the DataStage server (used by activities that stay in DataStage).</summary>
        public string DsjobPath { get; set; } = "dsjob";

        public string? DsjobServer { get; set; }

        /// <summary>Airflow SSH connection used to run dsjob on the DataStage server; null runs dsjob locally.</summary>
        public string? DsjobSshConnId { get; set; }

        /// <summary>Treat "finished with warnings" as a failure for activities without warning triggers.</summary>
        public bool FailOnWarning { get; set; }

        /// <summary>
        /// Let OK triggers fire when a job finishes with warnings. DataStage fires them only on "finished OK";
        /// by default the generated DAG does the same and skips the downstream tasks.
        /// </summary>
        public bool WarningsAreOk { get; set; }

        /// <summary>Also generate a DAG for every job that no sequence runs.</summary>
        public bool StandaloneJobDags { get; set; } = true;

        /// <summary>Copy the ds2af_runtime package next to the DAGs.</summary>
        public bool IncludeRuntime { get; set; } = true;

        /// <summary>Python package (inside the DAGs folder) that receives converted jobs.</summary>
        public string JobsPackage { get; set; } = "ds2af_jobs";

        /// <summary>DataStage data source (DSN, server, database) to Airflow connection id.</summary>
        public IDictionary<string, string> Connections { get; } = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
    }
}
