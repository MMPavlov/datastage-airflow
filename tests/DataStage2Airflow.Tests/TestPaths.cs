using System;
using System.IO;

namespace DataStage2Airflow.Tests
{
    internal static class TestPaths
    {
        public static string Samples => Path.Combine(AppContext.BaseDirectory, "samples");

        public static string SampleDsx => Path.Combine(Samples, "dsx");

        public static string SampleData => Path.Combine(Samples, "data");

        public static string PythonScripts => Path.Combine(AppContext.BaseDirectory, "Python");

        /// <summary>The repository root (the folder that holds DataStage2Airflow.sln).</summary>
        public static string RepositoryRoot
        {
            get
            {
                var directory = new DirectoryInfo(AppContext.BaseDirectory);
                while (directory != null && !File.Exists(Path.Combine(directory.FullName, "DataStage2Airflow.sln")))
                {
                    directory = directory.Parent;
                }

                return directory?.FullName ?? throw new InvalidOperationException("repository root not found");
            }
        }

        public static string NewTempDirectory(string name)
        {
            var path = Path.Combine(Path.GetTempPath(), "ds2af-tests", name + "-" + Guid.NewGuid().ToString("N").Substring(0, 8));
            Directory.CreateDirectory(path);
            return path;
        }
    }
}
