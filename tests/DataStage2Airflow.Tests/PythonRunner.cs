using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;

namespace DataStage2Airflow.Tests
{
    /// <summary>Runs the Python interpreter that has pandas: DS2AF_PYTHON, else the repository's .venv.</summary>
    internal static class PythonRunner
    {
        public static string Executable
        {
            get
            {
                var configured = Environment.GetEnvironmentVariable("DS2AF_PYTHON");
                if (!string.IsNullOrEmpty(configured)) return configured!;
                var root = TestPaths.RepositoryRoot;
                foreach (var candidate in new[] { Path.Combine(root, ".venv", "Scripts", "python.exe"), Path.Combine(root, ".venv", "bin", "python") })
                {
                    if (File.Exists(candidate)) return candidate;
                }

                return "python";
            }
        }

        public static (int Code, string Output, string Error) Run(IEnumerable<string> arguments, IDictionary<string, string>? environment = null, int timeoutMilliseconds = 300000)
        {
            var start = new ProcessStartInfo(Executable)
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                WorkingDirectory = TestPaths.RepositoryRoot,
            };
            foreach (var argument in arguments) start.ArgumentList.Add(argument);
            if (environment != null)
            {
                foreach (var pair in environment) start.Environment[pair.Key] = pair.Value;
            }

            using (var process = Process.Start(start)!)
            {
                var output = process.StandardOutput.ReadToEndAsync();
                var error = process.StandardError.ReadToEndAsync();
                if (!process.WaitForExit(timeoutMilliseconds))
                {
                    process.Kill(true);
                    throw new TimeoutException("python did not finish: " + string.Join(" ", arguments));
                }

                return (process.ExitCode, output.Result, error.Result);
            }
        }
    }
}
