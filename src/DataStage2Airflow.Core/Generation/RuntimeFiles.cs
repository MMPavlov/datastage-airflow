using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace DataStage2Airflow.Generation
{
    /// <summary>The ds2af_runtime Python package, embedded in this assembly.</summary>
    public static class RuntimeFiles
    {
        private const string Prefix = "pyruntime/";

        /// <summary>(relative path such as "ds2af_runtime/dsfunc.py", content) pairs.</summary>
        public static IReadOnlyList<KeyValuePair<string, string>> Load()
        {
            var assembly = typeof(RuntimeFiles).Assembly;
            var files = new List<KeyValuePair<string, string>>();
            foreach (var name in assembly.GetManifestResourceNames().OrderBy(n => n, StringComparer.Ordinal))
            {
                if (!name.StartsWith(Prefix, StringComparison.Ordinal)) continue;
                using (var stream = assembly.GetManifestResourceStream(name))
                {
                    if (stream == null) continue;
                    using (var reader = new StreamReader(stream, Encoding.UTF8))
                    {
                        files.Add(new KeyValuePair<string, string>(name.Substring(Prefix.Length).Replace('\\', '/'), reader.ReadToEnd()));
                    }
                }
            }

            return files;
        }
    }
}
