using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace DataStage2Airflow.Generation
{
    /// <summary>Receives generated files; paths are relative and use '/'.</summary>
    public interface IOutputSink
    {
        void Write(string relativePath, string content);
    }

    public sealed class DirectorySink : IOutputSink
    {
        private static readonly Encoding Utf8 = new UTF8Encoding(false);

        public DirectorySink(string root)
        {
            Root = Path.GetFullPath(root);
        }

        public string Root { get; }

        public void Write(string relativePath, string content)
        {
            if (Path.IsPathRooted(relativePath) || relativePath.Contains(".."))
            {
                throw new ArgumentException("output paths must be relative and stay inside the output directory: " + relativePath);
            }

            var path = Path.Combine(Root, relativePath.Replace('/', Path.DirectorySeparatorChar));
            var directory = Path.GetDirectoryName(path);
            if (!string.IsNullOrEmpty(directory)) Directory.CreateDirectory(directory);
            File.WriteAllText(path, content.Replace("\r\n", "\n"), Utf8);
        }
    }

    public sealed class MemorySink : IOutputSink
    {
        public Dictionary<string, string> Files { get; } = new Dictionary<string, string>(StringComparer.Ordinal);

        public void Write(string relativePath, string content) => Files[relativePath] = content;
    }
}
