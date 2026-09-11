using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using DataStage2Airflow.Dsx;
using DataStage2Airflow.Internal;
using DataStage2Airflow.Model;

namespace DataStage2Airflow.Generation.Emitters
{
    /// <summary>Parallel Sequential File (properties on the link pins, format in APT/SchemaFormat) and
    /// the server Sequential File stage (FileName, ColDelim, QuoteChar... on the pins).</summary>
    internal sealed class SequentialFileEmitter : StageEmitter
    {
        public override string Implementation => "dsio.read_delimited / dsio.write_delimited";

        public override bool Handles(Stage stage, JobKind kind) => TypeIs(stage, "PxSequentialFile", "CSeqFileStage");

        public override void Emit(StageContext c)
        {
            bool server = string.Equals(c.Stage.OleType, "CSeqFileStage", StringComparison.OrdinalIgnoreCase);
            if (server)
            {
                foreach (var link in c.Stage.Outputs) ServerRead(c, link);
                foreach (var link in c.Stage.Inputs) ServerWrite(c, link);
                return;
            }

            if (c.Stage.Outputs.Count > 0) ParallelRead(c);
            foreach (var link in c.Stage.Inputs) ParallelWrite(c, link);
        }

        private static void ParallelRead(StageContext c)
        {
            var main = c.Stage.Outputs[0];
            Link? reject = c.Stage.Outputs.Count > 1 ? c.Stage.Outputs[c.Stage.Outputs.Count - 1] : null;
            var props = main.SourceProperties;
            var files = FileNames(props, c.Stage.Properties);
            if (files.Count == 0)
            {
                c.Blocking($"no file name on output link {main.Name}");
                return;
            }

            var args = new List<string> { "ctx", "[" + string.Join(", ", files.Select(Py.Expand)) + "]", c.Columns(main) };
            args.AddRange(FormatArguments(Format(main.SourceMeta, c.Stage.Meta, props), c.Typed));
            if (FirstLineIsNames(props)) args.Add("header=True");
            var rejects = (props.Value("rejects") ?? string.Empty).Trim().ToLowerInvariant();
            if (reject != null) args.Add("reject=True");
            else if (rejects == "fail") args.Add("reject_mode=\"fail\"");
            var missing = props.Value("missingFile");
            if (missing != null) args.Add("missing=" + Py.Str(missing.Trim().ToLowerInvariant()));
            args.Add("stage=" + Py.Str(c.Stage.Name));
            c.Call(reject != null ? c.Assign(main, reject) : c.Assign(main), "dsio.read_delimited", args);
        }

        private static void ParallelWrite(StageContext c, Link link)
        {
            var props = link.TargetProperties;
            var files = FileNames(props, c.Stage.Properties);
            if (files.Count == 0)
            {
                c.Blocking($"no file name on input link {link.Name}");
                return;
            }

            if (files.Count > 1) c.Approximation("the stage lists several files; only the first is written");
            var args = new List<string> { "ctx", c.Var(link), Py.Expand(files[0]), c.Columns(link) };
            args.AddRange(FormatArguments(Format(link.TargetMeta, c.Stage.Meta, props), c.Typed));
            if (FirstLineIsNames(props)) args.Add("header=True");
            var mode = (props.ValueAny("append\\overwrite", "appendoverwrite", "fileUpdateMode", "updatemode") ?? "overwrite").Trim().ToLowerInvariant();
            if (mode.Contains("append")) args.Add("append=True");
            if (mode.Contains("create")) c.Note("file update mode 'create': an existing file is overwritten instead of failing the job");
            args.Add("stage=" + Py.Str(c.Stage.Name));
            c.Call(null, "dsio.write_delimited", args);
        }

        private static void ServerRead(StageContext c, Link link)
        {
            var pin = link.SourcePin;
            var file = Text.NullIfBlank(pin.Properties.GetAny("FileName", "File", "Filename"));
            if (file == null)
            {
                c.Blocking($"no FileName on output link {link.Name}");
                return;
            }

            var args = new List<string> { "ctx", "[" + Py.Expand(file) + "]", c.Columns(link) };
            ServerFormat(args, pin.Properties.GetAny("ColDelim", "Delimiter"), pin["QuoteChar"], pin["FixedWidth"] == "1");
            if (pin["ColHeaders"] == "1") args.Add("header=True");
            var nullString = pin.Properties.GetAny("NullString", "DefaultNullString", "NullValue");
            if (nullString != null) args.Add("null_value=" + Py.Str(nullString));
            args.Add("typed=False");
            args.Add("stage=" + Py.Str(c.Stage.Name));
            c.Call(c.Assign(link), "dsio.read_delimited", args);
        }

        private static void ServerWrite(StageContext c, Link link)
        {
            var pin = link.TargetPin;
            var file = pin == null ? null : Text.NullIfBlank(pin.Properties.GetAny("FileName", "File", "Filename"));
            if (pin == null || file == null)
            {
                c.Blocking($"no FileName on input link {link.Name}");
                return;
            }

            var args = new List<string> { "ctx", c.Var(link), Py.Expand(file), c.Columns(link) };
            ServerFormat(args, pin.Properties.GetAny("Delimiter", "ColDelim"), pin["QuoteChar"], pin["FixedWidth"] == "1");
            if (pin["ColHeaders"] == "1") args.Add("header=True");
            if (pin["Append"] == "1") args.Add("append=True");
            var nullString = pin.Properties.GetAny("NullString", "DefaultNullString", "NullValue");
            if (nullString != null) args.Add("null_value=" + Py.Str(nullString));
            if (c.Stage.Record["UnixFormat"] == "0") args.Add("line_end=\"\\r\\n\"");
            args.Add("typed=False");
            args.Add("stage=" + Py.Str(c.Stage.Name));
            c.Call(null, "dsio.write_delimited", args);
        }

        private static void ServerFormat(List<string> args, string? delimiter, string? quote, bool fixedWidth)
        {
            if (fixedWidth)
            {
                args.Add("delimiter=None");
                args.Add("fixed_width=True");
            }
            else
            {
                var d = ServerChar(delimiter, ",") ?? ",";
                if (d != ",") args.Add("delimiter=" + Py.Str(d));
            }

            args.Add("quote=" + Py.Str(ServerChar(quote, "\"")));
        }

        /// <summary>Server stages store characters as the character itself or as a 3-digit code; "000" means none.</summary>
        internal static string? ServerChar(string? value, string fallback)
        {
            if (value == null) return fallback;
            if (value.Length == 0 || value == "000" || value == "0") return null;
            if (value.Length == 3 && value.All(char.IsDigit))
            {
                return ((char)int.Parse(value, CultureInfo.InvariantCulture)).ToString();
            }

            return value.Substring(0, 1);
        }

        internal static List<string> FileNames(PropertySet linkProperties, PropertySet stageProperties)
        {
            var names = new List<string>();
            foreach (var props in new[] { linkProperties, stageProperties })
            {
                foreach (var key in new[] { "file", "filepattern", "fileplus", "pattern" })
                {
                    names.AddRange(props.GetAll(key).Select(n => n.Value.Trim()).Where(v => v.Length > 0));
                }

                if (names.Count > 0) break;
            }

            return names;
        }

        internal static bool FirstLineIsNames(PropertySet props) =>
            props.IsTrue("firstLineColumnNames") || props.IsTrue("first_line_is_column_names")
            || props.IsTrue("firstLineIsColumnNames") || props.IsTrue("first_line_column_names");

        /// <summary>Record format from APT/SchemaFormat, or from format properties on the link; PX defaults otherwise.</summary>
        internal static Dictionary<string, string> Format(MetaBag linkMeta, MetaBag stageMeta, PropertySet props)
        {
            var format = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            var schemaFormat = linkMeta.Get("SchemaFormat") ?? stageMeta.Get("SchemaFormat");
            bool found = schemaFormat != null;
            if (schemaFormat != null)
            {
                foreach (var pair in OshFormat.Parse(schemaFormat)) format[pair.Key] = pair.Value;
            }

            foreach (var key in new[] { "delim", "delim_string", "quote", "final_delim", "null_field", "record_delim", "record_delim_string", "date_format", "time_format", "timestamp_format" })
            {
                var value = props.Value(key);
                if (value != null && !format.ContainsKey(key))
                {
                    format[key] = value;
                    found = true;
                }
            }

            if (!found)
            {
                format["delim"] = ",";
                format["quote"] = "double";
            }

            return format;
        }

        internal static List<string> FormatArguments(Dictionary<string, string> format, bool typed)
        {
            var args = new List<string>();
            string? delimiter = ",";
            if (format.TryGetValue("delim_string", out var delimString)) delimiter = delimString;
            else if (format.TryGetValue("delim", out var delim)) delimiter = OshFormat.Symbol(delim);

            if (delimiter == null)
            {
                args.Add("delimiter=None");
                args.Add("fixed_width=True");
            }
            else if (delimiter != ",")
            {
                args.Add("delimiter=" + Py.Str(delimiter));
            }

            args.Add("quote=" + Py.Str(format.TryGetValue("quote", out var quote) ? OshFormat.Symbol(quote) : null));
            if (format.TryGetValue("final_delim", out var finalDelim))
            {
                var value = finalDelim.Trim().ToLowerInvariant();
                if (value.Length > 0 && value != "end" && value != "none") args.Add("final_delimiter=True");
            }

            if (format.TryGetValue("null_field", out var nullField)) args.Add("null_value=" + Py.Str(nullField));

            string? record = null;
            if (format.TryGetValue("record_delim_string", out var recordString)) record = recordString;
            else if (format.TryGetValue("record_delim", out var recordDelim)) record = OshFormat.Symbol(recordDelim);
            if (record != null && record != "\n" && record != "\r\n") args.Add("record_delimiter=" + Py.Str(record));

            foreach (var key in new[] { "date_format", "time_format", "timestamp_format" })
            {
                if (format.TryGetValue(key, out var value)) args.Add($"{key}={Py.Str(value)}");
            }

            if (!typed) args.Add("typed=False");
            return args;
        }
    }

    /// <summary>Data sets, file sets and lookup file sets, kept as pickled frames at the same paths.</summary>
    internal sealed class DataSetEmitter : StageEmitter
    {
        private static readonly string[] PathProperties = { "dataset", "file", "fileset", "lookup_fileset", "lookupfileset", "lookupFileSet" };

        public override string Implementation => "dsio.read_dataset / dsio.write_dataset (pickled frames)";

        public override bool Handles(Stage stage, JobKind kind) => TypeIs(stage, "PxDataSet", "PxFileSet", "PxLookupFileSet");

        public override void Emit(StageContext c)
        {
            if (!TypeIs(c.Stage, "PxDataSet")) c.Approximation($"{c.Stage.StageType} is stored as a pickled frame, not in the DataStage format");
            foreach (var link in c.Stage.Outputs)
            {
                var path = link.SourceProperties.ValueAny(PathProperties) ?? c.Stage.Properties.ValueAny(PathProperties);
                if (path == null)
                {
                    c.Blocking($"no data set path on link {link.Name}");
                    continue;
                }

                c.Call(c.Assign(link), "dsio.read_dataset", new List<string> { "ctx", Py.Expand(path.Trim()), c.Columns(link), "stage=" + Py.Str(c.Stage.Name) });
            }

            foreach (var link in c.Stage.Inputs)
            {
                var path = link.TargetProperties.ValueAny(PathProperties) ?? c.Stage.Properties.ValueAny(PathProperties);
                if (path == null)
                {
                    c.Blocking($"no data set path on link {link.Name}");
                    continue;
                }

                var policy = (link.TargetProperties.ValueAny("updatePolicy", "update_policy", "overwrite", "mode") ?? "overwrite").ToLowerInvariant();
                var mode = policy.Contains("append") ? "append" : policy.Contains("create") ? "create" : "overwrite";
                c.Call(null, "dsio.write_dataset", new List<string> { "ctx", c.Var(link), Py.Expand(path.Trim()), c.Columns(link), "mode=" + Py.Str(mode), "stage=" + Py.Str(c.Stage.Name) });
            }
        }
    }

    /// <summary>Server hashed files: keyed stores (writing an existing key replaces the row).</summary>
    internal sealed class HashedFileEmitter : StageEmitter
    {
        public override string Implementation => "dsio.read_hashed / dsio.write_hashed (keyed pickle files)";

        public override bool Handles(Stage stage, JobKind kind) => TypeIs(stage, "CHashedFileStage");

        public override void Emit(StageContext c)
        {
            var directory = Text.NullIfBlank(c.Stage.Record.Properties.GetAny("Directory", "DirectoryPath", "HashedFileDirectory", "Path"));
            var account = Text.NullIfBlank(c.Stage.Record.Properties.GetAny("Account", "AccountName"));
            if (directory == null && account != null) c.Note($"account {account}: hashed files are stored under DS2AF_HASHED_DIR");
            var dir = directory == null ? "None" : Py.Expand(directory);

            foreach (var link in c.Stage.Outputs)
            {
                var name = Text.NullIfBlank(link.SourcePin.Properties.GetAny("FileName", "HashedFileName", "File"));
                if (name == null)
                {
                    c.Blocking($"no hashed file name on link {link.Name}");
                    continue;
                }

                c.Call(c.Assign(link), "dsio.read_hashed", new List<string> { "ctx", Py.Expand(name), dir, c.Columns(link), "stage=" + Py.Str(c.Stage.Name) });
            }

            foreach (var link in c.Stage.Inputs)
            {
                var pin = link.TargetPin;
                var name = pin == null ? null : Text.NullIfBlank(pin.Properties.GetAny("FileName", "HashedFileName", "File"));
                if (pin == null || name == null)
                {
                    c.Blocking($"no hashed file name on link {link.Name}");
                    continue;
                }

                if (!link.Columns.Any(col => col.IsKey)) c.Approximation($"link {link.Name} has no key columns; the first column is used as the key");
                var args = new List<string> { "ctx", c.Var(link), Py.Expand(name), dir, c.Columns(link) };
                if (Text.ParseBool(pin.Properties.GetAny("ClearFile", "ClearFileBefore", "Clear")) == true) args.Add("clear=True");
                args.Add("stage=" + Py.Str(c.Stage.Name));
                c.Call(null, "dsio.write_hashed", args);
            }
        }
    }
}
