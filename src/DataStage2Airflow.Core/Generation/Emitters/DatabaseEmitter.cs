using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using DataStage2Airflow.Dsx;
using DataStage2Airflow.Model;

namespace DataStage2Airflow.Generation.Emitters
{
    /// <summary>
    /// Database stages of DataStage 7.x: Enterprise stages (PxOracle, PxDB2, PxODBC, PxTeradata...) and
    /// server plugins (ORAOCI8/9, DRS, ODBC, DB2 API...). Their property names differ per stage and
    /// release, so the query, table, data source and write mode are found by searching the properties.
    /// </summary>
    internal sealed class DatabaseEmitter : StageEmitter
    {
        private static readonly string[] Families =
        {
            "oracle", "oraoci", "db2", "odbc", "drs", "teradata", "informix", "sybase", "sqlserver", "mssql", "netezza", "redbrick", "oledb", "udb",
        };

        private static readonly HashSet<string> Structural = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "Identifier", "OLEType", "Readonly", "Name", "NextID", "InputPins", "OutputPins", "StageType", "Partner",
            "Columns", "Properties", "MetaBag", "LeftTextPos", "TopTextPos", "LinkMinimised", "AllowColumnMapping",
        };

        public override string Implementation => "dsio.read_sql / dsio.write_table (DB-API through Airflow connections)";

        public override bool Handles(Stage stage, JobKind kind) =>
            string.Equals(stage.OleType, "CODBCStage", StringComparison.OrdinalIgnoreCase)
            || Families.Any(f => stage.StageType.IndexOf(f, StringComparison.OrdinalIgnoreCase) >= 0);

        public override void Emit(StageContext c)
        {
            var connection = Connection(c);
            foreach (var link in c.Stage.Outputs) Read(c, link, connection);
            foreach (var link in c.Stage.Inputs) Write(c, link, connection);
        }

        private static IEnumerable<(string Name, string Value)> Properties(DsxRecord? record, PropertySet set)
        {
            if (record != null)
            {
                foreach (var pair in record.Properties)
                {
                    if (!Structural.Contains(pair.Key)) yield return (pair.Key, pair.Value);
                }
            }

            foreach (var node in set.Nodes)
            {
                yield return (node.Name, node.Value);
                foreach (var child in node.Children) yield return (child.Name, child.Value);
            }
        }

        private static List<(string Name, string Value)> All(StageContext c, Link link, bool output)
        {
            var own = output ? Properties(link.SourcePin, link.SourceProperties) : Properties(link.TargetPin, link.TargetProperties);
            return own.Concat(Properties(c.Stage.Record, c.Stage.Properties)).ToList();
        }

        private static string Connection(StageContext c)
        {
            string? source = null;
            var candidates = Properties(c.Stage.Record, c.Stage.Properties)
                .Concat(c.Stage.Outputs.SelectMany(l => Properties(l.SourcePin, l.SourceProperties)))
                .Concat(c.Stage.Inputs.SelectMany(l => Properties(l.TargetPin, l.TargetProperties)));
            foreach (var (name, value) in candidates)
            {
                var lower = name.ToLowerInvariant();
                if (lower.Contains("user") || lower.Contains("password") || lower.Contains("port") || value.Trim().Length == 0) continue;
                if (lower.Contains("dsn") || lower.Contains("datasource") || lower.Contains("data_source") || lower == "server"
                    || lower == "servername" || lower.Contains("database") || lower == "dbname" || lower.Contains("db_cs") || lower.Contains("instance"))
                {
                    source = value.Trim();
                    break;
                }
            }

            string id;
            if (source == null)
            {
                id = "ds_" + Naming.Snake(c.Stage.StageType);
                c.Approximation($"no data source found in the stage; it uses Airflow connection '{id}'");
            }
            else if (c.Options.Connections.TryGetValue(source, out var mapped))
            {
                id = mapped;
            }
            else if (source.IndexOf('#') >= 0)
            {
                c.Note($"data source {source} is parameterised: the Airflow connection id is its value at run time");
                return Py.Expand(source);
            }
            else
            {
                id = "ds_" + Naming.Snake(source);
            }

            c.State.Connections[c.Stage.Name] = id;
            c.Note($"Airflow connection '{id}'" + (source == null ? string.Empty : $" for data source {source}"));
            return $"CONNECTIONS[{Py.Str(c.Stage.Name)}]";
        }

        private static void Read(StageContext c, Link link, string connection)
        {
            var all = All(c, link, true);
            var sql = all.Select(p => p.Value).FirstOrDefault(v => IsStatement(v, "SELECT", "WITH"));
            if (sql == null)
            {
                var table = TableName(all);
                if (table == null)
                {
                    c.Blocking($"no SQL query or table name for output link {link.Name}");
                    return;
                }

                var where = all.Where(p => p.Name.IndexOf("where", StringComparison.OrdinalIgnoreCase) >= 0 && p.Value.Trim().Length > 0)
                    .Select(p => Regex.Replace(p.Value.Trim(), "^WHERE\\s+", string.Empty, RegexOptions.IgnoreCase)).FirstOrDefault();
                sql = $"SELECT {string.Join(", ", link.Columns.Select(col => col.Name))} FROM {table}" + (where != null ? " WHERE " + where : string.Empty);
                c.Note($"query built from table {table} and the columns of {link.Name}");
            }

            if (Regex.IsMatch(sql, "ORCHESTRATE\\.", RegexOptions.IgnoreCase))
            {
                c.Blocking("the query uses ORCHESTRATE.column placeholders (sparse lookup), which are not converted");
                return;
            }

            c.Call(c.Assign(link), "dsio.read_sql", new List<string> { "ctx", connection, Py.Expand(sql.Trim()), c.Columns(link), "stage=" + Py.Str(c.Stage.Name) });
        }

        private static void Write(StageContext c, Link link, string connection)
        {
            var all = All(c, link, false);
            var userSql = all.Select(p => p.Value.Trim()).FirstOrDefault(v => IsStatement(v, "INSERT", "UPDATE", "DELETE", "MERGE"));
            var table = TableName(all);
            if (userSql == null && table == null)
            {
                c.Blocking($"no table name or SQL statement for input link {link.Name}");
                return;
            }

            var modeText = string.Join(" ", all.Where(p => IsModeProperty(p.Name)).Select(p => p.Value)).ToLowerInvariant();
            var mode = WriteMode(modeText, c);
            var keys = link.Columns.Where(col => col.IsKey).Select(col => col.Name).ToList();
            if (userSql == null && keys.Count == 0 && (mode == "update" || mode == "upsert" || mode == "insert_update" || mode == "delete"))
            {
                c.Blocking($"write mode {mode} needs key columns on link {link.Name}");
                return;
            }

            var args = new List<string> { "ctx", c.Var(link), connection, table == null ? "None" : Py.Expand(table), c.Columns(link), "mode=" + Py.Str(mode) };
            if (keys.Count > 0) args.Add("keys=" + Py.List(keys));
            if (userSql != null)
            {
                args.Add("sql=" + Py.Expand(userSql));
                c.Note("user-defined SQL runs once per row with ORCHESTRATE.column placeholders bound");
            }

            if (mode == "replace") c.Approximation("'replace' deletes all rows and inserts; the table is not dropped and re-created");
            args.Add("stage=" + Py.Str(c.Stage.Name));
            c.Call(null, "dsio.write_table", args);
        }

        private static bool IsStatement(string value, params string[] keywords)
        {
            var text = value.TrimStart();
            return keywords.Any(k => text.StartsWith(k + " ", StringComparison.OrdinalIgnoreCase)
                                  || text.StartsWith(k + "\n", StringComparison.OrdinalIgnoreCase)
                                  || text.StartsWith(k + "\t", StringComparison.OrdinalIgnoreCase));
        }

        private static string? TableName(List<(string Name, string Value)> properties)
        {
            foreach (var (name, value) in properties)
            {
                var lower = name.ToLowerInvariant();
                if (!lower.Contains("table") || lower.Contains("action") || lower.Contains("create") || lower.Contains("drop")) continue;
                var v = value.Trim().Trim('"');
                if (v.Length > 0 && Regex.IsMatch(v, "^[#\\w.$\\[\\]]+$")) return v;
            }

            return null;
        }

        private static bool IsModeProperty(string name)
        {
            var lower = name.ToLowerInvariant();
            return lower.Contains("mode") || lower.Contains("action") || lower.Contains("upsert") || lower.Contains("writemethod");
        }

        private static string WriteMode(string text, StageContext c)
        {
            if (text.Trim().Length == 0) return "append";
            var t = text.Replace("without clearing", string.Empty);
            if (Regex.IsMatch(t.Trim(), "^[\\d\\s]+$"))
            {
                c.Approximation($"update action code '{text.Trim()}' is not interpreted; rows are appended");
                return "append";
            }

            if (t.Contains("replace existing rows")) return "upsert";
            if (t.Contains("truncate") || t.Contains("clear")) return "truncate";
            if (t.Contains("replace")) return "replace";
            bool update = t.Contains("update");
            bool insert = t.Contains("insert");
            if (update && insert) return t.IndexOf("update", StringComparison.Ordinal) < t.IndexOf("insert", StringComparison.Ordinal) ? "upsert" : "insert_update";
            if (t.Contains("upsert")) return "upsert";
            if (update) return "update";
            if (t.Contains("delete")) return "delete";
            return "append";
        }
    }
}
