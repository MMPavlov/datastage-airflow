using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using DataStage2Airflow.Expressions;
using DataStage2Airflow.Model;

namespace DataStage2Airflow.Generation
{
    /// <summary>
    /// The routines module: one Python function per DataStage server routine. Routines whose body is a
    /// single <c>Ans = expression</c> are translated; the others keep their BASIC source in the docstring
    /// and raise NotImplementedError until someone ports them.
    /// </summary>
    public static class RoutinesGenerator
    {
        public static string Generate(DsProject project, IEnumerable<string> referencedNames, out int translated)
        {
            translated = 0;
            var w = new PythonWriter();
            w.Docstring(
                $"Server routines of DataStage project {project.Name}, used by the converted jobs and sequences.\n\n" +
                "Each routine keeps its DataStage BASIC source in the docstring. Routines that consist of a single\n" +
                "Ans = <expression> were translated; port the others and remove the NotImplementedError.\n");
            w.Line();
            w.Line("from __future__ import annotations");
            w.Line();
            w.Line("from decimal import Decimal  # noqa: F401");
            w.Line();
            w.Line("from ds2af_runtime import dsfunc as F  # noqa: F401");

            var used = new HashSet<string>(StringComparer.Ordinal);
            var written = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var routine in project.Routines)
            {
                if (!written.Add(RoutineNames.Function(routine.Name))) continue;
                w.Line();
                w.Line();
                if (WriteRoutine(w, routine, used)) translated++;
            }

            foreach (var name in referencedNames)
            {
                var function = RoutineNames.Function(name);
                if (!written.Add(function)) continue;
                w.Line();
                w.Line();
                using (w.Block($"def {function}(*args):"))
                {
                    w.Docstring($"{name}: called by a sequence, but not included in the export.");
                    w.Line($"raise NotImplementedError({PythonTranslator.PyString("routine " + name + " is not in the DataStage export")})");
                }
            }

            return w.ToString();
        }

        private static bool WriteRoutine(PythonWriter w, DsRoutine routine, HashSet<string> used)
        {
            var function = RoutineNames.Function(routine.Name);
            var argNames = new HashSet<string>(StringComparer.Ordinal);
            var parameters = routine.Arguments.Select(a => Naming.Unique(Naming.Identifier(a), argNames)).ToList();
            var signature = parameters.Count > 0 ? string.Join(", ", parameters) : "*args";

            var doc = new StringBuilder();
            doc.Append(routine.Name);
            if (routine.Category.Length > 0) doc.Append($" ({routine.Category})");
            if (routine.Description.Length > 0) doc.Append(": ").Append(routine.Description);
            doc.Append('\n');
            if (routine.Source.Trim().Length > 0)
            {
                doc.Append("\nDataStage BASIC:\n\n");
                foreach (var line in routine.Source.Replace("\r\n", "\n").Split('\n')) doc.Append("    ").Append(line).Append('\n');
            }

            string? body = TranslateSingleAssignment(routine, parameters);
            using (w.Block($"def {function}({signature}):"))
            {
                w.Docstring(doc.ToString());
                if (body != null)
                {
                    w.Comment("Translated automatically from the single Ans = ... statement.");
                    w.Line("return " + body);
                }
                else
                {
                    w.Line($"raise NotImplementedError({PythonTranslator.PyString("routine " + routine.Name + " has not been ported from DataStage BASIC")})");
                }
            }

            return body != null;
        }

        private static string? TranslateSingleAssignment(DsRoutine routine, List<string> parameters)
        {
            var statements = routine.Source.Replace("\r\n", "\n").Split('\n')
                .Select(l => l.Trim())
                .Where(l => l.Length > 0 && !l.StartsWith("*", StringComparison.Ordinal) && !l.StartsWith("!", StringComparison.Ordinal)
                            && !l.StartsWith("REM ", StringComparison.OrdinalIgnoreCase) && !l.StartsWith("$", StringComparison.Ordinal))
                .ToList();
            if (statements.Count != 1) return null;
            var m = Regex.Match(statements[0], "^Ans\\s*=\\s*(.+)$", RegexOptions.IgnoreCase);
            if (!m.Success) return null;

            var resolver = new DictionaryResolver();
            for (int i = 0; i < routine.Arguments.Count && i < parameters.Count; i++) resolver.Add(routine.Arguments[i], parameters[i]);
            var result = new PythonTranslator(resolver, ExpressionDialect.Basic).TranslateValue(m.Groups[1].Value);
            return result.Ok ? result.Code : null;
        }
    }
}
