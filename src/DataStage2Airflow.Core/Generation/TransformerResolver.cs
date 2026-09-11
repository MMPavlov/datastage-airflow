using System;
using System.Collections.Generic;
using System.Linq;
using DataStage2Airflow.Expressions;
using DataStage2Airflow.Model;

namespace DataStage2Airflow.Generation
{
    /// <summary>Names visible inside a transformer: input and reference link columns, stage variables,
    /// job parameters, macros and @INROWNUM/@OUTROWNUM.</summary>
    internal sealed class TransformerResolver : INameResolver
    {
        private static readonly HashSet<string> Macros = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "DSJobName", "DSProjectName", "DSJobInvocationId", "DSJobController", "DSHostName",
            "DSJobStartDate", "DSJobStartTime", "DSJobStartTimestamp", "DSJobWaveNo",
        };

        private readonly StageContext _context;
        private readonly Link? _primary;
        private readonly Dictionary<string, (Link Link, string Row, string NotFound)> _references =
            new Dictionary<string, (Link, string, string)>(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, (string Var, DsLogicalType Type)> _stageVariables =
            new Dictionary<string, (string, DsLogicalType)>(StringComparer.Ordinal);

        public TransformerResolver(StageContext context, Link? primary)
        {
            _context = context;
            _primary = primary;
        }

        /// <summary>Set when a translated expression used @OUTROWNUM.</summary>
        public bool UsesOutRowNumber { get; set; }

        public void AddReference(Link link, string rowVariable, string notFoundVariable) =>
            _references[link.Name] = (link, rowVariable, notFoundVariable);

        public void AddStageVariable(string name, string variable, DsLogicalType type) =>
            _stageVariables[name] = (variable, type);

        public ResolvedName? Resolve(string name)
        {
            int dot = name.IndexOf('.');
            if (dot > 0)
            {
                var linkName = name.Substring(0, dot);
                var column = name.Substring(dot + 1);
                if (_primary != null && string.Equals(linkName, _primary.Name, StringComparison.OrdinalIgnoreCase))
                {
                    return ColumnOf(_primary, column, "row", false);
                }

                if (_references.TryGetValue(linkName, out var reference))
                {
                    if (string.Equals(column, "NOTFOUND", StringComparison.OrdinalIgnoreCase))
                    {
                        return new ResolvedName(reference.NotFound, DsLogicalType.Integer, false);
                    }

                    return ColumnOf(reference.Link, column, reference.Row, true);
                }
            }

            if (_stageVariables.TryGetValue(name, out var variable))
            {
                return new ResolvedName(variable.Var, variable.Type, true);
            }

            if (string.Equals(name, "@INROWNUM", StringComparison.OrdinalIgnoreCase))
            {
                return new ResolvedName("in_row_num", DsLogicalType.Integer, false);
            }

            if (string.Equals(name, "@OUTROWNUM", StringComparison.OrdinalIgnoreCase))
            {
                UsesOutRowNumber = true;
                return new ResolvedName("out_row_num", DsLogicalType.Integer, false);
            }

            return ResolveShared(_context, name);
        }

        public ResolvedName? ResolveParameterReference(string name) => ResolveShared(_context, name);

        public string? ResolveRoutine(string name) => ResolveRoutine(_context, name);

        /// <summary>Parameters and macros, valid in every expression of a job.</summary>
        public static ResolvedName? ResolveShared(StageContext context, string name)
        {
            var parameter = context.Job.FindParameter(name);
            if (parameter != null)
            {
                var type = parameter.Type == ParameterType.Integer ? DsLogicalType.Integer
                    : parameter.Type == ParameterType.Float ? DsLogicalType.Decimal
                    : DsLogicalType.String;
                return new ResolvedName($"ctx.params[{Py.Str(parameter.Name)}]", type, parameter.IsEnvironmentVariable);
            }

            if (Macros.Contains(name))
            {
                return new ResolvedName($"ctx.macro({Py.Str(name)})", DsLogicalType.String, false);
            }

            return null;
        }

        public static string? ResolveRoutine(StageContext context, string name)
        {
            var bare = name.StartsWith("DSU.", StringComparison.OrdinalIgnoreCase) ? name.Substring(4) : name;
            var routine = context.Project.FindRoutine(bare);
            if (routine == null) return null;
            context.State.UsesRoutines = true;
            return "routines." + RoutineNames.Function(routine.Name);
        }

        private ResolvedName ColumnOf(Link link, string column, string row, bool forceNullable)
        {
            var metadata = link.FindColumn(column);
            if (metadata == null)
            {
                _context.Approximation($"column {link.Name}.{column} is not in the link metadata");
                return new ResolvedName($"{row}.get({Py.Str(column)})", DsLogicalType.Unknown, true);
            }

            var type = _context.Typed ? metadata.LogicalType : DsLogicalType.String;
            return new ResolvedName($"{row}[{Py.Str(metadata.Name)}]", type, forceNullable || metadata.Nullable);
        }
    }

    /// <summary>Python names for DataStage routines, shared by jobs, sequences and the routines module.</summary>
    internal static class RoutineNames
    {
        public static string Function(string routineName)
        {
            var bare = routineName.StartsWith("DSU.", StringComparison.OrdinalIgnoreCase) ? routineName.Substring(4) : routineName;
            return Naming.Identifier(bare);
        }
    }
}
