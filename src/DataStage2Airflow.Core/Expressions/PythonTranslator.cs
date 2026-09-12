using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using DataStage2Airflow.Model;

namespace DataStage2Airflow.Expressions
{
    /// <summary>What a name in an expression stands for, as Python code.</summary>
    public sealed class ResolvedName
    {
        public ResolvedName(string code, DsLogicalType type = DsLogicalType.Unknown, bool nullable = true)
        {
            Code = code;
            Type = type;
            Nullable = nullable;
        }

        public string Code { get; }

        public DsLogicalType Type { get; }

        public bool Nullable { get; }
    }

    /// <summary>Supplies the meaning of names for one translation context (a transformer, a sequence...).</summary>
    public interface INameResolver
    {
        /// <summary>A bare or dotted name: link.column, stage variable, parameter, macro, @variable, Activity.$JobStatus.</summary>
        ResolvedName? Resolve(string name);

        /// <summary>A <c>#Name#</c> reference.</summary>
        ResolvedName? ResolveParameterReference(string name);

        /// <summary>The Python callable for a function that is not a DataStage built-in (a routine), or null.</summary>
        string? ResolveRoutine(string name);
    }

    public sealed class TranslationResult
    {
        public TranslationResult(string code, DsLogicalType type, bool isBoolean, bool nullable, IReadOnlyList<string> problems, IReadOnlyList<string> notes)
        {
            Code = code;
            Type = type;
            IsBoolean = isBoolean;
            Nullable = nullable;
            Problems = problems;
            Notes = notes;
        }

        public string Code { get; }

        public DsLogicalType Type { get; }

        public bool IsBoolean { get; }

        public bool Nullable { get; }

        /// <summary>Things that could not be translated; the generated code raises at run time where they are used.</summary>
        public IReadOnlyList<string> Problems { get; }

        /// <summary>Translated, but worth a look (calls to user routines).</summary>
        public IReadOnlyList<string> Notes { get; }

        public bool Ok => Problems.Count == 0;
    }

    /// <summary>
    /// Translates DataStage expressions to Python source using the <c>dsfunc</c> runtime module.
    /// Values keep DataStage semantics: null propagates through operators and functions, conditions use
    /// DataStage truthiness, and BASIC comparisons and arithmetic treat numeric strings as numbers.
    /// Plain Python operators are emitted only when both operands are non-null and of compatible types.
    /// </summary>
    public sealed class PythonTranslator
    {
        private const int PrecConditional = 1;
        private const int PrecOr = 2;
        private const int PrecAnd = 3;
        private const int PrecNot = 4;
        private const int PrecCompare = 5;
        private const int PrecAdd = 10;
        private const int PrecMul = 11;
        private const int PrecUnary = 12;
        private const int PrecPower = 13;
        private const int PrecAtom = 20;

        private static readonly Dictionary<string, Tuple<string, DsLogicalType, bool>> SystemValues =
            new Dictionary<string, Tuple<string, DsLogicalType, bool>>(StringComparer.OrdinalIgnoreCase)
            {
                ["@NULL"] = Tuple.Create("None", DsLogicalType.Unknown, true),
                ["@TRUE"] = Tuple.Create("1", DsLogicalType.Integer, false),
                ["@FALSE"] = Tuple.Create("0", DsLogicalType.Integer, false),
                ["@FM"] = Tuple.Create("{F}.FM", DsLogicalType.String, false),
                ["@AM"] = Tuple.Create("{F}.FM", DsLogicalType.String, false),
                ["@VM"] = Tuple.Create("{F}.VM", DsLogicalType.String, false),
                ["@SM"] = Tuple.Create("{F}.SM", DsLogicalType.String, false),
                ["@SVM"] = Tuple.Create("{F}.SM", DsLogicalType.String, false),
                ["@TM"] = Tuple.Create("{F}.TM", DsLogicalType.String, false),
                ["@DATE"] = Tuple.Create("{F}.basic_date()", DsLogicalType.Integer, false),
                ["@TIME"] = Tuple.Create("{F}.basic_time()", DsLogicalType.Integer, false),
                ["@DAY"] = Tuple.Create("{F}.basic_day()", DsLogicalType.Integer, false),
                ["@MONTH"] = Tuple.Create("{F}.basic_month()", DsLogicalType.Integer, false),
                ["@YEAR"] = Tuple.Create("{F}.basic_year()", DsLogicalType.Integer, false),
                ["@YEAR4"] = Tuple.Create("{F}.basic_year4()", DsLogicalType.Integer, false),
                ["@PARTITIONNUM"] = Tuple.Create("0", DsLogicalType.Integer, false),
                ["@NUMPARTITIONS"] = Tuple.Create("1", DsLogicalType.Integer, false),
            };

        private readonly INameResolver _resolver;
        private readonly ExpressionDialect _dialect;
        private readonly string _f;
        private List<string> _problems = new List<string>();
        private List<string> _notes = new List<string>();

        public PythonTranslator(INameResolver resolver, ExpressionDialect dialect, string runtimeAlias = "F")
        {
            _resolver = resolver;
            _dialect = dialect;
            _f = runtimeAlias;
        }

        public ExpressionDialect Dialect => _dialect;

        public TranslationResult Translate(string text)
        {
            _problems = new List<string>();
            _notes = new List<string>();
            Expr expr;
            try
            {
                expr = ExpressionParser.Parse(text, _dialect);
            }
            catch (ExpressionSyntaxException ex)
            {
                return new TranslationResult(
                    $"{_f}.untranslated({PyString(Collapse(text))})",
                    DsLogicalType.Unknown,
                    false,
                    true,
                    new[] { "syntax error: " + ex.Message },
                    Array.Empty<string>());
            }

            var py = Emit(expr);
            return new TranslationResult(py.Code, py.Type, py.IsBool, py.Nullable, _problems, _notes);
        }

        /// <summary>An expression used as a condition (constraint, trigger, IF): non-boolean values go through DataStage truthiness.</summary>
        public TranslationResult TranslateCondition(string text)
        {
            var result = Translate(text);
            if (result.IsBoolean) return result;
            return new TranslationResult($"{_f}.truth({result.Code})", DsLogicalType.Integer, true, false, result.Problems, result.Notes);
        }

        /// <summary>An expression whose value is stored (column derivation, stage variable): booleans become 1/0.</summary>
        public TranslationResult TranslateValue(string text)
        {
            var result = Translate(text);
            if (!result.IsBoolean) return result;
            return new TranslationResult($"{_f}.flag({result.Code})", DsLogicalType.Integer, false, result.Nullable, result.Problems, result.Notes);
        }

        // ------------------------------------------------------------------ emission

        private sealed class Py
        {
            public Py(string code, int prec, DsLogicalType type, bool nullable, bool isBool = false)
            {
                Code = code;
                Prec = prec;
                Type = type;
                Nullable = nullable;
                IsBool = isBool;
            }

            public string Code { get; }

            public int Prec { get; }

            public DsLogicalType Type { get; }

            public bool Nullable { get; }

            public bool IsBool { get; }
        }

        private Py Emit(Expr expr)
        {
            switch (expr)
            {
                case NumberExpr n:
                    return EmitNumber(n);
                case StringExpr s:
                    return new Py(PyString(s.Value), PrecAtom, DsLogicalType.String, false);
                case ParamRefExpr p:
                    return EmitParameterReference(p);
                case NameExpr n:
                    return EmitName(n.Name);
                case UnaryExpr u:
                    return EmitUnary(u);
                case BinaryExpr b:
                    return EmitBinary(b);
                case IfExpr i:
                    return EmitIf(i);
                case CallExpr c:
                    return EmitCall(c);
                case SubscriptExpr s:
                    return EmitSubscript(s);
                default:
                    throw new InvalidOperationException("unknown expression node " + expr.GetType().Name);
            }
        }

        private Py EmitNumber(NumberExpr n)
        {
            var text = n.Text;
            if (n.IsInteger)
            {
                var trimmed = text.TrimStart('0');
                return new Py(trimmed.Length == 0 ? "0" : trimmed, PrecAtom, DsLogicalType.Integer, false);
            }

            if (text.IndexOf('e') >= 0 || text.IndexOf('E') >= 0)
            {
                return new Py(text.StartsWith(".", StringComparison.Ordinal) ? "0" + text : text, PrecAtom, DsLogicalType.Float, false);
            }

            return new Py($"Decimal({PyString(text)})", PrecAtom, DsLogicalType.Decimal, false);
        }

        private Py EmitParameterReference(ParamRefExpr p)
        {
            var resolved = _resolver.ResolveParameterReference(p.Name) ?? _resolver.Resolve(p.Name);
            if (resolved != null) return new Py(resolved.Code, PrecAtom, resolved.Type, resolved.Nullable);
            return Unresolved("#" + p.Name + "#", "unknown parameter");
        }

        private Py EmitName(string name)
        {
            var resolved = _resolver.Resolve(name);
            if (resolved != null) return new Py(resolved.Code, AtomOrCall(resolved.Code), resolved.Type, resolved.Nullable);

            if (SystemValues.TryGetValue(name, out var system))
            {
                return new Py(system.Item1.Replace("{F}", _f), PrecAtom, system.Item2, system.Item3);
            }

            if (name.StartsWith("DSJS.", StringComparison.OrdinalIgnoreCase) || name.StartsWith("DSJ.", StringComparison.OrdinalIgnoreCase))
            {
                int dot = name.IndexOf('.');
                var group = name.Substring(0, dot).ToUpperInvariant();
                return new Py($"{_f}.{group}.{name.Substring(dot + 1).ToUpperInvariant().Replace('.', '_')}", PrecAtom, DsLogicalType.Integer, false);
            }

            return Unresolved(name, name.StartsWith("@", StringComparison.Ordinal) ? "unsupported system variable" : "unknown name");
        }

        private Py Unresolved(string name, string reason)
        {
            _problems.Add($"{reason} '{name}'");
            return new Py($"{_f}.unresolved({PyString(name)})", PrecAtom, DsLogicalType.Unknown, true);
        }

        private static int AtomOrCall(string code)
        {
            foreach (char c in code)
            {
                if (c == ' ' || c == '+' || c == '-' || c == '*' || c == '/') return PrecUnary;
            }

            return PrecAtom;
        }

        private Py EmitUnary(UnaryExpr u)
        {
            var operand = Emit(u.Operand);
            switch (u.Op)
            {
                case "+":
                    return operand;
                case "-":
                    if (operand.Code.Length > 0 && char.IsDigit(operand.Code[0]) && operand.Prec == PrecAtom)
                    {
                        return new Py("-" + operand.Code, PrecUnary, operand.Type, false);
                    }

                    if (IsPlainNumeric(operand))
                    {
                        return new Py("-" + Wrap(operand, PrecUnary), PrecUnary, operand.Type, false);
                    }

                    return new Py($"{_f}.neg({operand.Code})", PrecAtom, operand.Type, operand.Nullable);
                default:
                    // NOT of null is null (BASIC and SQL three-valued logic), so a null operand keeps a condition false.
                    if (operand.Nullable) return new Py($"{_f}.not_({operand.Code})", PrecAtom, DsLogicalType.Integer, true, true);
                    return new Py("not " + Wrap(AsCondition(operand), PrecNot), PrecNot, DsLogicalType.Integer, false, true);
            }
        }

        private Py EmitBinary(BinaryExpr b)
        {
            if (b.Op == BinaryOp.Concat) return EmitConcat(b);

            var left = Emit(b.Left);
            var right = Emit(b.Right);
            switch (b.Op)
            {
                case BinaryOp.Add:
                case BinaryOp.Subtract:
                case BinaryOp.Multiply:
                    return EmitArithmetic(b.Op, left, right);
                case BinaryOp.Divide:
                    return Call(_dialect == ExpressionDialect.Basic ? "basic_divide" : "divide", NumericResult(left.Type, right.Type, true), left.Nullable || right.Nullable, left, right);
                case BinaryOp.Power:
                    return Call(_dialect == ExpressionDialect.Basic ? "basic_pwr" : "pwr", DsLogicalType.Float, left.Nullable || right.Nullable, left, right);
                case BinaryOp.And:
                    return new Py(Wrap(AsCondition(left), PrecAnd) + " and " + Wrap(AsCondition(right), PrecAnd + 1), PrecAnd, DsLogicalType.Integer, false, true);
                case BinaryOp.Or:
                    return new Py(Wrap(AsCondition(left), PrecOr) + " or " + Wrap(AsCondition(right), PrecOr + 1), PrecOr, DsLogicalType.Integer, false, true);
                case BinaryOp.Matches:
                    var matches = Call("matches", DsLogicalType.Integer, left.Nullable || right.Nullable, left, right);
                    return new Py(matches.Code, PrecAtom, DsLogicalType.Integer, matches.Nullable, true);
                default:
                    return EmitComparison(b.Op, left, right);
            }
        }

        private Py EmitConcat(BinaryExpr b)
        {
            var parts = new List<Expr>();
            Flatten(b, parts);
            var emitted = parts.Select(Emit).ToList();
            bool nullable = emitted.Any(p => p.Nullable);
            return new Py($"{_f}.concat({string.Join(", ", emitted.Select(p => p.Code))})", PrecAtom, DsLogicalType.String, nullable);
        }

        private static void Flatten(Expr expr, List<Expr> parts)
        {
            if (expr is BinaryExpr b && b.Op == BinaryOp.Concat)
            {
                Flatten(b.Left, parts);
                Flatten(b.Right, parts);
                return;
            }

            parts.Add(expr);
        }

        private Py EmitArithmetic(BinaryOp op, Py left, Py right)
        {
            var type = NumericResult(left.Type, right.Type, false);
            if (IsPlainNumeric(left) && IsPlainNumeric(right) && !MixesDecimalAndFloat(left.Type, right.Type))
            {
                string symbol = op == BinaryOp.Add ? " + " : op == BinaryOp.Subtract ? " - " : " * ";
                int prec = op == BinaryOp.Multiply ? PrecMul : PrecAdd;
                return new Py(Wrap(left, prec) + symbol + Wrap(right, prec + 1), prec, type, false);
            }

            string name = op == BinaryOp.Add ? "add" : op == BinaryOp.Subtract ? "sub" : "mul";
            if (_dialect == ExpressionDialect.Basic) name = "basic_" + name;
            return Call(name, type, left.Nullable || right.Nullable, left, right);
        }

        private Py EmitComparison(BinaryOp op, Py left, Py right)
        {
            string name;
            string symbol;
            switch (op)
            {
                case BinaryOp.Eq:
                    name = "eq";
                    symbol = " == ";
                    break;
                case BinaryOp.Ne:
                    name = "ne";
                    symbol = " != ";
                    break;
                case BinaryOp.Lt:
                    name = "lt";
                    symbol = " < ";
                    break;
                case BinaryOp.Gt:
                    name = "gt";
                    symbol = " > ";
                    break;
                case BinaryOp.Le:
                    name = "le";
                    symbol = " <= ";
                    break;
                default:
                    name = "ge";
                    symbol = " >= ";
                    break;
            }

            if (_dialect == ExpressionDialect.Parallel && !left.Nullable && !right.Nullable && Comparable(left.Type, right.Type))
            {
                return new Py(Wrap(left, PrecCompare + 1) + symbol + Wrap(right, PrecCompare + 1), PrecCompare, DsLogicalType.Integer, false, true);
            }

            if (_dialect == ExpressionDialect.Basic) name = "basic_" + name;
            var call = Call(name, DsLogicalType.Integer, left.Nullable || right.Nullable, left, right);
            return new Py(call.Code, PrecAtom, DsLogicalType.Integer, call.Nullable, true);
        }

        private Py EmitIf(IfExpr i)
        {
            var condition = AsCondition(Emit(i.Condition));
            var then = Emit(i.Then);
            var otherwise = Emit(i.Else);
            var type = then.Type == otherwise.Type ? then.Type : MergeTypes(then.Type, otherwise.Type);
            var code = Wrap(then, PrecConditional + 1) + " if " + Wrap(condition, PrecConditional + 1) + " else " + Wrap(otherwise, PrecConditional);
            return new Py(code, PrecConditional, type, then.Nullable || otherwise.Nullable, then.IsBool && otherwise.IsBool);
        }

        private Py EmitCall(CallExpr c)
        {
            var args = c.Args.Select(Emit).ToList();
            var info = FunctionCatalog.Find(c.Name);
            if (info != null)
            {
                if (args.Count < info.MinArgs || args.Count > info.MaxArgs)
                {
                    var expected = info.MinArgs == info.MaxArgs ? info.MinArgs.ToString(CultureInfo.InvariantCulture) : $"{info.MinArgs}-{info.MaxArgs}";
                    _problems.Add($"{info.Name} expects {expected} argument(s), got {args.Count}");
                }

                if (info.Python == "set_null") return new Py("None", PrecAtom, DsLogicalType.Unknown, true);

                var type = (info.Flags & FunctionFlags.SameTypeAsArgument) != 0 && args.Count > 0 ? args[0].Type : info.Returns;
                bool nullable = info.HandlesNull ? info.Python == "null_to_value" && args.Count > 1 && args[1].Nullable : args.Any(a => a.Nullable);
                if (info.Python == "null_to_value" && args.Count > 1 && type == DsLogicalType.Unknown) type = MergeTypes(args[0].Type, args[1].Type);
                var call = Call(info.Python, type, nullable, args.ToArray());
                return info.IsBoolean ? new Py(call.Code, PrecAtom, DsLogicalType.Integer, nullable, true) : call;
            }

            var routine = _resolver.ResolveRoutine(c.Name);
            if (routine != null)
            {
                _notes.Add($"calls routine {c.Name}");
                return new Py($"{routine}({string.Join(", ", args.Select(a => a.Code))})", PrecAtom, DsLogicalType.Unknown, true);
            }

            _problems.Add($"unsupported function {c.Name}()");
            var allArgs = new List<string> { PyString(c.Name) };
            allArgs.AddRange(args.Select(a => a.Code));
            return new Py($"{_f}.unsupported({string.Join(", ", allArgs)})", PrecAtom, DsLogicalType.Unknown, true);
        }

        private Py EmitSubscript(SubscriptExpr s)
        {
            var target = Emit(s.Target);
            var args = s.Args.Select(Emit).ToList();
            var all = new List<Py> { target };
            all.AddRange(args);
            bool nullable = all.Any(a => a.Nullable);
            switch (args.Count)
            {
                case 1:
                    return Call("substr_right", DsLogicalType.String, nullable, all.ToArray());
                case 2:
                    return Call("substr", DsLogicalType.String, nullable, all.ToArray());
                default:
                    return Call("field", DsLogicalType.String, nullable, all.ToArray());
            }
        }

        // ------------------------------------------------------------------ helpers

        private Py Call(string function, DsLogicalType type, bool nullable, params Py[] args) =>
            new Py($"{_f}.{function}({string.Join(", ", args.Select(a => a.Code))})", PrecAtom, type, nullable);

        private Py AsCondition(Py value)
        {
            if (value.IsBool) return value;
            return new Py($"{_f}.truth({value.Code})", PrecAtom, DsLogicalType.Integer, false, true);
        }

        private bool IsPlainNumeric(Py value) =>
            _dialect == ExpressionDialect.Parallel && !value.Nullable && IsNumeric(value.Type);

        private static bool IsNumeric(DsLogicalType type) =>
            type == DsLogicalType.Integer || type == DsLogicalType.Decimal || type == DsLogicalType.Float;

        private static bool MixesDecimalAndFloat(DsLogicalType a, DsLogicalType b) =>
            (a == DsLogicalType.Decimal && b == DsLogicalType.Float) || (a == DsLogicalType.Float && b == DsLogicalType.Decimal);

        private static bool Comparable(DsLogicalType a, DsLogicalType b)
        {
            if (a == DsLogicalType.Unknown || b == DsLogicalType.Unknown) return false;
            if (IsNumeric(a) && IsNumeric(b)) return true;
            return a == b;
        }

        private static DsLogicalType NumericResult(DsLogicalType a, DsLogicalType b, bool division)
        {
            if (!IsNumeric(a) || !IsNumeric(b)) return DsLogicalType.Unknown;
            if (a == DsLogicalType.Float || b == DsLogicalType.Float) return DsLogicalType.Float;
            if (a == DsLogicalType.Decimal || b == DsLogicalType.Decimal) return DsLogicalType.Decimal;
            return division ? DsLogicalType.Float : DsLogicalType.Integer;
        }

        private static DsLogicalType MergeTypes(DsLogicalType a, DsLogicalType b)
        {
            if (a == b) return a;
            if (a == DsLogicalType.Unknown) return b;
            if (b == DsLogicalType.Unknown) return a;
            if (IsNumeric(a) && IsNumeric(b)) return NumericResult(a, b, false);
            return DsLogicalType.Unknown;
        }

        private static string Wrap(Py value, int minimumPrecedence) =>
            value.Prec < minimumPrecedence ? "(" + value.Code + ")" : value.Code;

        private static string Collapse(string text) => string.Join(" ", text.Split(new[] { ' ', '\t', '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries));

        /// <summary>A Python string literal; non-ASCII and control characters are escaped so sources stay ASCII.</summary>
        public static string PyString(string value)
        {
            var sb = new StringBuilder(value.Length + 2);
            sb.Append('"');
            for (int i = 0; i < value.Length; i++)
            {
                char c = value[i];
                switch (c)
                {
                    case '\\':
                        sb.Append("\\\\");
                        break;
                    case '"':
                        sb.Append("\\\"");
                        break;
                    case '\n':
                        sb.Append("\\n");
                        break;
                    case '\r':
                        sb.Append("\\r");
                        break;
                    case '\t':
                        sb.Append("\\t");
                        break;
                    default:
                        if (char.IsHighSurrogate(c) && i + 1 < value.Length && char.IsLowSurrogate(value[i + 1]))
                        {
                            int codePoint = char.ConvertToUtf32(c, value[i + 1]);
                            sb.Append("\\U").Append(codePoint.ToString("x8", CultureInfo.InvariantCulture));
                            i++;
                        }
                        else if (c < 0x20 || c == 0x7F)
                        {
                            sb.Append("\\x").Append(((int)c).ToString("x2", CultureInfo.InvariantCulture));
                        }
                        else if (c > 0x7E)
                        {
                            sb.Append("\\u").Append(((int)c).ToString("x4", CultureInfo.InvariantCulture));
                        }
                        else
                        {
                            sb.Append(c);
                        }

                        break;
                }
            }

            sb.Append('"');
            return sb.ToString();
        }
    }

    /// <summary>A resolver backed by a dictionary; handy for tests and for simple contexts.</summary>
    public sealed class DictionaryResolver : INameResolver
    {
        private readonly Dictionary<string, ResolvedName> _names = new Dictionary<string, ResolvedName>(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, string> _routines = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        public DictionaryResolver Add(string name, string code, DsLogicalType type = DsLogicalType.Unknown, bool nullable = true)
        {
            _names[name] = new ResolvedName(code, type, nullable);
            return this;
        }

        public DictionaryResolver AddRoutine(string name, string code)
        {
            _routines[name] = code;
            return this;
        }

        public ResolvedName? Resolve(string name) => _names.TryGetValue(name, out var r) ? r : null;

        public ResolvedName? ResolveParameterReference(string name) => Resolve(name);

        public string? ResolveRoutine(string name) => _routines.TryGetValue(name, out var r) ? r : null;
    }
}
