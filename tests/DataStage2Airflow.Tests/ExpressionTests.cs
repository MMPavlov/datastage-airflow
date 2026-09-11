using System.Linq;
using DataStage2Airflow.Expressions;
using DataStage2Airflow.Model;
using Xunit;

namespace DataStage2Airflow.Tests
{
    public class LexerTests
    {
        [Fact]
        public void Strings_can_use_three_delimiters()
        {
            var tokens = ExpressionLexer.Tokenize("\"a'b\" : 'c\"d' : \\e\"f'\\");
            var strings = tokens.Where(t => t.Kind == TokenKind.String).Select(t => t.Text).ToArray();
            Assert.Equal(new[] { "a'b", "c\"d", "e\"f'" }, strings);
        }

        [Fact]
        public void Hash_is_parameter_reference_or_not_equal()
        {
            var tokens = ExpressionLexer.Tokenize("#SourceDir# : X # 3");
            Assert.Equal(TokenKind.ParamRef, tokens[0].Kind);
            Assert.Equal("SourceDir", tokens[0].Text);
            Assert.True(tokens[3].IsOperator("#"));
        }

        [Fact]
        public void Identifiers_keep_dots_dollars_and_at_signs()
        {
            var names = ExpressionLexer.Tokenize("lnk.COL Job_1.$JobStatus @INROWNUM DSJS.RUNOK")
                .Where(t => t.Kind == TokenKind.Identifier).Select(t => t.Text).ToArray();
            Assert.Equal(new[] { "lnk.COL", "Job_1.$JobStatus", "@INROWNUM", "DSJS.RUNOK" }, names);
        }

        [Fact]
        public void Basic_alternative_operators()
        {
            var ops = ExpressionLexer.Tokenize("a >< b =< c => d ** e")
                .Where(t => t.Kind == TokenKind.Operator).Select(t => t.Text).ToArray();
            Assert.Equal(new[] { "<>", "<=", ">=", "^" }, ops);
        }
    }

    public class ParserTests
    {
        [Fact]
        public void Basic_and_or_share_precedence_left_to_right()
        {
            var expr = (BinaryExpr)ExpressionParser.Parse("A OR B AND C", ExpressionDialect.Basic);
            Assert.Equal(BinaryOp.And, expr.Op);
            Assert.Equal(BinaryOp.Or, ((BinaryExpr)expr.Left).Op);
        }

        [Fact]
        public void Parallel_and_binds_tighter_than_or()
        {
            var expr = (BinaryExpr)ExpressionParser.Parse("A OR B AND C", ExpressionDialect.Parallel);
            Assert.Equal(BinaryOp.Or, expr.Op);
            Assert.Equal(BinaryOp.And, ((BinaryExpr)expr.Right).Op);
        }

        [Fact]
        public void Concatenation_binds_looser_than_addition()
        {
            var expr = (BinaryExpr)ExpressionParser.Parse("\"A\" : 1 + 2", ExpressionDialect.Parallel);
            Assert.Equal(BinaryOp.Concat, expr.Op);
            Assert.Equal(BinaryOp.Add, ((BinaryExpr)expr.Right).Op);
        }

        [Fact]
        public void Nested_if_then_else_on_several_lines()
        {
            var expr = ExpressionParser.Parse("IF WSLength >= 20 Then\n  X\nElse If Y Then 1 Else 2", ExpressionDialect.Parallel);
            var outer = Assert.IsType<IfExpr>(expr);
            Assert.IsType<IfExpr>(outer.Else);
        }

        [Fact]
        public void Substring_forms()
        {
            Assert.Equal(2, Assert.IsType<SubscriptExpr>(ExpressionParser.Parse("x[1,3]", ExpressionDialect.Basic)).Args.Count);
            Assert.Single(Assert.IsType<SubscriptExpr>(ExpressionParser.Parse("x[3]", ExpressionDialect.Basic)).Args);
            Assert.Equal(3, Assert.IsType<SubscriptExpr>(ExpressionParser.Parse("x[',',2,1]", ExpressionDialect.Basic)).Args.Count);
        }

        [Fact]
        public void Syntax_errors_are_reported()
        {
            Assert.False(ExpressionParser.TryParse("If A Then B", ExpressionDialect.Basic, out _, out var error));
            Assert.Contains("ELSE", error);
            Assert.False(ExpressionParser.TryParse("Trim(a", ExpressionDialect.Basic, out _, out _));
        }

        [Fact]
        public void Parsed_derivation_form_is_accepted()
        {
            // DataStage's ParsedDerivation wraps every argument in parentheses.
            var call = Assert.IsType<CallExpr>(ExpressionParser.Parse("UtilityHashLookup((\"DS_JOBS\"), (input.JobName), (5))", ExpressionDialect.Basic));
            Assert.Equal(3, call.Args.Count);
        }
    }

    public class TranslatorTests
    {
        private static PythonTranslator Parallel()
        {
            var resolver = new DictionaryResolver()
                .Add("lnk.NAME", "row[\"NAME\"]", DsLogicalType.String, nullable: false)
                .Add("lnk.CITY", "row[\"CITY\"]", DsLogicalType.String, nullable: true)
                .Add("lnk.QTY", "row[\"QTY\"]", DsLogicalType.Integer, nullable: false)
                .Add("lnk.PRICE", "row[\"PRICE\"]", DsLogicalType.Decimal, nullable: false)
                .Add("lnk.RATE", "row[\"RATE\"]", DsLogicalType.Float, nullable: false)
                .Add("lnk.AMOUNT", "row[\"AMOUNT\"]", DsLogicalType.Decimal, nullable: true)
                .Add("RunDate", "p[\"RunDate\"]", DsLogicalType.String, nullable: false)
                .Add("@INROWNUM", "in_row_num", DsLogicalType.Integer, nullable: false)
                .AddRoutine("MyRoutine", "routines.my_routine");
            return new PythonTranslator(resolver, ExpressionDialect.Parallel);
        }

        private static PythonTranslator Basic() =>
            new PythonTranslator(new DictionaryResolver().Add("in.X", "row[\"X\"]", DsLogicalType.String, false), ExpressionDialect.Basic);

        [Fact]
        public void Functions_map_to_runtime()
        {
            var result = Parallel().Translate("UpCase(Trim(lnk.NAME))");
            Assert.True(result.Ok);
            Assert.Equal("F.upcase(F.trim(row[\"NAME\"]))", result.Code);
            Assert.Equal(DsLogicalType.String, result.Type);
        }

        [Fact]
        public void Plain_operators_for_non_null_compatible_numbers()
        {
            Assert.Equal("row[\"QTY\"] * row[\"PRICE\"]", Parallel().Translate("lnk.QTY * lnk.PRICE").Code);
            Assert.Equal("(row[\"QTY\"] + 1) * 2", Parallel().Translate("(lnk.QTY + 1) * 2").Code);
        }

        [Fact]
        public void Helpers_for_nullable_or_mixed_numbers()
        {
            Assert.Equal("F.mul(row[\"AMOUNT\"], 2)", Parallel().Translate("lnk.AMOUNT * 2").Code);
            Assert.Equal("F.mul(row[\"PRICE\"], row[\"RATE\"])", Parallel().Translate("lnk.PRICE * lnk.RATE").Code);
            Assert.Equal("F.divide(row[\"QTY\"], 2)", Parallel().Translate("lnk.QTY / 2").Code);
        }

        [Fact]
        public void Decimal_literals_stay_exact_and_leading_zeros_are_dropped()
        {
            Assert.Equal("row[\"PRICE\"] * Decimal(\"1.10\")", Parallel().Translate("lnk.PRICE * 1.10").Code);
            Assert.Equal("7", Parallel().Translate("007").Code);
        }

        [Fact]
        public void If_then_else_becomes_conditional_expression()
        {
            var result = Parallel().Translate("If lnk.NAME = 'A' Then 1 Else 0");
            Assert.Equal("1 if row[\"NAME\"] == \"A\" else 0", result.Code);
        }

        [Fact]
        public void Nullable_comparison_uses_null_aware_helper()
        {
            Assert.Equal("F.eq(row[\"CITY\"], \"X\")", Parallel().Translate("lnk.CITY = \"X\"").Code);
        }

        [Fact]
        public void Basic_dialect_uses_basic_helpers()
        {
            var t = Basic();
            Assert.Equal("F.basic_eq(row[\"X\"], \"1\")", t.Translate("in.X = \"1\"").Code);
            Assert.Equal("F.basic_add(row[\"X\"], 1)", t.Translate("in.X + 1").Code);
            Assert.Equal("F.substr(row[\"X\"], 1, 3)", t.Translate("in.X[1,3]").Code);
            Assert.Equal("F.substr_right(row[\"X\"], 2)", t.Translate("in.X[2]").Code);
            Assert.Equal("F.field(row[\"X\"], \",\", 2, 1)", t.Translate("in.X[\",\", 2, 1]").Code);
        }

        [Fact]
        public void Concatenation_is_flattened()
        {
            Assert.Equal("F.concat(row[\"NAME\"], \"-\", row[\"QTY\"], \"x\")", Parallel().Translate("lnk.NAME : '-' : lnk.QTY : 'x'").Code);
        }

        [Fact]
        public void Logical_operators_and_not()
        {
            var result = Parallel().Translate("Not(IsNull(lnk.CITY)) And lnk.QTY > 0");
            Assert.Equal("not F.is_null(row[\"CITY\"]) and row[\"QTY\"] > 0", result.Code);
            Assert.True(result.IsBoolean);
        }

        [Fact]
        public void Basic_or_then_and_keeps_left_to_right_grouping()
        {
            var t = new PythonTranslator(new DictionaryResolver().Add("A", "a", DsLogicalType.Integer, false).Add("B", "b", DsLogicalType.Integer, false).Add("C", "c", DsLogicalType.Integer, false), ExpressionDialect.Basic);
            Assert.Equal("(F.truth(a) or F.truth(b)) and F.truth(c)", t.Translate("A OR B AND C").Code);
        }

        [Fact]
        public void Status_constants_system_variables_and_set_null()
        {
            Assert.Equal("F.DSJS.RUNOK", Parallel().Translate("DSJS.RUNOK").Code);
            Assert.Equal("in_row_num", Parallel().Translate("@INROWNUM").Code);
            Assert.Equal("F.basic_date()", Parallel().Translate("@DATE").Code);
            Assert.Equal("None", Parallel().Translate("SetNull()").Code);
        }

        [Fact]
        public void Unknown_names_and_functions_are_problems_not_crashes()
        {
            var result = Parallel().Translate("Frobnicate(lnk.NAME) : Missing.COL");
            Assert.False(result.Ok);
            Assert.Equal(2, result.Problems.Count);
            Assert.Contains("F.unsupported(\"Frobnicate\"", result.Code);
            Assert.Contains("F.unresolved(\"Missing.COL\")", result.Code);
        }

        [Fact]
        public void Routines_resolve_through_the_resolver()
        {
            var result = Parallel().Translate("MyRoutine(lnk.NAME, 1)");
            Assert.True(result.Ok);
            Assert.Equal("routines.my_routine(row[\"NAME\"], 1)", result.Code);
            Assert.Single(result.Notes);
        }

        [Fact]
        public void Wrong_argument_count_is_a_problem()
        {
            Assert.False(Parallel().Translate("Left(lnk.NAME)").Ok);
        }

        [Fact]
        public void Syntax_errors_produce_placeholder()
        {
            var result = Parallel().Translate("Trim(lnk.NAME");
            Assert.False(result.Ok);
            Assert.StartsWith("F.untranslated(", result.Code);
        }

        [Fact]
        public void Conditions_and_values_wrap_as_needed()
        {
            Assert.Equal("F.truth(row[\"QTY\"])", Parallel().TranslateCondition("lnk.QTY").Code);
            Assert.Equal("F.flag(row[\"QTY\"] > 0)", Parallel().TranslateValue("lnk.QTY > 0").Code);
        }

        [Fact]
        public void Python_string_literals_are_escaped()
        {
            Assert.Equal("\"a\\\"b\\\\c\\n\\u00e9\"", PythonTranslator.PyString("a\"b\\c\né"));
        }
    }
}
