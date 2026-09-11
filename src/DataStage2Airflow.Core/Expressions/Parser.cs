using System;
using System.Collections.Generic;

namespace DataStage2Airflow.Expressions
{
    /// <summary>
    /// Recursive-descent parser for derivations, constraints, stage variables and sequence expressions.
    /// Precedence, loosest first: IF-THEN-ELSE, OR, AND (same level as OR in BASIC), NOT,
    /// comparisons (= &lt;&gt; # &lt; &gt; &lt;= &gt;= EQ NE LT GT LE GE MATCHES), concatenation (: CAT),
    /// + -, * /, ^ **, unary - +, then calls, [ ] subscripts and literals.
    /// </summary>
    public sealed class ExpressionParser
    {
        private readonly List<Token> _tokens;
        private readonly ExpressionDialect _dialect;
        private int _pos;

        private ExpressionParser(List<Token> tokens, ExpressionDialect dialect)
        {
            _tokens = tokens;
            _dialect = dialect;
        }

        public static Expr Parse(string text, ExpressionDialect dialect)
        {
            if (string.IsNullOrWhiteSpace(text)) throw new ExpressionSyntaxException("empty expression", 0);
            var parser = new ExpressionParser(ExpressionLexer.Tokenize(text), dialect);
            var expr = parser.ParseExpression();
            if (parser.Current.Kind != TokenKind.End)
            {
                throw new ExpressionSyntaxException($"unexpected '{parser.Current.Text}' at offset {parser.Current.Position}", parser.Current.Position);
            }

            return expr;
        }

        public static bool TryParse(string text, ExpressionDialect dialect, out Expr? expr, out string? error)
        {
            try
            {
                expr = Parse(text, dialect);
                error = null;
                return true;
            }
            catch (ExpressionSyntaxException ex)
            {
                expr = null;
                error = ex.Message;
                return false;
            }
        }

        private Token Current => _tokens[_pos];

        private Token Advance() => _tokens[_pos++];

        private Expr ParseExpression() => _dialect == ExpressionDialect.Basic ? ParseLogicalBasic() : ParseOr();

        // BASIC: AND and OR have equal precedence and associate left to right.
        private Expr ParseLogicalBasic()
        {
            var left = ParseNot();
            while (true)
            {
                BinaryOp? op = LogicalOperator(Current);
                if (op == null) return left;
                var at = Advance().Position;
                left = new BinaryExpr(op.Value, left, ParseNot()) { Position = at };
            }
        }

        private Expr ParseOr()
        {
            var left = ParseAnd();
            while (LogicalOperator(Current) == BinaryOp.Or)
            {
                var at = Advance().Position;
                left = new BinaryExpr(BinaryOp.Or, left, ParseAnd()) { Position = at };
            }

            return left;
        }

        private Expr ParseAnd()
        {
            var left = ParseNot();
            while (LogicalOperator(Current) == BinaryOp.And)
            {
                var at = Advance().Position;
                left = new BinaryExpr(BinaryOp.And, left, ParseNot()) { Position = at };
            }

            return left;
        }

        private static BinaryOp? LogicalOperator(Token token)
        {
            if (token.IsKeyword("AND") || token.IsOperator("&")) return BinaryOp.And;
            if (token.IsKeyword("OR") || token.IsOperator("!")) return BinaryOp.Or;
            return null;
        }

        private Expr ParseNot()
        {
            if (Current.IsKeyword("NOT"))
            {
                var at = Advance().Position;
                return new UnaryExpr("NOT", ParseNot()) { Position = at };
            }

            return ParseComparison();
        }

        private Expr ParseComparison()
        {
            var left = ParseConcat();
            while (true)
            {
                var op = ComparisonOperator(Current);
                if (op == null) return left;
                var at = Advance().Position;
                left = new BinaryExpr(op.Value, left, ParseConcat()) { Position = at };
            }
        }

        private static BinaryOp? ComparisonOperator(Token token)
        {
            if (token.Kind == TokenKind.Operator)
            {
                switch (token.Text)
                {
                    case "=": return BinaryOp.Eq;
                    case "<>":
                    case "#": return BinaryOp.Ne;
                    case "<": return BinaryOp.Lt;
                    case ">": return BinaryOp.Gt;
                    case "<=": return BinaryOp.Le;
                    case ">=": return BinaryOp.Ge;
                }

                return null;
            }

            if (token.Kind != TokenKind.Identifier) return null;
            switch (token.Text.ToUpperInvariant())
            {
                case "EQ": return BinaryOp.Eq;
                case "NE": return BinaryOp.Ne;
                case "LT": return BinaryOp.Lt;
                case "GT": return BinaryOp.Gt;
                case "LE": return BinaryOp.Le;
                case "GE": return BinaryOp.Ge;
                case "MATCHES":
                case "MATCH": return BinaryOp.Matches;
                default: return null;
            }
        }

        private Expr ParseConcat()
        {
            var left = ParseAdditive();
            while (Current.IsOperator(":") || Current.IsKeyword("CAT"))
            {
                var at = Advance().Position;
                left = new BinaryExpr(BinaryOp.Concat, left, ParseAdditive()) { Position = at };
            }

            return left;
        }

        private Expr ParseAdditive()
        {
            var left = ParseMultiplicative();
            while (Current.IsOperator("+") || Current.IsOperator("-"))
            {
                var token = Advance();
                var op = token.Text == "+" ? BinaryOp.Add : BinaryOp.Subtract;
                left = new BinaryExpr(op, left, ParseMultiplicative()) { Position = token.Position };
            }

            return left;
        }

        private Expr ParseMultiplicative()
        {
            var left = ParsePower();
            while (Current.IsOperator("*") || Current.IsOperator("/"))
            {
                var token = Advance();
                var op = token.Text == "*" ? BinaryOp.Multiply : BinaryOp.Divide;
                left = new BinaryExpr(op, left, ParsePower()) { Position = token.Position };
            }

            return left;
        }

        private Expr ParsePower()
        {
            var left = ParseUnary();
            if (Current.IsOperator("^"))
            {
                var at = Advance().Position;
                return new BinaryExpr(BinaryOp.Power, left, ParsePower()) { Position = at };
            }

            return left;
        }

        private Expr ParseUnary()
        {
            if (Current.IsOperator("-") || Current.IsOperator("+"))
            {
                var token = Advance();
                return new UnaryExpr(token.Text, ParseUnary()) { Position = token.Position };
            }

            return ParsePostfix();
        }

        private Expr ParsePostfix()
        {
            var expr = ParsePrimary();
            while (Current.Kind == TokenKind.LBracket)
            {
                var at = Advance().Position;
                var args = ParseArguments(TokenKind.RBracket);
                if (args.Count < 1 || args.Count > 3)
                {
                    throw new ExpressionSyntaxException($"substring takes 1 to 3 arguments (offset {at})", at);
                }

                expr = new SubscriptExpr(expr, args) { Position = at };
            }

            return expr;
        }

        private Expr ParsePrimary()
        {
            var token = Current;
            switch (token.Kind)
            {
                case TokenKind.Number:
                    Advance();
                    return new NumberExpr(token.Text) { Position = token.Position };

                case TokenKind.String:
                    Advance();
                    return new StringExpr(token.Text) { Position = token.Position };

                case TokenKind.ParamRef:
                    Advance();
                    return new ParamRefExpr(token.Text) { Position = token.Position };

                case TokenKind.LParen:
                    Advance();
                    var inner = ParseExpression();
                    Expect(TokenKind.RParen, ")");
                    return inner;

                case TokenKind.Identifier:
                    if (token.IsKeyword("IF")) return ParseIf();
                    if (IsReserved(token.Text))
                    {
                        throw new ExpressionSyntaxException($"unexpected keyword '{token.Text}' at offset {token.Position}", token.Position);
                    }

                    Advance();
                    if (Current.Kind == TokenKind.LParen)
                    {
                        Advance();
                        var args = ParseArguments(TokenKind.RParen);
                        return new CallExpr(token.Text, args) { Position = token.Position };
                    }

                    return new NameExpr(token.Text) { Position = token.Position };

                case TokenKind.End:
                    throw new ExpressionSyntaxException("unexpected end of expression", token.Position);

                default:
                    throw new ExpressionSyntaxException($"unexpected '{token.Text}' at offset {token.Position}", token.Position);
            }
        }

        private Expr ParseIf()
        {
            var at = Advance().Position;
            var condition = ParseExpression();
            ExpectKeyword("THEN");
            var then = ParseExpression();
            ExpectKeyword("ELSE");
            var otherwise = ParseExpression();
            return new IfExpr(condition, then, otherwise) { Position = at };
        }

        private List<Expr> ParseArguments(TokenKind close)
        {
            var args = new List<Expr>();
            if (Current.Kind == close)
            {
                Advance();
                return args;
            }

            while (true)
            {
                args.Add(ParseExpression());
                if (Current.Kind == TokenKind.Comma)
                {
                    Advance();
                    continue;
                }

                Expect(close, close == TokenKind.RParen ? ")" : "]");
                return args;
            }
        }

        private void Expect(TokenKind kind, string text)
        {
            if (Current.Kind != kind)
            {
                throw new ExpressionSyntaxException($"expected '{text}' but found '{Current.Text}' at offset {Current.Position}", Current.Position);
            }

            Advance();
        }

        private void ExpectKeyword(string keyword)
        {
            if (!Current.IsKeyword(keyword))
            {
                var found = Current.Kind == TokenKind.End ? "end of expression" : "'" + Current.Text + "'";
                throw new ExpressionSyntaxException($"expected {keyword} but found {found} at offset {Current.Position}", Current.Position);
            }

            Advance();
        }

        private static bool IsReserved(string word)
        {
            switch (word.ToUpperInvariant())
            {
                case "THEN":
                case "ELSE":
                case "AND":
                case "OR":
                case "NOT":
                case "EQ":
                case "NE":
                case "LT":
                case "GT":
                case "LE":
                case "GE":
                case "CAT":
                case "MATCHES":
                case "MATCH":
                    return true;
                default:
                    return false;
            }
        }
    }
}
