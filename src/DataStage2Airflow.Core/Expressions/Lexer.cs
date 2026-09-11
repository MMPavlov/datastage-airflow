using System;
using System.Collections.Generic;

namespace DataStage2Airflow.Expressions
{
    public enum TokenKind
    {
        Number,
        String,
        Identifier,
        ParamRef,
        Operator,
        LParen,
        RParen,
        LBracket,
        RBracket,
        Comma,
        End,
    }

    public readonly struct Token
    {
        public Token(TokenKind kind, string text, int position)
        {
            Kind = kind;
            Text = text;
            Position = position;
        }

        public TokenKind Kind { get; }

        public string Text { get; }

        public int Position { get; }

        public bool IsOperator(string op) => Kind == TokenKind.Operator && Text == op;

        public bool IsKeyword(string keyword) =>
            Kind == TokenKind.Identifier && string.Equals(Text, keyword, StringComparison.OrdinalIgnoreCase);

        public override string ToString() => $"{Kind} '{Text}' @{Position}";
    }

    public sealed class ExpressionSyntaxException : Exception
    {
        public ExpressionSyntaxException(string message, int position)
            : base(message)
        {
            Position = position;
        }

        public int Position { get; }
    }

    /// <summary>
    /// Tokenizer for DataStage expressions. BASIC details handled here: strings may be delimited by
    /// double quotes, single quotes or backslashes (no escapes inside); <c>#</c> is "not equal" unless it
    /// wraps a parameter name (<c>#Param#</c>); identifiers may contain dots and <c>$</c>
    /// (<c>lnk.COL</c>, <c>DSJS.RUNOK</c>, <c>Job_1.$JobStatus</c>) and start with <c>@</c> (<c>@INROWNUM</c>).
    /// </summary>
    public static class ExpressionLexer
    {
        public static List<Token> Tokenize(string text)
        {
            var tokens = new List<Token>();
            int i = 0;
            while (i < text.Length)
            {
                char c = text[i];
                if (char.IsWhiteSpace(c))
                {
                    i++;
                    continue;
                }

                int start = i;
                if (char.IsDigit(c) || (c == '.' && i + 1 < text.Length && char.IsDigit(text[i + 1])))
                {
                    i = ReadNumber(text, i);
                    tokens.Add(new Token(TokenKind.Number, text.Substring(start, i - start), start));
                    continue;
                }

                if (c == '"' || c == '\'' || c == '\\')
                {
                    int close = text.IndexOf(c, i + 1);
                    if (close < 0) throw new ExpressionSyntaxException($"unterminated string starting at offset {i}", i);
                    tokens.Add(new Token(TokenKind.String, text.Substring(i + 1, close - i - 1), start));
                    i = close + 1;
                    continue;
                }

                if (c == '#')
                {
                    int j = i + 1;
                    while (j < text.Length && IsIdentifierPart(text[j])) j++;
                    if (j > i + 1 && j < text.Length && text[j] == '#')
                    {
                        tokens.Add(new Token(TokenKind.ParamRef, text.Substring(i + 1, j - i - 1), start));
                        i = j + 1;
                        continue;
                    }

                    tokens.Add(new Token(TokenKind.Operator, "#", start));
                    i++;
                    continue;
                }

                if (IsIdentifierStart(c))
                {
                    i++;
                    while (i < text.Length && IsIdentifierPart(text[i])) i++;
                    while (i > start + 1 && text[i - 1] == '.') i--;
                    tokens.Add(new Token(TokenKind.Identifier, text.Substring(start, i - start), start));
                    continue;
                }

                char next = i + 1 < text.Length ? text[i + 1] : '\0';
                switch (c)
                {
                    case '(':
                        tokens.Add(new Token(TokenKind.LParen, "(", start));
                        break;
                    case ')':
                        tokens.Add(new Token(TokenKind.RParen, ")", start));
                        break;
                    case '[':
                        tokens.Add(new Token(TokenKind.LBracket, "[", start));
                        break;
                    case ']':
                        tokens.Add(new Token(TokenKind.RBracket, "]", start));
                        break;
                    case ',':
                        tokens.Add(new Token(TokenKind.Comma, ",", start));
                        break;
                    case '<':
                        if (next == '>' || next == '=')
                        {
                            tokens.Add(new Token(TokenKind.Operator, next == '>' ? "<>" : "<=", start));
                            i++;
                        }
                        else
                        {
                            tokens.Add(new Token(TokenKind.Operator, "<", start));
                        }

                        break;
                    case '>':
                        if (next == '=' || next == '<')
                        {
                            tokens.Add(new Token(TokenKind.Operator, next == '=' ? ">=" : "<>", start));
                            i++;
                        }
                        else
                        {
                            tokens.Add(new Token(TokenKind.Operator, ">", start));
                        }

                        break;
                    case '=':
                        if (next == '<' || next == '>')
                        {
                            tokens.Add(new Token(TokenKind.Operator, next == '<' ? "<=" : ">=", start));
                            i++;
                        }
                        else if (next == '=')
                        {
                            tokens.Add(new Token(TokenKind.Operator, "=", start));
                            i++;
                        }
                        else
                        {
                            tokens.Add(new Token(TokenKind.Operator, "=", start));
                        }

                        break;
                    case '!':
                        if (next == '=')
                        {
                            tokens.Add(new Token(TokenKind.Operator, "<>", start));
                            i++;
                        }
                        else
                        {
                            tokens.Add(new Token(TokenKind.Operator, "!", start));
                        }

                        break;
                    case '*':
                        if (next == '*')
                        {
                            tokens.Add(new Token(TokenKind.Operator, "^", start));
                            i++;
                        }
                        else
                        {
                            tokens.Add(new Token(TokenKind.Operator, "*", start));
                        }

                        break;
                    case '+':
                    case '-':
                    case '/':
                    case '^':
                    case ':':
                    case '&':
                        tokens.Add(new Token(TokenKind.Operator, c.ToString(), start));
                        break;
                    default:
                        throw new ExpressionSyntaxException($"unexpected character '{c}' at offset {i}", i);
                }

                i++;
            }

            tokens.Add(new Token(TokenKind.End, string.Empty, text.Length));
            return tokens;
        }

        private static int ReadNumber(string text, int i)
        {
            while (i < text.Length && char.IsDigit(text[i])) i++;
            if (i < text.Length && text[i] == '.')
            {
                i++;
                while (i < text.Length && char.IsDigit(text[i])) i++;
            }

            if (i < text.Length && (text[i] == 'e' || text[i] == 'E'))
            {
                int j = i + 1;
                if (j < text.Length && (text[j] == '+' || text[j] == '-')) j++;
                if (j < text.Length && char.IsDigit(text[j]))
                {
                    i = j;
                    while (i < text.Length && char.IsDigit(text[i])) i++;
                }
            }

            return i;
        }

        private static bool IsIdentifierStart(char c) => char.IsLetter(c) || c == '_' || c == '@' || c == '$';

        private static bool IsIdentifierPart(char c) => char.IsLetterOrDigit(c) || c == '_' || c == '.' || c == '$' || c == '%';
    }
}
