using System.Collections.Generic;

namespace DataStage2Airflow.Expressions
{
    /// <summary>Which flavour of the expression language a text is written in.</summary>
    public enum ExpressionDialect
    {
        /// <summary>DataStage BASIC: server transformers and job sequences. AND and OR share one precedence level.</summary>
        Basic,

        /// <summary>Parallel transformer expressions. AND binds tighter than OR.</summary>
        Parallel,
    }

    public enum BinaryOp
    {
        Add,
        Subtract,
        Multiply,
        Divide,
        Power,
        Concat,
        Eq,
        Ne,
        Lt,
        Gt,
        Le,
        Ge,
        And,
        Or,
        Matches,
    }

    public abstract class Expr
    {
        /// <summary>Offset of the expression in the source text.</summary>
        public int Position { get; set; }
    }

    public sealed class NumberExpr : Expr
    {
        public NumberExpr(string text)
        {
            Text = text;
        }

        public string Text { get; }

        public bool IsInteger => Text.IndexOf('.') < 0 && Text.IndexOf('e') < 0 && Text.IndexOf('E') < 0;
    }

    public sealed class StringExpr : Expr
    {
        public StringExpr(string value)
        {
            Value = value;
        }

        public string Value { get; }
    }

    /// <summary>
    /// A bare name: link.column, stage variable, job parameter, @SYSTEM variable, DSJS.* constant,
    /// macro (DSJobName) or Activity.$JobStatus. Resolution happens during translation.
    /// </summary>
    public sealed class NameExpr : Expr
    {
        public NameExpr(string name)
        {
            Name = name;
        }

        public string Name { get; }
    }

    /// <summary>A <c>#Param#</c> reference.</summary>
    public sealed class ParamRefExpr : Expr
    {
        public ParamRefExpr(string name)
        {
            Name = name;
        }

        public string Name { get; }
    }

    public sealed class UnaryExpr : Expr
    {
        public UnaryExpr(string op, Expr operand)
        {
            Op = op;
            Operand = operand;
        }

        /// <summary>"-", "+" or "NOT".</summary>
        public string Op { get; }

        public Expr Operand { get; }
    }

    public sealed class BinaryExpr : Expr
    {
        public BinaryExpr(BinaryOp op, Expr left, Expr right)
        {
            Op = op;
            Left = left;
            Right = right;
        }

        public BinaryOp Op { get; }

        public Expr Left { get; }

        public Expr Right { get; }
    }

    public sealed class IfExpr : Expr
    {
        public IfExpr(Expr condition, Expr then, Expr otherwise)
        {
            Condition = condition;
            Then = then;
            Else = otherwise;
        }

        public Expr Condition { get; }

        public Expr Then { get; }

        public Expr Else { get; }
    }

    public sealed class CallExpr : Expr
    {
        public CallExpr(string name, List<Expr> args)
        {
            Name = name;
            Args = args;
        }

        public string Name { get; }

        public List<Expr> Args { get; }
    }

    /// <summary>
    /// <c>x[start, length]</c> substring, <c>x[length]</c> trailing substring, or
    /// <c>x[delimiter, occurrence, count]</c> field extraction.
    /// </summary>
    public sealed class SubscriptExpr : Expr
    {
        public SubscriptExpr(Expr target, List<Expr> args)
        {
            Target = target;
            Args = args;
        }

        public Expr Target { get; }

        public List<Expr> Args { get; }
    }
}
