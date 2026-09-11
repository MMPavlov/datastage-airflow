using System;
using System.Collections.Generic;
using System.Linq;
using DataStage2Airflow.Model;

namespace DataStage2Airflow.Expressions
{
    [Flags]
    public enum FunctionFlags
    {
        None = 0,

        /// <summary>Returns true/false.</summary>
        Boolean = 1,

        /// <summary>Accepts null arguments without propagating null (IsNull, NullToValue...).</summary>
        HandlesNull = 2,

        /// <summary>Result has the type of the first argument (Abs, Mod...).</summary>
        SameTypeAsArgument = 4,
    }

    public sealed class FunctionInfo
    {
        internal FunctionInfo(string name, string python, int minArgs, int maxArgs, DsLogicalType returns, FunctionFlags flags)
        {
            Name = name;
            Python = python;
            MinArgs = minArgs;
            MaxArgs = maxArgs;
            Returns = returns;
            Flags = flags;
        }

        /// <summary>Canonical DataStage spelling.</summary>
        public string Name { get; }

        /// <summary>Function in the generated runtime's <c>dsfunc</c> module.</summary>
        public string Python { get; }

        public int MinArgs { get; }

        public int MaxArgs { get; }

        public DsLogicalType Returns { get; }

        public FunctionFlags Flags { get; }

        public bool IsBoolean => (Flags & FunctionFlags.Boolean) != 0;

        public bool HandlesNull => (Flags & FunctionFlags.HandlesNull) != 0;
    }

    /// <summary>
    /// DataStage transformer / BASIC functions and their implementations in <c>ds2af_runtime/dsfunc.py</c>.
    /// Names are matched case-insensitively (TRIM, Trim and trim are the same function in DataStage).
    /// </summary>
    public static class FunctionCatalog
    {
        private const DsLogicalType S = DsLogicalType.String;
        private const DsLogicalType I = DsLogicalType.Integer;
        private const DsLogicalType D = DsLogicalType.Decimal;
        private const DsLogicalType F = DsLogicalType.Float;
        private const DsLogicalType Dt = DsLogicalType.Date;
        private const DsLogicalType Tm = DsLogicalType.Time;
        private const DsLogicalType Ts = DsLogicalType.Timestamp;
        private const DsLogicalType U = DsLogicalType.Unknown;
        private const FunctionFlags Bool = FunctionFlags.Boolean;
        private const FunctionFlags NullOk = FunctionFlags.HandlesNull;
        private const FunctionFlags Same = FunctionFlags.SameTypeAsArgument;

        private static readonly Dictionary<string, FunctionInfo> ByName = new Dictionary<string, FunctionInfo>(StringComparer.OrdinalIgnoreCase);

        static FunctionCatalog()
        {
            // Strings
            Add("Trim", "trim", 1, 3, S);
            Add("Trims", "trim", 1, 1, S);
            Add("TrimF", "trim_f", 1, 1, S);
            Add("TrimB", "trim_b", 1, 1, S);
            Add("TrimLeadingTrailing", "trim_leading_trailing", 1, 1, S);
            Add("Len", "len_", 1, 1, I);
            Add("Left", "left", 2, 2, S);
            Add("Right", "right", 2, 2, S);
            Add("UpCase", "upcase", 1, 1, S, FunctionFlags.None, "UCase");
            Add("DownCase", "downcase", 1, 1, S, FunctionFlags.None, "LCase");
            Add("Index", "index", 3, 3, I);
            Add("Count", "count", 2, 2, I);
            Add("DCount", "dcount", 2, 2, I);
            Add("Field", "field", 3, 4, S);
            Add("Str", "str_repeat", 2, 2, S);
            Add("Space", "space", 1, 1, S);
            Add("Char", "char", 1, 1, S);
            Add("Seq", "seq", 1, 1, I);
            Add("Convert", "convert", 3, 3, S);
            Add("Ereplace", "ereplace", 3, 5, S, FunctionFlags.None, "Change");
            Add("Compare", "compare", 2, 3, I);
            Add("Alpha", "alpha", 1, 1, I, Bool);
            Add("Num", "num", 1, 1, I, Bool);
            Add("Fmt", "fmt", 2, 2, S);
            Add("Iconv", "iconv", 2, 2, U, FunctionFlags.None, "Iconvs");
            Add("Oconv", "oconv", 2, 2, S, FunctionFlags.None, "Oconvs");
            Add("Quote", "dquote", 1, 1, S, FunctionFlags.None, "DQuote");
            Add("SQuote", "squote", 1, 1, S);
            Add("Cats", "concat", 2, 2, S);
            Add("StripWhiteSpace", "strip_white_space", 1, 1, S);
            Add("CompactWhiteSpace", "compact_white_space", 1, 1, S);
            Add("PadString", "pad_string", 3, 3, S);
            Add("AlNum", "alnum", 1, 1, I, Bool);
            Add("Soundex", "soundex", 1, 1, S);
            Add("Substrings", "substr", 3, 3, S);
            Add("Dtx", "dtx", 1, 2, S);
            Add("Xtd", "xtd", 1, 1, I);
            Add("StringToUstring", "ustring", 1, 2, S, FunctionFlags.None, "UstringToString");
            Add("Like", "like", 2, 2, I, Bool);

            // Null handling
            Add("IsNull", "is_null", 1, 1, I, Bool | NullOk);
            Add("IsNotNull", "is_not_null", 1, 1, I, Bool | NullOk);
            Add("NullToValue", "null_to_value", 2, 2, U, NullOk);
            Add("NullToZero", "null_to_zero", 1, 1, U, NullOk | Same);
            Add("NullToEmpty", "null_to_empty", 1, 1, S, NullOk);
            Add("SetNull", "set_null", 0, 0, U, NullOk);
            Add("IsValid", "is_valid", 2, 2, I, Bool | NullOk);
            Add("IsValidDate", "is_valid_date", 1, 1, I, Bool | NullOk);
            Add("IsValidTime", "is_valid_time", 1, 1, I, Bool | NullOk);
            Add("IsValidTimestamp", "is_valid_timestamp", 1, 1, I, Bool | NullOk);
            Add("IsValidDecimal", "is_valid_decimal", 1, 2, I, Bool | NullOk);

            // Type conversion (parallel)
            Add("StringToDate", "string_to_date", 1, 2, Dt);
            Add("DateToString", "date_to_string", 1, 2, S);
            Add("StringToTime", "string_to_time", 1, 2, Tm);
            Add("TimeToString", "time_to_string", 1, 2, S);
            Add("StringToTimestamp", "string_to_timestamp", 1, 2, Ts);
            Add("TimestampToString", "timestamp_to_string", 1, 2, S);
            Add("TimestampToDate", "timestamp_to_date", 1, 1, Dt);
            Add("TimestampToTime", "timestamp_to_time", 1, 1, Tm);
            Add("DateToTimestamp", "date_to_timestamp", 1, 1, Ts);
            Add("StringToDecimal", "string_to_decimal", 1, 2, D);
            Add("DecimalToString", "decimal_to_string", 1, 2, S);
            Add("DecimalToDecimal", "decimal_to_decimal", 1, 2, D);
            Add("DecimalToDFloat", "decimal_to_dfloat", 1, 2, F);
            Add("DFloatToDecimal", "dfloat_to_decimal", 1, 2, D);
            Add("DfloatToStringNoExp", "dfloat_to_string_no_exp", 2, 2, S);
            Add("AsInteger", "as_integer", 1, 1, I);
            Add("AsFloat", "as_float", 1, 1, F, FunctionFlags.None, "AsDouble");
            Add("AsDecimal", "as_decimal", 1, 1, D);

            // Dates and times (parallel)
            Add("CurrentDate", "current_date", 0, 0, Dt);
            Add("CurrentTime", "current_time", 0, 0, Tm);
            Add("CurrentTimeMS", "current_time_ms", 0, 0, Tm);
            Add("CurrentTimestamp", "current_timestamp", 0, 0, Ts);
            Add("CurrentTimestampMS", "current_timestamp_ms", 0, 0, Ts);
            Add("DateFromDaysSince", "date_from_days_since", 1, 2, Dt);
            Add("DaysSinceFromDate", "days_since_from_date", 2, 2, I);
            Add("DateFromJulianDay", "date_from_julian_day", 1, 1, Dt);
            Add("JulianDayFromDate", "julian_day_from_date", 1, 1, I);
            Add("DateFromComponents", "date_from_components", 3, 3, Dt);
            Add("DateOffsetByDays", "date_offset_by_days", 2, 2, Dt);
            Add("DateOffsetByComponents", "date_offset_by_components", 4, 4, Dt);
            Add("TimeFromComponents", "time_from_components", 3, 4, Tm);
            Add("TimeFromMidnightSeconds", "time_from_midnight_seconds", 1, 1, Tm);
            Add("MidnightSecondsFromTime", "midnight_seconds_from_time", 1, 1, D);
            Add("TimestampFromDateTime", "timestamp_from_date_time", 2, 2, Ts);
            Add("TimestampFromSecondsSince", "timestamp_from_seconds_since", 1, 2, Ts);
            Add("SecondsSinceFromTimestamp", "seconds_since_from_timestamp", 2, 2, D);
            Add("TimestampFromTimet", "timestamp_from_timet", 1, 1, Ts);
            Add("TimetFromTimestamp", "timet_from_timestamp", 1, 1, I);
            Add("TimestampOffsetBySeconds", "timestamp_offset_by_seconds", 2, 2, Ts);
            Add("TimestampOffsetByComponents", "timestamp_offset_by_components", 7, 7, Ts);
            Add("TimeOffsetBySeconds", "time_offset_by_seconds", 2, 2, Tm);
            Add("TimeOffsetByComponents", "time_offset_by_components", 4, 4, Tm);
            Add("HoursFromTime", "hours_from_time", 1, 1, I);
            Add("MinutesFromTime", "minutes_from_time", 1, 1, I);
            Add("SecondsFromTime", "seconds_from_time", 1, 1, D);
            Add("MicroSecondsFromTime", "microseconds_from_time", 1, 1, I);
            Add("YearFromDate", "year_from_date", 1, 1, I);
            Add("MonthFromDate", "month_from_date", 1, 1, I);
            Add("MonthDayFromDate", "month_day_from_date", 1, 1, I);
            Add("WeekdayFromDate", "weekday_from_date", 1, 2, I);
            Add("YeardayFromDate", "yearday_from_date", 1, 1, I);
            Add("YearweekFromDate", "yearweek_from_date", 1, 1, I);
            Add("NextWeekdayFromDate", "next_weekday_from_date", 2, 2, Dt);
            Add("PreviousWeekdayFromDate", "previous_weekday_from_date", 2, 2, Dt);
            Add("NthWeekdayFromDate", "nth_weekday_from_date", 3, 3, Dt);

            // Dates and times (BASIC internal format: days since 31 Dec 1967, seconds since midnight)
            Add("Date", "basic_date", 0, 0, I);
            Add("Time", "basic_time", 0, 0, I);
            Add("TimeDate", "time_date", 0, 0, S);

            // Numbers
            Add("Abs", "abs_", 1, 1, U, Same);
            Add("Int", "int_", 1, 1, I);
            Add("Mod", "mod", 2, 2, U, Same);
            Add("Rem", "rem", 2, 2, U, Same);
            Add("Div", "div", 2, 2, F);
            Add("Neg", "neg", 1, 1, U, Same);
            Add("Sqrt", "sqrt", 1, 1, F);
            Add("Exp", "exp", 1, 1, F);
            Add("Ln", "ln", 1, 1, F);
            Add("Log10", "log10", 1, 1, F);
            Add("Pwr", "pwr", 2, 2, F);
            Add("Rand", "rand", 0, 0, I);
            Add("Random", "random", 0, 1, I);
            Add("Rnd", "rnd", 1, 1, I);
            Add("Ceil", "ceil", 1, 1, I);
            Add("Floor", "floor", 1, 1, I);
            Add("Sin", "sin", 1, 1, F);
            Add("Cos", "cos", 1, 1, F);
            Add("Tan", "tan", 1, 1, F);
            Add("ASin", "asin", 1, 1, F);
            Add("ACos", "acos", 1, 1, F);
            Add("ATan", "atan", 1, 1, F);
            Add("Max", "max_", 2, 2, U, Same);
            Add("Min", "min_", 2, 2, U, Same);
            Add("BitAnd", "bit_and", 2, 2, I);
            Add("BitOr", "bit_or", 2, 2, I);
            Add("BitXOr", "bit_xor", 2, 2, I);
            Add("SetBitOn", "set_bit_on", 2, 2, I);
            Add("SetBitOff", "set_bit_off", 2, 2, I);

            // Environment
            Add("GetEnvironment", "get_environment", 1, 1, S);
        }

        public static FunctionInfo? Find(string name) => ByName.TryGetValue(name, out var info) ? info : null;

        public static IEnumerable<FunctionInfo> All => ByName.Values.Distinct();

        private static void Add(string name, string python, int min, int max, DsLogicalType returns, FunctionFlags flags = FunctionFlags.None, params string[] aliases)
        {
            var info = new FunctionInfo(name, python, min, max, returns, flags);
            ByName[name] = info;
            foreach (var alias in aliases) ByName[alias] = info;
        }
    }
}
