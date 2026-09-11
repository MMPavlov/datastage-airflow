namespace DataStage2Airflow.Model
{
    /// <summary>The value categories the generator reasons about.</summary>
    public enum DsLogicalType
    {
        Unknown,
        String,
        Integer,
        Decimal,
        Float,
        Date,
        Time,
        Timestamp,
        Binary,
        Boolean,
    }

    /// <summary>DataStage stores column types as ODBC SQL type codes (SqlType "12" = VarChar).</summary>
    public static class SqlTypes
    {
        public static DsLogicalType Logical(int sqlType)
        {
            switch (sqlType)
            {
                case 1:
                case 12:
                case -1:
                case -8:
                case -9:
                case -10:
                    return DsLogicalType.String;
                case 2:
                case 3:
                    return DsLogicalType.Decimal;
                case 4:
                case 5:
                case -5:
                case -6:
                case -7:
                    return DsLogicalType.Integer;
                case 6:
                case 7:
                case 8:
                    return DsLogicalType.Float;
                case 9:
                case 91:
                    return DsLogicalType.Date;
                case 10:
                case 92:
                    return DsLogicalType.Time;
                case 11:
                case 93:
                    return DsLogicalType.Timestamp;
                case -2:
                case -3:
                case -4:
                    return DsLogicalType.Binary;
                default:
                    return DsLogicalType.Unknown;
            }
        }

        public static string Name(int sqlType) => sqlType switch
        {
            1 => "Char",
            12 => "VarChar",
            -1 => "LongVarChar",
            -8 => "NChar",
            -9 => "NVarChar",
            -10 => "LongNVarChar",
            2 => "Numeric",
            3 => "Decimal",
            4 => "Integer",
            5 => "SmallInt",
            -5 => "BigInt",
            -6 => "TinyInt",
            -7 => "Bit",
            6 => "Float",
            7 => "Real",
            8 => "Double",
            9 => "Date",
            91 => "Date",
            10 => "Time",
            92 => "Time",
            11 => "Timestamp",
            93 => "Timestamp",
            -2 => "Binary",
            -3 => "VarBinary",
            -4 => "LongVarBinary",
            _ => "Unknown(" + sqlType + ")",
        };

        public static string Describe(int sqlType, int precision, int scale)
        {
            var name = Name(sqlType);
            switch (Logical(sqlType))
            {
                case DsLogicalType.String:
                case DsLogicalType.Binary:
                    return precision > 0 ? $"{name}({precision})" : name;
                case DsLogicalType.Decimal:
                    return precision > 0 ? $"{name}({precision},{scale})" : name;
                default:
                    return name;
            }
        }
    }
}
