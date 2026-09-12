"""Unit tests for ds2af_runtime.dsfunc (stdlib unittest; run: python -m unittest discover -s tests/python)."""

import datetime as dt
import os
import sys
import unittest
from decimal import Decimal

HERE = os.path.dirname(os.path.abspath(__file__))
RUNTIME = os.path.join(HERE, "..", "..", "src", "DataStage2Airflow.Core", "Runtime", "python")
sys.path.insert(0, os.path.abspath(RUNTIME))

from ds2af_runtime import dsfunc as F  # noqa: E402
from ds2af_runtime.dsfunc import RowError  # noqa: E402


def basic_day(y, m, d):
    return (dt.date(y, m, d) - dt.date(1967, 12, 31)).days


class StringFunctions(unittest.TestCase):
    def test_trim_default_reduces_internal_spaces(self):
        self.assertEqual(F.trim("  a   b  "), "a b")

    def test_trim_options(self):
        self.assertEqual(F.trim("xxaxxbxx", "x", "A"), "ab")
        self.assertEqual(F.trim("xxaxxbxx", "x", "L"), "axxbxx")
        self.assertEqual(F.trim("xxaxxbxx", "x", "T"), "xxaxxb")
        self.assertEqual(F.trim("xxaxxbxx", "x", "B"), "axxb")
        self.assertEqual(F.trim("xxaxxbxx", "x", "R"), "axb")

    def test_trim_variants(self):
        self.assertEqual(F.trim_f("  a "), "a ")
        self.assertEqual(F.trim_b("  a "), "  a")
        self.assertEqual(F.trim_leading_trailing("  a  b "), "a  b")

    def test_null_propagates(self):
        self.assertIsNone(F.trim(None))
        self.assertIsNone(F.concat("a", None))
        self.assertIsNone(F.upcase(None))

    def test_field(self):
        self.assertEqual(F.field("a,b,c,d", ",", 2), "b")
        self.assertEqual(F.field("a,b,c,d", ",", 2, 2), "b,c")
        self.assertEqual(F.field("a,b", ",", 5), "")

    def test_index_count_dcount(self):
        self.assertEqual(F.index("abcabc", "bc", 2), 5)
        self.assertEqual(F.index("abc", "x", 1), 0)
        self.assertEqual(F.count("aaaa", "aa"), 3)
        self.assertEqual(F.dcount("a,b,c", ","), 3)
        self.assertEqual(F.dcount("", ","), 0)

    def test_substrings(self):
        self.assertEqual(F.substr("abcdef", 2, 3), "bcd")
        self.assertEqual(F.substr("abcdef", 0, 2), "ab")
        self.assertEqual(F.substr_right("abcdef", 2), "ef")
        self.assertEqual(F.left("abc", 2), "ab")
        self.assertEqual(F.right("abc", 5), "abc")

    def test_ereplace_and_convert(self):
        self.assertEqual(F.ereplace("aXbXc", "X", "-"), "a-b-c")
        self.assertEqual(F.ereplace("aXbXc", "X", "-", 1), "a-bXc")
        self.assertEqual(F.ereplace("aXbXc", "X", "-", 1, 2), "aXb-c")
        self.assertEqual(F.convert("abc", "xy", "aabbcc"), "xxyy")

    def test_concat_formats_numbers(self):
        self.assertEqual(F.concat("a", 1, Decimal("2.50"), 3.0), "a12.503")

    def test_soundex(self):
        self.assertEqual(F.soundex("Robert"), "R163")
        self.assertEqual(F.soundex("Rupert"), "R163")
        self.assertEqual(F.soundex("Tymczak"), "T522")
        self.assertEqual(F.soundex("Pfister"), "P236")

    def test_matches(self):
        self.assertTrue(F.matches("123", "3N"))
        self.assertTrue(F.matches("12A", "2N1A"))
        self.assertFalse(F.matches("ab", "3X"))
        self.assertTrue(F.matches("A-12", "1A'-'2N"))

    def test_fmt(self):
        self.assertEqual(F.fmt("12.5", "R2"), "12.50")
        self.assertEqual(F.fmt("abc", "10L"), "abc       ")
        self.assertEqual(F.fmt("abc", "10R"), "       abc")
        self.assertEqual(F.fmt("5", "3'0'R"), "005")


class Semantics(unittest.TestCase):
    def test_basic_comparisons_are_numeric_for_numeric_strings(self):
        self.assertTrue(F.basic_eq("1", "01"))
        self.assertFalse(F.basic_lt("10", "9"))
        self.assertFalse(F.basic_eq("abc", "ABC"))

    def test_parallel_comparisons_are_typed(self):
        self.assertFalse(F.eq("1", "01"))
        self.assertTrue(F.lt("10", "9"))
        self.assertTrue(F.eq(1, Decimal("1.0")))
        self.assertIsNone(F.eq(None, 1))

    def test_truth(self):
        self.assertFalse(F.truth("0"))
        self.assertTrue(F.truth("abc"))
        self.assertFalse(F.truth(None))
        self.assertFalse(F.truth(Decimal("0.0")))
        self.assertTrue(F.truth(True))

    def test_not_of_null_is_null(self):
        self.assertIsNone(F.not_(None))
        self.assertIs(F.not_(0), True)
        self.assertIs(F.not_("abc"), False)

    def test_arithmetic(self):
        self.assertEqual(F.add(1, Decimal("1.5")), Decimal("2.5"))
        self.assertEqual(F.basic_add("2", "3"), 5)
        self.assertEqual(F.basic_add("abc", 3), 3)
        self.assertEqual(F.basic_divide("7", "2"), Decimal("3.5"))
        self.assertIsNone(F.add(None, 1))
        with self.assertRaises(RowError):
            F.divide(1, 0)
        with self.assertRaises(RowError):
            F.add("abc", 1)

    def test_mod_has_sign_of_dividend(self):
        self.assertEqual(F.mod(-7, 3), -1)
        self.assertEqual(F.mod(7, -3), 1)

    def test_null_functions(self):
        self.assertEqual(F.null_to_value(None, "x"), "x")
        self.assertEqual(F.null_to_zero(None), 0)
        self.assertEqual(F.null_to_empty(None), "")
        self.assertTrue(F.is_null(float("nan")))

    def test_is_valid(self):
        self.assertTrue(F.is_valid("int32", "123"))
        self.assertFalse(F.is_valid("int8", "300"))
        self.assertFalse(F.is_valid("date", "2005-02-30"))


class Dates(unittest.TestCase):
    def test_string_to_date_formats(self):
        self.assertEqual(F.string_to_date("2005-06-15"), dt.date(2005, 6, 15))
        self.assertEqual(F.string_to_date("15/06/2005", "%dd/%mm/%yyyy"), dt.date(2005, 6, 15))
        self.assertEqual(F.string_to_date("Jun 5 2005", "%mmm %d %yyyy"), dt.date(2005, 6, 5))

    def test_two_digit_year_cutoff(self):
        self.assertEqual(F.string_to_date("05-06-15", "%yy-%mm-%dd"), dt.date(1905, 6, 15))
        self.assertEqual(F.string_to_date("05-06-15", "%1950yy-%mm-%dd"), dt.date(2005, 6, 15))

    def test_invalid_date_rejects_row(self):
        with self.assertRaises(RowError):
            F.string_to_date("2005-13-01")

    def test_date_to_string(self):
        self.assertEqual(F.date_to_string(dt.date(2005, 6, 5), "%dd.%mm.%yyyy"), "05.06.2005")
        self.assertEqual(F.date_to_string(dt.date(2005, 6, 5), "%mmm %d, %yyyy"), "Jun 5, 2005")

    def test_timestamps(self):
        ts = F.string_to_timestamp("Jun 15 10:20:30 2005", "%mmm %dd %hh:%nn:%ss %yyyy")
        self.assertEqual(ts, dt.datetime(2005, 6, 15, 10, 20, 30))
        self.assertEqual(F.timestamp_to_string(ts), "2005-06-15 10:20:30")

    def test_day_arithmetic(self):
        self.assertEqual(F.days_since_from_date("2005-06-15", "2005-06-01"), 14)
        self.assertEqual(F.date_offset_by_days(dt.date(2005, 1, 31), 1), dt.date(2005, 2, 1))
        self.assertEqual(F.date_offset_by_components(dt.date(2005, 1, 31), 0, 1, 0), dt.date(2005, 2, 28))

    def test_julian_day(self):
        self.assertEqual(F.julian_day_from_date(dt.date(2000, 1, 1)), 2451545)
        self.assertEqual(F.date_from_julian_day(2451545), dt.date(2000, 1, 1))

    def test_weekday(self):
        self.assertEqual(F.weekday_from_date(dt.date(2005, 6, 15)), 3)  # Wednesday, Sunday = 0

    def test_basic_date_conversions(self):
        self.assertEqual(F.oconv(0, "D"), "31 DEC 1967")
        self.assertEqual(F.iconv("2005-06-15", "D-YMD[4,2,2]"), basic_day(2005, 6, 15))
        self.assertEqual(F.oconv(basic_day(2005, 6, 15), "D-YMD[4,2,2]"), "2005-06-15")
        self.assertEqual(F.oconv(F.iconv("06/15/2005", "D/MDY[2,2,4]"), "D-YMD[4,2,2]"), "2005-06-15")
        self.assertEqual(F.oconv(F.iconv("15 JUN 2005", "D"), "D4/"), "06/15/2005")
        self.assertEqual(F.oconv(basic_day(2005, 6, 15), "D2/"), "06/15/05")

    def test_basic_decimal_and_time_conversions(self):
        self.assertEqual(F.oconv(12345, "MD2"), "123.45")
        self.assertEqual(F.oconv(1234567, "MD2,"), "12,345.67")
        self.assertEqual(F.iconv("123.45", "MD2"), 12345)
        self.assertEqual(F.oconv(45296, "MTS"), "12:34:56")
        self.assertEqual(F.oconv(45296, "MT"), "12:34")
        self.assertEqual(F.oconv(45296, "MTH"), "12:34PM")
        self.assertEqual(F.iconv("12:34:56", "MTS"), 45296)
        self.assertEqual(F.oconv("Hello", "MCU"), "HELLO")

    def test_decimal_conversions(self):
        self.assertEqual(F.string_to_decimal("12.50"), Decimal("12.50"))
        self.assertEqual(F.decimal_to_string(Decimal("12.50"), "suppress_zero"), "12.5")


if __name__ == "__main__":
    unittest.main()
