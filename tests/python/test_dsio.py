"""Unit tests for ds2af_runtime.dsio frames, rows, keys and delimited files (run: python -m unittest discover -s tests/python)."""

import datetime as dt
import os
import sys
import tempfile
import unittest
from decimal import Decimal

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
RUNTIME = os.path.join(HERE, "..", "..", "src", "DataStage2Airflow.Core", "Runtime", "python")
sys.path.insert(0, os.path.abspath(RUNTIME))

from ds2af_runtime import dsio  # noqa: E402
from ds2af_runtime.job import JobContext  # noqa: E402

COLUMNS = [
    dsio.Col("ID", "integer", 10, nullable=False, key=True),
    dsio.Col("NAME", "string", 20),
    dsio.Col("AMOUNT", "decimal", 8, 2),
    dsio.Col("DAY", "date"),
]


class Frames(unittest.TestCase):
    def test_frame_coerces_to_the_declared_types(self):
        f = dsio.frame([{"ID": "7", "NAME": 5, "AMOUNT": "1.239", "DAY": "2005-06-15", "EXTRA": 1}], COLUMNS)
        self.assertEqual(list(f.columns), ["ID", "NAME", "AMOUNT", "DAY"])
        self.assertEqual(next(dsio.rows(f)), {"ID": 7, "NAME": "5", "AMOUNT": Decimal("1.23"), "DAY": dt.date(2005, 6, 15)})

    def test_untyped_frames_keep_values_and_fill_absent_columns(self):
        f = dsio.frame([{"ID": "01"}], COLUMNS, typed=False)
        self.assertEqual(next(dsio.rows(f)), {"ID": "01", "NAME": None, "AMOUNT": None, "DAY": None})

    def test_empty_frames_have_the_columns(self):
        f = dsio.frame([], COLUMNS)
        self.assertEqual((len(f), list(f.columns)), (0, ["ID", "NAME", "AMOUNT", "DAY"]))
        self.assertEqual(list(dsio.rows(f)), [])

    def test_missing_values_become_none(self):
        f = pd.DataFrame({"A": ["x", None, float("nan"), pd.NA, pd.NaT, Decimal("NaN")]}, dtype=object)
        values = [r["A"] for r in dsio.rows(f)]
        self.assertEqual(values[:5], ["x", None, None, None, None])
        self.assertTrue(values[5].is_nan())

    def test_rows_cross_chunk_boundaries(self):
        records = [{"ID": i, "NAME": None if i % 2 else "n%d" % i} for i in range(5)]
        saved, dsio._CHUNK = dsio._CHUNK, 2
        try:
            f = dsio.frame(records, COLUMNS)
            self.assertEqual([r["ID"] for r in dsio.rows(f)], [0, 1, 2, 3, 4])
            self.assertEqual([r["NAME"] for r in dsio.rows(f)], ["n0", None, "n2", None, "n4"])
        finally:
            dsio._CHUNK = saved


class Keys(unittest.TestCase):
    def test_numbers_match_numeric_strings(self):
        for value in (5, "5", 5.0, Decimal("5.00")):
            self.assertEqual(dsio.key_of({"K": value}, ["K"]), ("5",))
        self.assertEqual(dsio.key_of({"K": 100}, ["K"]), dsio.key_of({"K": Decimal("1E+2")}, ["K"]))
        self.assertIsNone(dsio.key_of({"K": None}, ["K"]))
        self.assertIsNone(dsio.lookup_key("a", float("nan")))


class DelimitedFiles(unittest.TestCase):
    def test_write_then_read_round_trip(self):
        ctx = JobContext("T")
        source = dsio.frame([
            {"ID": 1, "NAME": 'a "b"', "AMOUNT": Decimal("-3.5"), "DAY": None},
            {"ID": 2, "NAME": None, "AMOUNT": None, "DAY": dt.date(2005, 1, 2)},
        ], COLUMNS)
        with tempfile.TemporaryDirectory() as folder:
            path = os.path.join(folder, "t.csv")
            self.assertEqual(dsio.write_delimited(ctx, source, path, COLUMNS, header=True), 2)
            with open(path, encoding="utf-8", newline="") as handle:
                self.assertEqual(handle.read(), 'ID,NAME,AMOUNT,DAY\n1,"a ""b""",-000003.50,\n2,,,2005-01-02\n')
            back = dsio.read_delimited(ctx, [path], COLUMNS, header=True, null_value="")
        self.assertEqual(list(dsio.rows(back)), list(dsio.rows(source)))

    def test_rejected_lines_and_chunked_writes(self):
        ctx = JobContext("T")
        with tempfile.TemporaryDirectory() as folder:
            path = os.path.join(folder, "t.txt")
            with open(path, "w", encoding="utf-8", newline="") as handle:
                handle.write("1|x|2.5|2005-01-01\n2|y\nz|y|1|2005-01-01\n4||0|\n")
            good, bad = dsio.read_delimited(ctx, [path], COLUMNS, delimiter="|", quote=None, null_value="", reject=True)
            self.assertEqual([r["ID"] for r in dsio.rows(good)], [1, 4])
            self.assertEqual([r["rejected"] for r in dsio.rows(bad)], ["2|y", "z|y|1|2005-01-01"])
            saved, dsio._CHUNK = dsio._CHUNK, 1
            try:
                out = os.path.join(folder, "out.txt")
                dsio.write_delimited(ctx, good, out, COLUMNS[:2] + [dsio.Col("MISSING")], delimiter="|", quote=None)
                with open(out, encoding="utf-8", newline="") as handle:
                    self.assertEqual(handle.read(), "1|x|\n4||\n")
            finally:
                dsio._CHUNK = saved

    def test_nan_and_infinity_are_rejected_as_decimals(self):
        ctx = JobContext("T")
        with tempfile.TemporaryDirectory() as folder:
            path = os.path.join(folder, "t.txt")
            with open(path, "w", encoding="utf-8", newline="") as handle:
                handle.write("1|a|NaN|\n2|b|-Infinity|\n3|c|1.5|\n")
            good, bad = dsio.read_delimited(ctx, [path], COLUMNS, delimiter="|", quote=None, null_value="", reject=True)
        self.assertEqual([r["ID"] for r in dsio.rows(good)], [3])
        self.assertEqual([r["rejected"] for r in dsio.rows(bad)], ["1|a|NaN|", "2|b|-Infinity|"])


if __name__ == "__main__":
    unittest.main()
