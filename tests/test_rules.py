"""业务规则单元测试：python -m unittest discover -s tests（或 pytest tests）"""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "spark_jobs"))

from lib.rules import normalize_address, parse_amount, parse_date, rent_basis  # noqa: E402


class TestNormalizeAddress(unittest.TestCase):
    def test_variants_map_to_same_key(self):
        variants = [
            "Unit 3/12 Smith Street",
            "3/12 Smith St",
            "U3/12 smith st.",
            "3/12 SMITH STREET, NSW 2000",
            "Unit 3, 12 Smith Street",
        ]
        keys = {normalize_address(v)[0] for v in variants}
        self.assertEqual(keys, {"3/12 SMITH ST"})

    def test_no_unit(self):
        self.assertEqual(normalize_address("45 George Road"), ("45 GEORGE RD", True))

    def test_typo_fix(self):
        fixes = {"SMTIH": "SMITH"}
        self.assertEqual(normalize_address("3/12 Smtih St", fixes), ("3/12 SMITH ST", True))

    def test_unparseable(self):
        self.assertEqual(normalize_address("behind the shop"), (None, False))
        self.assertEqual(normalize_address(None), (None, False))
        self.assertEqual(normalize_address("12 Smith Xyz"), (None, False))


class TestParseDate(unittest.TestCase):
    def test_full_formats(self):
        self.assertEqual(parse_date("2026-03-05"), ("2026-03-05", "ok"))
        self.assertEqual(parse_date("05/03/2026"), ("2026-03-05", "ok"))

    def test_no_year_with_anchor(self):
        # 锚点 2026-01-10，"28.12" 应推断为 2025-12-28（最近且不晚于锚点+60天）
        self.assertEqual(parse_date("28.12", "2026-01-10"), ("2025-12-28", "no_year_inferred"))
        self.assertEqual(parse_date("5.3", "2026-02-20"), ("2026-03-05", "no_year_inferred"))

    def test_no_year_without_anchor_is_not_guessed(self):
        self.assertEqual(parse_date("5.3"), (None, "no_year_unresolved"))

    def test_invalid_and_empty(self):
        self.assertEqual(parse_date("next week"), (None, "invalid"))
        self.assertEqual(parse_date(""), (None, "empty"))


class TestAmountAndRentBasis(unittest.TestCase):
    def test_parse_amount(self):
        self.assertEqual(parse_amount("$1,234.50"), 1234.5)
        self.assertEqual(parse_amount(" 600 "), 600.0)
        self.assertIsNone(parse_amount("tbc"))

    def test_rent_basis(self):
        self.assertEqual(rent_basis(0, None, 600), "no_tenancy")
        self.assertEqual(rent_basis(2, 300, 600), "room_rents")
        self.assertEqual(rent_basis(1, None, 600), "unclear")
        self.assertEqual(rent_basis(1, 650, 600), "unit_rent")
        self.assertEqual(rent_basis(1, 320, 600), "room_rent")


if __name__ == "__main__":
    unittest.main()
