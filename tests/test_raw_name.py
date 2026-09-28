"""L1 raw_name 生成规则（§3.3 全分支——导出名即物理身份，不静默改动）。

约束交集：ingest §3.2 name（1..64，[A-Za-z0-9_.:-]）⊂ M2 §5.1（1..128），
取严格侧 64。
"""

from __future__ import annotations

from thermio_gateway.contracts import is_valid_uplink_name
from thermio_gateway.pointmap import generate_raw_name


def make(taken: set[str] | None = None):
    return taken if taken is not None else set()


class TestGenerateRawName:
    def test_plain_name_unchanged(self):
        d = generate_raw_name("CHWS_T_9F", 101, "analog-value", 1, make())
        assert d.raw_name == "CHWS_T_9F"
        assert d.marks == ()

    def test_charset_allowed_punctuation(self):
        d = generate_raw_name("a.b:c-d_e", 101, "analog-value", 1, make())
        assert d.raw_name == "a.b:c-d_e"
        assert d.marks == ()

    def test_violating_chars_sanitized(self):
        d = generate_raw_name("CHWS T 9F#1", 101, "analog-value", 1, make())
        assert d.raw_name == "CHWS_T_9F_1"
        assert "sanitized" in d.marks

    def test_chinese_sanitized(self):
        original = "冷冻水供水温度"
        d = generate_raw_name(original, 101, "analog-value", 1, make())
        assert d.raw_name == "_" * len(original)
        assert "sanitized" in d.marks
        assert is_valid_uplink_name(d.raw_name)

    def test_leading_trailing_space_trimmed_without_mark(self):
        """首尾空白按 M2 trim 处理——不是字符替换，不打 sanitized 标记。"""
        d = generate_raw_name("  CHWS  ", 101, "analog-value", 1, make())
        assert d.raw_name == "CHWS"
        assert d.marks == ()

    def test_duplicate_gets_suffix(self):
        taken = {"PUMP"}
        d = generate_raw_name("PUMP", 101, "binary-value", 2, taken)
        assert d.raw_name == "PUMP_2"
        assert "deduped" in d.marks
        assert "PUMP_2" in taken

    def test_duplicate_chain(self):
        taken = {"PUMP", "PUMP_2"}
        d = generate_raw_name("PUMP", 101, "binary-value", 3, taken)
        assert d.raw_name == "PUMP_3"

    def test_overlong_truncated_to_64(self):
        original = "X" * 80
        d = generate_raw_name(original, 101, "analog-value", 1, make())
        assert len(d.raw_name) == 64
        assert "truncated" in d.marks

    def test_truncated_then_deduped_stays_within_64(self):
        taken = {"X" * 64}
        d = generate_raw_name("X" * 80, 101, "analog-value", 1, taken)
        assert len(d.raw_name) <= 64
        assert "truncated" in d.marks
        assert "deduped" in d.marks

    def test_empty_name_generated(self):
        d = generate_raw_name("", 519, "analog-value", 7, make())
        assert d.raw_name == "DEV519_analog-value_7"
        assert "generated" in d.marks

    def test_whitespace_only_name_generated(self):
        d = generate_raw_name("   ", 519, "analog-value", 7, make())
        assert d.raw_name == "DEV519_analog-value_7"
        assert "generated" in d.marks

    def test_all_outputs_satisfy_uplink_charset(self):
        cases = [
            ("CHWS T 9F#1", 1, "analog-value", 1),
            ("冷冻水温度", 2, "analog-input", 3),
            ("X" * 100, 3, "analog-value", 1),
            ("", 4, "binary-value", 2),
        ]
        taken: set[str] = set()
        for original, dev, otype, inst in cases:
            d = generate_raw_name(original, dev, otype, inst, taken)
            assert is_valid_uplink_name(d.raw_name), d.raw_name
