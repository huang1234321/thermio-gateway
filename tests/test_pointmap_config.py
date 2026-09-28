"""L1 pointmap ∩ config 会合（§3.2）+ adopt 版本迁移（§14.3-3）+ config 应用。"""

from __future__ import annotations

import pytest

from thermio_gateway.contracts import (
    ConfigPoint,
    GatewayConfig,
)
from thermio_gateway.db import meta_get, meta_set, open_db
from thermio_gateway.downlink.config import apply_config
from thermio_gateway.pointmap import adopt_staging, load_pointmap, replace_staging


@pytest.fixture()
def db(tmp_path):
    conn = open_db(str(tmp_path / "cache.db"))
    yield conn
    conn.close()


def seed_pointmap(db, rows):
    """直接以 pointmap 行灌库（绕过 staging，模拟已 adopt 状态）。"""
    with db:
        db.execute("DELETE FROM pointmap")
        db.executemany(
            "INSERT INTO pointmap (raw_name, bacnet_device_id, obj_type, obj_instance,"
            " unit_raw, writable, interval_s, enabled) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )


class TestApplyConfigMeet:
    def test_intersection_and_failures(self, db):
        seed_pointmap(
            db,
            [
                ("CHWS_T", 519, "analog-value", 1, "degC", 0, None, 1),
                ("OAT", 519, "analog-input", 2, "degF", 0, None, 1),
                ("PUMP", 519, "binary-value", 3, None, 0, None, 1),
                ("DISABLED_ONE", 519, "analog-value", 9, None, 0, None, 0),
            ],
        )
        cfg = GatewayConfig(
            job_id="job-1",
            points=[
                ConfigPoint(raw_name="CHWS_T", ref="CHWS_T", unit_raw="degC", unit_std="degC"),
                ConfigPoint(raw_name="OAT", ref="OAT", unit_raw="degF", unit_std="degC"),
                ConfigPoint(raw_name="PUMP", ref="PUMP", unit_raw=None, unit_std=None),
                ConfigPoint(raw_name="NOT_IN_MAP", ref="NOT_IN_MAP", unit_raw=None, unit_std=None),
                ConfigPoint(
                    raw_name="DISABLED_ONE", ref="DISABLED_ONE", unit_raw=None, unit_std=None
                ),
            ],
            offline_action=None,
        )
        active_set, ack = apply_config(db, cfg, {})
        assert ack.ok_count == 3
        assert {f.raw_name: f.reason for f in ack.failed} == {
            "NOT_IN_MAP": "POINT_NOT_IN_MAP",
            "DISABLED_ONE": "POINT_DISABLED",
        }
        assert [p.raw_name for p in active_set.points] == ["CHWS_T", "OAT", "PUMP"]
        # unit 会合：pointmap unit_raw 优先（物理真相），config unit_std 落下行换算
        assert active_set.units["OAT"] == ("degF", "degC")

    def test_empty_config_empties_active(self, db):
        seed_pointmap(db, [("CHWS_T", 519, "analog-value", 1, "degC", 0, None, 1)])
        cfg = GatewayConfig(job_id="job-2", points=[], offline_action=None)
        active_set, ack = apply_config(db, cfg, {})
        assert ack.ok_count == 0
        assert active_set.points == []


class TestAdoptStaging:
    def test_full_lifecycle_new_drift_missing_preserved(self, db):
        # 首轮：两点入 staging
        replace_staging(
            db,
            [
                ("CHWS_T", 519, "analog-value", 1, "degC", "desc"),
                ("OAT", 519, "analog-input", 2, "degF", None),
            ],
        )
        report = adopt_staging(db)
        assert report["added"] == ["CHWS_T", "OAT"]
        assert not report["address_drift"]

        # 人工调参（§3.2 sqlite3 直改：interval/writable/unit）
        db.execute(
            "UPDATE pointmap SET interval_s = 30, writable = 1, unit_raw = 'K'"
            " WHERE raw_name = 'CHWS_T'"
        )

        # 次轮重发现：CHWS_T 地址漂移（analog-value:1 → :12），OAT 消失，新增 PUMP
        replace_staging(
            db,
            [
                ("CHWS_T", 519, "analog-value", 12, "degC", "desc"),
                ("PUMP", 519, "binary-value", 3, None, None),
            ],
        )
        report = adopt_staging(db)
        # 地址漂移：跟随物理地址，raw_name 稳定（云端零感知）
        assert report["address_drift"] == [
            {
                "raw_name": "CHWS_T",
                "old": [519, "analog-value", 1],
                "new": [519, "analog-value", 12],
            }
        ]
        assert report["missing"] == ["OAT"]  # 不删不改（诚实 bad 行）
        assert report["added"] == ["PUMP"]

        points = {p.raw_name: p for p in load_pointmap(db)}
        assert (points["CHWS_T"].obj_type, points["CHWS_T"].obj_instance) == ("analog-value", 12)
        # 工程调参列不被覆盖
        assert points["CHWS_T"].interval_s == 30
        assert points["CHWS_T"].writable is True
        assert points["CHWS_T"].unit_raw == "K"
        # 消失点保留
        assert points["OAT"] is not None
        assert points["PUMP"].writable is False

    def test_adopt_is_idempotent(self, db):
        replace_staging(db, [("A", 1, "analog-value", 1, None, None)])
        adopt_staging(db)
        report = adopt_staging(db)
        assert report["added"] == []
        assert report["unchanged"] == ["A"]


class TestMeta:
    def test_seq_persists_across_reopen(self, db, tmp_path):
        from thermio_gateway.cache import next_seq

        assert next_seq(db) == 0
        assert next_seq(db) == 1
        db.close()
        reopened = open_db(str(tmp_path / "cache.db"))
        assert next_seq(reopened) == 2  # 跨重启不断裂（§5.1）
        reopened.close()

    def test_applied_job_id_roundtrip(self, db):
        meta_set(db, "applied_job_id", "job-42")
        assert meta_get(db, "applied_job_id") == "job-42"
