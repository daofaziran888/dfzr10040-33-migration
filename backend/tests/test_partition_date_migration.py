"""分区日期迁移与冷热分层切点核定测试。

覆盖任务叙述的完整链路：
旧文本格式（20261102）与新格式（2026-11-02）并存 -> 归一为可按日比较的日期键
-> 日期确认/暂扣 -> 以 2026-11-03 为切点执行迁冷 -> 凭原分区号取数核对层级
-> 放行确认后重跑 -> 同日两份快照各自只计一次，11 月 4 日留热层，无效日期保留待核。
"""
from __future__ import annotations

from datetime import date

import pytest

from app.fusion_engine import FusionEngine
from app.partition_migration import (
    STATUS_NORMALIZED,
    STATUS_PENDING_REVIEW,
    TIER_COLD,
    TIER_HOT,
    parse_partition_date,
)

WIDE_TABLE = "WT_DUAL_FORMAT"


@pytest.fixture
def engine() -> FusionEngine:
    return FusionEngine()


def _seed_dual_snapshots(engine: FusionEngine) -> dict[str, int]:
    """同表四份快照：11 月 2 日新旧格式各一份、11 月 4 日一份、无效日期一份。"""
    codes: dict[str, int] = {}

    snap = engine.create_snapshot(WIDE_TABLE, partition_key_value="20261102")
    snap.record_count = 1200
    snap.status_mark = "可用"
    codes["compact_1102"] = snap.partition_id

    snap = engine.create_snapshot(WIDE_TABLE, partition_key_value="2026-11-02")
    snap.record_count = 800
    snap.status_mark = "可用"
    codes["iso_1102"] = snap.partition_id

    snap = engine.create_snapshot(WIDE_TABLE, partition_key_value="2026-11-04")
    snap.record_count = 600
    snap.status_mark = "可用"
    codes["iso_1104"] = snap.partition_id

    snap = engine.create_snapshot(WIDE_TABLE, partition_key_value="2026-11-XX")
    snap.record_count = 300
    snap.status_mark = "可用"
    codes["invalid"] = snap.partition_id

    return codes


class TestPartitionDateParsing:
    """日期解析：两种写法归一到同一真实日期，字符串比较冲突被消除。"""

    def test_two_formats_resolve_to_same_day(self) -> None:
        compact = parse_partition_date("20261102")
        iso = parse_partition_date("2026-11-02")
        assert compact.valid and iso.valid
        assert compact.actual_date == iso.actual_date == date(2026, 11, 2)
        assert compact.normalized_partition_key == "2026-11-02"
        assert iso.normalized_partition_key == "2026-11-02"
        assert compact.original_format == "compact"
        assert iso.original_format == "iso"

    def test_string_comparison_conflict_documented(self) -> None:
        """复现并固化旧实现的冲突：字符串比较下旧格式 11-02 反而晚于 11-03。"""
        # 字符串比较：'-' 的 ASCII 小于数字，旧键被误判
        assert "20261102" > "2026-11-03"
        # 归一后按真实日期比较：11-02 严格早于切点 11-03
        parsed_old = parse_partition_date("20261102")
        parsed_cutoff = parse_partition_date("2026-11-03")
        assert parsed_old.actual_date < parsed_cutoff.actual_date

    def test_invalid_date_keeps_raw_value(self) -> None:
        parsed = parse_partition_date("2026-11-XX")
        assert not parsed.valid
        assert parsed.raw_value == "2026-11-XX"
        assert parsed.normalized_partition_key == ""
        # 非法日历日期同样判无效，不猜测补全
        assert not parse_partition_date("20261301").valid

    def test_cutoff_accepts_both_formats(self) -> None:
        assert parse_partition_date("20261103").actual_date == date(2026, 11, 3)
        assert parse_partition_date("2026-11-03").actual_date == date(2026, 11, 3)


class TestPartitionMigrationWorkflow:
    """完整迁移叙述：确认暂扣 -> 切点迁冷 -> 原号取数 -> 放行重跑。"""

    def test_full_migration_narrative(self, engine: FusionEngine) -> None:
        ids = _seed_dual_snapshots(engine)
        compact_snap = engine._snapshots[ids["compact_1102"]]
        iso_snap = engine._snapshots[ids["iso_1102"]]
        nov4_snap = engine._snapshots[ids["iso_1104"]]
        invalid_snap = engine._snapshots[ids["invalid"]]

        # 登记日期迁移明细：默认日期可确认；随后暂扣 ISO 那份 11-02 的确认
        for partition_id in ids.values():
            engine.register_snapshot_partition_migration(partition_id)
        held = engine.hold_partition_date(
            ids["iso_1102"], note="ISO 版 11-02 快照日期确认暂扣",
        )
        assert held is not None and held.confirmed is False

        details = {d["分区编号"]: d for d in engine.partition_migration_details()}

        # 旧文本格式那份先确认通过；新键按实际日期生成，原值保留
        compact_detail = details[compact_snap.partition_code]
        assert compact_detail["原分区键值"] == "20261102"
        assert compact_detail["新分区键"] == "2026-11-02"
        assert compact_detail["实际日期"] == "2026-11-02"
        assert compact_detail["日期迁移状态"] == STATUS_NORMALIZED
        assert compact_detail["已确认"] is True

        # 被暂扣的另一份 11-02：明细仍指向同一真实日期，但确认未放行
        held_detail = details[iso_snap.partition_code]
        assert held_detail["实际日期"] == "2026-11-02"
        assert held_detail["已确认"] is False
        assert "暂扣" in held_detail["审核备注"]

        # 无效日期：原值保留待核，没有新分区键，也不可确认
        invalid_detail = details[invalid_snap.partition_code]
        assert invalid_detail["日期迁移状态"] == STATUS_PENDING_REVIEW
        assert invalid_detail["原分区键值"] == "2026-11-XX"
        assert invalid_detail["新分区键"] == ""
        assert invalid_detail["已确认"] is False

        # 冷热分层策略按日期迁移明细核定真实候选，切点 2026-11-03
        strategy = engine.create_cold_store_strategy(WIDE_TABLE, hot_data_days=1)
        first_run = engine.migrate_to_cold_store(
            strategy.strategy_id, hot_partition_cutoff="2026-11-03",
        )
        assert first_run["ok"] is True
        assert first_run["cutoff_date"] == "2026-11-03"
        # 只迁已确认且真实日期早于切点的旧格式 11-02 这一份
        assert first_run["migrated_partitions"] == 1
        assert first_run["migrated_rows"] == 1200
        migrated_codes = {item["分区编号"] for item in first_run["migration_details"]}
        assert migrated_codes == {compact_snap.partition_code}

        exclusion = {row["分区编号"]: row["排除原因"] for row in first_run["excluded"]}
        assert iso_snap.partition_code in exclusion  # 暂扣
        assert nov4_snap.partition_code in exclusion  # 未早于切点
        assert invalid_snap.partition_code in exclusion  # 无效日期待核

        # 凭两个原分区号取数：同日两份快照各按各的编号读取，层级不同但明细完整
        compact_view = engine.get_partition_by_code(compact_snap.partition_code)
        iso_view = engine.get_partition_by_code(iso_snap.partition_code)
        assert compact_view is not None and iso_view is not None
        assert compact_view["原分区键值"] == "20261102"
        assert compact_view["存储层级"] == TIER_COLD
        assert compact_view["记录行数"] == 1200
        assert iso_view["原分区键值"] == "2026-11-02"
        assert iso_view["存储层级"] == TIER_HOT
        assert iso_view["记录行数"] == 800
        # 原分区键值原样保留，后续取数仍按实际日期与原编号
        assert compact_snap.partition_key_value == "20261102"
        assert iso_snap.partition_key_value == "2026-11-02"

        # 放行另一份 11-02 的日期确认后重跑：只补迁这一份，已在冷层的不重复计
        engine.confirm_partition_date(
            ids["iso_1102"], note="ISO 版 11-02 日期核对无误，放行",
        )
        second_run = engine.migrate_to_cold_store(
            strategy.strategy_id, hot_partition_cutoff="20261103",  # 切点换旧格式写法
        )
        assert second_run["cutoff_date"] == "2026-11-03"
        assert second_run["migrated_partitions"] == 1
        assert second_run["migrated_rows"] == 800
        assert {item["分区编号"] for item in second_run["migration_details"]} == {
            iso_snap.partition_code,
        }

        # 两份 11 月 2 日快照各自只计一次行数；再重跑为零，幂等
        assert strategy.migrated_rows == 2000
        third_run = engine.migrate_to_cold_store(
            strategy.strategy_id, hot_partition_cutoff="2026-11-03",
        )
        assert third_run["migrated_partitions"] == 0
        assert third_run["migrated_rows"] == 0
        assert strategy.migrated_rows == 2000

        # 终态核对：两份 11-02 在冷层，11-04 仍在热层，无效日期原值保留且在热层
        final = {d["分区编号"]: d for d in engine.partition_migration_details()}
        assert final[compact_snap.partition_code]["存储层级"] == TIER_COLD
        assert final[compact_snap.partition_code]["已计迁冷行数"] == 1200
        assert final[iso_snap.partition_code]["存储层级"] == TIER_COLD
        assert final[iso_snap.partition_code]["已计迁冷行数"] == 800
        assert final[nov4_snap.partition_code]["存储层级"] == TIER_HOT
        assert final[nov4_snap.partition_code]["已计迁冷行数"] == 0
        assert final[invalid_snap.partition_code]["存储层级"] == TIER_HOT
        assert final[invalid_snap.partition_code]["原分区键值"] == "2026-11-XX"
        assert final[invalid_snap.partition_code]["日期迁移状态"] == STATUS_PENDING_REVIEW

        # 同日两份快照仍可用各自原编号读取
        assert engine.get_partition_by_code(compact_snap.partition_code)["存储层级"] == TIER_COLD
        assert engine.get_partition_by_code(iso_snap.partition_code)["存储层级"] == TIER_COLD
        assert engine.get_partition_by_code("SNAP-9999") is None

    def test_nov4_stays_hot_under_nov3_cutoff(self, engine: FusionEngine) -> None:
        """11 月 4 日快照在 11-03 切点下永远是热层候选之外。"""
        ids = _seed_dual_snapshots(engine)
        for partition_id in ids.values():
            engine.register_snapshot_partition_migration(partition_id)
        strategy = engine.create_cold_store_strategy(WIDE_TABLE, hot_data_days=1)
        result = engine.migrate_to_cold_store(
            strategy.strategy_id, hot_partition_cutoff="20261103",
        )
        nov4 = engine._snapshots[ids["iso_1104"]]
        view = engine.get_partition_by_code(nov4.partition_code)
        assert view is not None
        assert view["存储层级"] == TIER_HOT
        assert nov4.status_mark == "可用"
        assert all(
            row["分区编号"] != nov4.partition_code
            for row in result["migration_details"]
        )

    def test_invalid_cutoff_rejected(self, engine: FusionEngine) -> None:
        """切点本身不可解析时拒绝执行，不做任何迁移。"""
        strategy = engine.create_cold_store_strategy(WIDE_TABLE, hot_data_days=1)
        result = engine.migrate_to_cold_store(
            strategy.strategy_id, hot_partition_cutoff="not-a-date",
        )
        assert result["ok"] is False
        assert "切点" in result["message"]
        assert strategy.migrated_rows == 0

    def test_hold_survives_registry_sync(self, engine: FusionEngine) -> None:
        """迁冷前的全量同步不会冲掉暂扣结论。"""
        ids = _seed_dual_snapshots(engine)
        engine.register_snapshot_partition_migration(ids["iso_1102"])
        engine.hold_partition_date(ids["iso_1102"])
        # 再次全量同步（迁冷内部也会做）
        engine.partition_migration_details()
        detail = engine._migration_registry.get(
            engine._snapshots[ids["iso_1102"]].partition_code,
        )
        assert detail is not None and detail.confirmed is False
