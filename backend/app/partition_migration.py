"""分区日期迁移：把旧文本格式分区键归一为可按日比较的日期键。

背景：分区日期从旧文本格式（如 ``20261102``）切换到新格式（``2026-11-02``）
的上线期内，同一张表会同时存在两种写法。此时不能直接做字符串比较——
ASCII 中 ``'-'``(45) 小于 ``'0'``(48)，所以 ``"20261102" > "2026-11-03"``，
11 月 2 日的旧分区在 11 月 3 日的迁冷切点下会被误判成热数据。

本模块统一把分区值解析成 :class:`datetime.date` 再参与比较：

* 解析成功：保留原分区值与原分区号，同时给出归一化新键 ``YYYY-MM-DD``；
* 解析失败：原值原样保留、标记 ``待核``，不产生新键，也不参与迁冷；
* 迁冷候选必须同时满足「真实日期早于切点」且「日期已确认」，
  已在冷层的分区重复执行不再重复计数（每个分区只计一次行数）。
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date
from typing import Any

from app.models import SnapshotPartition

# 旧紧凑格式 20261102
_COMPACT_DATE = re.compile(r"^(\d{4})(\d{2})(\d{2})$")
# 新 ISO 格式 2026-11-02
_ISO_DATE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})$")
# 兼容斜杠格式 2026/11/02
_SLASH_DATE = re.compile(r"^(\d{4})/(\d{2})/(\d{2})$")

FORMAT_COMPACT = "compact"
FORMAT_ISO = "iso"
FORMAT_SLASH = "slash"
FORMAT_INVALID = "invalid"

STATUS_NORMALIZED = "已归一"
STATUS_PENDING_REVIEW = "待核"

TIER_HOT = "热层"
TIER_COLD = "冷层"


@dataclass(frozen=True)
class ParsedPartitionDate:
    """分区值解析结果：有效日期 + 归一化新键，或无效标记。"""

    raw_value: str
    actual_date: date | None
    normalized_partition_key: str
    original_format: str

    @property
    def valid(self) -> bool:
        return self.actual_date is not None

    def to_dict(self) -> dict[str, Any]:
        return {
            "raw_value": self.raw_value,
            "actual_date": self.actual_date.isoformat() if self.actual_date else "",
            "normalized_partition_key": self.normalized_partition_key,
            "original_format": self.original_format,
            "valid": self.valid,
        }


def parse_partition_date(raw_value: Any) -> ParsedPartitionDate:
    """把分区键值解析成可按日比较的日期。

    支持 ``20261102``、``2026-11-02``、``2026/11/02`` 三种写法；
    无法解析或不是合法日历日期（如 ``20261301``）时返回无效结果，
    由调用方保留原值并挂起待核，绝不猜测补全。
    """
    text = str(raw_value or "").strip()
    for pattern, fmt_tag in (
        (_COMPACT_DATE, FORMAT_COMPACT),
        (_ISO_DATE, FORMAT_ISO),
        (_SLASH_DATE, FORMAT_SLASH),
    ):
        match = pattern.match(text)
        if not match:
            continue
        year, month, day = (int(part) for part in match.groups())
        try:
            actual = date(year, month, day)
        except ValueError:
            break
        return ParsedPartitionDate(
            raw_value=text,
            actual_date=actual,
            normalized_partition_key=actual.isoformat(),
            original_format=fmt_tag,
        )
    return ParsedPartitionDate(
        raw_value=text,
        actual_date=None,
        normalized_partition_key="",
        original_format=FORMAT_INVALID,
    )


@dataclass
class PartitionMigrationDetail:
    """单分区的日期迁移明细，供冷热分层流水线核定候选与后续取数。"""

    partition_code: str
    target_wide_table: str
    original_partition_value: str
    original_format: str
    actual_date: date | None
    normalized_partition_key: str
    date_migration_status: str
    confirmed: bool
    review_note: str
    record_count: int
    storage_tier: str = TIER_HOT
    counted_rows: int = 0
    snapshot: SnapshotPartition | None = field(default=None, repr=False)

    def to_dict(self) -> dict[str, Any]:
        return {
            "分区编号": self.partition_code,
            "目标宽表": self.target_wide_table,
            "原分区键值": self.original_partition_value,
            "原格式": self.original_format,
            "实际日期": self.actual_date.isoformat() if self.actual_date else "",
            "新分区键": self.normalized_partition_key,
            "日期迁移状态": self.date_migration_status,
            "已确认": self.confirmed,
            "审核备注": self.review_note,
            "记录行数": self.record_count,
            "存储层级": self.storage_tier,
            "已计迁冷行数": self.counted_rows,
        }


class PartitionMigrationRegistry:
    """分区日期迁移明细表：承载解析、确认暂扣与迁冷候选核定。"""

    def __init__(self) -> None:
        self._details: dict[str, PartitionMigrationDetail] = {}
        # 显式暂扣的分区号 -> 暂扣原因，同步新快照时持续生效
        self._holds: dict[str, str] = {}

    # ---- 登记与同步 ----

    def register(self, snapshot: SnapshotPartition,
                 confirmed: bool | None = None) -> PartitionMigrationDetail:
        """登记一个分区快照；已登记则刷新行数等可变明细，保留层级与确认结论。"""
        existing = self._details.get(snapshot.partition_code)
        if existing is not None:
            existing.record_count = snapshot.record_count
            existing.snapshot = snapshot
            return existing

        parsed = parse_partition_date(snapshot.partition_key_value)
        if parsed.valid:
            is_confirmed = (
                snapshot.partition_code not in self._holds
                if confirmed is None
                else confirmed
            )
            detail = PartitionMigrationDetail(
                partition_code=snapshot.partition_code,
                target_wide_table=snapshot.target_wide_table,
                original_partition_value=parsed.raw_value,
                original_format=parsed.original_format,
                actual_date=parsed.actual_date,
                normalized_partition_key=parsed.normalized_partition_key,
                date_migration_status=STATUS_NORMALIZED,
                confirmed=is_confirmed,
                review_note="" if is_confirmed else "日期确认暂扣，待放行",
                record_count=snapshot.record_count,
                snapshot=snapshot,
            )
        else:
            # 无效日期：原值保留待核，不允许确认，不参与任何按日比较
            detail = PartitionMigrationDetail(
                partition_code=snapshot.partition_code,
                target_wide_table=snapshot.target_wide_table,
                original_partition_value=parsed.raw_value,
                original_format=FORMAT_INVALID,
                actual_date=None,
                normalized_partition_key="",
                date_migration_status=STATUS_PENDING_REVIEW,
                confirmed=False,
                review_note="无效日期，原值保留待核",
                record_count=snapshot.record_count,
                snapshot=snapshot,
            )
        self._details[detail.partition_code] = detail
        return detail

    def sync(self, snapshots: list[SnapshotPartition]) -> None:
        """把引擎持有的全量快照同步进迁移明细注册表。"""
        for snapshot in snapshots:
            self.register(snapshot)

    # ---- 确认 / 暂扣 ----

    def confirm(self, partition_code: str, note: str = "") -> PartitionMigrationDetail | None:
        """放行某分区的日期确认；无效日期无实际日期可确认，仍保持待核。"""
        detail = self._details.get(partition_code)
        if detail is None:
            return None
        self._holds.pop(partition_code, None)
        if detail.actual_date is not None:
            detail.confirmed = True
            detail.review_note = note
        else:
            detail.confirmed = False
            detail.review_note = note or "无效日期，原值保留待核"
        return detail

    def hold(self, partition_code: str, note: str = "日期确认暂扣，待放行") -> PartitionMigrationDetail | None:
        """暂扣某分区的日期确认，迁冷候选核定将排除它。"""
        detail = self._details.get(partition_code)
        if detail is None:
            return None
        self._holds[partition_code] = note
        detail.confirmed = False
        detail.review_note = note
        return detail

    # ---- 查询 ----

    def list_details(self) -> list[PartitionMigrationDetail]:
        return list(self._details.values())

    def get(self, partition_code: str) -> PartitionMigrationDetail | None:
        return self._details.get(partition_code)

    # ---- 迁冷候选核定 ----

    def cold_candidates(
        self, cutoff: date, target_wide_table: str | None = None,
    ) -> tuple[list[PartitionMigrationDetail], list[dict[str, str]]]:
        """按真实切点核定迁冷候选。

        只有「实际日期严格早于切点」且「日期已确认」且「当前仍在热层」
        的分区才是候选；其余分区连同排除原因一并返回，保证核定可审计。
        """
        candidates: list[PartitionMigrationDetail] = []
        excluded: list[dict[str, str]] = []
        for detail in self._details.values():
            if target_wide_table and detail.target_wide_table != target_wide_table:
                continue
            if detail.storage_tier == TIER_COLD:
                reason = "已在冷层，不重复计数"
            elif detail.actual_date is None:
                reason = "无效日期待核，保留原值"
            elif not detail.confirmed:
                reason = "日期未确认，暂扣"
            elif not detail.actual_date < cutoff:
                reason = f"实际日期 {detail.actual_date.isoformat()} 未早于切点 {cutoff.isoformat()}"
            else:
                candidates.append(detail)
                continue
            excluded.append({"分区编号": detail.partition_code, "排除原因": reason})
        return candidates, excluded

    def mark_cold(self, detail: PartitionMigrationDetail) -> int:
        """把一个候选分区标记为冷层，返回本次计入的行数（只计一次）。"""
        if detail.storage_tier == TIER_COLD or detail.snapshot is None:
            return 0
        detail.storage_tier = TIER_COLD
        detail.counted_rows = detail.snapshot.record_count
        detail.snapshot.status_mark = "已迁移至冷存储"
        return detail.counted_rows
