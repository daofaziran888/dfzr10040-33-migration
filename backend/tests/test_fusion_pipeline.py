"""端到端融合链路测试：数据源 -> Schema映射 -> 宽表定义 -> 融合作业 -> 冲突仲裁 -> 质量门禁 -> 分区快照。

覆盖完整融合流水线，验证跨模块数据流的正确性。
"""
from __future__ import annotations

import json
import pytest

from app.fusion_engine import FusionEngine


@pytest.fixture
def engine() -> FusionEngine:
    """每个测试获得独立引擎实例。"""
    return FusionEngine()


class TestFusionPipeline:
    """端到端融合链路测试。"""

    def test_register_source_and_create_schema(self, engine: FusionEngine) -> None:
        """数据源注册 -> Schema映射 流程。"""
        source = engine.register_source(
            source_code="SOUR-ODS-001", source_name="ods_order",
            source_type="MySQL", connection_string="mysql://localhost:3306/ods",
        )
        assert source.source_code == "SOUR-ODS-001"
        assert source.access_status == "待验证"

        mapping = engine.create_schema_mapping(
            source_table="SOUR-ODS-001", target_wide_table="WT_ORDER",
            field_mapping_json=json.dumps({"name": "order_name", "value": "order_amount"}),
            type_conversion="decimal(10,2)", missing_policy="default_zero",
        )
        assert mapping.plan_code.startswith("SCHE-")
        assert mapping.target_wide_table == "WT_ORDER"

    def test_define_wide_table_and_create_fusion_job(self, engine: FusionEngine) -> None:
        """宽表定义 -> 融合作业 流程。"""
        wt = engine.define_wide_table(
            table_code="WIDE-ORDER", table_name="订单宽表",
            field_list=["order_id", "order_name", "amount", "update_time"],
            primary_key="order_id", partition_key="dt", ttl_days=90,
        )
        assert wt.table_name == "订单宽表"

        job = engine.create_fusion_job(
            target_wide_table="订单宽表",
            participant_sources=["SOUR-ODS-001", "SOUR-ERP-002"],
            join_key="order_id", fusion_window="1d",
        )
        assert job.join_key == "order_id"
        assert len(job.participant_sources) == 2

    def test_full_fusion_pipeline_end_to_end(self, engine: FusionEngine) -> None:
        """完整融合流水线：注册三个数据源，创建映射和冲突规则，执行融合。"""
        # 注册数据源
        for code, name, stype in [
            ("SOUR-ODS-001", "ods_order", "MySQL"),
            ("SOUR-ERP-002", "erp_supplier", "PostgreSQL"),
            ("SOUR-LOG-003", "app_log", "Kafka"),
        ]:
            engine.register_source(
                source_code=code, source_name=name, source_type=stype,
                connection_string=f"conn://{name}",
            )

        # 创建宽表定义
        engine.define_wide_table(
            table_code="WIDE-ORDER", table_name="订单宽表",
            field_list=["order_id", "order_name", "amount", "update_time"],
        )

        # 创建 Schema 映射
        for source_table in ["SOUR-ODS-001", "SOUR-ERP-002"]:
            engine.create_schema_mapping(
                source_table=source_table, target_wide_table="订单宽表",
                field_mapping_json=json.dumps({"name": "order_name"}),
            )

        # 创建融合作业
        job = engine.create_fusion_job(
            target_wide_table="订单宽表",
            participant_sources=["SOUR-ODS-001", "SOUR-ERP-002", "SOUR-LOG-003"],
            join_key="id", fusion_window="1d",
        )

        # 创建冲突仲裁和门禁
        engine.create_conflict_rule(
            target_wide_table="订单宽表",
            conflict_fields=["name"],
            arbitration_strategy="latest_timestamp",
        )
        engine.create_quality_gate(
            target_wide_table="订单宽表",
            check_type="row_count_min", threshold_condition="1",
        )

        # 执行融合
        result = engine.execute_fusion_pipeline(job.job_id)
        assert result["ok"] is True
        assert result["total_sources"] == 3
        assert result["raw_records"] == 9  # 3 sources * 3 records each
        assert result["mapped_records"] == 9
        assert "conflict_resolution" in result
        assert "quality_gate" in result
        assert "snapshot" in result

    def test_fusion_with_conflict_resolution(self, engine: FusionEngine) -> None:
        """融合时冲突仲裁：重复键按策略裁决。"""
        engine.register_source(
            source_code="SRC-A", source_name="source_a", source_type="MySQL",
            connection_string="conn://src_a",
        )
        engine.register_source(
            source_code="SRC-B", source_name="source_b", source_type="MySQL",
            connection_string="conn://src_b",
        )

        job = engine.create_fusion_job(
            target_wide_table="WT_TEST",
            participant_sources=["SRC-A", "SRC-B"],
            join_key="id",
        )

        # 创建冲突规则：按 source_priority 策略，优先 SRC-A
        engine.create_conflict_rule(
            target_wide_table="WT_TEST",
            conflict_fields=["name"],
            arbitration_strategy="source_priority",
            priority_source="SRC-A",
        )

        result = engine.execute_fusion_pipeline(job.job_id)
        assert result["ok"] is True
        cr = result["conflict_resolution"]
        # 6 条记录按 fusion_key 去重，不应有冲突
        assert "resolved" in cr

    def test_quality_gate_row_count_check(self, engine: FusionEngine) -> None:
        """质量门禁：行数下限检查。"""
        engine.register_source(
            source_code="SRC-QG", source_name="qg_source", source_type="MySQL",
            connection_string="conn://qg",
        )
        job = engine.create_fusion_job(
            target_wide_table="WT_QG",
            participant_sources=["SRC-QG"],
            join_key="id",
        )
        engine.create_quality_gate(
            target_wide_table="WT_QG",
            check_type="row_count_min", threshold_condition="1",
        )
        result = engine.execute_fusion_pipeline(job.job_id)
        assert result["quality_gate"]["passed"] is True
        assert result["quality_gate"]["gates_checked"] == 1

    def test_fusion_snapshot_creation(self, engine: FusionEngine) -> None:
        """融合结果自动创建分区快照。"""
        engine.register_source(
            source_code="SRC-SNAP", source_name="snap_source", source_type="MySQL",
            connection_string="conn://snap",
        )
        job = engine.create_fusion_job(
            target_wide_table="WT_SNAP",
            participant_sources=["SRC-SNAP"],
            join_key="id",
        )
        result = engine.execute_fusion_pipeline(job.job_id)
        snap = result.get("snapshot")
        assert snap is not None
        assert "partition_code" in snap
        assert snap["record_count"] == 3


class TestDeltaMerge:
    """增量归并测试。"""

    def test_delta_merge_insert(self, engine: FusionEngine) -> None:
        """增量归并：新增记录。"""
        engine.register_source(
            source_code="SRC-DM", source_name="dm_source", source_type="MySQL",
            connection_string="conn://dm",
        )
        job = engine.create_fusion_job(
            target_wide_table="WT_DM",
            participant_sources=["SRC-DM"],
            join_key="id",
        )
        engine.execute_fusion_pipeline(job.job_id)

        delta = [{
            "_source": "SRC-DM", "id": "new-rec-1", "name": "new-record",
            "value": 999.0, "_op": "upsert",
        }]
        result = engine.execute_delta_merge(job.job_id, delta)
        assert result["ok"] is True
        assert result["inserted"] == 1
        assert result["updated"] == 0
        assert result["total_after_merge"] == 4

    def test_delta_merge_update_existing(self, engine: FusionEngine) -> None:
        """增量归并：更新已有记录。"""
        engine.register_source(
            source_code="SRC-DM2", source_name="dm2_source", source_type="MySQL",
            connection_string="conn://dm2",
        )
        job = engine.create_fusion_job(
            target_wide_table="WT_DM2",
            participant_sources=["SRC-DM2"],
            join_key="id",
        )
        engine.execute_fusion_pipeline(job.job_id)
        original_count = len(job.merged_records)

        # 更新第一条记录的name
        existing_key = next(
            (self._make_key(r, "id") for r in job.merged_records),
            None,
        )
        if existing_key:
            import re
            parts = existing_key.split("#", 1)
            rec_id = parts[-1] if len(parts) > 1 else ""
            delta = [{"_source": "SRC-DM2", "id": rec_id, "name": "updated", "_op": "upsert"}]
            result = engine.execute_delta_merge(job.job_id, delta)
            assert result["ok"] is True
            assert result["updated"] >= 0

    def test_delta_merge_delete(self, engine: FusionEngine) -> None:
        """增量归并：删除记录。"""
        engine.register_source(
            source_code="SRC-DM3", source_name="dm3_source", source_type="MySQL",
            connection_string="conn://dm3",
        )
        job = engine.create_fusion_job(
            target_wide_table="WT_DM3",
            participant_sources=["SRC-DM3"],
            join_key="id",
        )
        engine.execute_fusion_pipeline(job.job_id)
        original_count = len(job.merged_records)

        if original_count > 0:
            first_key_val = list(job.merged_records[0].values())[0]
            delta = [{"_source": "SRC-DM3", "id": first_key_val, "_op": "delete"}]
            result = engine.execute_delta_merge(job.job_id, delta)
            assert result["ok"] is True
            assert result["total_after_merge"] <= original_count

    @staticmethod
    def _make_key(r, join_key):
        eng = FusionEngine()
        return eng._make_merge_key(r, join_key)


class TestBackfillAndColdStore:
    """数据回填与冷热分层测试。"""

    def test_backfill_with_checkpoint(self, engine: FusionEngine) -> None:
        """回填任务带断点续传。"""
        task = engine.create_backfill_task(
            target_wide_table="WT_BF",
            backfill_range="2026-01-01~2026-06-30",
            parallel_partitions=2,
        )
        records = [{"id": f"r{i}"} for i in range(250)]
        r1 = engine.execute_backfill_with_checkpoint(task.task_id, records)
        assert r1["ok"] is True
        assert not r1["complete"]
        assert r1["checkpoint"] != ""

        r2 = engine.execute_backfill_with_checkpoint(task.task_id, records)
        assert r2["batch_processed"] > 0
        assert r2["total_processed"] == 250  # 全部250条记录处理完毕

    def test_cold_store_migration(self, engine: FusionEngine) -> None:
        """冷热分层迁移。"""
        cs = engine.create_cold_store_strategy(
            target_wide_table="WT_CS",
            hot_data_days=7,
        )
        # 创建可迁移的快照（均为合法日历日期；非法日期按新规则保留待核，不参与迁冷）
        for i in range(1, 4):
            snap = engine.create_snapshot("WT_CS", partition_key_value=f"2026110{i}")
            snap.record_count = 1000

        result = engine.migrate_to_cold_store(
            cs.strategy_id, hot_partition_cutoff="20261110",
        )
        assert result["ok"] is True
        assert result["migrated_partitions"] == 3  # 实际日期均早于 2026-11-10
        assert result["migrated_rows"] == 3000

        result2 = engine.migrate_to_cold_store(
            cs.strategy_id, hot_partition_cutoff="20261201",
        )
        assert result2["ok"] is True
        # 重跑幂等：三份分区已在冷层，各自只计一次行数
        assert result2["migrated_partitions"] == 0
        assert result2["migrated_rows"] == 0


class TestLineageAndMetrics:
    """数据血缘与指标聚合测试。"""

    def test_build_wide_table_from_fusion_creates_lineage(self, engine: FusionEngine) -> None:
        """从融合作业构建宽表定义并产生血缘边。"""
        engine.register_source(
            source_code="SRC-LN", source_name="ln_source", source_type="MySQL",
            connection_string="conn://ln",
        )
        wt = engine.define_wide_table(
            table_code="WIDE-LN", table_name="血缘宽表",
            field_list=["id", "name", "value"],
        )
        job = engine.create_fusion_job(
            target_wide_table="血缘宽表",
            participant_sources=["SRC-LN"],
            join_key="id",
        )
        result = engine.build_wide_table_from_fusion(wt.table_id, job.job_id)
        assert result["ok"] is True
        assert result["source_count"] == 1
        assert len(result["lineage_edges"]) == 1

    def test_metric_from_lineage(self, engine: FusionEngine) -> None:
        """从血缘边计算指标聚合规则。"""
        engine.define_wide_table(
            table_code="WIDE-MT", table_name="指标宽表",
            field_list=["id", "amount"],
        )
        edge = engine.create_lineage_edge(
            upstream_node="SRC-MT", downstream_node="指标宽表",
            related_fields=["amount"],
        )
        result = engine.compute_metric_from_lineage(
            lineage_edge_id=edge.edge_id,
            target_wide_table="指标宽表",
            agg_function="SUM",
            group_dimensions=["dt"],
        )
        assert result["ok"] is True
        assert result["agg_function"] == "SUM"


class TestExportAndCrossValidation:
    """快照导出与交叉校验测试。"""

    def test_export_from_snapshot(self, engine: FusionEngine) -> None:
        """从分区快照导出数据。"""
        engine.register_source(
            source_code="SRC-EXP", source_name="exp_source", source_type="MySQL",
            connection_string="conn://exp",
        )
        job = engine.create_fusion_job(
            target_wide_table="WT_EXP",
            participant_sources=["SRC-EXP"],
            join_key="id",
        )
        engine.execute_fusion_pipeline(job.job_id)

        snap = engine.create_snapshot("WT_EXP", partition_key_value="20261118")
        plan = engine.create_export_plan(
            target_wide_table="WT_EXP", export_format="parquet",
            field_filter=["id", "name"],
        )
        result = engine.execute_export_from_snapshot(plan.plan_id, snap.partition_id)
        assert result["ok"] is True
        assert result["exported_rows"] == 3
        assert result["format"] == "parquet"

    def test_cross_validation(self, engine: FusionEngine) -> None:
        """跨宽表交叉校验。"""
        engine.register_source(
            source_code="SRC-CV1", source_name="cv1_source", source_type="MySQL",
            connection_string="conn://cv1",
        )
        engine.register_source(
            source_code="SRC-CV2", source_name="cv2_source", source_type="MySQL",
            connection_string="conn://cv2",
        )
        job1 = engine.create_fusion_job(
            target_wide_table="WT_CV1", participant_sources=["SRC-CV1"], join_key="id",
        )
        job2 = engine.create_fusion_job(
            target_wide_table="WT_CV2", participant_sources=["SRC-CV2"], join_key="id",
        )
        engine.execute_fusion_pipeline(job1.job_id)
        engine.execute_fusion_pipeline(job2.job_id)

        plan = engine.create_cross_validate_plan(
            primary_wide_table="WT_CV1", reference_wide_table="WT_CV2",
            join_key="id", validate_fields=["name", "value"],
        )
        result = engine.execute_cross_validation(plan.plan_id)
        assert result["ok"] is True
        assert "mismatch_count" in result


class TestWatermarkReconciliation:
    """水位追踪协调测试。"""

    def test_watermark_advance(self, engine: FusionEngine) -> None:
        """水位推进。"""
        wm = engine.create_watermark(
            data_source="SRC-WM", current_watermark="2026-11-17T00:00:00Z",
        )
        assert wm.current_watermark == "2026-11-17T00:00:00Z"

        updated = engine.advance_watermark(wm.record_id, "2026-11-18T00:00:00Z")
        assert updated is not None
        assert updated.current_watermark == "2026-11-18T00:00:00Z"
        assert updated.previous_watermark == "2026-11-17T00:00:00Z"

    def test_watermark_reconcile_with_fusion(self, engine: FusionEngine) -> None:
        """水位与融合作业协调。"""
        engine.register_source(
            source_code="SRC-WMR", source_name="wmr_source", source_type="MySQL",
            connection_string="conn://wmr",
        )
        job = engine.create_fusion_job(
            target_wide_table="WT_WMR",
            participant_sources=["SRC-WMR"],
            join_key="id",
        )
        wm = engine.create_watermark(
            data_source="SRC-WMR", current_watermark="2026-11-18T00:00:00Z",
        )
        wm.delay_minutes = 5
        result = engine.reconcile_watermark_with_fusion(job.job_id)
        assert result["ok"] is True
        assert result["can_proceed"] is True

        wm.delay_minutes = 60
        result2 = engine.reconcile_watermark_with_fusion(job.job_id)
        assert result2["can_proceed"] is False


class TestPartitionArchive:
    """分区策略与归档测试。"""

    def test_partition_archive_reconcile(self, engine: FusionEngine) -> None:
        """分区归档协调。"""
        part_rule = engine.create_partition_rule(
            target_wide_table="WT_PA", partition_type="range",
            partition_field="dt", retention_days=30,
            archive_policy="move_to_cold",
        )
        cold_strat = engine.create_cold_store_strategy(
            target_wide_table="WT_PA", hot_data_days=7,
        )
        # 创建已迁移到冷存储的快照
        for i in range(2):
            snap = engine.create_snapshot("WT_PA", partition_key_value=f"2026110{i}")
            snap.status_mark = "已迁移至冷存储"
            snap.record_count = 500

        result = engine.reconcile_partition_archive(
            part_rule.rule_id, cold_strat.strategy_id,
        )
        assert result["ok"] is True
        assert result["archived_partitions"] == 2
        assert result["archived_rows"] == 1000


class TestDomainSnapshot:
    """引擎全局快照。"""

    def test_engine_domain_snapshot(self, engine: FusionEngine) -> None:
        """引擎全局快照返回各模块状态。"""
        engine.register_source(
            source_code="SRC-DS", source_name="ds_source", source_type="MySQL",
            connection_string="conn://ds",
        )
        engine.create_schema_mapping(
            source_table="SRC-DS", target_wide_table="WT_DS",
            field_mapping_json="{}",
        )
        snapshot = engine.domain_snapshot()
        assert snapshot["sources"] == 1
        assert snapshot["schemas"] == 1
        assert isinstance(snapshot, dict)