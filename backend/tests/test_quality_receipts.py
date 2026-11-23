"""抽样检验回执与投递放行测试：台账回填、回执并发、版本锁定和投递取数。"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

from app.integration import (
    confirm_receipt, deliver_and_sample, interrupt_receipt, list_receipts,
    receipt_detail, retry_receipt, start_sample_inspection, submit_delivery,
)
from app.models import CleanedContent, StorageBatch
from app.store import store


# ---------------------------------------------------------------------------
# 测试辅助
# ---------------------------------------------------------------------------

def _make_batch(batch_id: str, version: str = "v1", status: str = "校验通过",
                row_count: int = 10, job: str = "JOB-RCPT") -> dict:
    """登记一个批次台账行（模拟批次存储落库后的版本）。"""
    rows = store.rows("batch_store")
    row = {
        "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
        "批次编号": batch_id, "来源作业": job, "数据行数": row_count,
        "存储路径": f"/data/batches/{job}/", "压缩格式": "gzip",
        "Schema版本": version, "校验摘要": f"sum-{batch_id}-{version}",
        "入库时间": "2026-11-23", "status": status,
        "pending": False, "abnormal": status == "校验失败",
    }
    rows.append(row)
    return row


def _plan_rows(batch_id: str) -> list[dict]:
    return [r for r in store.rows("quality_sample") if r.get("目标批次") == batch_id]


# ---------------------------------------------------------------------------
# 差异修复：抽样算出有效条数后，质量抽样台账同步落库
# ---------------------------------------------------------------------------

def test_pipeline_sample_results_backfill_existing_plan() -> None:
    """已有空白抽样计划时，抽样结果回填台账行，不再留白。"""
    batch_id = "BATCH-RCPT-FIX1"
    store.rows("quality_sample").append({
        "id": 9001, "计划编号": "QS-RCPT-FIX1", "目标批次": batch_id,
        "抽样比例": "0.5", "抽样方式": "随机抽样", "样本数量": 2,
        "检验结果": "", "偏差阈值": "0.05", "最近采样": "",
        "status": "待采样", "pending": True, "abnormal": False,
    })
    records = [
        CleanedContent(source_url=f"u{i}", extract_rule_id="R1", fields={"t": f"v{i}"})
        for i in range(4)
    ]
    batch = StorageBatch(
        batch_id=batch_id, source_job_id="JOB-FIX1", row_count=4,
        status="校验通过", records=records, schema_version="v1",
        checksum="abcdef", storage_path="/data/x/",
    )

    _, qs = deliver_and_sample(batch)

    plan = _plan_rows(batch_id)[0]
    assert plan["检验结果"], "台账应回填检验结果"
    assert "有效" in plan["检验结果"], f"台账应能查到有效条数，实际: {plan['检验结果']}"
    assert plan["样本数量"] == qs.sample_size
    assert plan["status"] == "已通过"


def test_pipeline_sample_results_persisted_for_new_plan() -> None:
    """自动建计划时先算后存，台账行直接带有效条数。"""
    batch_id = "BATCH-RCPT-FIX2"
    records = [
        CleanedContent(source_url=f"u{i}", extract_rule_id="R1", fields={"t": f"v{i}"})
        for i in range(6)
    ]
    batch = StorageBatch(
        batch_id=batch_id, source_job_id="JOB-FIX2", row_count=6,
        status="校验通过", records=records, schema_version="v1",
        checksum="abcdef", storage_path="/data/y/",
    )

    _, qs = deliver_and_sample(batch)

    plan = _plan_rows(batch_id)[0]
    assert plan["检验结果"] == qs.check_result
    assert "有效" in plan["检验结果"]
    # 流水线自检回执同步落库，投递可沿用
    receipts = list_receipts(batch_id=batch_id)
    assert len(receipts) == 1
    assert receipts[0]["status"] == "已确认"
    assert receipts[0]["batch_version"] == "v1"


# ---------------------------------------------------------------------------
# 回执冲突：同次检验并发提交只留一份待核回执
# ---------------------------------------------------------------------------

def test_inspection_creates_pending_receipt_and_conflict_merges() -> None:
    """重复提交同一批次版本的检验，合并到同一份待核回执。"""
    batch_id = "BATCH-RCPT-C1"
    _make_batch(batch_id, version="v1")

    r1, _, kind1 = start_sample_inspection(batch_id, sample_size=2, operator="值班员A")
    assert kind1 == "created"
    assert r1["status"] == "待核"
    assert r1["sample_size"] == 2
    assert r1["valid_count"] == 2

    r2, message2, kind2 = start_sample_inspection(batch_id, sample_size=2, operator="值班员B")
    assert kind2 == "conflict"
    assert r2["receipt_id"] == r1["receipt_id"]
    assert "合并" in message2
    assert len(list_receipts(batch_id=batch_id)) == 1


def test_concurrent_inspection_single_pending_receipt() -> None:
    """多线程同时提交抽样，只生成一份待核回执。"""
    batch_id = "BATCH-RCPT-C2"
    _make_batch(batch_id, version="v1")

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(
            lambda _: start_sample_inspection(batch_id, sample_size=2), range(8)))

    receipts = list_receipts(batch_id=batch_id)
    assert len(receipts) == 1, f"并发提交应只留一份回执，实际: {len(receipts)}"
    assert {r[0]["receipt_id"] for r in results if r[0]} == {receipts[0]["receipt_id"]}


def test_confirmed_receipt_is_reused() -> None:
    """已有已确认回执时，再次抽样沿用已有检验结论。"""
    batch_id = "BATCH-RCPT-C3"
    _make_batch(batch_id, version="v1")
    r1, _, _ = start_sample_inspection(batch_id, sample_size=2)
    confirm_receipt(r1["receipt_id"])

    r2, message, kind = start_sample_inspection(batch_id, sample_size=2)

    assert kind == "reused"
    assert r2["receipt_id"] == r1["receipt_id"]
    assert "沿用" in message
    assert len(list_receipts(batch_id=batch_id)) == 1


# ---------------------------------------------------------------------------
# 并发确认：同次检验只生效一次
# ---------------------------------------------------------------------------

def test_concurrent_confirm_takes_effect_once() -> None:
    """多线程确认同一回执，只有一次生效，且台账只同步一次。"""
    batch_id = "BATCH-RCPT-C4"
    _make_batch(batch_id, version="v1")
    receipt, _, _ = start_sample_inspection(batch_id, sample_size=2)
    receipt_id = receipt["receipt_id"]

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: confirm_receipt(receipt_id), range(8)))

    assert all(r[0] is not None for r in results), "并发确认不应报错"
    effective = [r for r in results if "检验结论生效" in r[1]]
    assert len(effective) == 1, f"确认只应生效一次，实际: {len(effective)}"
    confirmed = [r for r in list_receipts(batch_id=batch_id) if r["status"] == "已确认"]
    assert len(confirmed) == 1
    # 抽样结果同步到质量抽样台账
    plan = _plan_rows(batch_id)[0]
    assert plan["status"] == "已通过"
    assert plan["检验结果"] == confirmed[0]["check_result"]


# ---------------------------------------------------------------------------
# 中断与恢复：各自留痕，回执明细可查首次中断及后续完成
# ---------------------------------------------------------------------------

def test_interrupt_and_retry_keep_separate_records() -> None:
    """中断的回执保留记录，重试生成新次数的回执，历程完整可查。"""
    batch_id = "BATCH-RCPT-C5"
    _make_batch(batch_id, version="v2")
    r1, _, _ = start_sample_inspection(batch_id, sample_size=2)

    interrupted, _ = interrupt_receipt(r1["receipt_id"], operator="值班员")
    assert interrupted["status"] == "已中断"

    denied, _ = confirm_receipt(r1["receipt_id"])
    assert denied is None, "已中断的回执不能再确认"

    r2, message = retry_receipt(r1["receipt_id"], operator="值班员")
    assert r2["attempt"] == 2
    assert r2["resumed_from"] == r1["receipt_id"]
    assert r2["batch_version"] == "v2", "重试的抽样输入应锁定在原批次版本"
    assert r2["status"] == "待核"

    detail = receipt_detail(r2["receipt_id"])
    attempts = {c["attempt"]: c["status"] for c in detail["chain"]}
    assert attempts == {1: "已中断", 2: "待核"}, "明细应同时查到首次中断和恢复记录"

    confirm_receipt(r2["receipt_id"])
    detail_after = receipt_detail(r2["receipt_id"])
    attempts_after = {c["attempt"]: c["status"] for c in detail_after["chain"]}
    assert attempts_after == {1: "已中断", 2: "已确认"}, "明细应查到首次中断及后续完成"


def test_confirmed_receipt_cannot_be_interrupted() -> None:
    """已确认回执不可中断。"""
    batch_id = "BATCH-RCPT-C6"
    _make_batch(batch_id, version="v1")
    receipt, _, _ = start_sample_inspection(batch_id, sample_size=2)
    confirm_receipt(receipt["receipt_id"])

    result, _ = interrupt_receipt(receipt["receipt_id"])
    assert result is None


# ---------------------------------------------------------------------------
# 质检详情：样本与对应批次校验结果可复核
# ---------------------------------------------------------------------------

def test_receipt_detail_shows_samples_and_batch_validation() -> None:
    """回执明细包含两个样本和对应批次的校验结果。"""
    batch_id = "BATCH-RCPT-D1"
    _make_batch(batch_id, version="v1", row_count=5)
    receipt, _, _ = start_sample_inspection(batch_id, sample_size=2)

    detail = receipt_detail(receipt["receipt_id"])

    assert len(detail["samples"]) == 2, "应能复核两个样本"
    assert all(s["valid"] for s in detail["samples"])
    assert all(s["digest"] for s in detail["samples"])
    assert detail["batch"] is not None
    assert detail["batch"]["status"] == "校验通过"
    assert detail["batch"]["校验摘要"] == f"sum-{batch_id}-v1"
    assert detail["batch_checksum"] == f"sum-{batch_id}-v1"


# ---------------------------------------------------------------------------
# 投递放行：凭批次版本取数，不用旧版本报告提前放行
# ---------------------------------------------------------------------------

def test_delivery_released_by_current_version_receipt() -> None:
    """当前版本已有已确认回执时，投递从同一份回执取数放行。"""
    batch_id = "BATCH-RCPT-V1"
    _make_batch(batch_id, version="v1")
    receipt, _, _ = start_sample_inspection(batch_id, sample_size=2)
    confirm_receipt(receipt["receipt_id"])

    row, message, decision = submit_delivery(batch_id=batch_id)

    assert decision == "放行"
    assert row["status"] == "已完成"
    assert row["质检回执"] == receipt["receipt_id"]
    assert row["批次版本"] == "v1"
    assert receipt["receipt_id"] in row["取数依据"], "视图应呈现取数依据"
    assert receipt["check_result"] in row["取数依据"]


def test_delivery_not_released_by_previous_version_report() -> None:
    """批次升到 v2 后，v1 的已确认报告不能提前放行，v2 检验完成后才放行。"""
    batch_id = "BATCH-RCPT-V2"
    _make_batch(batch_id, version="v1")
    r1, _, _ = start_sample_inspection(batch_id, sample_size=2)
    confirm_receipt(r1["receipt_id"])
    row_v1, _, decision_v1 = submit_delivery(batch_id=batch_id)
    assert decision_v1 == "放行"

    # 批次版本升级到 v2
    _make_batch(batch_id, version="v2")
    row2, message2, decision2 = submit_delivery(batch_id=batch_id)
    assert decision2 == "待核", "v2 暂无已确认回执，不能用 v1 的报告放行"
    assert row2["status"] == "待投递"
    assert row2["批次版本"] == "v2"
    assert r1["receipt_id"] not in row2["取数依据"]

    # v2 第一次抽样中断并重试
    r2, _, _ = start_sample_inspection(batch_id, sample_size=2)
    assert r2["batch_version"] == "v2"
    interrupt_receipt(r2["receipt_id"])
    r3, _ = retry_receipt(r2["receipt_id"])

    # 中断恢复期间投递仍待核
    row3, _, decision3 = submit_delivery(batch_id=batch_id)
    assert decision3 == "待核"

    # v2 检验确认后放行，取数依据锁定 v2 回执
    confirm_receipt(r3["receipt_id"])
    row4, _, decision4 = submit_delivery(batch_id=batch_id)
    assert decision4 == "放行"
    assert row4["质检回执"] == r3["receipt_id"]
    assert row4["批次版本"] == "v2"
    assert r3["receipt_id"] in row4["取数依据"]

    # 回执明细能查到首次中断及后续完成
    detail = receipt_detail(r3["receipt_id"])
    attempts = {c["attempt"]: c["status"] for c in detail["chain"]}
    assert attempts == {1: "已中断", 2: "已确认"}


def test_delivery_held_when_conclusion_deviation() -> None:
    """已确认但结论为偏差告警时，投递沿用结论保持待核。"""
    batch_id = "BATCH-RCPT-V3"
    _make_batch(batch_id, version="v1", status="校验失败", row_count=4)
    receipt, _, _ = start_sample_inspection(batch_id, sample_size=2)
    assert receipt["conclusion"] == "偏差告警"
    confirm_receipt(receipt["receipt_id"])

    row, message, decision = submit_delivery(batch_id=batch_id)

    assert decision == "待核"
    assert row["status"] == "待投递"
    assert "偏差告警" in row["取数依据"]


def test_delivery_submit_creates_task_when_missing() -> None:
    """投递任务不存在时自动登记，并保持待核。"""
    batch_id = "BATCH-RCPT-V4"
    _make_batch(batch_id, version="v1")

    row, message, decision = submit_delivery(batch_id=batch_id)

    assert row is not None
    assert decision == "待核"
    assert row["数据来源"] == batch_id
    assert row["批次版本"] == "v1"
    assert "暂无已确认回执" in row["取数依据"]
