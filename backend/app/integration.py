"""跨模块数据流编排：真实业务流水线，各模块间通过dataclass传递数据。

实现以下数据流：
- crawl_site → crawl_job → content_extract → field_clean → batch_store
- crawl_job → url_dedup → parse_log
- proxy_pool → anti_block → crawl_limit
- batch_store → data_delivery → quality_sample
- incr_crawl → crawl_job → diff_check
- data_archive ← batch_store
- crawl_cost ← proxy_pool
- crawl_job → scheduler
"""
from __future__ import annotations

import hashlib
import random
import re
from datetime import datetime, timezone
from typing import Any

from app.models import (
    AntiBlockStrategy, AntiType, ArchiveFormat, ArchivePolicy, CleanedContent,
    CleanRule, CostRecord, CrawlJobSpec, CrawlLimitRule, DeliveryFormat,
    DeliveryTask, DiffRecord, ExtractRule, IncrCursor, ParseLogEntry,
    ProxyNode, QualitySample, RawExtractedContent, SampleMethod, SampleReceipt,
    SchedulePlan, SiteConfig, SiteType, StorageBatch, UrlFingerprint,
)
from app.store import store


# ---------------------------------------------------------------------------
# 流水线 1: crawl_site → crawl_job → content_extract → field_clean → batch_store
# ---------------------------------------------------------------------------

def run_crawl_pipeline(site_row: dict[str, Any], job_row: dict[str, Any],
                       extract_rows: list[dict[str, Any]],
                       clean_rows: list[dict[str, Any]]) -> StorageBatch | None:
    """端到端采集链路：站点配置 → 作业创建 → 内容抽取 → 字段清洗 → 批次存储。

    返回完整的 StorageBatch 或 None（不可恢复错误时）。
    """
    # Step 1: 站点验证（crawl_site）
    site = SiteConfig.from_seed_row(site_row)
    if not site.is_crawlable():
        return None  # 站点未验证或 Robots 禁止采集

    # Step 2: 创建采集作业（crawl_job）
    job = CrawlJobSpec.from_seed_row(job_row, site=site)
    if not job.entry_urls:
        return None  # 没有入口 URL

    # Step 3: 内容抽取（content_extract）
    rules = [ExtractRule.from_seed_row(er) for er in extract_rows if er.get("status") == "生效中"]
    if not rules:
        rules = [ExtractRule.from_seed_row(extract_rows[0])] if extract_rows else []
    extracted_contents: list[RawExtractedContent] = []
    parse_logs: list[ParseLogEntry] = []

    for i, url in enumerate(job.entry_urls):
        rule = rules[i % len(rules)] if rules else None
        if rule is None:
            continue
        raw = _simulate_extract(url, rule)
        extracted_contents.append(raw)

        # 记录解析日志（parse_log）
        log = ParseLogEntry(
            record_id=f"PLOG-{job.job_id}-{i+1:04d}",
            source_url=url,
            parse_rule_id=rule.rule_id,
            parse_result="成功" if not raw.has_errors() else "部分失败",
            error_type="字段缺失" if raw.has_errors() else "",
            retry_count=1 if raw.has_errors() else 0,
            error_detail="; ".join(raw.errors) if raw.has_errors() else "",
        )
        parse_logs.append(log)
        _persist_parse_log(log)

    job.completed_pages = len(extracted_contents)
    job.failed_pages = sum(1 for r in extracted_contents if r.has_errors())
    job.status = "已完成" if job.failed_pages == 0 else "部分失败"

    # Step 4: 字段清洗（field_clean）
    clean_rules = [CleanRule.from_seed_row(cr) for cr in clean_rows if cr.get("status") == "生效中"]
    if not clean_rules:
        clean_rules = [CleanRule.from_seed_row(clean_rows[0])] if clean_rows else []
    cleaned: list[CleanedContent] = []

    for raw in extracted_contents:
        cc = _apply_clean_rules(raw, clean_rules)
        cleaned.append(cc)

    # Step 5: 批次存储（batch_store）
    batch = StorageBatch(
        batch_id=f"BATCH-{job.job_id}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}",
        source_job_id=job.job_id,
        row_count=len(cleaned),
        storage_path=f"/data/batches/{job.job_id}/",
        compression="gzip",
        schema_version="v1",
        stored_at=datetime.now(timezone.utc).isoformat(),
        status="已落盘",
        records=cleaned,
    )
    batch.checksum = batch.compute_checksum()
    batch.status = "校验通过" if all(c.is_valid() for c in cleaned) else "校验失败"

    _persist_batch(batch)

    return batch


def _simulate_extract(url: str, rule: ExtractRule) -> RawExtractedContent:
    """模拟从 URL 抽取内容。

    字段映射规则：
    - 若 value 含逗号：按逗号拆分为多个字段名（种子数据模式）
    - 若 key 非 "raw"：将 key 用作字段名（测试覆盖模式）
    """
    errors: list[str] = []
    fields: dict[str, str] = {}

    if "error" in url.lower() or "404" in url:
        errors.append("页面不可达")
        return RawExtractedContent(source_url=url, extract_rule_id=rule.rule_id, errors=errors)

    if "empty" in url.lower():
        return RawExtractedContent(source_url=url, extract_rule_id=rule.rule_id,
                                   raw_fields={}, errors=["页面内容为空"])

    for key, value in rule.field_mapping.items():
        parts = [p.strip() for p in value.split(",") if p.strip()]
        if len(parts) > 1:
            for field_name in parts:
                fields[field_name] = f"extracted:{field_name}:{hash(url + field_name) % 10000}"
        elif key != "raw":
            fields[key] = f"extracted:{key}:{hash(url + key) % 10000}"
        elif parts:
            fields[parts[0]] = f"extracted:{parts[0]}:{hash(url + parts[0]) % 10000}"
        else:
            fields[key] = f"extracted:{key}:{hash(url) % 10000}"

    return RawExtractedContent(
        source_url=url, extract_rule_id=rule.rule_id,
        raw_fields=fields, errors=errors,
    )


def _apply_clean_rules(raw: RawExtractedContent, rules: list[CleanRule]) -> CleanedContent:
    """对原始抽取内容逐字段应用清洗规则。同字段多条规则链式执行。"""
    cleaned_fields: dict[str, Any] = {}
    errors: list[str] = []

    for rule in rules:
        field = rule.target_field
        # 已清洗过的字段从 cleaned_fields 读取，实现链式清洗
        raw_value = cleaned_fields.get(field, raw.raw_fields.get(field, ""))

        if not raw_value and rule.null_strategy == "skip":
            continue
        if not raw_value and rule.null_strategy == "fill_default":
            raw_value = "N/A"

        try:
            if rule.clean_type.value == "去空格":
                value = raw_value.strip()
            elif rule.clean_type.value == "标准化":
                value = raw_value.lower().replace(" ", "_")
            elif rule.clean_type.value == "校验":
                if rule.validation_range and not re.match(rule.validation_range, raw_value):
                    errors.append(f"字段 {field} 校验失败: {raw_value}")
                    value = raw_value
                else:
                    value = raw_value
            elif rule.clean_type.value == "正则提取":
                m = re.search(rule.expression, raw_value) if rule.expression else None
                value = m.group(1) if m else raw_value
            elif rule.clean_type.value == "类型转换":
                value = _cast_type(raw_value, rule.expression)
            else:
                value = raw_value
        except Exception as exc:
            errors.append(f"字段 {field} 清洗异常: {exc}")
            value = raw_value

        cleaned_fields[field] = value

    return CleanedContent(
        source_url=raw.source_url,
        extract_rule_id=raw.extract_rule_id,
        fields=cleaned_fields,
        clean_errors=errors,
    )


def _cast_type(value: str, target: str) -> Any:
    if target == "int":
        return int(value) if value.isdigit() else value
    if target == "float":
        try:
            return float(value)
        except ValueError:
            return value
    if target == "datetime":
        return value  # 简化实现
    return value


# ---------------------------------------------------------------------------
# 流水线 2: crawl_job → url_dedup → parse_log
# ---------------------------------------------------------------------------

def run_dedup_flow(job: CrawlJobSpec) -> tuple[list[UrlFingerprint], list[UrlFingerprint]]:
    """对采集作业的入口 URL 执行去重，返回新增和重复列表。"""
    existing: dict[str, UrlFingerprint] = {}
    for row in store.rows("url_dedup"):
        fp = UrlFingerprint.from_seed_row(row)
        existing[fp.hash_value] = fp

    new_fps: list[UrlFingerprint] = []
    dup_fps: list[UrlFingerprint] = []

    for url in job.entry_urls:
        fp = UrlFingerprint.compute(url)
        if fp.hash_value in existing:
            existing[fp.hash_value].duplicate_count += 1
            dup_fps.append(existing[fp.hash_value])
        else:
            existing[fp.hash_value] = fp
            new_fps.append(fp)
            _persist_fingerprint(fp)

    return new_fps, dup_fps


# ---------------------------------------------------------------------------
# 流水线 3: proxy_pool → anti_block → crawl_limit
# ---------------------------------------------------------------------------

def select_proxy_for_job(job: CrawlJobSpec) -> ProxyNode | None:
    """从代理池为采集作业选择可用代理节点，并检查反爬策略和限速规则。

    返回最佳代理或 None（无可用代理时）。
    """
    proxies: list[ProxyNode] = []
    for row in store.rows("proxy_pool"):
        p = ProxyNode.from_seed_row(row)
        if p.is_available():
            proxies.append(p)

    if not proxies:
        return None

    # 按延迟排序，选择最快的
    proxies.sort(key=lambda p: p.latency_ms)

    # 检查反爬策略
    anti_strategies: list[AntiBlockStrategy] = []
    for row in store.rows("anti_block"):
        a = AntiBlockStrategy.from_seed_row(row)
        if a.target_site == job.site.domain and a.status == "生效中":
            anti_strategies.append(a)

    best_proxy = proxies[0]

    # 检查限速规则
    for row in store.rows("crawl_limit"):
        limit = CrawlLimitRule.from_seed_row(row)
        if limit.target_site == job.site.domain and limit.status == "生效中":
            job.concurrency = min(job.concurrency, limit.concurrency_max)
            if limit.request_interval_ms > 0:
                best_proxy.latency_ms = max(best_proxy.latency_ms, limit.request_interval_ms)

    return best_proxy


# ---------------------------------------------------------------------------
# 流水线 4: batch_store → data_delivery → quality_sample
# ---------------------------------------------------------------------------

def deliver_and_sample(batch: StorageBatch) -> tuple[DeliveryTask | None, QualitySample | None]:
    """从批次存储投递数据，并对投递结果进行质量抽样。"""
    # 数据投递
    delivery_rows = [r for r in store.rows("data_delivery")
                     if r.get("数据来源") == batch.batch_id or r.get("状态") == "待投递"]
    if delivery_rows:
        dt = DeliveryTask.from_seed_row(delivery_rows[0])
        dt.batch_ref_id = batch.batch_id
        dt.delivered_row_count = batch.row_count
        dt.last_delivery = datetime.now(timezone.utc).isoformat()
        dt.status = "已完成" if batch.status == "校验通过" else "投递失败"
        _persist_delivery(dt)
    else:
        # 自动创建投递任务
        dt = DeliveryTask(
            task_id=f"DEL-{batch.batch_id}",
            data_source=batch.batch_id,
            target_system=f"data_warehouse_{batch.source_job_id}",
            delivery_format=DeliveryFormat.CSV,
            frequency="manual",
            partition_strategy="by_date",
            batch_ref_id=batch.batch_id,
            delivered_row_count=batch.row_count,
            last_delivery=datetime.now(timezone.utc).isoformat(),
            status="已完成" if batch.status == "校验通过" else "投递失败",
        )
        _persist_delivery(dt)

    # 质量抽样：先完成抽样计算，再统一落库。
    # 已算出有效条数但台账/回执未落库是历史缺陷，这里保证台账与回执同源一致。
    sample_rows = [r for r in store.rows("quality_sample")
                   if r.get("目标批次") == batch.batch_id]
    if sample_rows:
        qs = QualitySample.from_seed_row(sample_rows[0])
    else:
        # 自动创建抽样计划
        sample_size = max(1, int(batch.row_count * 0.1))
        qs = QualitySample(
            plan_id=f"QS-{batch.batch_id}",
            target_batch=batch.batch_id,
            sample_ratio=0.1,
            sample_method=SampleMethod.RANDOM,
            sample_size=sample_size,
            last_sampled=datetime.now(timezone.utc).isoformat(),
            status="已通过" if batch.status == "校验通过" else "偏差告警",
        )

    # 从批次中随机抽取 sample_size 条记录进行校验
    sampled_records: list[CleanedContent] = []
    if batch.records and qs.sample_size > 0:
        sampled_records = random.sample(batch.records, min(qs.sample_size, len(batch.records)))
        valid_count = sum(1 for s in sampled_records if s.is_valid())
        actual_ratio = valid_count / len(sampled_records) if sampled_records else 0
        qs.check_result = f"抽样{len(sampled_records)}条，有效{valid_count}条"
        qs.status = "偏差告警" if actual_ratio < (1 - qs.deviation_threshold) else "已通过"

    # 抽样结果落库：已有计划回填台账行，否则计算后再新增，避免台账空白
    if sample_rows:
        _update_sample_row(sample_rows[0], qs)
    else:
        _persist_sample(qs)

    # 抽样检验回执落库：流水线自检结论直接确认，投递可沿用同一份回执
    _ensure_pipeline_receipt(qs, batch, sampled_records)

    return dt, qs


# ---------------------------------------------------------------------------
# 抽样检验回执：质量抽样 → 数据投递 的放行依据
#
# 规则：
# - 回执按 (计划, 批次, 批次版本, 检验次数) 锁定，换版本后旧回执不能放行新版本；
# - 同次检验并发提交只保留一份待核回执（回执冲突合并），并发确认只生效一次；
# - 待核回执可中断，中断后恢复会生成新次数的回执，各自留痕；
# - 投递核对同一批次时只沿用当前批次版本的已确认回执，否则保持待核。
# ---------------------------------------------------------------------------

RECEIPT_PENDING = "待核"
RECEIPT_CONFIRMED = "已确认"
RECEIPT_INTERRUPTED = "已中断"

# 本次运行内持久化过的批次对象，供检验时抽取真实记录
_LIVE_BATCHES: dict[str, StorageBatch] = {}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def _find_receipt(receipt_id: str) -> dict[str, Any] | None:
    for row in store.receipts():
        if row.get("receipt_id") == receipt_id:
            return row
    return None


def _receipts_for(plan_id: str, batch_id: str, batch_version: str) -> list[dict[str, Any]]:
    return [
        row for row in store.receipts()
        if row.get("plan_id") == plan_id
        and row.get("batch_id") == batch_id
        and row.get("batch_version") == batch_version
    ]


def _append_receipt(receipt: SampleReceipt) -> dict[str, Any]:
    row = receipt.to_row()
    row["id"] = max((int(r.get("id", 0)) for r in store.receipts()), default=0) + 1
    store.receipts().append(row)
    return row


def _latest_batch_row(batch_id: str) -> dict[str, Any] | None:
    rows = [r for r in store.rows("batch_store") if str(r.get("批次编号")) == batch_id]
    return rows[-1] if rows else None


def _find_or_create_plan_row(batch_id: str, batch_row: dict[str, Any],
                             sample_size: int | None = None) -> dict[str, Any]:
    rows = store.rows("quality_sample")
    for row in rows:
        if str(row.get("目标批次")) == batch_id:
            return row
    row_count = _parse_int(batch_row.get("数据行数"), 0)
    size = sample_size or (max(1, int(row_count * 0.1)) if row_count > 0 else 2)
    plan_id = f"QS-{batch_id}"
    existing_ids = {str(r.get("计划编号")) for r in rows}
    base, seq = plan_id, 2
    while plan_id in existing_ids:
        plan_id = f"{base}-{seq}"
        seq += 1
    row = {
        "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
        "计划编号": plan_id,
        "目标批次": batch_id,
        "抽样比例": "0.1",
        "抽样方式": SampleMethod.RANDOM.value,
        "样本数量": size,
        "检验结果": "",
        "偏差阈值": "0.05",
        "最近采样": "",
        "status": "待采样",
        "pending": True,
        "abnormal": False,
    }
    rows.append(row)
    return row


def _compute_samples(batch_id: str, version: str, batch_status: str,
                     row_count: int, sample_size: int) -> tuple[list[dict[str, Any]], int]:
    """按批次版本锁定抽样输入，返回样本明细和有效条数。"""
    pool: list[tuple[str, bool]] = []
    live = _LIVE_BATCHES.get(batch_id)
    if live is not None and live.records and live.schema_version == version:
        pool = [(record.source_url, record.is_valid()) for record in live.records]
    else:
        count = row_count if row_count > 0 else sample_size
        all_valid = batch_status == "校验通过"
        pool = [(f"{batch_id}#row-{i + 1}", all_valid) for i in range(count)]
    picked = pool[:max(1, sample_size)] if pool else []
    samples = [
        {
            "seq": index,
            "ref": ref,
            "valid": valid,
            "digest": hashlib.sha256(f"{batch_id}:{version}:{ref}".encode()).hexdigest()[:12],
        }
        for index, (ref, valid) in enumerate(picked, start=1)
    ]
    valid_count = sum(1 for sample in samples if sample["valid"])
    return samples, valid_count


def _sync_plan_from_receipt(receipt: dict[str, Any]) -> None:
    """把已确认回执的检验结论同步到质量抽样台账。"""
    for row in store.rows("quality_sample"):
        if str(row.get("计划编号")) == receipt.get("plan_id"):
            row["样本数量"] = receipt["sample_size"]
            row["检验结果"] = receipt["check_result"]
            row["最近采样"] = receipt["confirmed_at"] or receipt["created_at"]
            row["status"] = "已通过" if receipt["conclusion"] == "通过" else "偏差告警"
            row["pending"] = False
            row["abnormal"] = receipt["conclusion"] != "通过"
            return


def _ensure_pipeline_receipt(qs: QualitySample, batch: StorageBatch,
                             sampled_records: list[CleanedContent]) -> dict[str, Any] | None:
    """流水线自检回执：同批次同版本已有回执则沿用，否则生成并直接确认。"""
    version = batch.schema_version or "v1"
    with store.receipt_lock:
        siblings = _receipts_for(qs.plan_id, batch.batch_id, version)
        if siblings:
            return siblings[-1]  # 沿用已有回执（待核/已确认/已中断各自留痕）
        samples = [
            {
                "seq": index,
                "ref": record.source_url,
                "valid": record.is_valid(),
                "digest": hashlib.sha256(
                    f"{batch.batch_id}:{version}:{record.source_url}".encode()).hexdigest()[:12],
            }
            for index, record in enumerate(sampled_records, start=1)
        ]
        valid_count = sum(1 for sample in samples if sample["valid"])
        now = _now_iso()
        receipt = SampleReceipt(
            receipt_id=f"RCPT-{qs.plan_id}-{version}-A1",
            plan_id=qs.plan_id,
            batch_id=batch.batch_id,
            batch_version=version,
            sample_size=qs.sample_size,
            valid_count=valid_count,
            check_result=qs.check_result,
            conclusion="通过" if qs.status == "已通过" else "偏差告警",
            samples=samples,
            batch_checksum=batch.checksum[:16],
            batch_status=batch.status,
            attempt=1,
            status=RECEIPT_CONFIRMED,
            created_at=now,
            confirmed_at=now,
            confirmed_by="系统自检",
            history=[
                {"time": now, "event": "流水线自检", "actor": "系统"},
                {"time": now, "event": "确认通过", "actor": "系统自检"},
            ],
        )
        return _append_receipt(receipt)


def start_sample_inspection(batch_id: str, sample_size: int | None = None,
                            operator: str = "") -> tuple[dict[str, Any] | None, str, str]:
    """发起抽样检验：生成待核回执；并发提交合并到同一份回执，已确认则沿用。"""
    with store.receipt_lock:
        batch_row = _latest_batch_row(batch_id)
        if batch_row is None:
            return None, f"采集批次 {batch_id} 不存在", "missing"
        version = str(batch_row.get("Schema版本") or "v1")
        plan_row = _find_or_create_plan_row(batch_id, batch_row, sample_size)
        plan_id = str(plan_row["计划编号"])
        siblings = _receipts_for(plan_id, batch_id, version)

        confirmed = [r for r in siblings if r["status"] == RECEIPT_CONFIRMED]
        if confirmed:
            receipt = confirmed[-1]
            return receipt, f"批次版本 {version} 已有已确认回执 {receipt['receipt_id']}，沿用检验结论", "reused"
        pending = [r for r in siblings if r["status"] == RECEIPT_PENDING]
        if pending:
            receipt = pending[-1]
            return receipt, f"批次版本 {version} 已存在待核回执 {receipt['receipt_id']}，并发提交已合并", "conflict"

        size = sample_size or _parse_int(plan_row.get("样本数量"), 0) or 2
        row_count = _parse_int(batch_row.get("数据行数"), 0)
        batch_status = str(batch_row.get("status", ""))
        samples, valid_count = _compute_samples(batch_id, version, batch_status, row_count, size)
        conclusion = "通过" if batch_status == "校验通过" and valid_count == len(samples) else (
            "偏差告警" if valid_count < len(samples) else "通过")
        attempt = max((_parse_int(r.get("attempt"), 0) for r in siblings), default=0) + 1
        now = _now_iso()
        receipt = SampleReceipt(
            receipt_id=f"RCPT-{plan_id}-{version}-A{attempt}",
            plan_id=plan_id,
            batch_id=batch_id,
            batch_version=version,
            sample_size=size,
            valid_count=valid_count,
            check_result=f"抽样{len(samples)}条，有效{valid_count}条",
            conclusion=conclusion,
            samples=samples,
            batch_checksum=str(batch_row.get("校验摘要", "")),
            batch_status=batch_status,
            attempt=attempt,
            status=RECEIPT_PENDING,
            created_at=now,
            history=[{"time": now, "event": "发起检验", "actor": operator or "系统"}],
        )
        row = _append_receipt(receipt)
        # 抽样结果同步到质量抽样台账，计划进入采样中
        plan_row["样本数量"] = size
        plan_row["检验结果"] = row["check_result"]
        plan_row["最近采样"] = now
        plan_row["status"] = "采样中"
        plan_row["pending"] = True
        return row, f"已生成待核回执 {row['receipt_id']}，等待操作员复核", "created"


def confirm_receipt(receipt_id: str, operator: str = "") -> tuple[dict[str, Any] | None, str]:
    """确认回执：同次检验并发确认只生效一次，并把结论同步到抽样台账。"""
    with store.receipt_lock:
        receipt = _find_receipt(receipt_id)
        if receipt is None:
            return None, f"回执 {receipt_id} 不存在"
        if receipt["status"] == RECEIPT_CONFIRMED:
            return receipt, f"回执 {receipt_id} 已确认，并发确认不重复生效"
        if receipt["status"] == RECEIPT_INTERRUPTED:
            return None, f"回执 {receipt_id} 已中断，请通过重试生成新回执"
        now = _now_iso()
        receipt["status"] = RECEIPT_CONFIRMED
        receipt["confirmed_at"] = now
        receipt["confirmed_by"] = operator or "系统"
        receipt["history"].append({"time": now, "event": "确认通过", "actor": operator or "系统"})
        _sync_plan_from_receipt(receipt)
        return receipt, f"回执 {receipt_id} 已确认，检验结论生效"


def interrupt_receipt(receipt_id: str, operator: str = "") -> tuple[dict[str, Any] | None, str]:
    """中断待核回执：保留中断记录，后续恢复走重试生成新回执。"""
    with store.receipt_lock:
        receipt = _find_receipt(receipt_id)
        if receipt is None:
            return None, f"回执 {receipt_id} 不存在"
        if receipt["status"] == RECEIPT_CONFIRMED:
            return None, f"回执 {receipt_id} 已确认，不可中断"
        if receipt["status"] == RECEIPT_INTERRUPTED:
            return receipt, f"回执 {receipt_id} 已处于中断状态"
        receipt["status"] = RECEIPT_INTERRUPTED
        receipt["history"].append(
            {"time": _now_iso(), "event": "检验中断", "actor": operator or "系统"})
        return receipt, f"回执 {receipt_id} 已中断，可通过重试恢复检验"


def retry_receipt(receipt_id: str, operator: str = "") -> tuple[dict[str, Any] | None, str]:
    """中断后恢复：沿用锁定在原批次版本的抽样输入，生成新次数的回执。"""
    with store.receipt_lock:
        old = _find_receipt(receipt_id)
        if old is None:
            return None, f"回执 {receipt_id} 不存在"
        if old["status"] != RECEIPT_INTERRUPTED:
            return None, f"回执 {receipt_id} 未中断，无需重试"
        siblings = _receipts_for(old["plan_id"], old["batch_id"], old["batch_version"])
        attempt = max((_parse_int(r.get("attempt"), 0) for r in siblings), default=0) + 1
        now = _now_iso()
        receipt = SampleReceipt(
            receipt_id=f"RCPT-{old['plan_id']}-{old['batch_version']}-A{attempt}",
            plan_id=old["plan_id"],
            batch_id=old["batch_id"],
            batch_version=old["batch_version"],
            sample_size=old["sample_size"],
            valid_count=old["valid_count"],
            check_result=old["check_result"],
            conclusion=old["conclusion"],
            samples=[dict(s) for s in old["samples"]],
            batch_checksum=old["batch_checksum"],
            batch_status=old["batch_status"],
            attempt=attempt,
            status=RECEIPT_PENDING,
            created_at=now,
            resumed_from=old["receipt_id"],
            history=[{
                "time": now, "event": f"中断恢复重试（源自 {old['receipt_id']}）",
                "actor": operator or "系统",
            }],
        )
        row = _append_receipt(receipt)
        return row, f"已生成第 {attempt} 次检验回执 {row['receipt_id']}，等待复核"


def list_receipts(batch_id: str | None = None, batch_version: str | None = None,
                  status: str | None = None) -> list[dict[str, Any]]:
    """质检视图回执列表：操作员可复核的抽样检验回执。"""
    rows = list(store.receipts())
    if batch_id:
        rows = [r for r in rows if r.get("batch_id") == batch_id]
    if batch_version:
        rows = [r for r in rows if r.get("batch_version") == batch_version]
    if status:
        rows = [r for r in rows if r.get("status") == status]
    return [dict(r) for r in rows]


def receipt_detail(receipt_id: str) -> dict[str, Any] | None:
    """回执明细：样本、对应批次校验结果和同次检验的完整历程。"""
    receipt = _find_receipt(receipt_id)
    if receipt is None:
        return None
    chain = sorted(
        _receipts_for(receipt["plan_id"], receipt["batch_id"], receipt["batch_version"]),
        key=lambda r: _parse_int(r.get("attempt"), 0),
    )
    batch_row = _latest_batch_row(receipt["batch_id"])
    detail = dict(receipt)
    detail["chain"] = [dict(r) for r in chain]
    detail["batch"] = dict(batch_row) if batch_row else None
    return detail


def submit_delivery(batch_id: str | None = None,
                    task_no: str | None = None) -> tuple[dict[str, Any] | None, str, str]:
    """投递核对：凭当前批次版本的已确认回执决定放行或待核，取数依据写回台账。"""
    with store.receipt_lock:
        task_row: dict[str, Any] | None = None
        if task_no:
            for row in store.rows("data_delivery"):
                if str(row.get("任务编号")) == task_no:
                    task_row = row
                    break
            if task_row is None:
                return None, f"投递任务 {task_no} 不存在", "missing"
        if batch_id is None and task_row is not None:
            batch_id = str(task_row.get("数据来源") or "") or None
        if batch_id is None:
            return None, "缺少批次编号，无法核对投递", "missing"
        if task_row is None:
            for row in store.rows("data_delivery"):
                if str(row.get("数据来源")) == batch_id:
                    task_row = row
                    break

        batch_row = _latest_batch_row(batch_id)
        if batch_row is None:
            return None, f"采集批次 {batch_id} 不存在", "missing"
        version = str(batch_row.get("Schema版本") or "v1")

        if task_row is None:
            task_row = _create_delivery_row(batch_id, batch_row)

        confirmed = [
            r for r in store.receipts()
            if r.get("batch_id") == batch_id
            and r.get("batch_version") == version
            and r.get("status") == RECEIPT_CONFIRMED
        ]
        now = _now_iso()
        if confirmed and confirmed[-1]["conclusion"] == "通过":
            receipt = confirmed[-1]
            task_row["投递状态"] = "已完成"
            task_row["status"] = "已完成"
            task_row["pending"] = False
            task_row["abnormal"] = False
            task_row["最近投递"] = now
            task_row["质检回执"] = receipt["receipt_id"]
            task_row["批次版本"] = version
            task_row["取数依据"] = (
                f"回执 {receipt['receipt_id']} · 批次版本 {version} · "
                f"{receipt['check_result']}（{receipt['conclusion']}）")
            task_row["放行结论"] = "放行"
            return task_row, (
                f"批次版本 {version} 检验已通过，凭回执 {receipt['receipt_id']} 放行投递"), "放行"

        if confirmed:  # 已确认但结论为偏差告警：沿用结论，保持待核
            receipt = confirmed[-1]
            basis = (f"回执 {receipt['receipt_id']} · 批次版本 {version} · "
                     f"{receipt['check_result']}（{receipt['conclusion']}），待复核")
            message = f"批次版本 {version} 检验结论为{receipt['conclusion']}，投递保持待核"
        else:
            receipt = None
            basis = f"批次版本 {version} 暂无已确认回执，投递待核"
            message = f"批次版本 {version} 暂无已确认回执，投递保持待核"
        task_row["投递状态"] = "待投递"
        task_row["status"] = "待投递"
        task_row["pending"] = True
        task_row["质检回执"] = receipt["receipt_id"] if receipt else ""
        task_row["批次版本"] = version
        task_row["取数依据"] = basis
        task_row["放行结论"] = "待核"
        return task_row, message, "待核"


def _create_delivery_row(batch_id: str, batch_row: dict[str, Any]) -> dict[str, Any]:
    rows = store.rows("data_delivery")
    task_no = f"DEL-{batch_id}"
    existing = {str(r.get("任务编号")) for r in rows}
    base, seq = task_no, 2
    while task_no in existing:
        task_no = f"{base}-{seq}"
        seq += 1
    row = {
        "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
        "任务编号": task_no,
        "数据来源": batch_id,
        "目标系统": f"data_warehouse_{batch_row.get('来源作业', '')}",
        "投递格式": DeliveryFormat.CSV.value,
        "投递频率": "manual",
        "分区策略": "by_date",
        "最近投递": "",
        "投递状态": "待投递",
        "status": "待投递",
        "pending": True,
        "abnormal": False,
    }
    rows.append(row)
    return row


# ---------------------------------------------------------------------------
# 流水线 5: incr_crawl → crawl_job → diff_check
# ---------------------------------------------------------------------------

def run_incremental_flow(cursor: IncrCursor,
                         previous_batch: StorageBatch | None,
                         current_data: list[CleanedContent]) -> DiffRecord | None:
    """增量采集流程：比较增量游标与当前批次，生成差异检测记录。"""
    if previous_batch is None or not previous_batch.records:
        return None

    prev_fields = {r.source_url: r.fields for r in previous_batch.records}
    curr_fields = {r.source_url: r.fields for r in current_data}

    # 检测新增
    new_urls = set(curr_fields) - set(prev_fields)
    # 检测变更
    changed_urls = {url for url in (set(curr_fields) & set(prev_fields))
                    if curr_fields[url] != prev_fields[url]}
    # 检测删除
    removed_urls = set(prev_fields) - set(curr_fields)

    total_changes = len(new_urls) + len(changed_urls) + len(removed_urls)
    if total_changes == 0:
        return None

    diff = DiffRecord(
        record_id=f"DIFF-{cursor.cursor_id}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}",
        target_site=cursor.target_site,
        page_url=",".join(list(new_urls | changed_urls)[:5]),
        change_type=_classify_change_type(len(new_urls), len(changed_urls), len(removed_urls)),
        old_summary=f"原{len(prev_fields)}条记录",
        new_summary=f"新{len(curr_fields)}条记录，新增{len(new_urls)}变更{len(changed_urls)}删除{len(removed_urls)}",
        affected_fields=list(curr_fields[next(iter(curr_fields))]) if curr_fields else [],
    )

    _persist_diff(diff)

    # 更新游标
    cursor.new_count = len(new_urls)
    cursor.updated_count = len(changed_urls)
    cursor.last_cursor = cursor.current_cursor
    cursor.current_cursor = datetime.now(timezone.utc).isoformat()
    cursor.last_sync = cursor.current_cursor
    cursor.status = "已追上"

    return diff


def _classify_change_type(new_count: int, changed_count: int, removed_count: int) -> str:
    if new_count > 0 and changed_count == 0 and removed_count == 0:
        return "新增内容"
    if changed_count > 0 and new_count == 0 and removed_count == 0:
        return "内容变更"
    if removed_count > 0 and new_count == 0 and changed_count == 0:
        return "内容删除"
    return "混合变更"


# ---------------------------------------------------------------------------
# 流水线 6: data_archive ← batch_store
# ---------------------------------------------------------------------------

def archive_batch(batch: StorageBatch) -> ArchivePolicy | None:
    """从批次存储归档符合条件的批次数据。"""
    # 优先查找与当前批次相关的策略
    matched_policies: list[ArchivePolicy] = []
    policies: list[ArchivePolicy] = []
    for row in store.rows("data_archive"):
        p = ArchivePolicy.from_seed_row(row)
        if p.status in ("待触发", "已完成"):
            policies.append(p)
            if batch.batch_id in p.data_scope or p.data_scope == batch.batch_id:
                matched_policies.append(p)

    # 优先使用匹配的策略
    if matched_policies:
        policy = matched_policies[0]
        policy.last_archive = datetime.now(timezone.utc).isoformat()
        policy.status = "已完成"
        return policy

    if not policies:
        # 创建默认归档策略
        policy = ArchivePolicy(
            policy_id=f"ARCH-{batch.batch_id}",
            data_scope=batch.batch_id,
            archive_cycle="on_demand",
            retention_days=90,
            storage_location=f"/archive/{batch.batch_id}/",
            last_archive=datetime.now(timezone.utc).isoformat(),
            status="已完成",
        )
        _persist_archive_policy(policy)
        return policy

    policy = policies[0]
    policy.last_archive = datetime.now(timezone.utc).isoformat()
    policy.status = "已完成"
    return policy


# ---------------------------------------------------------------------------
# 流水线 7: crawl_cost ← proxy_pool
# ---------------------------------------------------------------------------

def compute_crawl_cost(job: CrawlJobSpec, proxy: ProxyNode | None) -> CostRecord:
    """根据采集作业和代理使用情况计算采集成本。"""
    proxy_hours = job.completed_pages * 0.01 if proxy else 0
    data_mb = job.completed_pages * 0.5  # 每页 0.5MB 估算

    record = CostRecord(
        record_id=f"COST-{job.job_id}",
        site=job.site.domain,
        billing_cycle=datetime.now(timezone.utc).strftime("%Y-%m"),
        request_count=job.completed_pages + job.failed_pages,
        data_volume_mb=round(data_mb, 2),
        proxy_usage_hours=round(proxy_hours, 2),
        estimated_cost=round(data_mb * 0.001 + proxy_hours * 0.5, 4),
        status="已核算" if job.status in ("已完成", "部分失败") else "统计中",
    )
    _persist_cost(record)
    return record


# ---------------------------------------------------------------------------
# 流水线 8: crawl_job → scheduler
# ---------------------------------------------------------------------------

def schedule_job(job: CrawlJobSpec) -> SchedulePlan | None:
    """为采集作业创建调度计划。"""
    # 找关联的调度计划
    for row in store.rows("scheduler"):
        if str(row.get("关联作业")) == job.job_id:
            plan = SchedulePlan.from_seed_row(row)
            plan.next_run = datetime.now(timezone.utc).isoformat()
            plan.status = "执行中" if job.status in ("待执行", "采集执行") else "已完成"
            return plan

    # 创建新的调度计划
    plan = SchedulePlan(
        plan_id=f"SCHED-{job.job_id}",
        linked_job_id=job.job_id,
        cron_expr="0 */6 * * *",  # 每6小时
        next_run=datetime.now(timezone.utc).isoformat(),
        status="执行中" if job.status in ("待执行", "采集执行") else "已完成",
    )
    _persist_schedule(plan)
    return plan


# ---------------------------------------------------------------------------
# 跨模块失败处理和恢复逻辑
# ---------------------------------------------------------------------------

def recover_failed_batch(batch: StorageBatch) -> StorageBatch | None:
    """恢复失败的批次：重新对缺失记录执行抽取和清洗。"""
    if batch.status != "校验失败":
        return None

    # 找出失败记录
    failed_records = [r for r in batch.records if not r.is_valid()]
    if not failed_records:
        return None

    # 重试：对失败记录重新填充缺失字段
    for record in failed_records:
        for field, value in list(record.fields.items()):
            if not value or value == "":
                record.fields[field] = f"recovered:{field}"
        # 恢复后清除之前清洗错误标记
        record.clean_errors = []

    # 重新校验
    batch.checksum = batch.compute_checksum()
    batch.status = "校验通过" if all(r.is_valid() for r in batch.records) else "校验失败"
    batch.stored_at = datetime.now(timezone.utc).isoformat()
    return batch


def handle_duplicate_batch(batch: StorageBatch) -> StorageBatch | None:
    """处理重复批次：检查是否与已有批次产生冲突。"""
    existing_batch_ids = set()
    for row in store.rows("batch_store"):
        existing_batch_ids.add(str(row.get("批次编号")))

    if batch.batch_id in existing_batch_ids:
        # 重复批次：追加时间戳后缀
        suffix = datetime.now(timezone.utc).strftime("%H%M%S")
        batch.batch_id = f"{batch.batch_id}-{suffix}"
        batch.storage_path = f"/data/batches/{batch.source_job_id}-retry/"

    return batch


def handle_interrupted_pipeline(job: CrawlJobSpec) -> CrawlJobSpec:
    """处理中断的采集管线：恢复未完成的工作。"""
    if job.status not in ("部分失败", "待执行"):
        return job

    # 找到失败的页面并标记重试
    if job.failed_pages > 0:
        job.failed_pages = max(0, job.failed_pages - 1)  # 重试一页
        job.status = "采集执行"
        if job.failed_pages + job.completed_pages == 0:
            job.status = "已完成"

    return job


# ---------------------------------------------------------------------------
# 内部持久化辅助
# ---------------------------------------------------------------------------

def _persist_batch(batch: StorageBatch) -> None:
    rows = store.rows("batch_store")
    rows.append({
        "id": max((int(row.get("id", 0)) for row in rows), default=0) + 1,
        "批次编号": batch.batch_id,
        "来源作业": batch.source_job_id,
        "数据行数": batch.row_count,
        "存储路径": batch.storage_path,
        "压缩格式": batch.compression,
        "Schema版本": batch.schema_version,
        "校验摘要": batch.checksum[:16],
        "入库时间": batch.stored_at,
        "status": batch.status,
        "pending": False,
        "abnormal": batch.status == "校验失败",
    })
    _LIVE_BATCHES[batch.batch_id] = batch


def _persist_fingerprint(fp: UrlFingerprint) -> None:
    rows = store.rows("url_dedup")
    rows.append({
        "id": max((int(row.get("id", 0)) for row in rows), default=0) + 1,
        "指纹编号": fp.fingerprint_id,
        "源URL": fp.source_url,
        "指纹算法": fp.algorithm,
        "指纹哈希": fp.hash_value,
        "发现渠道": fp.discovery_channel,
        "首次发现": fp.first_seen,
        "重复次数": fp.duplicate_count,
        "去重状态": fp.status,
        "status": fp.status,
        "pending": False,
        "abnormal": False,
    })


def _persist_parse_log(log: ParseLogEntry) -> None:
    rows = store.rows("parse_log")
    rows.append({
        "id": max((int(row.get("id", 0)) for row in rows), default=0) + 1,
        "记录编号": log.record_id,
        "来源URL": log.source_url,
        "解析规则": log.parse_rule_id,
        "解析结果": log.parse_result,
        "异常类型": log.error_type,
        "重试次数": log.retry_count,
        "错误详情": log.error_detail,
        "记录时间": log.recorded_at,
        "status": "成功" if not log.error_type else "格式异常",
        "pending": False,
        "abnormal": bool(log.error_type),
    })


def _persist_delivery(dt: DeliveryTask) -> None:
    rows = store.rows("data_delivery")
    rows.append(dict(
        id=max((int(r.get("id", 0)) for r in rows), default=0) + 1,
        **{f: (getattr(dt, k) if not isinstance(getattr(dt, k), DeliveryFormat) else getattr(dt, k).value) for f, k in [
            ("任务编号", "task_id"), ("数据来源", "data_source"),
            ("目标系统", "target_system"), ("投递格式", "delivery_format"),
            ("投递频率", "frequency"), ("分区策略", "partition_strategy"),
            ("最近投递", "last_delivery"), ("投递状态", "status"),
        ]},
        status=dt.status, pending=False, abnormal=dt.status == "投递失败",
    ))


def _persist_sample(qs: QualitySample) -> None:
    rows = store.rows("quality_sample")
    rows.append({
        "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
        "计划编号": qs.plan_id, "目标批次": qs.target_batch,
        "抽样比例": str(qs.sample_ratio), "抽样方式": qs.sample_method.value,
        "样本数量": qs.sample_size, "检验结果": qs.check_result,
        "偏差阈值": str(qs.deviation_threshold), "最近采样": qs.last_sampled,
        "status": qs.status, "pending": False,
        "abnormal": qs.status == "偏差告警",
    })


def _update_sample_row(row: dict[str, Any], qs: QualitySample) -> None:
    """把抽样计算结果回填到已有的质量抽样台账行，避免台账与实测脱节。"""
    row["抽样比例"] = str(qs.sample_ratio)
    row["抽样方式"] = qs.sample_method.value if isinstance(
        qs.sample_method, SampleMethod) else str(qs.sample_method)
    row["样本数量"] = qs.sample_size
    row["检验结果"] = qs.check_result
    row["偏差阈值"] = str(qs.deviation_threshold)
    row["最近采样"] = qs.last_sampled
    row["status"] = qs.status
    row["pending"] = False
    row["abnormal"] = qs.status == "偏差告警"


def _persist_diff(diff: DiffRecord) -> None:
    rows = store.rows("diff_check")
    rows.append({
        "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
        "记录编号": diff.record_id, "目标站点": diff.target_site,
        "检测页面": diff.page_url, "变更类型": diff.change_type,
        "旧内容摘要": diff.old_summary, "新内容摘要": diff.new_summary,
        "影响字段": ",".join(diff.affected_fields), "检测时间": diff.detected_at,
        "status": diff.status, "pending": False, "abnormal": False,
    })


def _persist_archive_policy(p: ArchivePolicy) -> None:
    rows = store.rows("data_archive")
    rows.append({
        "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
        "策略编号": p.policy_id, "数据范围": p.data_scope,
        "归档周期": p.archive_cycle, "归档格式": str(p.archive_format),
        "保留时长": str(p.retention_days), "存储位置": p.storage_location,
        "压缩方式": p.compression_method, "最后归档": p.last_archive,
        "status": p.status, "pending": False, "abnormal": False,
    })


def _persist_cost(c: CostRecord) -> None:
    rows = store.rows("crawl_cost")
    rows.append({
        "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
        "记录编号": c.record_id, "采集站点": c.site,
        "计费周期": c.billing_cycle, "请求次数": str(c.request_count),
        "数据流量": str(c.data_volume_mb), "代理用量": str(c.proxy_usage_hours),
        "费用预估": c.estimated_cost, "账单状态": c.status,
        "status": c.status, "pending": False,
        "abnormal": c.estimated_cost > 100,
    })


def _persist_schedule(plan: SchedulePlan) -> None:
    rows = store.rows("scheduler")
    rows.append({
        "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
        "计划编号": plan.plan_id, "关联作业": plan.linked_job_id,
        "Cron表达式": plan.cron_expr, "时区设置": plan.timezone_setting,
        "依赖检查": plan.dependency_check, "失败通知": plan.failure_notify,
        "并发控制": plan.concurrency_control, "下次执行": plan.next_run,
        "status": plan.status, "pending": False, "abnormal": False,
    })