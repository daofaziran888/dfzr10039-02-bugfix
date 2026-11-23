"""抽样检验回执与投递放行接口：质检视图复核回执，投递工作台凭批次版本取数。

路由挂在模块路径下（/api/quality_sample/receipts、/api/data_delivery/submit），
注册顺序必须在通用资源路由之前，避免被 /api/{module}/{entry_id} 抢先匹配。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from app import integration
from app.schemas import ActionResult

router = APIRouter(prefix="/api", tags=["抽样检验回执"])


class InspectPayload(BaseModel):
    batch_id: str
    sample_size: int | None = None
    operator: str | None = None


class DeliverySubmitPayload(BaseModel):
    batch_id: str | None = None
    task_no: str | None = None


@router.get("/quality_sample/receipts")
def list_receipts(
    batch_id: str | None = Query(default=None),
    batch_version: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> dict[str, Any]:
    """质检视图回执列表：操作员可复核的抽样检验回执。"""
    items = integration.list_receipts(
        batch_id=batch_id, batch_version=batch_version, status=status)
    return {"items": items, "total": len(items)}


@router.get("/quality_sample/receipts/{receipt_id}")
def receipt_detail(receipt_id: str) -> dict[str, Any]:
    """回执明细：样本、对应批次校验结果和首次中断/后续完成历程。"""
    detail = integration.receipt_detail(receipt_id)
    if detail is None:
        raise HTTPException(status_code=404, detail=f"回执 {receipt_id} 不存在")
    return detail


@router.post("/quality_sample/inspect", response_model=ActionResult)
def start_inspection(payload: InspectPayload) -> ActionResult:
    """发起抽样检验：生成待核回执；并发提交合并，已确认则沿用结论。"""
    receipt, message, _ = integration.start_sample_inspection(
        payload.batch_id.strip(),
        sample_size=payload.sample_size,
        operator=(payload.operator or "").strip(),
    )
    return ActionResult(ok=receipt is not None, message=message, entry=receipt)


@router.post("/quality_sample/receipts/{receipt_id}/confirm", response_model=ActionResult)
def confirm_receipt(receipt_id: str,
                    operator: str | None = Query(default=None)) -> ActionResult:
    """确认回执：同次检验并发确认只生效一次。"""
    receipt, message = integration.confirm_receipt(receipt_id, operator=operator or "")
    return ActionResult(ok=receipt is not None, message=message, entry=receipt)


@router.post("/quality_sample/receipts/{receipt_id}/interrupt", response_model=ActionResult)
def interrupt_receipt(receipt_id: str,
                      operator: str | None = Query(default=None)) -> ActionResult:
    """中断待核回执：保留中断记录。"""
    receipt, message = integration.interrupt_receipt(receipt_id, operator=operator or "")
    return ActionResult(ok=receipt is not None, message=message, entry=receipt)


@router.post("/quality_sample/receipts/{receipt_id}/retry", response_model=ActionResult)
def retry_receipt(receipt_id: str,
                  operator: str | None = Query(default=None)) -> ActionResult:
    """中断后恢复：生成新次数的回执，与原回执各自留痕。"""
    receipt, message = integration.retry_receipt(receipt_id, operator=operator or "")
    return ActionResult(ok=receipt is not None, message=message, entry=receipt)


@router.post("/data_delivery/submit", response_model=ActionResult)
def submit_delivery(payload: DeliverySubmitPayload) -> ActionResult:
    """投递核对：凭当前批次版本的已确认回执决定放行或待核。"""
    row, message, _ = integration.submit_delivery(
        batch_id=(payload.batch_id or "").strip() or None,
        task_no=(payload.task_no or "").strip() or None,
    )
    return ActionResult(ok=row is not None, message=message, entry=row)
