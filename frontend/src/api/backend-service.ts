import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type {
  ActionResult,
  EntryRow,
  OverviewResult,
  PageResult,
  ReceiptDetail,
  ReceiptListResult,
} from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

// ---------------------------------------------------------------------------
// 抽样检验回执与投递放行
// ---------------------------------------------------------------------------

export async function listReceipts(batchId = ''): Promise<ReceiptListResult> {
  const query = batchId ? `?batch_id=${encodeURIComponent(batchId)}` : ''
  const response = await request(`/api/quality_sample/receipts${query}`)
  if (!response.ok) throw new Error('抽样检验回执读取失败')
  return await response.json()
}

export async function getReceiptDetail(receiptId: string): Promise<ReceiptDetail> {
  const response = await request(`/api/quality_sample/receipts/${encodeURIComponent(receiptId)}`)
  if (!response.ok) throw new Error('回执明细读取失败')
  return await response.json()
}

export async function startInspection(batchId: string, sampleSize?: number): Promise<ActionResult> {
  const response = await request('/api/quality_sample/inspect', {
    method: 'POST',
    body: JSON.stringify({ batch_id: batchId, sample_size: sampleSize ?? null }),
  })
  return await response.json()
}

export async function receiptAction(
  receiptId: string,
  action: 'confirm' | 'interrupt' | 'retry',
): Promise<ActionResult> {
  const response = await request(
    `/api/quality_sample/receipts/${encodeURIComponent(receiptId)}/${action}`,
    { method: 'POST' },
  )
  return await response.json()
}

export async function submitDelivery(batchId: string): Promise<ActionResult> {
  const response = await request('/api/data_delivery/submit', {
    method: 'POST',
    body: JSON.stringify({ batch_id: batchId }),
  })
  return await response.json()
}
