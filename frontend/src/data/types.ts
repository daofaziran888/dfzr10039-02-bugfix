/** 纯前端数据层的公共类型：与全栈版后端返回的结构保持一致，换回后端时页面不用改。 */

export type EntryRow = {
  id: number
  status: string
  pending: boolean
  abnormal: boolean
  [field: string]: string | number | boolean
}

export type ModuleMeta = {
  key: string
  name: string
  entity: string
  desc: string
  fields: string[]
  statuses: string[]
  actions: string[]
  actionTargets: Record<string, string>
  metrics: string[]
}

export type PageResult = {
  items: EntryRow[]
  total: number
  page: number
  size: number
}

export type ActionResult = {
  ok: boolean
  message: string
  entry?: EntryRow | null
}

export type OverviewResult = {
  cards: { label: string; value: number }[]
  modules: { name: string; created: number; pending: number; abnormal: number }[]
}

/** 抽样检验回执：质检视图复核对象，投递放行的取数依据 */
export type ReceiptSample = {
  seq: number
  ref: string
  valid: boolean
  digest: string
}

export type ReceiptHistory = {
  time: string
  event: string
  actor: string
}

export type SampleReceipt = {
  id: number
  receipt_id: string
  plan_id: string
  batch_id: string
  batch_version: string
  sample_size: number
  valid_count: number
  check_result: string
  conclusion: string
  samples: ReceiptSample[]
  batch_checksum: string
  batch_status: string
  attempt: number
  status: string
  created_at: string
  confirmed_at: string
  confirmed_by: string
  resumed_from: string
  history: ReceiptHistory[]
}

export type ReceiptDetail = SampleReceipt & {
  chain: SampleReceipt[]
  batch: Record<string, string | number | boolean> | null
}

export type ReceiptListResult = {
  items: SampleReceipt[]
  total: number
}
