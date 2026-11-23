<template>
  <section class="panel" data-testid="receipt-panel">
    <header class="panel-head">
      <h3>抽样检验回执</h3>
      <p class="panel-desc">校验通过的采集批次完成抽样后，回执在此落库；操作员可复核样本与批次校验结果后确认，投递凭已确认回执放行。</p>
    </header>

    <form class="panel-form" @submit.prevent="inspect">
      <label class="panel-field">
        <span>目标批次</span>
        <input v-model.trim="inspectBatch" data-testid="inspect-batch" placeholder="批次编号，如 BATC-0003" required />
      </label>
      <label class="panel-field">
        <span>样本数量</span>
        <input v-model.number="inspectSize" data-testid="inspect-size" type="number" min="1" placeholder="默认 2" />
      </label>
      <button class="btn primary" data-testid="inspect-submit" type="submit" :disabled="busy">
        发起抽样检验
      </button>
      <label class="panel-field">
        <span>按批次过滤</span>
        <input v-model.trim="filterBatch" data-testid="receipt-filter" placeholder="留空显示全部" @input="reload" />
      </label>
    </form>

    <p v-if="message" class="panel-message" data-testid="receipt-message">{{ message }}</p>
    <p v-if="errorMessage" class="error-text">{{ errorMessage }}</p>

    <table class="data-table">
      <thead>
        <tr>
          <th>回执编号</th>
          <th>目标批次</th>
          <th>批次版本</th>
          <th>检验次数</th>
          <th>样本数量</th>
          <th>有效条数</th>
          <th>检验结果</th>
          <th>检验结论</th>
          <th>回执状态</th>
          <th>复核操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="receipt in receipts" :key="receipt.receipt_id" :data-testid="`receipt-${receipt.receipt_id}`">
          <td>{{ receipt.receipt_id }}</td>
          <td>{{ receipt.batch_id }}</td>
          <td>{{ receipt.batch_version }}</td>
          <td>第 {{ receipt.attempt }} 次</td>
          <td>{{ receipt.sample_size }}</td>
          <td>{{ receipt.valid_count }}</td>
          <td>{{ receipt.check_result }}</td>
          <td>{{ receipt.conclusion }}</td>
          <td>{{ receipt.status }}</td>
          <td class="row-actions">
            <button class="link" type="button" :data-testid="`receipt-detail-${receipt.receipt_id}`" @click="openDetail(receipt)">
              复核详情
            </button>
            <button
              v-if="receipt.status === '待核'"
              class="link"
              type="button"
              :data-testid="`receipt-confirm-${receipt.receipt_id}`"
              @click="act(receipt, 'confirm')"
            >
              确认通过
            </button>
            <button
              v-if="receipt.status === '待核'"
              class="link"
              type="button"
              :data-testid="`receipt-interrupt-${receipt.receipt_id}`"
              @click="act(receipt, 'interrupt')"
            >
              中断
            </button>
            <button
              v-if="receipt.status === '已中断'"
              class="link"
              type="button"
              :data-testid="`receipt-retry-${receipt.receipt_id}`"
              @click="act(receipt, 'retry')"
            >
              重试
            </button>
          </td>
        </tr>
        <tr v-if="!receipts.length">
          <td colspan="10" class="empty-state">暂无抽样检验回执，可先发起抽样检验</td>
        </tr>
      </tbody>
    </table>

    <div v-if="detail" class="modal-backdrop" data-testid="receipt-detail-modal">
      <div class="modal">
        <h3>回执明细 · {{ detail.receipt_id }}</h3>
        <p class="panel-desc">
          目标批次 {{ detail.batch_id }} · 批次版本 {{ detail.batch_version }} ·
          第 {{ detail.attempt }} 次检验 · 状态 {{ detail.status }}
          <template v-if="detail.resumed_from"> · 恢复自 {{ detail.resumed_from }}</template>
        </p>

        <h4>样本复核（{{ detail.samples.length }} 个，有效 {{ detail.valid_count }} 个）</h4>
        <table class="data-table">
          <thead>
            <tr><th>序号</th><th>样本来源</th><th>是否有效</th><th>样本摘要</th></tr>
          </thead>
          <tbody>
            <tr v-for="sample in detail.samples" :key="sample.seq" :data-testid="`sample-${sample.seq}`">
              <td>{{ sample.seq }}</td>
              <td>{{ sample.ref }}</td>
              <td>{{ sample.valid ? '有效' : '无效' }}</td>
              <td>{{ sample.digest }}</td>
            </tr>
          </tbody>
        </table>

        <h4>对应批次校验结果</h4>
        <p v-if="detail.batch" class="panel-desc" data-testid="receipt-batch-check">
          批次状态：{{ detail.batch.status }} · 校验摘要：{{ detail.batch['校验摘要'] }} ·
          数据行数：{{ detail.batch['数据行数'] }} · Schema版本：{{ detail.batch['Schema版本'] }}
        </p>

        <h4>检验历程（首次中断及后续完成可查）</h4>
        <ul class="receipt-chain">
          <li v-for="item in detail.chain" :key="item.receipt_id" :data-testid="`chain-${item.receipt_id}`">
            <strong>第 {{ item.attempt }} 次</strong> · {{ item.receipt_id }} · {{ item.status }}
            <span v-for="(event, index) in item.history" :key="index" class="chain-event">
              {{ event.time }} {{ event.event }}（{{ event.actor }}）
            </span>
          </li>
        </ul>

        <div class="page-actions">
          <button class="btn" type="button" data-testid="receipt-detail-close" @click="detail = null">关闭</button>
        </div>
      </div>
    </div>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import {
  getReceiptDetail,
  listReceipts,
  receiptAction,
  startInspection,
} from '@/api/backend-service'
import type { ReceiptDetail, SampleReceipt } from '@/data/types'

const emit = defineEmits<{ changed: [] }>()

const receipts = ref<SampleReceipt[]>([])
const detail = ref<ReceiptDetail | null>(null)
const inspectBatch = ref('')
const inspectSize = ref<number | undefined>(2)
const filterBatch = ref('')
const message = ref('')
const errorMessage = ref('')
const busy = ref(false)

async function reload() {
  errorMessage.value = ''
  try {
    const payload = await listReceipts(filterBatch.value)
    receipts.value = payload.items
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '回执列表读取失败'
  }
}

async function inspect() {
  busy.value = true
  message.value = ''
  errorMessage.value = ''
  try {
    const result = await startInspection(inspectBatch.value, inspectSize.value || undefined)
    message.value = result.message
    if (!result.ok) throw new Error(result.message)
    await reload()
    emit('changed')
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '发起检验失败'
  } finally {
    busy.value = false
  }
}

async function act(receipt: SampleReceipt, action: 'confirm' | 'interrupt' | 'retry') {
  message.value = ''
  errorMessage.value = ''
  try {
    const result = await receiptAction(receipt.receipt_id, action)
    message.value = result.message
    if (!result.ok) throw new Error(result.message)
    await reload()
    emit('changed')
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '回执操作失败'
  }
}

async function openDetail(receipt: SampleReceipt) {
  errorMessage.value = ''
  try {
    detail.value = await getReceiptDetail(receipt.receipt_id)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '回执明细读取失败'
  }
}

onMounted(reload)
</script>
