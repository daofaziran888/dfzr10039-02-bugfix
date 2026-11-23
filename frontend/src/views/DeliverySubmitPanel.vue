<template>
  <section class="panel" data-testid="delivery-submit-panel">
    <header class="panel-head">
      <h3>投递核对</h3>
      <p class="panel-desc">提交批次后，凭当前批次版本的已确认抽样检验回执决定放行或待核；旧版本报告不能提前放行。</p>
    </header>

    <form class="panel-form" @submit.prevent="submit">
      <label class="panel-field">
        <span>批次编号</span>
        <input v-model.trim="batchId" data-testid="submit-batch" placeholder="批次编号，如 BATC-0003" required />
      </label>
      <button class="btn primary" data-testid="submit-delivery" type="submit" :disabled="busy">
        提交投递核对
      </button>
    </form>

    <p
      v-if="decision"
      class="panel-message"
      :data-decision="decision"
      data-testid="delivery-decision"
    >
      {{ decision === '放行' ? '✅ 放行' : '⏸ 待核' }} · {{ message }}
    </p>
    <p v-if="errorMessage" class="error-text">{{ errorMessage }}</p>

    <h4 class="panel-subhead">取数依据（投递从同一份抽样检验回执取数）</h4>
    <table class="data-table">
      <thead>
        <tr>
          <th>任务编号</th>
          <th>数据来源</th>
          <th>批次版本</th>
          <th>质检回执</th>
          <th>取数依据</th>
          <th>放行结论</th>
          <th>投递状态</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in basisRows" :key="String(row.id)" :data-testid="`basis-${row.id}`">
          <td>{{ row['任务编号'] }}</td>
          <td>{{ row['数据来源'] }}</td>
          <td>{{ row['批次版本'] || '—' }}</td>
          <td>{{ row['质检回执'] || '—' }}</td>
          <td>{{ row['取数依据'] || '—' }}</td>
          <td>{{ row['放行结论'] || '—' }}</td>
          <td>{{ row.status }}</td>
        </tr>
        <tr v-if="!basisRows.length">
          <td colspan="7" class="empty-state">暂无投递核对记录，提交批次后在此呈现取数依据</td>
        </tr>
      </tbody>
    </table>
  </section>
</template>

<script setup lang="ts">
import { computed, ref } from 'vue'

import { submitDelivery } from '@/api/backend-service'
import type { EntryRow } from '@/data/types'

const props = defineProps<{ rows: EntryRow[] }>()
const emit = defineEmits<{ changed: [] }>()

const batchId = ref('')
const message = ref('')
const decision = ref('')
const errorMessage = ref('')
const busy = ref(false)

const basisRows = computed(() =>
  props.rows.filter((row) => row['取数依据'] || row['质检回执'] || row['批次版本']),
)

async function submit() {
  busy.value = true
  message.value = ''
  decision.value = ''
  errorMessage.value = ''
  try {
    const result = await submitDelivery(batchId.value)
    message.value = result.message
    if (!result.ok) throw new Error(result.message)
    decision.value = String(result.entry?.['放行结论'] ?? '')
    emit('changed')
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '投递核对失败'
  } finally {
    busy.value = false
  }
}
</script>
