<template>
  <section class="page" data-module="runtime">
    <header class="page-head">
      <div>
        <h2>运行实例管理</h2>
        <p class="page-desc">
          记录每次ETL作业的运行实例，追踪执行状态和数据吞吐量；失败传播清单按编排图分区依赖快照生成。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" @click="openCreate">登记运行记录</button>
        <button class="btn" type="button" @click="exportRows">导出运行实例清单</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" type="submit">查询</button>
      <button class="btn ghost" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in columns" :key="column">{{ column }}</th>
          <th>编排图</th>
          <th>失败传播状态</th>
          <th>生效快照</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)">
          <td v-for="column in columns" :key="column">{{ row[column] ?? '—' }}</td>
          <td>{{ row['编排图编号'] || '—' }}</td>
          <td>
            <span :class="['badge', stateClass(row['失败传播状态'])]">
              {{ row['失败传播状态'] || '未生成' }}
            </span>
          </td>
          <td>{{ row['生效快照'] || '—' }}</td>
          <td class="row-actions">
            <button class="link" type="button" @click="openPropagation(row)">失败传播清单</button>
            <button class="link" type="button" @click="generatePropagation(row)">生成传播清单</button>
            <button
              v-for="action in actions"
              :key="action"
              class="link"
              type="button"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 4" class="empty-state">暂无运行实例数据，可先登记运行记录</td>
        </tr>
      </tbody>
    </table>

    <section v-if="view" class="panel">
      <header class="panel-head">
        <h3>失败传播清单 · {{ view['运行记录编号'] || '' }}</h3>
        <button class="btn ghost" type="button" @click="view = null">收起</button>
      </header>
      <p class="panel-desc">
        编排图 {{ view['图编号'] || view['编排图编号'] }} ·
        状态
        <span :class="['badge', stateClass(view['状态'])]">{{ view['状态'] }}</span>
        <span v-if="view['快照编号']">· 快照 {{ view['快照编号'] }}</span>
        <span v-if="view['清单来源']">· {{ view['清单来源'] }}</span>
      </p>

      <div v-if="(view['阻断原因'] || []).length" class="alert">
        <strong>阻断原因（{{ (view['阻断原因'] || []).length }} 条）</strong>
        <ul>
          <li v-for="(reason, index) in view['阻断原因'] || []" :key="index">{{ reason }}</li>
        </ul>
        <button v-if="view['可重试']" class="btn" type="button" @click="retryGeneration">重试生成</button>
      </div>
      <p v-if="view['说明']" class="hint">{{ view['说明'] }}</p>

      <table v-if="(view['失败传播清单'] || []).length" class="data-table">
        <thead>
          <tr>
            <th>触发节点</th><th>直接影响对象</th><th>受影响节点</th>
            <th>受影响数据表分区</th><th>传播层级</th><th>说明</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in view['失败传播清单']" :key="item.触发节点">
            <td>{{ item.触发节点 }}</td>
            <td>{{ item.直接影响对象 }}</td>
            <td>{{ (item.受影响节点 || []).join('、') || '无' }}</td>
            <td>{{ (item.受影响数据表分区 || []).join('、') || '无' }}</td>
            <td>{{ item.传播层级 }}</td>
            <td>{{ item['传播说明'] }}</td>
          </tr>
        </tbody>
      </table>
      <p v-else class="empty-state">
        暂无可用失败传播清单：{{ view['说明'] || '请先完成编排图拓扑预检并发布快照' }}
      </p>
    </section>

    <footer class="page-foot">
      <span>共 {{ total }} 条运行实例记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
      <span v-if="notice" class="notice-text">{{ notice }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null | string[]>

type PropagationItem = {
  触发节点: string
  直接影响对象: string
  受影响节点: string[]
  受影响数据表分区: string[]
  传播层级: number
  传播说明: string
}

type PropagationView = {
  状态: string
  运行记录编号?: string
  图编号?: string
  编排图编号?: string
  快照编号?: string
  清单来源?: string
  阻断原因?: string[]
  说明?: string
  可重试?: boolean
  失败传播清单?: PropagationItem[]
}

const ENDPOINT = '/api/runtime'
const columns = ["记录编号", "作业名称", "开始时间", "结束时间", "执行状态", "读取行数", "写入行数", "错误信息"]
const actions = ["启动作业", "终止作业", "标记成功"]
const filterFields = ["记录编号", "作业名称"]

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const notice = ref('')
const filters = ref<Record<string, string>>({})
const view = ref<PropagationView | null>(null)
const activeRunId = ref<number | null>(null)

const stats = computed(() => [
  { label: '运行记录总数', value: total.value },
  { label: '运行中记录', value: rows.value.filter((row) => row['status'] === '运行中').length },
  {
    label: '传播阻断',
    value: rows.value.filter((row) => ['阻断', '异常'].includes(String(row['失败传播状态']))).length,
  },
])

function stateClass(state: unknown) {
  const value = String(state ?? '')
  if (['生效', '成功'].includes(value)) return 'badge-ok'
  if (['阻断', '异常'].includes(value)) return 'badge-bad'
  return 'badge-muted'
}

function resetFilters() {
  filters.value = {}
  void reload()
}

function exportRows() {
  window.open(`${ENDPOINT}/export`, '_blank')
}

function openCreate() {
  errorMessage.value = '运行记录登记入口尚未接入审批流'
}

async function readResult(response: Response) {
  const payload = await response.json().catch(() => ({}))
  return {
    ok: Boolean(payload.ok),
    message: String(payload.message ?? (response.ok ? '' : `接口返回 ${response.status}`)),
    entry: payload.entry as Record<string, unknown> | undefined,
  }
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  notice.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action }),
    })
    if (!response.ok) {
      throw new Error('运行实例动作未生效，请稍后重试')
    }
    const result = await readResult(response)
    if (!result.ok) {
      errorMessage.value = result.message || '运行实例操作未通过校验'
      return
    }
    notice.value = result.message
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '运行实例操作失败'
  }
}

async function loadPropagation(row: Row, quiet = false) {
  if (!quiet) {
    errorMessage.value = ''
    notice.value = ''
  }
  const response = await request(`${ENDPOINT}/${row.id}/propagation`)
  if (!response.ok) {
    throw new Error('失败传播清单读取失败')
  }
  const payload = (await response.json()) as PropagationView
  view.value = payload
  activeRunId.value = Number(row.id)
  if (payload['状态'] !== '生效' && (payload['阻断原因'] || []).length) {
    errorMessage.value = `失败传播清单不可用：${(payload['阻断原因'] || []).join('；')}`
  }
  return payload
}

async function openPropagation(row: Row) {
  try {
    await loadPropagation(row)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '失败传播清单读取失败'
  }
}

async function generatePropagation(row: Row) {
  errorMessage.value = ''
  notice.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/propagation`, {
      method: 'POST',
      body: JSON.stringify({}),
    })
    const result = await readResult(response)
    if (!result.ok) {
      // 生成失败也要把阻断原因展示出来，并允许继续重试。
      errorMessage.value = result.message || '失败传播清单生成未通过'
      const entryView = (result.entry ?? {}) as unknown as PropagationView
      view.value = entryView
      activeRunId.value = Number(row.id)
      return
    }
    notice.value = result.message
    await loadPropagation(row, true)
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '失败传播清单生成失败'
  }
}

async function retryGeneration() {
  const row = rows.value.find((item) => Number(item.id) === activeRunId.value)
  if (row) {
    await generatePropagation(row)
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('运行记录列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '运行实例列表读取失败'
  }
}

onMounted(reload)
</script>
