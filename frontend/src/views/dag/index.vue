<template>
  <section class="page" data-module="dag">
    <header class="page-head">
      <div>
        <h2>任务编排管理</h2>
        <p class="page-desc">
          编排ETL任务的有向无环图，定义节点间依赖关系和并发执行策略；发布前先做拓扑完整性预检。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" @click="openCreate">登记编排图</button>
        <button class="btn" type="button" @click="exportRows">导出任务编排清单</button>
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
          <th>预检状态</th>
          <th>生效快照</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)">
          <td v-for="column in columns" :key="column">{{ row[column] ?? '—' }}</td>
          <td>
            <span :class="['badge', precheckClass(row)]">{{ row['预检状态'] || '未预检' }}</span>
          </td>
          <td>{{ row['生效快照'] || '—' }}</td>
          <td class="row-actions">
            <button class="link" type="button" @click="openPrecheck(row)">拓扑预检</button>
            <button class="link" type="button" @click="publishRow(row)">发布快照</button>
            <button class="link" type="button" @click="openDetail(row)">分区依赖</button>
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
          <td :colspan="columns.length + 3" class="empty-state">暂无任务编排数据，可先登记编排图</td>
        </tr>
      </tbody>
    </table>

    <section v-if="detail" class="panel">
      <header class="panel-head">
        <h3>分区依赖数据 · {{ detail.dagLabel }}</h3>
        <button class="btn ghost" type="button" @click="detail = null">收起</button>
      </header>
      <p class="panel-desc">
        来源：{{ detail.view['来源'] || detail.view['快照编号'] || '—' }}
        <span v-if="detail.view['可使用快照']">· 旧快照 {{ detail.view['可使用快照'] }} {{ detail.view['旧快照状态'] }}</span>
      </p>

      <div v-if="(detail.view['阻断原因'] || []).length" class="alert">
        <strong>阻断原因（{{ (detail.view['阻断原因'] || []).length }} 条）</strong>
        <ul>
          <li v-for="(reason, index) in detail.view['阻断原因'] || []" :key="index">{{ reason }}</li>
        </ul>
        <button class="btn" type="button" @click="precheckFromDetail">重试拓扑预检</button>
        <span v-if="detail.view['说明']" class="hint">{{ detail.view['说明'] }}</span>
      </div>

      <table v-if="(detail.view['分区依赖数据'] || []).length" class="data-table">
        <thead>
          <tr>
            <th>数据表</th><th>分区</th><th>产出节点</th><th>层级</th><th>调度优先级</th>
            <th>上游节点</th><th>环</th><th>孤立</th><th>受影响数据表分区</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in detail.view['分区依赖数据']" :key="`${item.数据表}.${item.分区}`">
            <td>{{ item.数据表 || '—' }}</td>
            <td>{{ item.分区 || '—' }}</td>
            <td>{{ item.产出节点 }}</td>
            <td>{{ item.层级 ?? '—' }}</td>
            <td>{{ item.调度优先级 ?? '—' }}</td>
            <td>{{ (item.上游节点 || []).join('、') || '—' }}</td>
            <td>{{ item.所属环 || '无' }}</td>
            <td>{{ item.是否孤立 ? '是' : '否' }}</td>
            <td>{{ (item.受影响数据表分区 || []).join('、') || '—' }}</td>
          </tr>
        </tbody>
      </table>
      <p v-else class="empty-state">
        {{ detail.view['说明'] || '暂无分区依赖数据：请先保存画布并执行拓扑预检' }}
      </p>

      <h4 class="panel-sub">失败传播清单</h4>
      <table v-if="(detail.view['失败传播清单'] || []).length" class="data-table">
        <thead>
          <tr>
            <th>触发节点</th><th>直接影响对象</th><th>受影响节点</th>
            <th>受影响数据表分区</th><th>传播层级</th><th>说明</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in detail.view['失败传播清单']" :key="item.触发节点">
            <td>{{ item.触发节点 }}</td>
            <td>{{ item.直接影响对象 }}</td>
            <td>{{ (item.受影响节点 || []).join('、') || '无' }}</td>
            <td>{{ (item.受影响数据表分区 || []).join('、') || '无' }}</td>
            <td>{{ item.传播层级 }}</td>
            <td>{{ item.传播说明 }}</td>
          </tr>
        </tbody>
      </table>
      <p v-else class="empty-state">暂无失败传播清单，发布预检通过的快照后会同步生成</p>

      <h4 class="panel-sub">多次保存的版本决议</h4>
      <table v-if="(detail.decisions || []).length" class="data-table">
        <thead>
          <tr><th>对象类型</th><th>对象编号</th><th>结论</th><th>采用保存</th><th>采用版本</th></tr>
        </thead>
        <tbody>
          <tr v-for="(item, index) in detail.decisions" :key="index">
            <td>{{ item.对象类型 }}</td>
            <td>{{ item.对象编号 }}</td>
            <td>{{ item.结论 }}</td>
            <td>{{ item.采用保存编号 }}</td>
            <td>{{ item.采用版本 }}</td>
          </tr>
        </tbody>
      </table>
      <p v-else class="empty-state">该编排图只有一次保存，没有版本合并决议</p>
    </section>

    <footer class="page-foot">
      <span>共 {{ total }} 条任务编排记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
      <span v-if="notice" class="notice-text">{{ notice }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null | string[]>

type PartitionRow = {
  数据表: string
  分区: string
  产出节点: string
  层级: number | null
  调度优先级: number | null
  上游节点: string[]
  所属环: string | null
  是否孤立: boolean
  受影响数据表分区: string[]
}

type PropagationRow = {
  触发节点: string
  直接影响对象: string
  受影响节点: string[]
  受影响数据表分区: string[]
  传播层级: number
  传播说明: string
}

type Decision = {
  对象类型: string
  对象编号: string
  结论: string
  采用保存编号: string
  采用版本: number
}

type PartitionView = {
  状态: string
  来源?: string
  快照编号?: string
  预检编号?: string
  阻断原因?: string[]
  说明?: string
  可使用快照?: string
  旧快照状态?: string
  分区依赖数据?: PartitionRow[]
  失败传播清单?: PropagationRow[]
}

const ENDPOINT = '/api/dag'
const columns = ["图编号", "图名称", "拓扑结构", "起始节点数", "最大并行度", "失败传播策略", "调度优先级", "生效时间范围"]
const actions = ["提交编排评审", "停用编排"]
const filterFields = ["图编号", "图名称"]

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const notice = ref('')
const filters = ref<Record<string, string>>({})
const detail = ref<{ dagLabel: string; view: PartitionView; decisions: Decision[] } | null>(null)

const stats = computed(() => [
  { label: '编排图总数', value: total.value },
  { label: '已发布编排', value: rows.value.filter((row) => row['status'] === '已发布').length },
  {
    label: '预检阻断',
    value: rows.value.filter((row) => ['阻断', '异常'].includes(String(row['预检状态']))).length,
  },
])

function precheckClass(row: Row) {
  const state = String(row['预检状态'] ?? '')
  if (state === '通过') return 'badge-ok'
  if (state === '阻断' || state === '异常') return 'badge-bad'
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
  errorMessage.value = '编排图登记入口尚未接入审批流'
}

async function readResult(response: Response): Promise<{ ok: boolean; message: string; entry?: Record<string, unknown> }> {
  const payload = await response.json().catch(() => ({}))
  return {
    ok: Boolean(payload.ok),
    message: String(payload.message ?? (response.ok ? '' : `接口返回 ${response.status}`)),
    entry: payload.entry,
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
      throw new Error('任务编排动作未生效，请稍后重试')
    }
    const result = await readResult(response)
    if (!result.ok) {
      errorMessage.value = result.message || '任务编排操作未通过校验'
      return
    }
    notice.value = result.message
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '任务编排操作失败'
  }
}

async function openPrecheck(row: Row) {
  errorMessage.value = ''
  notice.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/prechecks`, {
      method: 'POST',
      body: JSON.stringify({ 说明: '页面触发拓扑预检' }),
    })
    const result = await readResult(response)
    const entry = (result.entry ?? {}) as Record<string, unknown>
    const reasons = (entry['阻断原因'] as string[] | undefined) ?? []
    if (!result.ok) {
      errorMessage.value = reasons.length ? reasons.join('；') : result.message || '拓扑预检未通过'
    } else {
      notice.value = `拓扑预检通过：${JSON.stringify(entry['指标'] ?? {})}`
    }
    await reload()
    // 预检结果直接展开，阻断原因不用再去别处找。
    detail.value = {
      dagLabel: String(row['图编号'] ?? ''),
      view: (entry as unknown as PartitionView) ?? { 状态: '未预检' },
      decisions: (entry['版本决议'] as Decision[] | undefined) ?? [],
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '拓扑预检失败'
  }
}

async function precheckFromDetail() {
  const row = rows.value.find((item) => String(item['图编号']) === detail.value?.dagLabel)
  if (row) {
    await openPrecheck(row)
  }
}

async function publishRow(row: Row) {
  errorMessage.value = ''
  notice.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/publish`, {
      method: 'POST',
      body: JSON.stringify({ 发布说明: '页面发布拓扑快照' }),
    })
    const result = await readResult(response)
    if (!result.ok) {
      errorMessage.value = result.message || '拓扑快照发布未通过预检'
      return
    }
    notice.value = result.message
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '拓扑快照发布失败'
  }
}

async function openDetail(row: Row) {
  errorMessage.value = ''
  notice.value = ''
  try {
    const [dependencyResponse, mergeResponse] = await Promise.all([
      request(`${ENDPOINT}/${row.id}/partition-dependencies`),
      request(`${ENDPOINT}/${row.id}/merge`),
    ])
    if (!dependencyResponse.ok) {
      throw new Error('分区依赖数据读取失败')
    }
    const view = (await dependencyResponse.json()) as PartitionView
    const merge = mergeResponse.ok
      ? ((await mergeResponse.json()) as { 版本决议?: Decision[] })
      : { 版本决议: [] }
    detail.value = {
      dagLabel: String(row['图编号'] ?? ''),
      view,
      decisions: merge['版本决议'] ?? [],
    }
    if (view['状态'] !== '生效' && (view['阻断原因'] ?? []).length) {
      errorMessage.value = `分区依赖数据未生成：${(view['阻断原因'] ?? []).join('；')}`
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '分区依赖数据读取失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('编排图列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '任务编排列表读取失败'
  }
}

onMounted(reload)
</script>
