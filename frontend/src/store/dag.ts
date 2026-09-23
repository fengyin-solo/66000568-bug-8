import { defineStore } from 'pinia'
import { ref } from 'vue'
import axios from 'axios'
import type { DAGWorkflow, ExecutionInfo } from '@/types'
export const useDAGStore = defineStore('dag', () => {
  const loading = ref(false)
  const workflow = ref<DAGWorkflow | null>(null)
  const execution = ref<ExecutionInfo | null>(null)
  const wsConnected = ref(false)
  const workers = ref(3)
  const strategy = ref('fifo')
  // 服务端时钟与本地时钟的差值，用于冷却倒计时按服务端时间计算
  const clockOffset = ref(0)

  let ws: WebSocket|null = null

  function applySnapshot(d: ExecutionInfo) {
    execution.value = d
    if (d.serverTime) clockOffset.value = d.serverTime - Date.now() / 1000
    // 刷新页面后只有执行快照、没有创建态工作流时，用快照里的工作流补齐画布
    if (!workflow.value) workflow.value = d.workflow
  }

  async function hydrate() {
    // 页面重新打开：先通过 REST 拿到最新执行快照，再补工作流，避免显示旧值
    try {
      const { data } = await axios.get<ExecutionInfo | null>('/api/state')
      if (data) applySnapshot(data)
    } catch {}
    if (!workflow.value) {
      try {
        const { data } = await axios.post('/api/workflow', { name: 'data-pipeline' })
        workflow.value = data
      } catch {}
    }
  }

  function connectWS() {
    ws = new WebSocket(`ws://${location.hostname}:8000/ws`)
    ws.onopen = () => { wsConnected.value = true }
    ws.onclose = () => { wsConnected.value = false }
    ws.onmessage = (e) => {
      try { applySnapshot(JSON.parse(e.data)) }
      catch {}
    }
  }

  async function createWorkflow(name: string) {
    loading.value = true
    try { const { data } = await axios.post('/api/workflow', { name }) ; workflow.value = data }
    finally { loading.value = false }
  }

  async function run() {
    if (!workflow.value) return
    loading.value = true
    try { const { data } = await axios.post('/api/run', { workflowId: workflow.value.id, workers: workers.value, strategy: strategy.value }) ; applySnapshot(data) }
    finally { loading.value = false }
  }

  function disconnectWS() { ws?.close(); ws = null }
  return { loading, workflow, execution, wsConnected, workers, strategy, clockOffset, connectWS, hydrate, createWorkflow, run, disconnectWS }
})
