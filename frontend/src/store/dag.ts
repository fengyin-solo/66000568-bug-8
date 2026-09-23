import { defineStore } from 'pinia'
import { ref } from 'vue'
import axios from 'axios'
import type { DAGWorkflow, ExecutionInfo } from '@/types'

function stamped(d: ExecutionInfo): ExecutionInfo {
  return { ...d, receivedAt: Date.now() / 1000 }
}

export const useDAGStore = defineStore('dag', () => {
  const loading = ref(false)
  const workflow = ref<DAGWorkflow | null>(null)
  const execution = ref<ExecutionInfo | null>(null)
  const wsConnected = ref(false)
  const workers = ref(3)
  const strategy = ref('fifo')

  let ws: WebSocket|null = null
  let reconnectTimer: ReturnType<typeof setTimeout>|null = null

  function connectWS() {
    ws = new WebSocket(`ws://${location.hostname}:8000/ws`)
    ws.onopen = () => {
      wsConnected.value = true
      // 断线重连后，若已有执行记录则补取最新快照
      if (workflow.value && !execution.value) {
        loadExecution(workflow.value.id)
      }
    }
    ws.onmessage = (e) => {
      try { execution.value = stamped(JSON.parse(e.data)) } catch {}
    }
    ws.onclose = () => {
      wsConnected.value = false
      // 自动重连，保证刷新/短暂断连后仍能收到补发的最新状态
      if (!reconnectTimer) {
        reconnectTimer = setTimeout(() => { reconnectTimer = null; connectWS() }, 1000)
      }
    }
  }

  async function loadExecution(id: number) {
    try {
      const { data } = await axios.get(`/api/execution/${id}`)
      if (data.workflow) execution.value = stamped(data)
    } catch {}
  }

  // 重新打开页面时恢复上次工作流及其最新执行状态，而不是空白/旧值
  async function restoreLatest() {
    const id = Number(localStorage.getItem('lastWorkflowId') || 0)
    if (!id) return
    try {
      const { data } = await axios.get(`/api/execution/${id}`)
      if (data.workflow) {
        workflow.value = data.workflow
        execution.value = stamped(data)
      }
    } catch {}
  }

  async function createWorkflow(name: string) {
    loading.value = true
    try {
      const { data } = await axios.post('/api/workflow', { name })
      workflow.value = data
      execution.value = null
      localStorage.setItem('lastWorkflowId', String(data.id))
    }
    finally { loading.value = false }
  }

  async function run() {
    if (!workflow.value) return
    loading.value = true
    try {
      const { data } = await axios.post('/api/run', {
        workflowId: workflow.value.id, workers: workers.value, strategy: strategy.value
      })
      execution.value = stamped(data)
      localStorage.setItem('lastWorkflowId', String(workflow.value.id))
    } finally { loading.value = false }
  }

  function disconnectWS() {
    if (reconnectTimer) { clearTimeout(reconnectTimer); reconnectTimer = null }
    ws?.close(); ws = null
  }
  return { loading, workflow, execution, wsConnected, workers, strategy, connectWS, createWorkflow, run, loadExecution, restoreLatest, disconnectWS }
})
