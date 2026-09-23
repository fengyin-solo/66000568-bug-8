<template>
  <div class="panel">
    <h4>⚡ 熔断器状态</h4>
    <div v-for="cb in breakers" :key="cb.taskId" class="cb-row" :class="rowClass(cb.state)">
      <span class="cb-task">{{ cb.taskId }}</span>
      <span class="cb-state">{{ stateLabel(cb.state) }}</span>
      <span class="cb-count">{{ cb.failureCount }} 次失败</span>
      <span v-if="cooldownLeft(cb) > 0" class="cb-cooldown">冷却 {{ cooldownLeft(cb).toFixed(1) }}s</span>
    </div>
    <div v-if="!breakers.length" class="empty">无熔断保护激活</div>
  </div>
</template>

<script setup lang="ts">
import { computed, ref, onMounted, onUnmounted } from 'vue'
import { useDAGStore } from '../store/dag'
import type { CircuitBreaker } from '../types'

const store = useDAGStore()
const breakers = computed(() => store.execution?.circuitBreakers || [])

// 本地每秒（实际 200ms）走字，驱动冷却倒计时；真实状态以服务端推送为准
const now = ref(Date.now() / 1000)
let timer: ReturnType<typeof setInterval>
onMounted(() => { timer = setInterval(() => { now.value = Date.now() / 1000 }, 200) })
onUnmounted(() => clearInterval(timer))

// 用服务端时间戳与接收时刻对齐，避免前后端时钟偏差导致倒计时错误
function cooldownLeft(cb: CircuitBreaker): number {
  const info = store.execution
  if (cb.state !== 'OPEN' || !info?.serverTime || !info.receivedAt) return 0
  const drift = now.value - info.receivedAt
  return Math.max(0, cb.cooldownUntil - info.serverTime - drift)
}

function rowClass(state: string): string {
  if (state === 'OPEN') return 'open'
  if (state === 'HALF_OPEN') return 'half-open'
  return 'closed'
}

function stateLabel(state: string): string {
  return { OPEN: '打开', HALF_OPEN: '观察中', CLOSED: '正常' }[state] ?? state
}
</script>
<style scoped>
.panel{background:#1a1a2e;border-radius:8px;padding:10px;border:1px solid #2a2a4a}
.panel h4{color:#f87171;font-size:12px;margin-bottom:6px}
.cb-row{display:flex;align-items:center;gap:8px;padding:4px 6px;border-radius:4px;font-size:11px;margin:2px 0}
.cb-row.open{background:#ef444415}
.cb-row.half-open{background:#fbbf2415}
.cb-task{color:#ccc;font-weight:600;min-width:80px}.cb-state{font-weight:700;min-width:42px}.cb-count{color:#888;font-size:10px}
.cb-cooldown{color:#f87171;font-size:10px;margin-left:auto;font-family:monospace}
.open .cb-state{color:#ef4444}.closed .cb-state{color:#22c55e}.half-open .cb-state{color:#fbbf24}
.empty{color:#4a5568;font-size:11px}
</style>
