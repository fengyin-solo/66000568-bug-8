<template>
  <div class="panel">
    <h4>⚡ 熔断器状态</h4>
    <div v-for="cb in breakers" :key="cb.taskId" class="cb-row" :class="stateClass(cb.state)">
      <span class="cb-task">{{ cb.taskId }}</span>
      <span class="cb-state">{{ stateLabel(cb.state) }}</span>
      <span class="cb-count" v-if="cb.state === 'OPEN'">冷却 {{ remaining(cb.cooldownUntil) }}s</span>
      <span class="cb-count" v-else-if="cb.state === 'HALF_OPEN'">试探中 · {{ cb.failureCount }} 次失败</span>
      <span class="cb-count" v-else>{{ cb.failureCount }} 次失败</span>
    </div>
    <div v-if="!breakers.length" class="empty">无熔断保护激活</div>
  </div>
</template>

<script setup lang="ts">
import { computed, ref, onMounted, onUnmounted } from 'vue'
import { useDAGStore } from '../store/dag'
import type { CircuitBreaker } from '@/types'
const store = useDAGStore()
const breakers = computed(() => store.execution?.circuitBreakers || [])

// 心跳：驱动冷却倒计时逐秒走动（服务器时间 + 时钟偏移换算）
const now = ref(Date.now() / 1000)
let timer: number | undefined
onMounted(() => { timer = window.setInterval(() => { now.value = Date.now() / 1000 }, 250) })
onUnmounted(() => { clearInterval(timer) })

function remaining(cooldownUntil: number) {
  return Math.max(0, Math.ceil(cooldownUntil - (now.value + store.clockOffset)))
}

function stateLabel(state: string) {
  return { OPEN: '打开', HALF_OPEN: '观察中', CLOSED: '正常' }[state] || state
}
function stateClass(state: string) { return state.toLowerCase() }
</script>
<style scoped>
.panel{background:#1a1a2e;border-radius:8px;padding:10px;border:1px solid #2a2a4a}
.panel h4{color:#f87171;font-size:12px;margin-bottom:6px}
.cb-row{display:flex;gap:8px;padding:4px 6px;border-radius:4px;font-size:11px;margin:2px 0}
.cb-row.open{background:#ef444415}
.cb-row.half_open{background:#fbbf2415}
.cb-task{color:#ccc;font-weight:600}.cb-state{font-weight:700}.cb-count{color:#888;font-size:10px}
.open .cb-state{color:#ef4444}.closed .cb-state{color:#22c55e}.half_open .cb-state{color:#fbbf24}
.empty{color:#4a5568;font-size:11px}
</style>
