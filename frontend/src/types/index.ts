export interface TaskNode { id: string; name: string; deps: string[]; x: number; y: number; status: string; startTime?: number; endTime?: number; retries: number }
export interface DAGWorkflow { id: number; name: string; nodes: TaskNode[]; edges: [string,string][] }
export interface ExecutionLog { taskId: string; status: string; timestamp: number; message: string }
export interface CircuitBreaker { taskId: string; failureCount: number; state: string; cooldownUntil: number }
export interface ExecutionInfo { workflow: DAGWorkflow; logs: ExecutionLog[]; circuitBreakers: CircuitBreaker[]; completed: boolean; serverTime?: number }
