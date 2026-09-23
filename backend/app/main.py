import asyncio, time, random, json, threading
from collections import defaultdict, deque
from typing import Optional
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI(title="DAG Workflow Engine")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

ACTIVE_CLIENTS = []
WORKFLOW_ID = 0

# 最近一次执行快照：供新连接的 WebSocket 补发、供刷新页面时 GET 恢复
LATEST_SNAPSHOT = None
# 执行序号：旧执行线程的推送不得覆盖新执行的快照
RUN_SEQ = 0

FAILURE_THRESHOLD = 3
COOLDOWN_SECONDS = 5
TICK_SECONDS = 0.3


class WorkflowCreate(BaseModel):
    name: str = "data-pipeline"

class RunRequest(BaseModel):
    workflowId: int
    workers: int = 3
    strategy: str = "fifo"
    failPolicy: Optional[dict] = None  # 可选：{taskId: 前N次调度强制失败}，用于测试/演示熔断流转


def generate_dag_workflow(name: str):
    """Create a realistic DAG pipeline"""
    nodes = [
        {"id": "extract", "name": "数据提取", "deps": [], "duration": 2.0},
        {"id": "validate", "name": "数据校验", "deps": ["extract"], "duration": 1.5},
        {"id": "clean_a", "name": "清洗分支A", "deps": ["validate"], "duration": 1.8},
        {"id": "clean_b", "name": "清洗分支B", "deps": ["validate"], "duration": 1.2},
        {"id": "transform", "name": "数据转换", "deps": ["clean_a"], "duration": 3.0},
        {"id": "enrich", "name": "数据增强", "deps": ["clean_a", "clean_b"], "duration": 2.0},
        {"id": "aggregate", "name": "聚合计算", "deps": ["transform", "enrich"], "duration": 2.5},
        {"id": "quality", "name": "质量检查", "deps": ["aggregate"], "duration": 1.0},
        {"id": "export_db", "name": "入库", "deps": ["quality"], "duration": 1.8},
        {"id": "export_report", "name": "报表生成", "deps": ["quality"], "duration": 2.2},
        {"id": "notify", "name": "通知", "deps": ["export_db", "export_report"], "duration": 0.5},
    ]
    positions = [
        (0, 0), (0, 1), (-1, 2), (1, 2), (-1, 3),
        (0.5, 3), (-0.3, 4), (-0.3, 5), (-1, 6), (0.5, 6), (-0.3, 7)
    ]
    for i, n in enumerate(nodes):
        n["x"] = positions[i][0] * 2.5 + 2.5
        n["y"] = positions[i][1] * 0.9
        n["status"] = "PENDING"
        n["retries"] = 0
        n["startTime"] = None
        n["endTime"] = None

    edges = []
    for n in nodes:
        for d in n["deps"]:
            edges.append([d, n["id"]])

    return {"nodes": [{
        "id": n["id"], "name": n["name"], "deps": n["deps"],
        "x": n["x"], "y": n["y"], "status": n["status"],
        "startTime": None, "endTime": None, "retries": n["retries"]
    } for n in nodes], "edges": edges, "durations": {n["id"]: n["duration"] for n in nodes}}


async def broadcast(payload: dict):
    """在主事件循环里向所有客户端推送同一份快照，并清理失效连接。"""
    text = json.dumps(payload)
    dead = []
    for ws in list(ACTIVE_CLIENTS):
        try:
            await ws.send_text(text)
        except Exception:
            dead.append(ws)
    for ws in dead:
        if ws in ACTIVE_CLIENTS:
            ACTIVE_CLIENTS.remove(ws)


@app.post("/api/workflow")
def create_workflow(req: WorkflowCreate):
    global WORKFLOW_ID
    WORKFLOW_ID += 1
    dag = generate_dag_workflow(req.name)
    return {"id": WORKFLOW_ID, "name": req.name, "nodes": dag["nodes"], "edges": dag["edges"],
            "_durations": dag["durations"]}


@app.get("/api/state")
def get_state():
    """返回最近一次执行快照，供页面重新打开后恢复到最新状态。"""
    return LATEST_SNAPSHOT


@app.post("/api/run")
async def run_workflow(req: RunRequest):
    global LATEST_SNAPSHOT, RUN_SEQ
    RUN_SEQ += 1
    run_seq = RUN_SEQ

    dag = generate_dag_workflow("workflow")
    payload = {
        "workflow": {"id": req.workflowId, "name": "workflow", "nodes": dag["nodes"], "edges": dag["edges"]},
        "logs": [], "circuitBreakers": [], "completed": False, "serverTime": time.time()
    }
    LATEST_SNAPSHOT = payload

    # 在 async 路由内拿到真正运行着的事件循环，交给工作线程做跨线程推送
    loop = asyncio.get_running_loop()
    t = threading.Thread(target=execute_workflow,
                         args=(dag, req.workers, req.strategy, loop, run_seq, req.failPolicy), daemon=True)
    t.start()
    return payload


def execute_workflow(dag, workers, strategy, loop, run_seq, fail_policy=None):
    """fail_policy: {taskId: 前N次调度强制失败}，仅用于测试"""
    nodes = dag["nodes"]
    durations = dag["durations"]
    edges = dag["edges"]
    in_degree = defaultdict(int)
    adj = defaultdict(list)
    for u, v in edges:
        in_degree[v] += 1
        adj[u].append(v)

    # BFS topological sort
    ready = deque([n["id"] for n in nodes if in_degree[n["id"]] == 0])
    node_map = {n["id"]: n for n in nodes}
    logs = []
    cb_state = defaultdict(lambda: {"failureCount": 0, "state": "CLOSED", "cooldownUntil": 0})
    running_tasks = {}
    completed = set()
    dispatch_count = defaultdict(int)

    def publish(completed_flag=False):
        global LATEST_SNAPSHOT
        payload = {
            "workflow": {"id": 1, "name": "workflow", "nodes": nodes, "edges": edges},
            "logs": logs[-30:],
            # 固定按 DAG 节点顺序输出，保证接口次序与页面明细一致
            "circuitBreakers": [
                {"taskId": n["id"], **cb_state[n["id"]]}
                for n in nodes if n["id"] in cb_state
            ],
            "completed": completed_flag,
            "serverTime": time.time()
        }
        # 只有最新一轮执行才能写快照/推送，避免旧线程覆盖新状态
        if run_seq == RUN_SEQ:
            LATEST_SNAPSHOT = payload
            future = asyncio.run_coroutine_threadsafe(broadcast(payload), loop)
            future.add_done_callback(lambda f: f.exception())  # 消费可能的异常
        time.sleep(TICK_SECONDS)

    while ready or running_tasks:
        # 本轮仍在冷却、暂不能调度的任务，放到一边，避免队首忙等阻塞其他任务
        blocked = []

        # Start tasks
        now = time.time()
        while ready and len(running_tasks) < workers:
            tid = ready.popleft()
            node = node_map[tid]
            cb = cb_state[tid]
            if cb["state"] == "OPEN":
                if now < cb["cooldownUntil"]:
                    blocked.append(tid)
                    continue
                # 冷却结束：OPEN -> HALF_OPEN，放入试探请求
                cb["state"] = "HALF_OPEN"
                logs.append({"taskId": tid, "status": "CIRCUIT_HALF_OPEN", "timestamp": time.time(),
                             "message": f"冷却结束，{node['name']} 进入观察中，放入试探请求"})

            node["status"] = "RUNNING"
            node["startTime"] = time.time()

            # Simulate task execution (random success/failure)
            dispatch_count[tid] += 1
            if fail_policy and dispatch_count[tid] <= fail_policy.get(tid, 0):
                will_fail = True
            else:
                will_fail = random.random() < 0.12  # 12% failure rate
            runtime = durations.get(tid, 1.5) * random.uniform(0.7, 1.3)
            running_tasks[tid] = {
                "end_time": time.time() + runtime,
                "will_fail": will_fail,
                "retries": node["retries"]
            }
            logs.append({"taskId": tid, "status": "RUNNING", "timestamp": time.time(), "message": f"开始执行 {node['name']}"})

        # 被冷却挡住的任务放回队首，下一轮（0.3s 后）再检查
        for tid in reversed(blocked):
            ready.appendleft(tid)

        # Check completed tasks
        now = time.time()
        finished = []
        for tid, info in running_tasks.items():
            if now >= info["end_time"]:
                node = node_map[tid]
                cb = cb_state[tid]
                if info["will_fail"] and node["retries"] < 3:
                    node["retries"] += 1
                    cb["failureCount"] += 1
                    if cb["state"] == "HALF_OPEN":
                        # 观察中试探失败：重新 OPEN，再走一轮冷却
                        cb["failureCount"] = FAILURE_THRESHOLD
                        cb["state"] = "OPEN"
                        cb["cooldownUntil"] = now + COOLDOWN_SECONDS
                        node["status"] = "CIRCUIT_OPEN"
                        logs.append({"taskId": tid, "status": "CIRCUIT_OPEN", "timestamp": now,
                                     "message": f"试探失败，{node['name']} 重新熔断，冷却{COOLDOWN_SECONDS}s"})
                    elif cb["failureCount"] >= FAILURE_THRESHOLD:
                        # 连续失败达到阈值：CLOSED -> OPEN
                        cb["state"] = "OPEN"
                        cb["cooldownUntil"] = now + COOLDOWN_SECONDS
                        node["status"] = "CIRCUIT_OPEN"
                        logs.append({"taskId": tid, "status": "CIRCUIT_OPEN", "timestamp": now,
                                     "message": f"熔断! {FAILURE_THRESHOLD}次连续失败，冷却{COOLDOWN_SECONDS}s"})
                    else:
                        node["status"] = "PENDING"
                        logs.append({"taskId": tid, "status": "FAILED", "timestamp": now, "message": f"重试 {node['retries']}/3"})
                    ready.appendleft(tid)
                else:
                    node["status"] = "SUCCESS"
                    node["endTime"] = now
                    completed.add(tid)
                    # 恢复正常：HALF_OPEN/CLOSED -> CLOSED，失败计数归零
                    cb["failureCount"] = 0
                    cb["state"] = "CLOSED"
                    cb["cooldownUntil"] = 0
                    gave_up = info["will_fail"]
                    msg = f"重试耗尽，放行 {node['name']}" if gave_up else f"完成 {node['name']}"
                    logs.append({"taskId": tid, "status": "SUCCESS", "timestamp": now, "message": msg})
                    for next_tid in adj[tid]:
                        in_degree[next_tid] -= 1
                        if in_degree[next_tid] == 0:
                            ready.append(next_tid)
                finished.append(tid)

        for tid in finished:
            del running_tasks[tid]

        publish(False)
        if len(completed) == len(nodes):
            break

    publish(True)


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    ACTIVE_CLIENTS.append(ws)
    # 新连接（含刷新页面后重连）立即补发最近一次快照，避免只看到旧值/空值
    if LATEST_SNAPSHOT is not None:
        try:
            await ws.send_text(json.dumps(LATEST_SNAPSHOT))
        except Exception:
            if ws in ACTIVE_CLIENTS:
                ACTIVE_CLIENTS.remove(ws)
            return
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        if ws in ACTIVE_CLIENTS:
            ACTIVE_CLIENTS.remove(ws)
