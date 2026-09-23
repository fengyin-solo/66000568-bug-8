import asyncio, time, random, json, threading, os
from collections import defaultdict, deque
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI(title="DAG Workflow Engine")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

ACTIVE_CLIENTS = []
WORKFLOW_ID = 0
MAIN_LOOP = None
# workflowId -> 最近一次状态快照，供 HTTP 重放与新 WS 连接补发
LATEST_EXECUTION: dict = {}

FAILURE_THRESHOLD = 3
COOLDOWN_SECONDS = 5
TICK_SECONDS = 0.3

class WorkflowCreate(BaseModel):
    name: str = "data-pipeline"

class RunRequest(BaseModel):
    workflowId: int
    workers: int = 3
    strategy: str = "fifo"


@app.on_event("startup")
def _capture_event_loop():
    # uvicorn 在主线程中启动事件循环；工作线程需要借它向 WS 客户端推送
    global MAIN_LOOP
    MAIN_LOOP = asyncio.get_event_loop()


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


def new_circuit_breaker():
    return {"failureCount": 0, "state": "CLOSED", "cooldownUntil": 0.0, "touched": False}


def cb_record_failure(cb: dict, now: float):
    """CLOSED 连续失败达阈值 -> OPEN；HALF_OPEN 试探失败 -> 重新 OPEN 并重置冷却。"""
    cb["touched"] = True
    cb["failureCount"] += 1
    if cb["state"] == "HALF_OPEN":
        cb["state"] = "OPEN"
        cb["cooldownUntil"] = now + COOLDOWN_SECONDS
    elif cb["failureCount"] >= FAILURE_THRESHOLD:
        cb["state"] = "OPEN"
        cb["cooldownUntil"] = now + COOLDOWN_SECONDS


def cb_record_success(cb: dict):
    """成功（含 HALF_OPEN 试探成功）-> CLOSED，计数归零。"""
    cb["failureCount"] = 0
    cb["state"] = "CLOSED"
    cb["cooldownUntil"] = 0.0


@app.post("/api/workflow")
def create_workflow(req: WorkflowCreate):
    global WORKFLOW_ID
    WORKFLOW_ID += 1
    dag = generate_dag_workflow(req.name)
    return {"id": WORKFLOW_ID, "name": req.name, "nodes": dag["nodes"], "edges": dag["edges"],
            "_durations": dag["durations"]}


def build_payload(workflow_id, name, nodes, edges, logs, cb_state, node_order, completed_flag):
    # 熔断器固定按 DAG 节点顺序输出，保证接口与页面次序一致；只暴露展示所需字段
    breakers = [{
        "taskId": tid,
        "failureCount": cb_state[tid]["failureCount"],
        "state": cb_state[tid]["state"],
        "cooldownUntil": cb_state[tid]["cooldownUntil"],
    } for tid in node_order if cb_state[tid]["touched"]]
    return {
        "workflow": {"id": workflow_id, "name": name, "nodes": nodes, "edges": edges},
        "logs": logs[-30:],
        "circuitBreakers": breakers,
        "completed": completed_flag,
        "serverTime": time.time(),
    }


@app.get("/api/execution/{workflow_id}")
def get_execution(workflow_id: int):
    """重放最近一次执行快照，刷新页面也能看到最新状态。"""
    payload = LATEST_EXECUTION.get(workflow_id)
    if payload is None:
        return {"workflow": None, "logs": [], "circuitBreakers": [],
                "completed": False, "serverTime": time.time()}
    return payload


@app.post("/api/run")
def run_workflow(req: RunRequest):
    dag = generate_dag_workflow("workflow")
    initial = {
        "workflow": {"id": req.workflowId, "name": "workflow", "nodes": dag["nodes"], "edges": dag["edges"]},
        "logs": [], "circuitBreakers": [], "completed": False, "serverTime": time.time(),
    }
    LATEST_EXECUTION[req.workflowId] = initial
    t = threading.Thread(target=execute_workflow,
                         args=(dag, req.workers, req.strategy, req.workflowId), daemon=True)
    t.start()
    return initial


def broadcast(payload):
    if MAIN_LOOP is None:
        return
    dead = []
    for ws in ACTIVE_CLIENTS:
        try:
            asyncio.run_coroutine_threadsafe(ws.send_text(json.dumps(payload)), MAIN_LOOP)
        except Exception:
            dead.append(ws)
    for ws in dead:
        if ws in ACTIVE_CLIENTS:
            ACTIVE_CLIENTS.remove(ws)


def execute_workflow(dag, workers, strategy, workflow_id=1):
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
    node_order = [n["id"] for n in nodes]
    logs = []
    cb_state = {tid: new_circuit_breaker() for tid in node_order}
    running_tasks = {}
    completed = set()

    def send_update(completed_flag=False):
        payload = build_payload(workflow_id, "workflow", nodes, edges,
                                logs, cb_state, node_order, completed_flag)
        LATEST_EXECUTION[workflow_id] = payload
        broadcast(payload)

    def release_downstream(tid):
        for next_tid in adj[tid]:
            in_degree[next_tid] -= 1
            if in_degree[next_tid] == 0:
                ready.append(next_tid)

    while ready or running_tasks:
        now = time.time()

        # Start tasks. 熔断 OPEN 且仍在冷却中的任务本轮跳过（排到队尾），
        # 不阻塞同批其他可执行任务，也不再忙等空转。
        blocked = []
        while ready and len(running_tasks) < workers:
            tid = ready.popleft()
            node = node_map[tid]
            cb = cb_state[tid]

            if cb["state"] == "OPEN" and now < cb["cooldownUntil"]:
                blocked.append(tid)
                continue
            if cb["state"] == "OPEN":
                # 冷却结束 -> 进入观察中，放行一次试探
                cb["state"] = "HALF_OPEN"
                logs.append({"taskId": tid, "status": "CIRCUIT_HALF_OPEN", "timestamp": now,
                             "message": f"冷却结束，进入观察中，试探执行 {node['name']}"})

            node["status"] = "RUNNING"
            node["startTime"] = now

            # Simulate task execution (random success/failure)
            failure_rate = float(os.environ.get("CB_FAILURE_RATE", "0.12"))
            will_fail = random.random() < failure_rate
            runtime = durations.get(tid, 1.5) * random.uniform(0.7, 1.3)
            running_tasks[tid] = {
                "end_time": now + runtime,
                "will_fail": will_fail,
            }
            logs.append({"taskId": tid, "status": "RUNNING", "timestamp": now,
                         "message": f"开始执行 {node['name']}"})
        ready.extend(blocked)

        # Check completed tasks
        now = time.time()
        finished = []
        for tid, info in running_tasks.items():
            if now < info["end_time"]:
                continue
            node = node_map[tid]
            cb = cb_state[tid]

            if info["will_fail"]:
                was_open = cb["state"] == "OPEN"
                cb_record_failure(cb, now)
                opened_now = cb["state"] == "OPEN" and not was_open

                if node["retries"] < 3:
                    node["retries"] += 1
                    node["status"] = "PENDING"
                    ready.appendleft(tid)
                    logs.append({"taskId": tid, "status": "FAILED", "timestamp": now,
                                 "message": f"执行失败，重试 {node['retries']}/3（连续失败 {cb['failureCount']} 次）"})
                    if opened_now:
                        logs.append({"taskId": tid, "status": "CIRCUIT_OPEN", "timestamp": now,
                                     "message": f"熔断打开! 连续失败达到 {FAILURE_THRESHOLD} 次，冷却 {COOLDOWN_SECONDS}s"})
                else:
                    # 重试用尽。若这次失败是冷却结束后的观察试探，再给最后一次
                    # 试探机会（新的冷却周期后）；试探成功仍可正常恢复，
                    # 再次失败才彻底放弃，保证流程不会无限等待
                    if cb["state"] == "OPEN" and not cb.get("probeUsed"):
                        cb["probeUsed"] = True
                        node["status"] = "PENDING"
                        ready.appendleft(tid)
                        logs.append({"taskId": tid, "status": "FAILED", "timestamp": now,
                                     "message": f"已达最大重试次数 {node['retries']}/3，冷却后将再试探一次 {node['name']}"})
                    else:
                        node["status"] = "FAILED"
                        node["endTime"] = now
                        completed.add(tid)
                        logs.append({"taskId": tid, "status": "FAILED", "timestamp": now,
                                     "message": f"试探仍失败，放弃 {node['name']}"})
                        release_downstream(tid)
            else:
                node["status"] = "SUCCESS"
                node["endTime"] = now
                completed.add(tid)
                was_half_open = cb["state"] == "HALF_OPEN"
                cb_record_success(cb)
                logs.append({"taskId": tid, "status": "SUCCESS", "timestamp": now,
                             "message": f"{'观察试探通过，熔断器恢复正常；' if was_half_open else ''}完成 {node['name']}"})
                release_downstream(tid)

            finished.append(tid)

        for tid in finished:
            del running_tasks[tid]

        send_update()
        if len(completed) == len(nodes):
            break

        # 有运行中的任务时按最快结束时间唤醒；否则按最早冷却结束时间唤醒（最多 TICK 秒）
        if running_tasks:
            wake = min(i["end_time"] for i in running_tasks.values()) - time.time()
        else:
            cooldown_waits = [cb_state[tid]["cooldownUntil"] - time.time()
                              for tid in ready if cb_state[tid]["state"] == "OPEN"]
            wake = min(cooldown_waits) if cooldown_waits else TICK_SECONDS
        time.sleep(min(TICK_SECONDS, max(0.05, wake)))

    send_update(True)


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    ACTIVE_CLIENTS.append(ws)
    # 新连接（含刷新页面后重连）立即补发最近一次快照
    if LATEST_EXECUTION:
        latest = LATEST_EXECUTION[max(LATEST_EXECUTION.keys())]
        try:
            await ws.send_text(json.dumps(latest))
        except Exception:
            pass
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        if ws in ACTIVE_CLIENTS:
            ACTIVE_CLIENTS.remove(ws)
