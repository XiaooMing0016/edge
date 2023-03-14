import asyncio
import logging
import requests
import uvicorn
from fastapi import FastAPI, HTTPException, UploadFile, File

from task import reconnaissance_task, tasks, image_process, track_task

app = FastAPI()

# 设置日志记录器
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

node_ip = {
    '0': 'http://34.135.240.45',
    '1': 'http://34.170.128.54',
    '2': 'http://34.30.67.124',
    '3': 'http://34.28.200.46',
}


@app.get("/")
async def root():
    return {"message": "Hello World"}


# 创建任务

@app.get("/task/init/{destination_type}/{task_name}/{task_id}/{i}")
async def init_task(destination_type: str, task_name: str, task_id: str, i: str):
    if destination_type == 'master':
        destination = 'http://35.228.80.43'
    elif destination_type == 'edge':
        destination = 'http://34.130.234.56'
    tasks[task_id] = {"status": "created"}
    if task_name == 'reconnaissance':
        asyncio.create_task(reconnaissance_task(task_id, destination, i))
    elif task_name == 'track':
        asyncio.create_task(track_task(task_id, destination, i))
    else:
        raise HTTPException(status_code=404, detail=f"Task {task_name} not found")
    # 记录日志
    logger.info(f"Created reconnaissance task {task_id} for {destination}")
    # 返回任务id和任务创建状态
    return {"task_id": task_id, "status": "init"}


# 边缘节点创建任务
@app.get("/task/init/{destination_type}/{task_name}/{task_id}")
async def init_task(destination_type: str, task_name: str, task_id: str):
    for i in range(4):
        url = f"{node_ip[str(i)]}/task/init/{destination_type}/{task_name}/{task_id}/{str(i)}"
        response = requests.request("GET", url)
        logger.info(f"Created reconnaissance task {task_id} for {destination_type} node {str(i)}")
        logger.info(f'url: {url}, response: {response.text}')


# 查看侦查任务状态
@app.get("/task/status/{task_id}")
async def status_task(task_id: str):
    if task_id not in tasks:
        # 如果任务不存在，则记录日志并返回HTTP404错误
        logger.error(f"Task {task_id} not found")
        raise HTTPException(status_code=404, detail=f"Task {task_id} not found")
    # 如果任务存在，则返回任务状态
    return tasks[task_id]

# 边缘节点停止任务
@app.get("/task/stop/{task_id}")
async def edge_stop_task(task_id: str):
    for i in range(4):
        response = requests.request("GET", f"{node_ip[str(i)]}/task/stop/{task_id}/{str(i)}")
        logger.info(f"Stopped reconnaissance task {task_id} for node {str(i)}")
        logger.info(f'url: f"{node_ip[str(i)]}/task/stop/{task_id}/{str(i)}", response: {response.text}')


# 停止任务
@app.get("/task/stop/{task_id}/{i}")
async def stop_task(task_id: str):
    if task_id not in tasks:
        # 如果任务不存在，则记录日志并返回HTTP404错误
        logger.error(f"Task {task_id} not found")
        raise HTTPException(status_code=404, detail=f"Task {task_id} not found")
    # 如果任务存在，则将任务状态设为"stopped"，记录日志，并返回成功消息
    tasks[task_id]["status"] = "stopped"
    logger.info(f"Stopped reconnaissance task {task_id}")
    return {"message": f"Task {task_id} stopped"}


# 接受图片并储存，并进行压缩处理，随后发送图片至云节点
@app.post("/image/{task_id}")
async def receive_image(task_id: str, image: UploadFile = File(...)):
    # 图片处理
    asyncio.create_task(image_process(task_id, image))
    # 返回成功消息
    return {"message": f"Image {task_id}_image.jpg received"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
