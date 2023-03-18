import json
import logging
import time
from typing import Dict
import requests
import uvicorn
from fastapi import FastAPI, HTTPException

app = FastAPI()

# 设置日志记录器
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_tasks: Dict[str, Dict] = {
    'task_id': {
        'task_node': {
            'task_id': 'task_id',  # 任务id
            'task_node': 'task_node',  # 任务节点
            'task_name': 'task_name',  # 任务名称
            'task_type': 'task_type',  # 任务类型
            'task_type_name': 'task_type_name',  # 任务类型名称
            'task_priority': 'task_priority',  # 任务优先级
            'task_destination': 'task_destination',  # 任务目的地
            'task_status': 'task_status',  # 任务状态
            'task_progress': 'task_progress',  # 任务进度
            'task_start_time': 'task_start_time',  # 任务开始时间
            'task_end_time': 'task_end_time',  # 任务结束时间
        }
    }
}

_node_ip = {
    '0': 'http://34.135.240.45',
    '1': 'http://34.170.128.54',
    '2': 'http://34.30.67.124',
    '3': 'http://34.28.200.46',
}


@app.get("/")
async def root():
    try:
        # 从json文件中读取更新task
        with open('tasks.json', 'r') as f:
            file_tasks = json.load(f)
        for task_id in file_tasks:
            _tasks[task_id] = file_tasks[task_id]
    except Exception as e:
        logger.warning(f"Failed to read tasks.json: {e}")
    return {"message": "Hello World"}


# 边缘节点创建任务
@app.get("/task/init/{task_type_name}/{task_id}/{task_name}/{priority}")
async def init_task(task_type_name: str, task_id: str, task_name: str, priority: str):
    logger.info(f"Received task {task_id} from cloud node，start task planning and reassignment.")
    for i in range(4):
        try:
            response = requests.request('GET', f"{_node_ip[i]}/task/init/{task_type_name}/{str(i)}/"
                                               f"{task_name}/{priority}")
            if response.status_code == 200:
                logger.info(f"Task {task_id}  assigned to node {str(i)}")
                _tasks[task_id][str[i]] = {"task_id": task_id, "task_node": str[i],
                                           "task_name": task_name, "task_type_name": task_type_name,
                                           "task_priority": priority, "task_status": "created",
                                           "creat_time": (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))}
            else:
                logger.warning(f"Task {task_id} failed to assign to node {str(i)}, response: {response.text}")
                raise HTTPException(status_code=404, detail=f"Task {task_id} failed to assign to node {str(i)}")
        except Exception as e:
            logger.error(f"Task {task_id} failed to assign to node {str(i)}: {e}")
            raise HTTPException(status_code=404, detail=f"Task {task_id} failed to assign to node {str(i)}")
    with open('tasks.json', 'w') as f:
        json.dump(_tasks, f)
    return {"message": "Task assigned"}


# 查询任务状态
@app.get("/task/status/{task_id}")
async def get_task_status(task_id: str):
    if task_id in _tasks:
        return {_tasks[task_id]}
    else:
        return {'message': 'task_id does not exist'}


# 边缘节点停止任务
@app.get("/task/stop/{task_id}")
async def edge_stop_task(task_id: str):
    logger.info(f"Received stop task {task_id} from cloud node")
    for i in range(4):
        try:
            response = requests.request('GET', f"{_node_ip[i]}/task/end/{task_id}")
            if response.status_code == 200:
                _tasks[task_id][str(i)]['task_status'] = 'end'
                _tasks[task_id][str(i)]['task_end_time'] = (
                    time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
                logger.info(f"Task {task_id} node {str(i)} stopped successfully")
            else:
                logger.warning(f'Stop task {task_id} node {str(i)} is failed')
                raise HTTPException(status_code=404, detail=f"Stop task {task_id} node {str(i)} is failed")
        except Exception as e:
            logger.error(f'Stop task {task_id} is error, error: {e}')
            raise HTTPException(status_code=404, detail=f"Stop task {task_id} is error")

    with open('tasks.json', 'w') as f:
        json.dump(_tasks, f)
    return {"message": "Task stopped"}


# 边缘节点处理数据
@app.get("/task/process/{task_id}/{node_id}/{image_num}")
async def edge_process_data(task_id: str, node_id: str, image_num: str):
    logger.info(f"Received process data {task_id} from node {node_id}, start processing data")
    time.sleep(2)
    try:
        response = requests.request('GET', f"http://35.228.80.43/task/process/{task_id}/edge/{image_num}")
        if response.status_code == 200:
            logger.info(f"Task {task_id} node {node_id} process data upload cloud node successfully")
        else:
            logger.warning(f"Task {task_id} node {node_id} process data upload cloud node failed")
            raise HTTPException(status_code=404, detail=f"Task {task_id} node {node_id} "
                                                        f"process data upload cloud node failed, "
                                                        f"warning: {response.text}")
    except Exception as e:
        logger.error(f"Task {task_id} node {node_id} process data upload cloud node failed, error: {e}")
        raise HTTPException(status_code=404, detail=f"Task {task_id} node {node_id} "
                                                    f"process data upload cloud node failed, error: {e}")
    return {"message": "Task data processed"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
