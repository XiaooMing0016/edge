import asyncio
import concurrent
import datetime
import logging
import time
from typing import Dict

import aiohttp

# 存储所有任务状态的字典
tasks: Dict[str, Dict] = {}

# 设置日志记录器
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# 侦查任务
async def reconnaissance_task(task_id: str, destination: str, node_id) -> None:
    logger.info(f'创建侦查任务{task_id}')
    tasks[task_id] = {"status": "running"}

    with concurrent.futures.ThreadPoolExecutor() as executor:
        for i in range(360):
            if task_id not in tasks or tasks[task_id]["status"] == "stopped":
                logger.info(f'任务{task_id}被停止')
                break
            # 每1s发送一张图片
            try:
                time.sleep(10)
                logger.info(f'任务{task_id}开始发送图片,第{i}张,time:{datetime.datetime.now().isoformat()}')
                await asyncio.get_event_loop().run_in_executor(executor, send_image, destination, task_id, str(i), node_id)
                logger.info(f'任务{task_id}发送图片成功,第{i}张,time:{datetime.datetime.now().isoformat()}')

            except Exception as e:
                logger.error(f'task：{task_id},发送图片时出错：{str(e)}')
                continue

            # 更新任务状态
            tasks[task_id]["status"] = "running"
            tasks[task_id]["last_update"] = datetime.datetime.now().isoformat()

    if task_id in tasks:
        tasks[task_id]["status"] = "completed"

    logger.info(f'任务{task_id}执行完毕')
    return None


# 异步发送图片
async def send_image(url: str, task_id: str, _id: str, node_id: str) -> None:
    async with aiohttp.ClientSession() as session:
        with open('image/image.jpg', 'rb') as file:
            image = file.read()
            async with session.post(url + '/image/' + task_id + "_" + _id + "_" + node_id, data={'image': image}) as response:
                response_status = response.status
                response_text = await response.text()
                logger.info(f'发送图片，返回状态码{response_status}，返回内容{response_text}')


# 图片处理
async def image_process(task_id: str, image) -> None:
    # 压缩图片50%
    image.thumbnail((image.width // 2, image.height // 2))
    # 保存图片到reimage文件夹
    with open(f"reimage/{task_id}_image.jpg", "wb") as buffer:
        buffer.write(await image.read())
    # 上传图片到云节点
    await send_image("http://35.228.80.43", task_id, '')
    # 删除已上传的图片
    os.remove(f"reimage/{task_id}_image.jpg")


# 创建追踪任务
async def track_task(task_id: str, destination: str, node_id: str) -> None:
    pass
