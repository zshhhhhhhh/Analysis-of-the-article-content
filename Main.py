# -*- coding: utf-8 -*-
"""
基于 Redis 可靠队列的多线程爬虫消费者
"""

from __future__ import annotations

import json
import logging
import datetime
import signal
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Optional, Dict, Any

import redis
import requests
from lxml import etree

from TQ9 import TQ, retry_on_network_error, NonRetryableError

# ==================== 日志配置 ====================
start_time = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
log_filename = f'parser_{start_time}.log'

COLORS = {
    'INFO': '\033[97m',
    'WARNING': '\033[93m',
    'ERROR': '\033[91m',
    'SUCCESS': '\033[92m',
    'RESET': '\033[0m'
}

SUCCESS_LEVEL = 25
logging.addLevelName(SUCCESS_LEVEL, 'SUCCESS')


def _success(self, message, *args, **kwargs):
    if self.isEnabledFor(SUCCESS_LEVEL):
        self._log(SUCCESS_LEVEL, message, args, **kwargs)


logging.Logger.success = _success


class ColorFormatter(logging.Formatter):
    def format(self, record):
        color = COLORS.get(record.levelname, COLORS['RESET'])
        msg = super().format(record)
        return f"{color}{msg}{COLORS['RESET']}"


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

file_handler = logging.FileHandler(log_filename, encoding='utf-8')
file_handler.setFormatter(logging.Formatter('%(asctime)s [%(threadName)s] %(levelname)s: %(message)s'))
logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setFormatter(ColorFormatter('%(asctime)s [%(threadName)s] %(levelname)s: %(message)s'))
logger.addHandler(console_handler)


# ============================== 统一的请求配置 ==============================

COMMON_HEADERS = {} # 自定义 headers

COMMON_COOKIES = {} # 自定义 cookie

BASE_DIR = r"D:\KPS_CC\aaa标准"  # 自定义本地存储


# ============================== Redis配置 ==============================

@dataclass(frozen=True)
class QueueConfig:
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 1

    pending_key: str = ""  # 待提取列表
    processing_key: str = ""  # 正在执行列表
    processing_meta_key: str = ""   # 正在执行hash列表 Hash: tracking_id -> {raw, taken_at}  

    # 死信队列按失败阶段拆分
    dead_fetch_key: str = ""     # 请求阶段失败，重试耗尽 / 不可重试
    dead_parse_key: str = ""     # 解析阶段失败，重试耗尽
    dead_other_key: str = ""     # 持久化等其他阶段失败，重试耗尽

    brpoplpush_timeout_sec: int = 5
    max_retries: int = 3

    visibility_timeout_sec: float = 300.0   # 超过这么久没 ack/fail，视为 worker 卡死，reaper 回收
    reaper_interval_sec: float = 30.0

CONFIG = QueueConfig()


# ============================== 可靠队列封装 ==============================

class ReliableRedisQueue:
    

    ERROR_TYPE_TO_DEAD_KEY = {
        "fetch_error": "dead_fetch_key",
        "parse_error": "dead_parse_key",
    }  # 其余未匹配到的 error_type 一律落到 dead_other_key

    def __init__(self, r: redis.Redis, cfg: QueueConfig = CONFIG):
        self.r = r
        self.cfg = cfg

    def push(self, task: str | Dict[str, Any]) -> None:
        """支持直接传入 URL 字符串或字典，字典将序列化为 JSON。"""
        if isinstance(task, dict):
            task_str = json.dumps(task, ensure_ascii=False)
        else:
            task_str = task
        self.r.lpush(self.cfg.pending_key, task_str)

    def pop(self, worker_id: str) -> Optional[Dict[str, Any]]:
        raw = self.r.brpoplpush(
            self.cfg.pending_key, self.cfg.processing_key,
            timeout=self.cfg.brpoplpush_timeout_sec,
        )
        if raw is None:
            return None

        raw_str = raw.decode("utf-8") if isinstance(raw, bytes) else raw
        try:
            task = json.loads(raw_str)
            if not isinstance(task, dict):
                raise ValueError("任务 JSON 不是字典")
        except (json.JSONDecodeError, ValueError):
            # 旧版纯 URL 字符串，自动包装
            task = {"url": raw_str}
            logger.warning("队列中存在非 JSON 数据，已包装为任务: %s", raw_str)

        if "url" not in task:
            logger.error("任务格式无效（无 url 字段）: %s", raw_str)
            self.r.lrem(self.cfg.processing_key, 1, raw_str)
            return None

        tracking_id = uuid.uuid4().hex
        task["_raw"] = raw_str
        task.setdefault("_retry_count", 0)
        task["_tracking_id"] = tracking_id

        meta = json.dumps({
            "worker_id": worker_id,
            "taken_at": time.time(),
            "raw": raw_str,
        }, ensure_ascii=False)
        self.r.hset(self.cfg.processing_meta_key, tracking_id, meta)
        return task

    def ack(self, task: Dict[str, Any]) -> None:
        """确认处理完成，从 processing 中移除并清掉 meta。"""
        raw_str = task.get("_raw", task.get("url"))
        tracking_id = task.get("_tracking_id")

        pipe = self.r.pipeline()
        pipe.lrem(self.cfg.processing_key, 1, raw_str)
        if tracking_id:
            pipe.hdel(self.cfg.processing_meta_key, tracking_id)
        pipe.execute()

    def fail(self, task: Dict[str, Any], error_type: str = "other_error",
              reason: str = "", force_dead: bool = False) -> None:
        """
        处理失败。
        error_type 建议传 "fetch_error" / "parse_error" / "other_error"，
        决定耗尽重试后分流进哪个死信队列；未识别的 error_type 统一落到
        dead_other_key。
        force_dead=True 用于 NonRetryableError 这类"重试没有意义"的场景，
        跳过剩余重试次数，直接判死，避免空耗 max_retries 次请求。
        """
        url = task.get("url", "")
        raw_str = task.get("_raw", url)
        tracking_id = task.get("_tracking_id")
        retry_count = task.get("_retry_count", 0) + 1

        # 无论是否耗尽，都先把这次占用的 processing 条目和 meta 清掉
        pipe = self.r.pipeline()
        pipe.lrem(self.cfg.processing_key, 1, raw_str)
        if tracking_id:
            pipe.hdel(self.cfg.processing_meta_key, tracking_id)
        pipe.execute()

        # 干净任务：剔除内部字段，避免 _raw/_retry_count/_tracking_id 污染业务数据
        clean_task = {k: v for k, v in task.items() if not k.startswith('_')}

        if force_dead or retry_count >= self.cfg.max_retries:
            dead_key_attr = self.ERROR_TYPE_TO_DEAD_KEY.get(error_type, "dead_other_key")
            dead_key = getattr(self.cfg, dead_key_attr)
            logger.warning("URL 判定进入死信（重试=%d，force_dead=%s），归类为 [%s]，移入 %s: %s  原因: %s", retry_count, force_dead, error_type, dead_key, url, reason,)
            dead_record = json.dumps({
                "task": clean_task,
                "url": url,
                "error_type": error_type,
                "fail_count": retry_count,
                "force_dead": force_dead,
                "reason": reason,
                "ts": time.time(),
            }, ensure_ascii=False)
            self.r.lpush(dead_key, dead_record)
        else:
            logger.info("URL 处理失败（第 %d 次，阶段=%s），重新入队重试: %s  原因: %s", retry_count, error_type, url, reason,)
            clean_task["_retry_count"] = retry_count
            self.r.lpush(self.cfg.pending_key, json.dumps(clean_task, ensure_ascii=False))

    def reap(self, force: bool = False) -> int:
        """
        巡检 processing_meta_key，把超时（或 force=True 时全部）未确认的
        任务连同原始 raw 字符串放回 pending，并清理对应 meta。

        force=True：忽略超时时间，无条件全部回收——用于进程启动时一次性
        捞回上次异常退出的残留（此时不会有任何 worker 正在处理这些任务，
        全部视为"没跑完"是安全的）。
        force=False：只回收确实超过 visibility_timeout_sec 还没有 ack/fail
        的任务——用于运行期间的独立巡检线程，应对 worker 线程卡死/异常
        退出但进程本身仍在运行的场景。
        """
        now = time.time()
        reaped = 0
        all_meta = self.r.hgetall(self.cfg.processing_meta_key)
        for raw_tid, raw_meta in all_meta.items():
            tracking_id = raw_tid.decode("utf-8") if isinstance(raw_tid, bytes) else raw_tid
            try:
                meta = json.loads(raw_meta)
                taken_at = meta.get("taken_at", 0)
                raw_str = meta.get("raw")
            except Exception:
                taken_at = 0
                raw_str = None

            if force or (now - taken_at > self.cfg.visibility_timeout_sec):
                pipe = self.r.pipeline()
                if raw_str is not None:
                    pipe.lrem(self.cfg.processing_key, 1, raw_str)
                    pipe.lpush(self.cfg.pending_key, raw_str)
                pipe.hdel(self.cfg.processing_meta_key, tracking_id)
                pipe.execute()
                reaped += 1
                logger.warning("回收残留/超时任务: tracking_id=%s（已等待 %.0fs，force=%s）", tracking_id, now - taken_at, force,)
        return reaped


# ============================== 业务逻辑 ==============================

CONTENT_XPATH = '//div[@class="padd-zyygwj"]'
REMARK_XPATH = ""


@retry_on_network_error(max_retries=3)
def fetch_html(session: requests.Session, url: str) -> str:
    resp = session.get(url, timeout=20)
    if resp.status_code == 404:
        raise NonRetryableError(f"404 Not Found: {url}")
    resp.raise_for_status()
    resp.encoding = resp.apparent_encoding
    logger.info(f"内容长度: {len(resp.text)}")
    return resp.text


def parse_and_extract(tq: TQ, html: str, url: str):
    tree = etree.HTML(html)
    if tree is None:
        raise ValueError(f"HTML 解析失败: {url}")
    content_nodes = tree.xpath(CONTENT_XPATH)
    if REMARK_XPATH:
        remark_nodes = tree.xpath(REMARK_XPATH)
    else:
        remark_nodes = ""
    text_list, attachments = tq.parse_nodes_sync(content_nodes, url)
    remarks = tq.get_remarks(remark_nodes) if remark_nodes else {}
    return text_list, attachments, remarks


def persist(url: str, text_list: list, attachments: list, remarks: dict,
            task_extra: Optional[Dict[str, Any]] = None) -> None:
    extra_info = task_extra or {}
    track_id = uuid.uuid4().hex

    record = {
        "task_id": track_id,
        "url": url,
        "title": extra_info.get("title"),
        "publish_time": extra_info.get("publish_time"),
        "main_body": text_list,
        "attachments": attachments,
        "remarks": remarks,
        "type": extra_info.get("rrr")
    }
    # db.insert(record)   # 纯 insert，不做 upsert
    logger.info(f"持久化: track_id={track_id} 附件数={len(attachments)}")


# ============================== Worker 主循环 ==============================

@dataclass
class WorkerStats:
    processed: int = 0
    failed: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock)

    def incr(self, field_name: str):
        with self.lock:
            setattr(self, field_name, getattr(self, field_name) + 1)


def worker_loop(worker_idx: int, tq: TQ, queue: ReliableRedisQueue,
                 stop_event: threading.Event, stats: WorkerStats):
    worker_id = f"worker-{worker_idx}-{uuid.uuid4().hex[:6]}"
    session = requests.Session()
    session.headers.update(COMMON_HEADERS)
    session.cookies.update(COMMON_COOKIES)
    logger.info(f"worker 启动: {worker_id}")

    while not stop_event.is_set():
        task = queue.pop(worker_id)
        if task is None:
            continue

        url = task["url"]
        # 提取所有额外业务信息（剔除内部字段）
        task_extra = {
            k: v for k, v in task.items()
            if k not in ("url", "_raw", "_retry_count", "_tracking_id")
        }

        error_type = "other_error"   # 默认归类，未落入具体阶段的失败都归到这里
        force_dead = False
        try:
            try:
                html = fetch_html(session, url)
            except NonRetryableError:
                error_type = "fetch_error"
                force_dead = True   # 例如 404，重试没有意义，直接判死
                raise
            except Exception:
                error_type = "fetch_error"
                raise

            try:
                text_list, attachments, remarks = parse_and_extract(tq, html, url)
            except Exception:
                error_type = "parse_error"
                raise

            try:
                persist(url, text_list, attachments, remarks, task_extra=task_extra)
            except Exception:
                error_type = "other_error"
                raise

            queue.ack(task)
            stats.incr("processed")

        except Exception as exc:
            logger.error(f"处理失败 url={url} 阶段={error_type}: {exc}", exc_info=True)
            queue.fail(task, error_type=error_type, reason=str(exc), force_dead=force_dead)
            stats.incr("failed")

    session.close()
    logger.info(f"worker 退出: {worker_id}")


def reaper_loop(queue: ReliableRedisQueue, stop_event: threading.Event, interval_sec: float):
    """运行期独立巡检线程：定期回收卡死/异常退出线程遗留的超时任务。"""
    while not stop_event.wait(interval_sec):
        try:
            n = queue.reap(force=False)
            if n:
                logger.info(f"Reaper 本轮回收 {n} 个超时任务")
        except Exception as exc:
            logger.error(f"Reaper 巡检出错: {exc}", exc_info=True)


# ============================== 入口 ==============================

def main(num_workers: int = 8):
    r = redis.Redis(
        host=CONFIG.redis_host, port=CONFIG.redis_port, db=CONFIG.redis_db,
        decode_responses=False,
    )
    queue = ReliableRedisQueue(r, CONFIG)
    stop_event = threading.Event()
    stats = WorkerStats()

    # 启动自愈：一次性无条件回收上次异常退出残留在 processing 里的任务
    recovered = queue.reap(force=True)
    if recovered:
        logger.info(f"启动自愈：回收了 {recovered} 个遗留的处理中任务")

    def handle_signal(signum, _frame):
        logger.info(f"收到退出信号 {signum}，开始优雅停止……")
        stop_event.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    with TQ(base_dir=BASE_DIR, headers=COMMON_HEADERS, cookies=COMMON_COOKIES,
            image_workers=4, attachment_workers=2) as tq:

        reaper_thread = threading.Thread(
            target=reaper_loop, args=(queue, stop_event, CONFIG.reaper_interval_sec),
            daemon=True, name="Reaper",
        )
        reaper_thread.start()

        with ThreadPoolExecutor(max_workers=num_workers, thread_name_prefix="CrawlWorker") as pool:
            futures = [
                pool.submit(worker_loop, i, tq, queue, stop_event, stats)
                for i in range(num_workers)
            ]
            try:
                for fut in futures:
                    fut.result()
            except KeyboardInterrupt:
                stop_event.set()
                for fut in futures:
                    fut.result()

        stop_event.set()
        reaper_thread.join(timeout=10)

    logger.info(f"全部退出。累计处理成功={stats.processed} 失败={stats.failed}")
    logger.info(f"tq.stats() = {tq.stats()}")


if __name__ == "__main__":
    main(num_workers=1)
