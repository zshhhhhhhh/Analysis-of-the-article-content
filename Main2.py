import requests
import json
import logging
import datetime
import random
import time
import re
import redis
import os
import uuid
import threading
import signal
from functools import wraps
from redis.connection import ConnectionPool
from requests.adapters import HTTPAdapter

# ==================== 基础配置（环境变量覆盖） ====================
CONFIG = {
    'PDF_SAVE_PATH': os.getenv('FAO_PDF_PATH', ''), # 自定义本地文件存储位置
    'JSONL_DIR': os.getenv('FAO_JSONL_DIR', r''), # 自定义本地jsonl文件存储位置
    'JSONL_NAME': "20251020_10_140_84_10_9656_1690888579000_1.jsonl", # 自定义jsonl文件名称
    'NUM_WORKERS': int(os.getenv('FAO_WORKERS', '3')),  # 线程数量
    'REDIS_HOST': os.getenv('FAO_REDIS_HOST', '127.0.0.1'), # Redis配置
    'REDIS_PORT': int(os.getenv('FAO_REDIS_PORT', '6379')), # Redis配置
    'REDIS_DB': int(os.getenv('FAO_REDIS_DB', '0')), # Redis配置
    'REDIS_PASSWORD': os.getenv('FAO_REDIS_PASSWORD', ''), # Redis配置
    'REDIS_QUEUE_NAME': 'FAO', # Redis配置
    'REDIS_BUG_QUEUE': 'FAO_BUG', # Redis配置
    'REDIS_NET_ERROR_QUEUE': 'FAO_NET_ERROR', # Redis配置
    'REDIS_NOT_PDF_QUEUE': 'FAO_NOT_PDF', # Redis配置
    'REDIS_PROCESSING_QUEUE': 'FAO_PROCESSING', # Redis配置
    'PROCESSING_TASK_TIMEOUT': 600,
    'PROCESSING_RECOVERY_INTERVAL': 120,
    'MAX_RETRIES': 7,
    'RETRY_SLEEP_MIN': 7,
    'RETRY_SLEEP_MAX': 11,
    'REQUEST_TIMEOUT': (10, 60),
    'API_TIMEOUT': 30,
    'XSRF_TOKEN': os.getenv('FAO_XSRF_TOKEN', '1708f941-2a9f-4630-a897-7a5327027f0e'),
    'HTTP_POOL_CONNECTIONS': 4,
    'HTTP_POOL_MAXSIZE': 10,
    # 每个 worker 处理多少个任务后重建一次 session（重置 TCP 连接状态）
    'SESSION_RECYCLE_INTERVAL': int(os.getenv('FAO_SESSION_RECYCLE_INTERVAL', '1000')),
    # 无论任务数是否达标，session 存活超过多少秒也强制重建（默认 30 分钟）
    'SESSION_RECYCLE_TIME_INTERVAL': int(os.getenv('FAO_SESSION_RECYCLE_TIME_INTERVAL', str(30 * 60))),
}

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


# ==================== 自定义异常 ====================
class RetryableError(Exception):
    """可重试异常（网络波动、超时、4xx、5xx）"""
    pass

class NonRetryableError(Exception):
    """不可重试异常（数据格式错误、业务逻辑无效等）"""
    pass


# ==================== 全局控制 ====================
json_lock = threading.Lock()

# 全局 Redis 连接池（所有线程共享）
_redis_pool = ConnectionPool(
    host=CONFIG['REDIS_HOST'],
    port=CONFIG['REDIS_PORT'],
    db=CONFIG['REDIS_DB'],
    password=CONFIG['REDIS_PASSWORD'],
    decode_responses=True,
    max_connections=CONFIG['NUM_WORKERS'] + 5
)

def get_redis_client():
    return redis.StrictRedis(connection_pool=_redis_pool)


def save_jsonl(data):
    with json_lock:
        filename = os.path.join(CONFIG['JSONL_DIR'], CONFIG["JSONL_NAME"])
        with open(filename, 'a', encoding='utf-8') as f:
            f.write(json.dumps(data, ensure_ascii=False) + '\n')
    logger.success("保存元数据成功: %s, URL: %s", str(data.get('title', '')), str(data.get('url', '')))


# ==================== 重试装饰器 ====================
def is_retryable_error(exception):
    if isinstance(exception, RetryableError):
        return True
    if isinstance(exception, (requests.ConnectionError, requests.Timeout)):
        return True
    if isinstance(exception, requests.HTTPError):
        status = getattr(exception.response, 'status_code', None)
        return status is not None and (status == 429 or 500 <= status < 600)
    return False


def retry_on_network_error(max_retries=None, sleep_min=None, sleep_max=None):
    if max_retries is None:
        max_retries = CONFIG['MAX_RETRIES']
    if sleep_min is None:
        sleep_min = CONFIG['RETRY_SLEEP_MIN']
    if sleep_max is None:
        sleep_max = CONFIG['RETRY_SLEEP_MAX']

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            wz_id = kwargs.get('wz_id') or (args[1] if len(args) > 1 else 'unknown')
            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except NonRetryableError:
                    raise
                except Exception as e:
                    if not is_retryable_error(e):
                        logger.error("[%s] 不可重试错误 wz_id=%s: %s", func.__name__, wz_id, e)
                        raise
                    last_exception = e
                    logger.error("[%s] 第 %d/%d 次失败 wz_id=%s: %s", func.__name__, attempt, max_retries, wz_id, e)
                    if attempt < max_retries:
                        sleep_seconds = random.randint(sleep_min, sleep_max)
                        logger.info("[%s] 等待 %ds 后重试... wz_id=%s", func.__name__, sleep_seconds, wz_id)
                        time.sleep(sleep_seconds)
            logger.error("[%s] 重试耗尽，最终失败 wz_id=%s", func.__name__, wz_id)
            raise last_exception.with_traceback(last_exception.__traceback__)
        return wrapper
    return decorator


# ==================== 元数据提取工具 ====================
def extract_meta_field(metadata, field, as_list=True, sep=',  '):
    items = metadata.get(field, [])
    values = []
    for item in items:
        if isinstance(item, dict) and 'value' in item:
            values.append(item['value'])
        elif isinstance(item, str):
            values.append(item)
    if as_list:
        return values
    return sep.join(values) if values else ''


def safe_get(d, *keys):
    for key in keys:
        if isinstance(d, dict):
            d = d.get(key)
        else:
            return None
    return d


def _raise_for_request_error(e):
    """统一将 requests 异常转换为 RetryableError，全部可重试"""
    if e.response is not None:
        raise RetryableError(f"HTTP {e.response.status_code}") from e
    raise RetryableError(f"请求失败: {e}") from e


# ==================== 基础 API 函数 ====================
@retry_on_network_error()
def get_language_english(session, wz_id, keywords):
    logger.info(f'get_language_english:{wz_id}------keywords:{keywords}')
    if not isinstance(keywords, str):
        raise NonRetryableError("keywords 必须为字符串")

    match = re.fullmatch(r'([A-Za-z]+)(\d+)([A-Za-z]*)', keywords)

    prefix = match.group(1) + match.group(2)

    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "zh;q=1,zh-CN;q=0.1,en;q=0.09,en-GB;q=0.08,en-US;q=0.06999999999999999",
        "cache-control": "no-cache",
        "content-type": "application/json; charset=utf-8",
        "pragma": "no-cache",
        "priority": "u=1, i",
        "referer": f"https://openknowledge.fao.org/items/{wz_id}",
        "sec-ch-ua": "\"Microsoft Edge\";v=\"149\", \"Chromium\";v=\"149\", \"Not)A;Brand\";v=\"24\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36 Edg/149.0.0.0"
    }
    cookies = {
        "klaro-anonymous": "%7B%22authentication%22%3Atrue%2C%22preferences%22%3Atrue%2C%22acknowledgement%22%3Atrue%2C%22google-analytics%22%3Afalse%7D",
        "dsLanguage": "zh",
    }
    params = {
        "page": "0",
        "size": "5",
        "configuration": "item",
        "query": f"(\n        fao.identifier.jobnumber_keyword:/{prefix.lower()}[A-Za-z]*/\n        OR fao.identifier.jobnumber_keyword:/{prefix.upper()}[A-Za-z]*/)\n        -fao.identifier.jobnumber_keyword:{keywords.upper()}\n        AND archived:true"
    }
    url = ""
    try:
        resp = session.get(url, headers=headers, cookies=cookies, params=params, timeout=CONFIG['API_TIMEOUT'])
        time.sleep(random.randint(2, 4))
        resp.raise_for_status()
    except requests.RequestException as e:
        _raise_for_request_error(e)

    data = resp.json()
    objects = safe_get(data, '_embedded', 'searchResult', '_embedded', 'objects')

    for obj in objects:
        meta = safe_get(obj, '_embedded', 'indexableObject', 'metadata')
        if not meta:
            continue
        lang_list = safe_get(meta, 'dc.language.iso')
        if lang_list and isinstance(lang_list, list) and len(lang_list) > 0:
            lang = lang_list[0].get('value', '') if isinstance(lang_list[0], dict) else ''
            if lang == "English":
                indexable_id = safe_get(obj, '_embedded', 'indexableObject', 'id')
                logger.success(f"-----------找到该文章的英文版----------")
                return {"id": indexable_id, "language": lang, "zz": 1}
    logger.error(f"-----------未找到该文章的英文版----------")
    return {"id": "", "language": "", "zz": 0}


def get_pdf(session, wz_id):
    MAX_CONTIUE = 7
    while MAX_CONTIUE >0:
        MAX_CONTIUE -= 1
        logger.info(f'get_pdf:{wz_id}')
        headers = {
            "accept": "application/json, text/plain, */*",
            "accept-language": "zh;q=1,zh-CN;q=0.1,en;q=0.09,en-GB;q=0.08,en-US;q=0.06999999999999999",
            "cache-control": "no-cache",
            "content-type": "application/json; charset=utf-8",
            "pragma": "no-cache",
            "priority": "u=1, i",
            "referer": f"https://openknowledge.fao.org/items/{wz_id}",
            "sec-ch-ua": "\"Microsoft Edge\";v=\"149\", \"Chromium\";v=\"149\", \"Not)A;Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36 Edg/149.0.0.0"
        }
        cookies = {
            "klaro-anonymous": "%7B%22authentication%22%3Atrue%2C%22preferences%22%3Atrue%2C%22acknowledgement%22%3Atrue%2C%22google-analytics%22%3Afalse%7D",
            "dsLanguage": "zh",
        }
        url = ""
        params = {}
        try:
            resp = session.get(url, headers=headers, cookies=cookies, params=params, timeout=CONFIG['API_TIMEOUT'])
            time.sleep(random.randint(2, 4))
            resp.raise_for_status()
        except requests.RequestException as e:
            _raise_for_request_error(e)

        data = resp.json()
        bundles = safe_get(data, '_embedded', 'bundles')

        for bundle in bundles:
            bitstreams = safe_get(bundle, '_embedded', 'bitstreams', '_embedded', 'bitstreams')
            for bitstream in bitstreams:
                name = bitstream.get('name', '')
                if name.lower().endswith('.pdf'):
                    pdf_id = bitstream.get('id', '')
                    if not pdf_id:
                        continue
                    logger.success(f'找到PDF id: {pdf_id}')
                    return {"name": name, "pdf_id": pdf_id, "zz": 1}
        return {"name": "", "pdf_id": "", "zz": 0}


def download_pdf(session, track_id, bitstream_id):
    if not bitstream_id:
        raise NonRetryableError("bitstream_id 为空")
    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
        "cache-control": "no-cache",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36 Edg/149.0.0.0"
    }
    url = ""
    save_path = os.path.join(CONFIG['PDF_SAVE_PATH'], f"{track_id}.pdf")
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    try:
        with session.get(url, headers=headers, stream=True, timeout=CONFIG['REQUEST_TIMEOUT']) as resp:
            resp.raise_for_status()
            content_type = resp.headers.get('Content-Type', '')
            if 'application/pdf' not in content_type:
                raise NonRetryableError(f"非PDF内容，Content-Type: {content_type}")
            with open(save_path, 'wb') as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
    except NonRetryableError:
        try:
            if os.path.exists(save_path):
                os.remove(save_path)
        except OSError as cleanup_err:
            logger.error("清理残留 PDF 文件失败: %s", cleanup_err)
        raise
    except Exception as download_err:
        try:
            if os.path.exists(save_path):
                os.remove(save_path)
        except OSError as cleanup_err:
            logger.error("清理残留 PDF 文件失败: %s", cleanup_err)
        if is_retryable_error(download_err):
            raise RetryableError(f"PDF 下载异常: {download_err}") from download_err
        raise

    logger.success("PDF 下载成功: %s", save_path)
    return True


# ==================== 业务流程 ====================
@retry_on_network_error()
def english_detail(session, wz_id):
    logger.info(f'english_detail:{wz_id}')
    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "zh;q=1,zh-CN;q=0.1,en;q=0.09,en-GB;q=0.08,en-US;q=0.06999999999999999",
        "cache-control": "no-cache",
        "content-type": "application/json; charset=utf-8",
        "pragma": "no-cache",
        "priority": "u=1, i",
        "referer": "",
        "sec-ch-ua": "\"Microsoft Edge\";v=\"149\", \"Chromium\";v=\"149\", \"Not)A;Brand\";v=\"24\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36 Edg/149.0.0.0",
        "x-referrer": "/",
        "x-xsrf-token": CONFIG['XSRF_TOKEN'],
    }
    cookies = {
        "klaro-anonymous": "%7B%22authentication%22%3Atrue%2C%22preferences%22%3Atrue%2C%22acknowledgement%22%3Atrue%2C%22google-analytics%22%3Afalse%7D",
        "dsLanguage": "zh",
    }
    url = ""
    params = {
        "embed": [
            "owningCollection/parentCommunity/parentCommunity",
            "relationships",
            "version/versionhistory",
            "thumbnail",
            "relateditemlistconfigs"
        ]
    }
    try:
        resp = session.get(url, headers=headers, cookies=cookies, params=params, timeout=CONFIG['API_TIMEOUT'])
        resp.raise_for_status()
    except requests.RequestException as e:
        _raise_for_request_error(e)

    result = resp.json()
    metadata = result.get('metadata', {})

    title = extract_meta_field(metadata, 'dc.title', False)
    subtitle = extract_meta_field(metadata, 'dc.title.subtitle', False)
    authors = extract_meta_field(metadata, 'dc.contributor.author', False)
    publish_times = extract_meta_field(metadata, 'dc.date.issued', False)
    abstracts = extract_meta_field(metadata, 'dc.description.abstract', False)
    languages = extract_meta_field(metadata, 'dc.language.iso', False)
    publishers = extract_meta_field(metadata, 'dc.publisher', False)
    placeofpublications = extract_meta_field(metadata, 'fao.placeofpublication', False)
    numberofpages = extract_meta_field(metadata, 'dc.format.numberofpages', False)
    visibilitytypes = extract_meta_field(metadata, 'fao.visibilitytype', False)
    isbns = extract_meta_field(metadata, 'dc.identifier.isbn', False)
    ispartofseries = extract_meta_field(metadata, 'dc.relation.ispartofseries', False)
    numbers = extract_meta_field(metadata, 'dc.relation.number', False)
    uris = extract_meta_field(metadata, 'dc.identifier.uri', False)
    dois = extract_meta_field(metadata, 'fao.identifier.doi', False)
    agrovocs = extract_meta_field(metadata, 'fao.subject.agrovoc', False)
    citations = extract_meta_field(metadata, 'fao.citation', False)

    pdf_info = get_pdf(session, wz_id)
    if not pdf_info or not pdf_info.get('zz') or not pdf_info.get('pdf_id'):
        logger.warning("该文章没有 PDF 附件: %s", wz_id)
        return (False, "no_pdf")

    track_id = str(uuid.uuid4())

    try:
        download_pdf(session, track_id, pdf_info['pdf_id'])
    except NonRetryableError:
        raise
    except RetryableError:
        raise
    except Exception as e:
        raise RetryableError(f"download_pdf 未预期异常: {e}") from e

    main_file = [{"name": f"{track_id}.pdf", "path": f"/FAO/{track_id}.pdf"}]
    data_save = {
        "track_id": track_id,
        "url": uris,
        "category": ["首页", "知识产品", "著作"],
        "publish_time": publish_times,
        "title": f"{title} {subtitle}".strip(),
        "abstract": abstracts,
        "language": languages,
        "authors": authors,
        "publisher": publishers,
        "placeofpublication": placeofpublications,
        "numberofpage": numberofpages,
        "visibilitytype": visibilitytypes,
        "isbn": isbns,
        "ispartofserie": ispartofseries,
        "numbers": numbers,
        "doi": dois,
        "agrovoc": agrovocs,
        "citation": citations,
        "main_file": main_file,
        "type": "fao_publication"
    }

    try:
        save_jsonl(data_save)
    except Exception as e:
        pdf_path = os.path.join(CONFIG['PDF_SAVE_PATH'], f"{track_id}.pdf")
        try:
            if os.path.exists(pdf_path):
                os.remove(pdf_path)
        except OSError as cleanup_err:
            logger.error("JSONL 失败后回滚 PDF 文件失败: %s", cleanup_err)
        logger.error("JSONL 写入失败: %s", e)
        raise NonRetryableError(f"JSONL 写入失败: {e}") from e

    return (True, None)

def get_detail(session, wz_id, handle, languages):
    logger.info(f'get_detail:{wz_id}')
    lang_str = ''
    if isinstance(languages, list):
        lang_str = ', '.join(languages)
    elif isinstance(languages, str):
        lang_str = languages
    logger.info(f'语言: lang_str:{lang_str}')
    if 'English' not in lang_str:
        eng = get_language_english(session, wz_id, handle)
        if eng and eng.get('zz'):
            return english_detail(session, eng['id'])
        else:
            logger.warning("未找到英文版本，使用原语言版本")
            return english_detail(session, wz_id)
    else:
        return english_detail(session, wz_id)


# ==================== 处理中队列恢复线程 ====================
def _get_task_start_time(task_json):
    try:
        task = json.loads(task_json)
        return float(task.get('enqueue_ts', 0))
    except Exception:
        return 0

def recovery_worker(shutdown_event):
    logger.info(
        "恢复线程启动，每 %ds 检查一次，超时 %ds 的任务将被移回主队列",
        CONFIG['PROCESSING_RECOVERY_INTERVAL'],
        CONFIG['PROCESSING_TASK_TIMEOUT'],
    )
    r = get_redis_client()
    try:
        while not shutdown_event.is_set():
            if shutdown_event.wait(CONFIG['PROCESSING_RECOVERY_INTERVAL']):
                break
            try:
                now = time.time()
                tasks = r.lrange(CONFIG['REDIS_PROCESSING_QUEUE'], 0, -1)
                recovered = 0
                for task_json in tasks:
                    start_ts = _get_task_start_time(task_json)
                    if start_ts == 0 or (now - start_ts) > CONFIG['PROCESSING_TASK_TIMEOUT']:
                        removed = r.lrem(CONFIG['REDIS_PROCESSING_QUEUE'], 1, task_json)
                        if removed:
                            r.rpush(CONFIG['REDIS_QUEUE_NAME'], task_json)
                            recovered += 1
                            logger.debug("恢复超时任务到主队列: %.80s", task_json)
                if recovered:
                    logger.success("本轮恢复 %d 个超时任务", recovered)
            except redis.RedisError as e:
                logger.error("恢复线程 Redis 错误: %s", e)
    finally:
        logger.info("恢复线程已退出")


# ==================== 消费者工作线程 ====================
def _make_session():
    session = requests.Session()
    adapter = HTTPAdapter(
        pool_connections=CONFIG['HTTP_POOL_CONNECTIONS'],
        pool_maxsize=CONFIG['HTTP_POOL_MAXSIZE'],
    )
    session.mount('https://', adapter)
    session.mount('http://', adapter)
    return session

def worker(worker_id, shutdown_event):
    local_session = _make_session()
    local_redis = get_redis_client()
    logger.info("Worker-%d 启动", worker_id)

    # 用于周期性重建 session 的计数器 / 计时器
    tasks_since_recycle = 0
    recycle_interval = CONFIG['SESSION_RECYCLE_INTERVAL']
    recycle_time_interval = CONFIG['SESSION_RECYCLE_TIME_INTERVAL']
    last_recycle_ts = time.time()

    try:
        while not shutdown_event.is_set():
            task_json = None
            stamped_json = None
            raw = None

            try:
                task_json = local_redis.brpoplpush(
                    CONFIG['REDIS_QUEUE_NAME'],
                    CONFIG['REDIS_PROCESSING_QUEUE'],
                    timeout=2
                )
                if task_json is None:
                    continue

                task = json.loads(task_json)
                task['enqueue_ts'] = time.time()
                stamped_json = json.dumps(task, ensure_ascii=False)
                pipe = local_redis.pipeline()
                pipe.lrem(CONFIG['REDIS_PROCESSING_QUEUE'], 1, task_json)
                pipe.rpush(CONFIG['REDIS_PROCESSING_QUEUE'], stamped_json)
                pipe.execute()
                raw = stamped_json

                logger.info("[Worker-%d] 拉取任务: %s...", worker_id, raw)

                wz_id = task['wz_id']
                handle = task['handle']
                languages = task['languages']

                success = False
                reason = None
                try:
                    success, reason = get_detail(local_session, wz_id, handle, languages)
                except NonRetryableError as e:
                    logger.error("数据异常，不重试 wz_id=%s: %s", wz_id, e)
                    success = False
                    reason = "data_error"
                except Exception as e:
                    if is_retryable_error(e):
                        logger.exception("[Worker-%d] 网络重试耗尽 wz_id=%s: %s", worker_id, wz_id, e)
                        success = False
                        reason = "net_error"
                    else:
                        logger.exception("[Worker-%d] 未知异常 wz_id=%s: %s", worker_id, wz_id, e)
                        success = False
                        reason = "data_error"

                local_redis.lrem(CONFIG['REDIS_PROCESSING_QUEUE'], 1, raw)

                if reason == "no_pdf":
                    local_redis.rpush(CONFIG['REDIS_NOT_PDF_QUEUE'], raw)
                    logger.warning("无PDF，已记录到 FAO_NOT_PDF wz_id=%s", wz_id)
                elif reason == "net_error":
                    local_redis.rpush(CONFIG['REDIS_NET_ERROR_QUEUE'], raw)
                    logger.error("网络错误，已记录到 FAO_NET_ERROR wz_id=%s", wz_id)
                elif reason == "data_error":
                    local_redis.rpush(CONFIG['REDIS_BUG_QUEUE'], raw)
                    logger.error("数据异常，已记录到 FAO_BUG wz_id=%s", wz_id)

                # ---- 任务处理完成后检查是否需要重建 session ----
                # 触发条件二选一：处理任务数达到阈值，或 session 存活时间超过阈值
                tasks_since_recycle += 1
                now_ts = time.time()
                elapsed = now_ts - last_recycle_ts

                should_recycle_by_count = recycle_interval > 0 and tasks_since_recycle >= recycle_interval
                should_recycle_by_time = recycle_time_interval > 0 and elapsed >= recycle_time_interval

                if should_recycle_by_count or should_recycle_by_time:
                    old_session = local_session
                    try:
                        local_session = _make_session()
                    except Exception as recycle_err:
                        logger.error(
                            "[Worker-%d] 重建 session 失败，继续使用旧 session: %s",
                            worker_id, recycle_err
                        )
                    else:
                        try:
                            old_session.close()
                        except Exception as close_err:
                            logger.error("[Worker-%d] 关闭旧 session 失败: %s", worker_id, close_err)
                        trigger = "任务数" if should_recycle_by_count else "存活时长"
                        logger.info(
                            "[Worker-%d] 触发条件=%s（已处理 %d 个任务，存活 %.0fs），重建 session 以重置 TCP 连接",
                            worker_id, trigger, tasks_since_recycle, elapsed
                        )
                        tasks_since_recycle = 0
                        last_recycle_ts = now_ts

            except redis.ConnectionError:
                logger.error("Redis 连接错误，尝试重连...")
                backup_task = stamped_json or task_json
                if backup_task:
                    try:
                        with open("failed_tasks_backup.txt", "a", encoding="utf-8") as f:
                            f.write(backup_task + "\n")
                        logger.warning("已将任务写入本地备份文件: %s", backup_task)
                    except Exception:
                        logger.error("写入本地备份文件失败，任务可能丢失: %s", backup_task)
                time.sleep(30)
                local_redis = get_redis_client()

            except Exception as e:
                logger.exception("Worker-%d 循环异常: %s", worker_id, e)
                backup_task = raw or stamped_json or task_json
                if backup_task:
                    try:
                        local_redis.rpush(CONFIG['REDIS_BUG_QUEUE'], backup_task)
                        local_redis.lrem(CONFIG['REDIS_PROCESSING_QUEUE'], 1, backup_task)
                        logger.error("兜底异常已记录到 FAO_BUG: %s", backup_task)
                    except Exception as redis_err:
                        logger.error("兜底写入 FAO_BUG 也失败 redis_err=%s，任务可能丢失: %s", redis_err, backup_task)
                time.sleep(5)

    finally:
        local_session.close()
        logger.info("Worker-%d 资源已释放并退出", worker_id)


# ==================== 信号处理 ====================
def create_signal_handler(shutdown_event):
    def handler(sig, frame):
        logger.info("收到退出信号，准备停止所有 Worker...")
        shutdown_event.set()
    return handler


# ==================== 主程序入口 ====================
if __name__ == "__main__":
    if not CONFIG['XSRF_TOKEN']:
        raise RuntimeError("FAO_XSRF_TOKEN 未配置，请设置环境变量后启动")

    shutdown_event = threading.Event()

    recovery_thread = threading.Thread(
        target=recovery_worker,
        args=(shutdown_event,),
        name="RecoveryThread",
        daemon=True
    )
    recovery_thread.start()

    worker_threads = []
    for i in range(CONFIG['NUM_WORKERS']):
        t = threading.Thread(
            target=worker,
            args=(i + 1, shutdown_event),
            name=f"Worker-{i + 1}",
            daemon=False
        )
        worker_threads.append(t)
        t.start()

    signal.signal(signal.SIGINT, create_signal_handler(shutdown_event))
    signal.signal(signal.SIGTERM, create_signal_handler(shutdown_event))

    logger.info("已启动 %d 个 Worker 和 1 个恢复线程，等待任务...", CONFIG['NUM_WORKERS'])

    shutdown_event.wait()

    for t in worker_threads:
        t.join(timeout=30)

    logger.info("程序已完全退出")
