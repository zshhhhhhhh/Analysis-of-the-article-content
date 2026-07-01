# -*- coding: utf-8 -*-
"""
通用爬虫工具类（优化版 v2）
目标：百万/千万级 HTML 稳定运行，内存可控，失败可恢复，线程安全。
"""

from __future__ import annotations
import csv
import hashlib
import os
import queue
import re
import threading
import time
import random
import uuid
import tempfile
from collections import OrderedDict, defaultdict
from concurrent.futures import Future
from dataclasses import dataclass
from functools import wraps
from typing import (TypeAlias, NamedTuple, TypedDict, List, Dict, Any, Callable, Optional, Set, Tuple, Union)
from urllib.parse import urljoin, urlparse
import filetype
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from charset_normalizer import from_bytes
from loguru import logger
from lxml import etree
from pypinyin import lazy_pinyin, Style



DownloadResult: TypeAlias = tuple[bool, str]

class TextToken(TypedDict):
    type: str
    content: str

class ImageFutureToken(TypedDict):
    type: str
    future: Future[DownloadResult]

class HtmlFutureToken(TypedDict):
    type: str
    html: str
    futures: Dict[str, Future[DownloadResult]]
    spill_path: Optional[str]

Token = Union[TextToken, ImageFutureToken, HtmlFutureToken]


@dataclass(frozen=True)
class CrawlerConfig:
    max_retries:          int   = 7
    retry_sleep_base:     float = 2.0
    retry_sleep_max:      float = 60.0
    page_sleep_sec:       float = 1.0
    asset_sleep_sec:      float = 2.0
    ct_cache_maxsize:     int   = 4_096
    domain_cache_maxsize: int   = 8_192
    image_workers:        int   = 3
    image_queue_cap:      int   = 2_048
    attachment_workers:   int   = 2
    attachment_queue_cap: int   = 1_024
    result_cache_maxsize: int   = 16_384
    min_file_size_kb:     int   = 1
    stop_timeout_sec:     float = 60.0
    pool_connections:     int   = 20
    pool_maxsize:         int   = 20
    transport_retries:    int   = 2
    raw_html_spill_bytes: int   = 512 * 1024
    write_bus_workers:    int   = 2
    # --- 动态超时相关 ---
    dynamic_timeout_enabled: bool  = True   # 是否按域名历史耗时动态调整超时
    timeout_connect:          float = 10.0   # 连接超时固定不变
    timeout_min:               float = 10.0   # 动态超时下限，避免抖动一次就被误判
    timeout_max:               float = 120.0  # 动态超时上限，避免单个慢域名拖死 worker
    timeout_multiplier:        float = 4.0    # 动态超时 = 历史平均耗时 * 该倍数
    timeout_ema_alpha:         float = 0.3    # EMA 平滑系数，越大越敏感
    # --- 异常残留文件清理相关 ---
    tmp_file_prefix:          str   = '.tmp_'   # 原子写使用的临时文件前缀
    stale_tmp_max_age_sec:    float = 3600.0    # 启动时清理多久之前的残留临时文件

CONFIG = CrawlerConfig()


class RetryableError(Exception): ...
class NonRetryableError(Exception): ...
class BusClosedError(Exception): ...
class CircuitBreakerOpenError(Exception): ...


class CircuitBreaker:

    def __init__(self, failure_threshold: int = 10, recovery_timeout: float = 30.0):
        self._failure_count = 0
        self._failure_threshold = failure_threshold
        self._recovery_timeout = recovery_timeout
        self._last_failure_time = 0.0
        self._lock = threading.Lock()
        self._half_open_probe_inflight = False

    def record_failure(self):
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.monotonic()

            self._half_open_probe_inflight = False

    def record_success(self):
        with self._lock:
            self._failure_count = 0
            self._half_open_probe_inflight = False

    def allow_request(self) -> bool:
        with self._lock:
            if self._failure_count < self._failure_threshold:
                return True
            elapsed = time.monotonic() - self._last_failure_time
            if elapsed < self._recovery_timeout:
                return False

            if self._half_open_probe_inflight:
                return False
            self._half_open_probe_inflight = True
            return True


class PerDomainCircuitBreaker:
  
    def __init__(self, failure_threshold: int = 10, recovery_timeout: float = 30.0, maxsize: int = CONFIG.domain_cache_maxsize):
        self._failure_threshold = failure_threshold
        self._recovery_timeout = recovery_timeout
        self._maxsize = maxsize
        self._state: OrderedDict[str, CircuitBreaker] = OrderedDict()
        self._meta_lock = threading.Lock()

    def _get(self, domain: str) -> CircuitBreaker:
        with self._meta_lock:
            breaker = self._state.get(domain)
            if breaker is None:
                breaker = CircuitBreaker(self._failure_threshold, self._recovery_timeout)
                self._state[domain] = breaker
                if len(self._state) > self._maxsize:
                    self._state.popitem(last=False)
            else:
                self._state.move_to_end(domain)
            return breaker

    @staticmethod
    def _domain_of(url: str) -> str:
        try:
            return urlparse(url).netloc
        except Exception:
            return url

    def allow_request(self, url: str) -> bool:
        return self._get(self._domain_of(url)).allow_request()

    def record_success(self, url: str) -> None:
        self._get(self._domain_of(url)).record_success()

    def record_failure(self, url: str) -> None:
        self._get(self._domain_of(url)).record_failure()


class StatsCollector:
    def __init__(self):
        self.lock = threading.Lock()
        self._counters = defaultdict(int)
        self._gauges = defaultdict(float)

    def incr(self, key: str, delta: int = 1):
        with self.lock:
            self._counters[key] += delta

    def set_gauge(self, key: str, value: float):
        with self.lock:
            self._gauges[key] = value

    def snapshot(self) -> Dict[str, Any]:
        with self.lock:
            return {'counters': dict(self._counters), 'gauges': dict(self._gauges),}


def _is_retryable(exc: Exception) -> bool:
    if isinstance(exc, RetryableError):
        return True
    if isinstance(exc, (requests.ConnectionError, requests.Timeout)):
        return True
    if isinstance(exc, requests.HTTPError):
        status = getattr(exc.response, 'status_code', None)
        return status is not None and (status == 429 or 500 <= status < 600)
    return False

def retry_on_network_error(max_retries: int   = CONFIG.max_retries, base_sleep:  float = CONFIG.retry_sleep_base, max_sleep:   float = CONFIG.retry_sleep_max,):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exc: Exception | None = None
            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except NonRetryableError:
                    raise
                except Exception as exc:
                    if not _is_retryable(exc):
                        logger.error("[{}] 不可重试错误 (attempt={}): {}", func.__name__, attempt, exc)
                        raise
                    last_exc = exc
                    cap = min(base_sleep * (2 ** (attempt - 1)), max_sleep)
                    delay = random.uniform(0, cap)
                    logger.warning("[{}] 第 {}/{} 次失败，{:.1f}s 后重试: {}", func.__name__, attempt, max_retries, delay, exc)
                    if attempt < max_retries:
                        time.sleep(delay)
            logger.warning("[{}] 重试耗尽（共 {} 次），最终失败: {}", func.__name__, max_retries, last_exc)
            raise last_exc
        return wrapper
    return decorator


_MIME_TO_EXT: Dict[str, str] = {
    "image/jpeg": ".jpg", "image/png": ".png", "image/gif": ".gif",
    "image/webp": ".webp", "image/bmp": ".bmp", "image/tiff": ".tiff",
    "image/x-icon": ".ico", "image/svg+xml": ".svg",
    "image/heic": ".heic", "image/heif": ".heif",
    "application/pdf": ".pdf",
    "application/msword": ".doc",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
    "application/vnd.ms-excel": ".xls",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
    "application/vnd.ms-powerpoint": ".ppt",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation": ".pptx",
    "application/rtf": ".rtf", "text/plain": ".txt", "text/html": ".html",
    "text/css": ".css", "text/javascript": ".js", "application/json": ".json",
    "application/xml": ".xml", "text/csv": ".csv", "application/epub+zip": ".epub",
    "application/vnd.oasis.opendocument.text": ".odt",
    "application/vnd.oasis.opendocument.spreadsheet": ".ods",
    "application/vnd.oasis.opendocument.presentation": ".odp",
    "application/zip": ".zip", "application/x-rar": ".rar",
    "application/x-7z-compressed": ".7z", "application/x-tar": ".tar",
    "application/gzip": ".gz", "application/x-bzip2": ".bz2",
    "video/mp4": ".mp4", "video/x-msvideo": ".avi",
    "video/x-matroska": ".mkv", "video/webm": ".webm",
    "video/quicktime": ".mov", "video/mpeg": ".mpeg",
    "video/x-flv": ".flv", "video/x-ms-wmv": ".wmv",
    "audio/mpeg": ".mp3", "audio/wav": ".wav",
    "audio/x-flac": ".flac", "audio/aac": ".aac",
    "audio/ogg": ".ogg", "audio/webm": ".weba",
    "application/octet-stream": ".bin",
    "application/x-sqlite3": ".db",
}

_DOWNLOADABLE_SUFFIXES: frozenset[str] = frozenset({
    '.xls', '.xlsx', '.doc', '.docx', '.ppt', '.pptx',
    '.pdf', '.zip', '.rar', '.7z', '.txt', '.csv',
    '.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp',
    '.mp4', '.avi', '.mkv', '.mp3', '.wav', '.flac',
    '.xml', '.json',
})

_PAGE_SUFFIXES: frozenset[str] = frozenset({'.html', '.htm', '.php', '.asp', '.aspx', '.jsp', '.shtml',})

_SKIP_URL_SUBSTRINGS: tuple[str, ...] = ('mp.weixin.qq.com/s?__biz=',)

_ATTACHMENT_PATH_HINTS: tuple[str, ...] = (
    '/attach', '/attachment', '/upload', '/uploads',
    '/download', '/downloads', '/file/', '/files/',
    '/fileview', '/getfile', 'wp-content/uploads',
)


def _safe_future_result(fut: Future[DownloadResult], timeout: float = 300.0) -> DownloadResult:
    try:
        return fut.result(timeout=timeout)
    except Exception as exc:

        is_timeout = isinstance(exc, TimeoutError)
        level = "warning" if is_timeout else "error"
        getattr(logger, level)(
            "Future 结果获取失败 [{}] timeout={}: {}",
            type(exc).__name__, is_timeout, exc,
        )
        return (False, '0')

def _safe_filename(name: str) -> str:
    base = re.sub(r'[^\w.\-]', '_', os.path.basename(name))
    return base if base else '_unnamed'

def _makedirs_for(filepath: str) -> None:
    parent = os.path.dirname(filepath)
    if parent:
        os.makedirs(parent, exist_ok=True)


def _atomic_write(filepath: str, content: bytes) -> None:

    _makedirs_for(filepath)
    directory = os.path.dirname(filepath) or '.'
    fd, tmp_path = tempfile.mkstemp(prefix=CONFIG.tmp_file_prefix, dir=directory)
    try:
        with os.fdopen(fd, 'wb') as f:
            f.write(content)
            f.flush()
            os.fsync(f.fileno())
        try:

            os.chmod(tmp_path, 0o644)
        except OSError:
            pass
        os.replace(tmp_path, filepath)
    except Exception:
        try:
            os.remove(tmp_path)
        except OSError:
            pass
        raise


def _cleanup_stale_tmp_files(directory: str, prefix: str, max_age_sec: float = CONFIG.stale_tmp_max_age_sec) -> None:

    if not directory or not os.path.isdir(directory):
        return
    now = time.time()
    try:
        for root, _dirs, files in os.walk(directory):
            for name in files:
                if not name.startswith(prefix):
                    continue
                fpath = os.path.join(root, name)
                try:
                    if now - os.path.getmtime(fpath) > max_age_sec:
                        os.remove(fpath)
                        logger.info("启动清理：删除残留临时文件 {}", fpath)
                except OSError as exc:
                    logger.debug("清理残留临时文件失败 {}: {}", fpath, exc)
    except Exception as exc:
        logger.warning("清理残留临时文件目录失败 {}: {}", directory, exc)


def is_downloadable_url(url: str, *, attachment_hint: bool = False) -> bool:
    if not url.startswith(('http://', 'https://')):
        return False
    if '@' in urlparse(url).netloc:
        return False
    for sub in _SKIP_URL_SUBSTRINGS:
        if sub in url:
            return False
    path = url.split('?')[0].split('#')[0]
    basename = os.path.basename(path)
    if '.' in basename:
        suffix = '.' + basename.rsplit('.', 1)[-1].lower()
        if suffix in _PAGE_SUFFIXES:
            return False
        if suffix in _DOWNLOADABLE_SUFFIXES:
            return True
        return True
    if attachment_hint or _looks_like_attachment_path(url):
        return True
    return True

def _looks_like_attachment_path(url: str) -> bool:
    lower = url.lower()
    return any(kw in lower for kw in _ATTACHMENT_PATH_HINTS)


class _DomainRateLimiter:
    def __init__(self, maxsize: int = CONFIG.domain_cache_maxsize) -> None:
        self._maxsize = maxsize
        self._state: OrderedDict[str, dict] = OrderedDict()
        self._meta_lock = threading.Lock()

    def _entry(self, domain: str) -> dict:
        with self._meta_lock:
            entry = self._state.get(domain)
            if entry is None:
                entry = {'lock': threading.Lock(), 'ts': {}}
                self._state[domain] = entry
                if len(self._state) > self._maxsize:
                    self._state.popitem(last=False)
            else:
                self._state.move_to_end(domain)
            return entry

    def wait(self, url: str, bucket: str, min_interval: float) -> None:
        if min_interval <= 0:
            return
        domain = urlparse(url).netloc
        entry = self._entry(domain)
        with entry['lock']:
            last = entry['ts'].get(bucket, 0.0)
            elapsed = time.monotonic() - last
            if elapsed < min_interval:
                time.sleep(min_interval - elapsed)
            entry['ts'][bucket] = time.monotonic()


class _DomainLatencyTracker:

    def __init__(self, maxsize: int = CONFIG.domain_cache_maxsize, alpha: float = CONFIG.timeout_ema_alpha):
        self._maxsize = maxsize
        self._alpha = alpha
        self._state: OrderedDict[str, float] = OrderedDict()
        self._lock = threading.Lock()

    def observe(self, domain: str, elapsed: float) -> None:
        if elapsed <= 0:
            return
        with self._lock:
            prev = self._state.get(domain)
            ema = elapsed if prev is None else (self._alpha * elapsed + (1 - self._alpha) * prev)
            self._state[domain] = ema
            self._state.move_to_end(domain)
            if len(self._state) > self._maxsize:
                self._state.popitem(last=False)

    def get_ema(self, domain: str) -> Optional[float]:
        with self._lock:
            v = self._state.get(domain)
            if v is not None:
                self._state.move_to_end(domain)
            return v


class TimeoutJoinQueue(queue.Queue):
    def join_with_timeout(self, timeout: float | None = None) -> bool:
        with self.all_tasks_done:
            if timeout is None:
                while self.unfinished_tasks:
                    self.all_tasks_done.wait()
                return True
            deadline = time.monotonic() + timeout
            while self.unfinished_tasks:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self.all_tasks_done.wait(remaining)
            return True


class HttpClient:
    _DEFAULT_HEADERS: dict[str, str] = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Pragma": "no-cache",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36",
    }

    def __init__(
        self,
        headers:           Optional[Dict[str, str]] = None,
        cookies:           Optional[Dict[str, str]] = None,
        timeout:           int   = 60,
        page_interval:     float = CONFIG.page_sleep_sec,
        asset_interval:    float = CONFIG.asset_sleep_sec,
        pool_connections:  int   = CONFIG.pool_connections,
        pool_maxsize:      int   = CONFIG.pool_maxsize,
        transport_retries: int   = CONFIG.transport_retries,
        dynamic_timeout:   bool  = CONFIG.dynamic_timeout_enabled,
    ) -> None:
        self.timeout = timeout
        self._page_interval = page_interval
        self._asset_interval = asset_interval
        self._closed = False
        self._session = requests.Session()
        self._session.headers.update(self._DEFAULT_HEADERS)
        if headers:
            self._session.headers.update(headers)
        if cookies:
            self._session.cookies.update(cookies)
        self._custom_header_keys: Set[str] = set(headers or {})
        retry_strategy = Retry(total=transport_retries, connect=transport_retries, read=0, status_forcelist=(), backoff_factor=0.5, raise_on_status=False,)
        adapter = HTTPAdapter(pool_connections=pool_connections, pool_maxsize=pool_maxsize, max_retries=retry_strategy,)
        self._session.mount('http://', adapter)
        self._session.mount('https://', adapter)
        self._rate_limiter = _DomainRateLimiter(CONFIG.domain_cache_maxsize)
        self._dynamic_timeout = dynamic_timeout
        self._latency_tracker = _DomainLatencyTracker(CONFIG.domain_cache_maxsize, CONFIG.timeout_ema_alpha)

    def _interval_for(self, bucket: str) -> float:
        return self._page_interval if bucket == 'page' else self._asset_interval

    def _resolve_timeout(self, url: str):

        if not self._dynamic_timeout:
            return self.timeout
        domain = urlparse(url).netloc
        ema = self._latency_tracker.get_ema(domain)
        if ema is None:
            return self.timeout
        read_timeout = ema * CONFIG.timeout_multiplier
        read_timeout = max(CONFIG.timeout_min, min(read_timeout, CONFIG.timeout_max))
        return (CONFIG.timeout_connect, read_timeout)

    def _observe_latency(self, url: str, elapsed: float) -> None:
        if not self._dynamic_timeout:
            return
        try:
            self._latency_tracker.observe(urlparse(url).netloc, elapsed)
        except Exception as exc:
            logger.debug("记录请求耗时失败: {}", exc)

    def update_credentials(self, headers=None, cookies=None, replace=True) -> None:
        if headers is not None:
            if replace:
                for k in self._custom_header_keys:
                    self._session.headers.pop(k, None)
                self._custom_header_keys = set(headers)
            else:
                self._custom_header_keys.update(headers)
            self._session.headers.update(headers)
        if cookies is not None:
            if replace:
                self._session.cookies.clear()
            self._session.cookies.update(cookies)

    def get(self, url, *, bucket='page', **kwargs):
        if self._closed:
            raise NonRetryableError("HttpClient 已关闭")
        self._rate_limiter.wait(url, bucket, self._interval_for(bucket))
        timeout = kwargs.pop('timeout', None)
        if timeout is None:
            timeout = self._resolve_timeout(url)
        start = time.monotonic()
        try:
            resp = self._session.get(url, timeout=timeout, **kwargs)
        except requests.Timeout:

            raise
        self._observe_latency(url, time.monotonic() - start)
        return resp

    def head(self, url, *, bucket='probe', **kwargs):
        if self._closed:
            raise NonRetryableError("HttpClient 已关闭")
        self._rate_limiter.wait(url, bucket, self._interval_for(bucket))
        timeout = kwargs.pop('timeout', None)
        if timeout is None:
            timeout = self._resolve_timeout(url)
        start = time.monotonic()
        try:
            resp = self._session.head(url, timeout=timeout, allow_redirects=True, **kwargs)
        except requests.Timeout:
            raise
        self._observe_latency(url, time.monotonic() - start)
        return resp

    def close(self):
        self._closed = True
        self._session.close()

    def __enter__(self): return self
    def __exit__(self, *_): self.close()


class EnhancedFutureCache:
    def __init__(self, result_maxsize: int = CONFIG.result_cache_maxsize):
        self._result: OrderedDict[str, Future[DownloadResult]] = OrderedDict()
        self._running: Dict[str, Future[DownloadResult]] = {}
        self._lock = threading.Lock()
        self._maxsize = result_maxsize

    def get_or_create(self, key: str) -> Tuple[Future[DownloadResult], bool]:
        with self._lock:
            if key in self._result:
                fut = self._result[key]
                self._result.move_to_end(key)
                return fut, False
            if key in self._running:
                return self._running[key], False
            fut: Future[DownloadResult] = Future()
            self._running[key] = fut
            return fut, True

    def complete(self, key: str, future: Future[DownloadResult]) -> None:
        with self._lock:
            if key in self._running and self._running[key] is future:
                del self._running[key]
                self._result[key] = future
                self._result.move_to_end(key)
                while len(self._result) > self._maxsize:
                    self._result.popitem(last=False)

    def evict(self, key: str, expected_future: Optional[Future[DownloadResult]] = None) -> None:
        with self._lock:
            if expected_future is not None:
                if self._running.get(key) is expected_future:
                    del self._running[key]
                elif self._result.get(key) is expected_future:
                    del self._result[key]
            else:
                self._running.pop(key, None)
                self._result.pop(key, None)

    def qsize(self) -> int:
        with self._lock:
            return len(self._running) + len(self._result)


_UNRELIABLE_CONTENT_TYPES: frozenset[str] = frozenset({'text/html', 'text/plain', 'application/octet-stream',})

class ExtResolver:
    def __init__(self, http: HttpClient) -> None:
        self._http = http
        self._head_cache: Dict[str, str] = {}
        self._cache_lock = threading.Lock()

    @retry_on_network_error()
    def _head_content_type(self, url: str) -> str:
        with self._cache_lock:
            if url in self._head_cache:
                return self._head_cache[url]
        try:
            resp = self._http.head(url, bucket='probe')
            ct = resp.headers.get('Content-Type', '').split(';')[0].strip().lower()
        except Exception as exc:
            logger.warning("HEAD 请求失败: {}  url={}", exc, url)
            ct = ''
        with self._cache_lock:
            self._head_cache[url] = ct
            if len(self._head_cache) > CONFIG.ct_cache_maxsize:
                self._head_cache.pop(next(iter(self._head_cache)))
        return ct

    def resolve(self, content: bytes, url: str = '', content_type_hint: str = '') -> str:
        if content:
            try:
                kind = filetype.guess(content)
                if kind and kind.mime in _MIME_TO_EXT:
                    return _MIME_TO_EXT[kind.mime]
            except Exception as exc:
                logger.debug("filetype 识别出错: {}", exc)
        path = url.split('?')[0].split('#')[0] if url else ''
        basename = os.path.basename(path)
        url_suffix = '.' + basename.rsplit('.', 1)[-1].lower() if '.' in basename else ''
        url_suffix_known = url_suffix in _DOWNLOADABLE_SUFFIXES
        hint = (content_type_hint or '').split(';')[0].strip().lower()
        if hint and hint in _MIME_TO_EXT:
            if hint not in _UNRELIABLE_CONTENT_TYPES or not url_suffix_known:
                return _MIME_TO_EXT[hint]
        if url_suffix_known:
            return url_suffix
        if url:
            ct = self._head_content_type(url)
            if ct and ct in _MIME_TO_EXT and ct not in _UNRELIABLE_CONTENT_TYPES:
                return _MIME_TO_EXT[ct]
        if url_suffix:
            return url_suffix
        logger.warning("无法识别后缀，回退 .bin  url={}", url)
        return '.bin'


class WriteTask(NamedTuple):
    kind: str
    payload: dict
    done_future: Optional[Future[bool]] = None


_WRITE_STOP = object()


class WriteBus:
    def __init__(self, num_workers: int = CONFIG.write_bus_workers):
        self._queues: List[TimeoutJoinQueue] = [TimeoutJoinQueue() for _ in range(num_workers)]
        self._closed = False
        self._workers: List[threading.Thread] = []
        for i in range(num_workers):
            t = threading.Thread(target=self._loop, args=(self._queues[i],), daemon=True, name=f"WriteBus-{i}")
            t.start()
            self._workers.append(t)
        self._num_workers = num_workers

    def _get_queue(self, key: str) -> TimeoutJoinQueue:
        idx = hash(key) % self._num_workers
        return self._queues[idx]

    def write_file(self, path: str, content: bytes, *, wait: bool = False) -> Optional[Future[bool]]:

        if self._closed:
            logger.warning("WriteBus 已关闭，忽略 write_file: {}", path)
            if wait:
                fut: Future[bool] = Future()
                fut.set_exception(BusClosedError("WriteBus 已关闭"))
                return fut
            return None
        done_future: Optional[Future[bool]] = Future() if wait else None
        q = self._get_queue(path)
        q.put(WriteTask('file', {'path': path, 'content': content}, done_future))
        return done_future

    def write_csv_row(self, row: tuple, filepath: str) -> None:
        if self._closed:
            logger.warning("WriteBus 已关闭，忽略 csv: {}", filepath)
            return
        q = self._get_queue(filepath)
        q.put(WriteTask('csv', {'row': row, 'filepath': filepath}, None))

    def qsize(self) -> int:
        return sum(q.qsize() for q in self._queues)

    def _loop(self, q: TimeoutJoinQueue):
        csv_handles: Dict[str, tuple] = {}

        def _get_csv_writer(filepath):
            if filepath not in csv_handles:
                _makedirs_for(filepath)
                fobj = open(filepath, 'a', encoding='utf-8', newline='')
                csv_handles[filepath] = (fobj, csv.writer(fobj))
            return csv_handles[filepath][1]

        try:

            while True:
                item = q.get()
                try:
                    if item is _WRITE_STOP:
                        break
                    task: WriteTask = item
                    try:
                        if task.kind == 'file':
                            p = task.payload
                            _atomic_write(p['path'], p['content'])
                        elif task.kind == 'csv':
                            p = task.payload
                            writer = _get_csv_writer(p['filepath'])
                            writer.writerow(p['row'])
                            csv_handles[p['filepath']][0].flush()

                        if task.done_future is not None and not task.done_future.done():
                            task.done_future.set_result(True)
                    except Exception as exc:
                        logger.error("WriteBus 写入异常: {}  task={}", exc, item, exc_info=True)

                        if task.done_future is not None and not task.done_future.done():
                            task.done_future.set_exception(exc)
                finally:
                    q.task_done()
        except Exception as exc:

            logger.error("WriteBus 工作线程异常退出: {}", exc, exc_info=True)
        finally:
            for fobj, _ in csv_handles.values():
                try:
                    fobj.close()
                except Exception:
                    pass

    def stop(self, timeout: float = CONFIG.stop_timeout_sec):
        self._closed = True
        for q in self._queues:
            if not q.join_with_timeout(timeout):
                logger.warning("WriteBus 某队列在 {:.1f}s 内未排空", timeout)
            q.put(_WRITE_STOP)
        for t in self._workers:
            t.join(timeout=timeout)
            if t.is_alive():
                logger.warning("WriteBus 线程未退出")


class _DownloadTask(NamedTuple):
    url: str
    future: Future[DownloadResult]
    trace_id: str

_DL_STOP = object()

class _IsolatedDownloadBus:
    def __init__(self, name: str, base_path: str, http: HttpClient, ext: ExtResolver, write_bus: WriteBus, num_workers: int, queue_cap: int, min_bytes: int, breaker: PerDomainCircuitBreaker, stats: StatsCollector):
        self.name = name
        self._http = http
        self._ext = ext
        self._write_bus = write_bus
        self._min_bytes = min_bytes
        self._closed = False
        self._breaker = breaker
        self._stats = stats
        self._queue: TimeoutJoinQueue = TimeoutJoinQueue(maxsize=queue_cap)
        self._cache = EnhancedFutureCache()
        self._workers = []
        for i in range(num_workers):
            t = threading.Thread(target=self._worker, daemon=True, name=f"{name}Worker-{i}")
            t.start()
            self._workers.append(t)
        self.base_path = base_path

    @staticmethod
    def sha256_name(url: str) -> str:
        return hashlib.sha256(url.encode('utf-8')).hexdigest()

    def submit(self, url: str, trace_id: str) -> Future[DownloadResult]:
        if self._closed:
            fut: Future[DownloadResult] = Future()
            fut.set_exception(BusClosedError(f"{self.name} 已关闭"))
            return fut
        cache_key = url
        fut, is_owner = self._cache.get_or_create(cache_key)
        if not is_owner:
            return fut
        if not self._breaker.allow_request(url):
            logger.error("[{}] 该域名熔断中，拒绝提交: {}", self.name, url)
            self._cache.evict(cache_key, fut)
            fut.set_exception(CircuitBreakerOpenError(f"域名熔断中: {urlparse(url).netloc}"))
            return fut
        try:
            self._queue.put(_DownloadTask(url, fut, trace_id))
        except Exception as exc:
            self._cache.evict(cache_key, fut)
            if not fut.done():
                fut.set_exception(exc)
        return fut

    def _worker(self):
        while True:
            item = self._queue.get()
            try:
                if item is _DL_STOP:
                    break
                task: _DownloadTask = item
                cache_key = task.url
                try:
                    result = self._do_download(task.url, task.trace_id)
                    if not task.future.done():
                        task.future.set_result(result)
                    self._breaker.record_success(task.url)
                    self._stats.incr(f"{self.name}_success")
                except Exception as exc:
                    self._breaker.record_failure(task.url)
                    self._stats.incr(f"{self.name}_failure")
                    logger.error("[{}] 下载失败 {}: {}", self.name, task.url, exc)
                    if not task.future.done():
                        task.future.set_exception(exc)
                finally:
                    self._cache.complete(cache_key, task.future)
            finally:
                self._queue.task_done()

    @retry_on_network_error()
    def _do_download(self, url: str, trace_id: str) -> DownloadResult:
        resp = self._http.get(url, bucket='asset')
        resp.raise_for_status()
        content = resp.content
        if len(content) < self._min_bytes:
            logger.warning("[{}] 文件过小 ({} B)，跳过: {}", trace_id, len(content), url)
            return (False, '0')
        ct_hint = resp.headers.get('Content-Type', '')
        ext = self._ext.resolve(content, url, content_type_hint=ct_hint)
        filename = self.sha256_name(url) + ext
        filepath = os.path.join(self.base_path, filename)


        write_future = self._write_bus.write_file(filepath, content, wait=True)
        try:
            write_future.result(timeout=CONFIG.stop_timeout_sec)
        except Exception as exc:
            logger.error("[{}] 文件落盘失败，判定为下载失败: {}  file={}", trace_id, exc, filepath)
            raise

        return (True, filename)

    def qsize(self) -> int:
        return self._queue.qsize()

    def stats(self) -> dict:
        return {'queue': self.qsize(), 'cache_size': self._cache.qsize(),}

    def stop(self, timeout: float = CONFIG.stop_timeout_sec):
        self._closed = True
        if not self._queue.join_with_timeout(timeout):
            logger.warning("{} 队列未排空", self.name)
        for _ in self._workers:
            self._queue.put(_DL_STOP)
        for w in self._workers:
            w.join(timeout=timeout)
            if w.is_alive():
                logger.warning("{} 工作线程未退出", self.name)

class ImageDownloadBus(_IsolatedDownloadBus):
    def __init__(self, base_dir: str, http, ext, write_bus, *args, **kwargs):
        super().__init__("Image", os.path.join(base_dir, 'image'), http, ext, write_bus, *args, **kwargs)
        self.csv_path = os.path.join(base_dir, 'image_urls', 'image_urls.csv')

    def _do_download(self, url: str, trace_id: str) -> DownloadResult:
        result = super()._do_download(url, trace_id)
        if result[0]:
            filename = result[1]
            self._write_bus.write_csv_row(
                (filename, url, f'image/{filename[:2]}/{filename}'),
                self.csv_path)
        return result

class AttachmentDownloadBus(_IsolatedDownloadBus):
    def __init__(self, base_dir: str, http, ext, write_bus, *args, **kwargs):
        super().__init__("Attachment", os.path.join(base_dir, 'attachment_file'), http, ext, write_bus, *args, **kwargs)


class ParseContext:
    def __init__(self, image_bus: ImageDownloadBus, attach_bus: AttachmentDownloadBus, page_url: str, budget: list, spill_dir: str):
        self.image_bus = image_bus
        self.attach_bus = attach_bus
        self.page_url = page_url
        self.budget = budget
        self.spill_dir = spill_dir


NodeHandler = Callable[[Any, ParseContext, int], Tuple[List[Token], List[dict]]]


class ContentParser:
    def __init__(self, image_bus: ImageDownloadBus, attach_bus: AttachmentDownloadBus, call_budget: int = 50_000, spill_dir: str | None = None):
        self._image_bus = image_bus
        self._attach_bus = attach_bus
        self._budget = call_budget
        self._spill_dir = spill_dir or os.path.join(os.getcwd(), '.tq_spill')
        self._handlers: Dict[str, NodeHandler] = {}
        self._register_default_handlers()

    def _register_default_handlers(self):
        self._handlers['table'] = self._handle_table
        self._handlers['tbody'] = self._handle_table
        self._handlers['tr'] = self._handle_table
        self._handlers['td'] = self._handle_table
        self._handlers['th'] = self._handle_table
        self._handlers['ul'] = self._handle_table
        self._handlers['ol'] = self._handle_table
        self._handlers['li'] = self._handle_table
        self._handlers['img'] = self._handle_img
        self._handlers['a'] = self._handle_a
        self._handlers['br'] = lambda node,ctx,depth: ([TextToken(type='text', content='\n')], [])
        self._handlers['hr'] = lambda node,ctx,depth: ([TextToken(type='text', content='\n')], [])

    def register_handler(self, tag: str, handler: NodeHandler):
        self._handlers[tag] = handler

    @staticmethod
    def _make_absolute(href: str, page_url: str) -> str:
        try:
            return href if href.startswith('http') else urljoin(page_url, href)
        except Exception:
            return href

    def _handle_table(self, node, ctx: ParseContext, depth: int) -> Tuple[List[Token], List[dict]]:
        tokens: List[Token] = []
        attachments: List[dict] = []
        img_futures: Dict[str, Future[DownloadResult]] = {}
        try:
            for img in node.xpath('.//img'):

                if ctx.budget[0] <= 0:
                    if ctx.budget[0] == 0:
                        logger.warning(
                            "ContentParser 节点预算已耗尽（表格内 img），后续节点将被丢弃  page_url={}",
                            ctx.page_url,
                        )
                        ctx.budget[0] = -1
                    break
                ctx.budget[0] -= 1
                try:
                    img_url = img.attrib.get('data-src') or img.attrib.get('src', '')
                    if not img_url:
                        continue
                    abs_url = self._make_absolute(img_url, ctx.page_url)
                    fut = ctx.image_bus.submit(abs_url, uuid.uuid4().hex)
                    identifier = uuid.uuid4().hex
                    img.attrib['src'] = identifier
                    img.attrib.pop('data-src', None)
                    img_futures[identifier] = fut
                except Exception as exc:
                    logger.debug("表格内 img 处理失败: {}", exc)
            raw_html = etree.tostring(node, encoding='unicode', method='html')
            if len(raw_html.encode('utf-8', errors='ignore')) > CONFIG.raw_html_spill_bytes:
                os.makedirs(self._spill_dir, exist_ok=True)
                fd, spill_path = tempfile.mkstemp(prefix='raw_html_', suffix='.tmp', dir=self._spill_dir)
                with os.fdopen(fd, 'w', encoding='utf-8') as f:
                    f.write(raw_html)
                tokens.append(HtmlFutureToken(type='html_future', html='', futures=img_futures, spill_path=spill_path))
            else:
                tokens.append(HtmlFutureToken(type='html_future', html=raw_html, futures=img_futures, spill_path=None))
        except Exception as exc:
            logger.warning("序列化表格出错: {}", exc)
        try:
            for a in node.xpath('.//a'):

                if ctx.budget[0] <= 0:
                    if ctx.budget[0] == 0:
                        logger.warning(
                            "ContentParser 节点预算已耗尽（表格内 a），后续节点将被丢弃  page_url={}",
                            ctx.page_url,
                        )
                        ctx.budget[0] = -1
                    break
                ctx.budget[0] -= 1
                stub = self._make_attachment_stub(a, ctx)
                if stub:
                    attachments.append(stub)
        except Exception as exc:
            logger.debug("表格内链接提取失败: {}", exc)
        return tokens, attachments

    def _handle_img(self, node, ctx: ParseContext, depth: int) -> Tuple[List[Token], List[dict]]:
        tokens = []
        try:
            img_url = node.attrib.get('data-src') or node.attrib.get('src', '')
            if img_url:
                abs_url = self._make_absolute(img_url, ctx.page_url)
                fut = ctx.image_bus.submit(abs_url, uuid.uuid4().hex)
                tokens.append(ImageFutureToken(type='image_future', future=fut))
        except Exception as exc:
            logger.debug("<img> 处理失败: {}", exc)
        return tokens, []

    def _handle_a(self, node, ctx: ParseContext, depth: int) -> Tuple[List[Token], List[dict]]:
        tokens = []
        attachments = []
        try:
            if node.text and node.text.strip():
                tokens.append(TextToken(type='text', content=node.text.strip()))
        except Exception:
            pass
        stub = self._make_attachment_stub(node, ctx)
        if stub:
            attachments.append(stub)
        for child in self._iter_children(node):
            c_tokens, c_atts = self._dispatch(child, ctx, depth+1)
            tokens.extend(c_tokens)
            attachments.extend(c_atts)
        try:
            if node.tail and node.tail.strip():
                tokens.append(TextToken(type='text', content=node.tail.strip()))
        except Exception:
            pass
        return tokens, attachments

    def _make_attachment_stub(self, node, ctx: ParseContext) -> Optional[dict]:
        try:
            hrefs = node.xpath('./@href')
        except Exception:
            return None
        if not hrefs:
            return None
        href = hrefs[0]
        if not isinstance(href, str) or 'javascript:' in href:
            return None
        full_url = self._make_absolute(href, ctx.page_url)
        hint = False
        try:
            if node.attrib.get('download') is not None:
                hint = True
            rel = (node.attrib.get('rel') or '').lower()
            if 'attachment' in rel:
                hint = True
        except Exception:
            pass
        hint = hint or _looks_like_attachment_path(full_url)
        fut = ctx.attach_bus.submit(full_url, uuid.uuid4().hex)
        return {'_future': fut, '_url': full_url}

    def _dispatch(self, node, ctx: ParseContext, depth: int) -> Tuple[List[Token], List[dict]]:
        if ctx.budget[0] <= 0:
            if ctx.budget[0] == 0:

                logger.warning(
                    "ContentParser 节点预算已耗尽，后续节点将被丢弃  page_url={}",
                    ctx.page_url,
                )
                ctx.budget[0] = -1
            return [], []
        ctx.budget[0] -= 1
        try:
            tag = getattr(node, 'tag', None)
        except Exception:
            return [], []
        if tag in ('script', 'style', etree.Comment, etree.ProcessingInstruction):
            return [], []
        handler = self._handlers.get(tag)
        if handler:
            return handler(node, ctx, depth)
        tokens = []
        attachments = []
        try:
            if node.text and node.text.strip():
                tokens.append(TextToken(type='text', content=node.text.strip()))
        except Exception:
            pass
        for child in self._iter_children(node):
            c_tokens, c_atts = self._dispatch(child, ctx, depth+1)
            tokens.extend(c_tokens)
            attachments.extend(c_atts)
        try:
            if node.tail and node.tail.strip():
                tokens.append(TextToken(type='text', content=node.tail.strip()))
        except Exception:
            pass
        return tokens, attachments

    @staticmethod
    def _iter_children(node):
        try:
            return list(node)
        except Exception:
            return []

    def parse_nodes(self, nodes: List, page_url: str) -> Tuple[List[Token], List[dict]]:
        all_tokens: List[Token] = []
        all_attachments: List[dict] = []
        budget = [self._budget]
        ctx = ParseContext(self._image_bus, self._attach_bus, page_url, budget, self._spill_dir)
        for node in nodes:
            t, a = self._dispatch(node, ctx, 0)
            all_tokens.extend(t)
            all_attachments.extend(a)
        return all_tokens, all_attachments

    @staticmethod
    def resolve_tokens(tokens: List[Token]) -> List[str]:
        result: List[str] = []
        for token in tokens:
            if token['type'] == 'text':
                result.append(token['content'])
            elif token['type'] == 'image_future':
                ok, name = _safe_future_result(token['future'])
                if ok:
                    result.append(f'<$<img>$>{name}<$<\\img>$>')
            elif token['type'] == 'html_future':
                html = token['html']
                if token.get('spill_path'):
                    try:
                        with open(token['spill_path'], 'r', encoding='utf-8') as f:
                            html = f.read()
                    except Exception as exc:
                        logger.warning("读取落盘节点失败 {}: {}", token['spill_path'], exc)
                        html = ''
                    finally:
                        try:
                            os.remove(token['spill_path'])
                        except OSError:
                            pass
                for identifier, fut in token['futures'].items():
                    ok, name = _safe_future_result(fut)
                    replacement = f'<$<img>$>{name}<$<\\img>$>' if ok else ''
                    html = html.replace(identifier, replacement)
                result.append(html)
        return result

    @staticmethod
    def resolve_attachments(stubs: List[dict]) -> List[dict]:
        result = []
        for stub in stubs:
            fut = stub.get('_future')
            if fut is None:
                continue
            ok, name = _safe_future_result(fut)
            if ok:
                result.append({'name': name, 'path': f'attachment_file/{name}'})
        return result

    def parse_nodes_sync(self, nodes: List, page_url: str) -> Tuple[List[str], List[dict]]:
        tokens, stubs = self.parse_nodes(nodes, page_url)
        return self.resolve_tokens(tokens), self.resolve_attachments(stubs)


class RemarkParser:
    _symbol_re  = re.compile(r'[\[\]【】]')
    _space_re   = re.compile(r'\s+')
    _kv_line_re = re.compile(r'^(.+?)[：:]\s*(.*)$')

    @staticmethod
    def _pinyin_key(text: str) -> str:
        try:
            return ''.join(lazy_pinyin(text, style=Style.FIRST_LETTER)).lower().strip()
        except Exception:
            return ''

    @staticmethod
    def _parse_table(table) -> dict[str, str]:
        kv: dict[str, str] = {}
        try:
            for row in table.xpath('.//tr'):
                try:
                    cells = row.xpath('.//td | .//th')
                    for i in range(0, len(cells), 2):
                        k = cells[i].xpath('string(.)').strip().rstrip('：:').strip()
                        if i + 1 < len(cells):
                            v = cells[i + 1].xpath('string(.)').strip()
                            if k and v:
                                kv[k] = v
                except Exception as exc:
                    logger.debug("table row 解析失败: {}", exc)
        except Exception as exc:
            logger.debug("table 解析失败: {}", exc)
        return kv

    @staticmethod
    def _parse_div_kv(div) -> dict[str, str] | None:
        try:
            sub_divs = div.xpath('./div')
        except Exception:
            return None
        if not sub_divs:
            return None
        pairs: list[tuple[str, str]] = []
        for sub in sub_divs:
            try:
                spans = sub.xpath('./span')
                if len(spans) != 2:
                    logger.debug("_parse_div_kv: 跳过 span 数异常的子 div (got {})", len(spans))
                    continue
                k = spans[0].xpath('string(.)').strip().rstrip('：:').strip()
                v = spans[1].xpath('string(.)').strip()
                if k and v:
                    pairs.append((k, v))
            except Exception as exc:
                logger.debug("div kv 子节点解析失败: {}", exc)
        return dict(pairs) if pairs else None

    def parse(self, remark_doc: list) -> dict:
        if not remark_doc:
            return {}

        remarks: dict[str, list] = defaultdict(list)

        def _add_kv(k: str, v: str) -> None:
            pk = self._pinyin_key(k)
            if pk:
                remarks[pk].append(f"{k}: {v}")

        for node in remark_doc:
            try:
                tag = getattr(node, 'tag', None)

                if tag == 'table':
                    for k, v in self._parse_table(node).items():
                        _add_kv(k, v)
                    continue

                if tag == 'div':
                    kv = self._parse_div_kv(node)
                    if kv:
                        for k, v in kv.items():
                            _add_kv(k, v)
                        continue

                try:
                    full_text = node.xpath('string(.)').strip()
                except Exception:
                    continue
                if not full_text:
                    continue

                lines   = [ln.strip() for ln in full_text.splitlines() if ln.strip()]
                matches = [self._kv_line_re.match(ln) for ln in lines]
                ratio   = sum(1 for m in matches if m) / len(matches) if matches else 0

                if len(matches) >= 2 and ratio >= 0.8:
                    pairs = [(m.group(1).strip(), m.group(2).strip()) for m in matches if m]
                    if pairs:
                        for k, v in pairs:
                            if k and v:
                                _add_kv(k, v)
                        continue

                clean = self._space_re.sub(' ', self._symbol_re.sub('', full_text)).strip()
                if not clean:
                    continue

                pk = self._pinyin_key(clean)
                if not pk:
                    continue

                try:
                    resources = (
                        [f"src:{u}" for u in node.xpath('.//img/@src | .//video/@src | .//source/@src')]
                        + [f"href:{u}" for u in node.xpath('.//a/@href')]
                    )
                except Exception:
                    resources = []

                remarks[pk].append(f"{clean} | {' | '.join(resources)}" if resources else clean)

            except (etree.XPathError, AttributeError, TypeError) as exc:
                logger.warning(
                    "备注节点解析失败 (tag={}, exc={}): {}",
                    getattr(node, 'tag', '?'), type(exc).__name__, exc,
                )

        return {k: (v[0] if len(v) == 1 else v) for k, v in remarks.items()}


class TQ:
    def __init__(self, base_dir: str, headers: Optional[Dict] = None, cookies: Optional[Dict] = None, min_file_size_kb: int = CONFIG.min_file_size_kb, call_budget: int = 50_000, image_workers: int = CONFIG.image_workers, attachment_workers: int = CONFIG.attachment_workers,):
        self.base_dir = base_dir
        self._closed = False
        self.stats_collector = StatsCollector()
        self.image_breaker = PerDomainCircuitBreaker(failure_threshold=15, recovery_timeout=30)
        self.attach_breaker = PerDomainCircuitBreaker(failure_threshold=15, recovery_timeout=30)

        spill_dir = os.path.join(base_dir, '.spill')
        try:
            _cleanup_stale_tmp_files(base_dir, prefix=CONFIG.tmp_file_prefix)
            _cleanup_stale_tmp_files(spill_dir, prefix='raw_html_')
        except Exception as exc:
            logger.warning("启动清理残留文件时出错（不影响正常运行）: {}", exc)

        pool_size = max(CONFIG.pool_maxsize, image_workers + attachment_workers + 4)
        self._http = HttpClient(headers, cookies, pool_connections=pool_size, pool_maxsize=pool_size)
        self._ext = ExtResolver(self._http)
        self._write_bus = WriteBus(num_workers=CONFIG.write_bus_workers)

        self._image_bus = ImageDownloadBus(
            base_dir, self._http, self._ext, self._write_bus,
            num_workers=image_workers,
            queue_cap=CONFIG.image_queue_cap,
            min_bytes=min_file_size_kb * 1024,
            breaker=self.image_breaker,
            stats=self.stats_collector)
        self._attach_bus = AttachmentDownloadBus(
            base_dir, self._http, self._ext, self._write_bus,
            num_workers=attachment_workers,
            queue_cap=CONFIG.attachment_queue_cap,
            min_bytes=min_file_size_kb * 1024,
            breaker=self.attach_breaker,
            stats=self.stats_collector)

        self._parser = ContentParser(
            self._image_bus, self._attach_bus,
            call_budget=call_budget,
            spill_dir=spill_dir)
        self._remark_parser = RemarkParser()

    def update_credentials(self, headers=None, cookies=None, replace=True):
        self._http.update_credentials(headers, cookies, replace)

    @retry_on_network_error()
    def _fetch_raw(self, url: str) -> bytes:
        resp = self._http.get(url, bucket='page')
        logger.info("HTTP {}  {}", resp.status_code, url)
        resp.raise_for_status()
        return resp.content

    def get_xpath(self, url, content_xpath, remark_xpath=None):
        raw = self._fetch_raw(url)
        try:
            detected = from_bytes(raw).best()
            html = str(detected) if detected else raw.decode('utf-8', errors='replace')
        except Exception:
            html = raw.decode('utf-8', errors='replace')
        if not html.strip():
            raise NonRetryableError(f"HTML 内容为空: {url}")
        tree = etree.HTML(html)
        if tree is None:
            raise NonRetryableError(f"etree.HTML() 返回 None: {url}")
        try:
            content_nodes = tree.xpath(content_xpath)
        except etree.XPathError as exc:
            raise NonRetryableError(f"content_xpath 无效: {content_xpath!r}") from exc
        remark_nodes = None
        if remark_xpath:
            try:
                remark_nodes = tree.xpath(remark_xpath)
            except etree.XPathError as exc:
                logger.warning("remark_xpath 无效: {}", exc)
        return content_nodes, remark_nodes

    def parse_nodes(self, nodes, page_url):
        return self._parser.parse_nodes(nodes, page_url)

    def parse_nodes_sync(self, nodes, page_url):
        return self._parser.parse_nodes_sync(nodes, page_url)

    def get_remarks(self, remark_doc):
        return self._remark_parser.parse(remark_doc)

    def download_attachment(self, url, *, attachment_hint=True):
        fut = self._attach_bus.submit(url, uuid.uuid4().hex)
        return _safe_future_result(fut)

    def download_image(self, url):
        fut = self._image_bus.submit(url, uuid.uuid4().hex)
        return _safe_future_result(fut)

    def stats(self):
        base = self.stats_collector.snapshot()
        base['image_bus'] = self._image_bus.stats()
        base['attach_bus'] = self._attach_bus.stats()
        base['write_queue'] = self._write_bus.qsize()
        return base

    def close(self):
        if self._closed:
            return
        self._closed = True
        self._image_bus.stop(CONFIG.stop_timeout_sec)
        self._attach_bus.stop(CONFIG.stop_timeout_sec)
        self._http.close()
        self._write_bus.stop(CONFIG.stop_timeout_sec)

    def __enter__(self): return self
    def __exit__(self, *_): self.close()
