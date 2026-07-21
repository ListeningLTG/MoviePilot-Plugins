"""115 API 限速与风控指标（移植自 p115strmhelper utils/limiter）。"""

from __future__ import annotations

from contextlib import contextmanager
from threading import Lock
from time import monotonic, sleep, time
from typing import Callable, Iterator, Optional, Tuple

from app.log import logger


class RateLimiter:
    """精确控制 QPS。"""

    def __init__(self, qps: float):
        if qps <= 0:
            qps = float("inf")
        self.interval = 1.0 / qps
        self.lock = Lock()
        self.next_call_time = monotonic()

    def acquire(self) -> None:
        sleep_duration = 0.0
        with self.lock:
            now = monotonic()
            sleep_duration = self.next_call_time - now
            self.next_call_time = max(now, self.next_call_time) + self.interval
        if sleep_duration > 0:
            sleep(sleep_duration)


class ApiEndpointCooldown:
    """独立冷却时间与线程锁的 API 端点。"""

    def __init__(self, api_callable: Callable, cooldown: float, name: str = ""):
        self.api_callable = api_callable
        self.cooldown = max(0.0, float(cooldown))
        self.name = name or getattr(api_callable, "__name__", "api")
        self.lock = Lock()
        self.last_call_time = monotonic() - self.cooldown

    def __call__(self, payload: dict) -> dict:
        if self.cooldown > 0:
            sleep_duration = 0.0
            with self.lock:
                now = monotonic()
                elapsed = now - self.last_call_time
                if elapsed < self.cooldown:
                    sleep_duration = self.cooldown - elapsed
            if sleep_duration > 0:
                sleep(sleep_duration)
            with self.lock:
                self.last_call_time = monotonic()
        return self.api_callable(payload)


# speed_mode -> (app_http, app_https, cookie_api) 秒，与 p115strmhelper core/p115.py 一致
SPEED_MODE_COOLDOWNS: dict[int, Tuple[float, float, float]] = {
    0: (0.25, 0.25, 0.75),
    1: (0.5, 0.5, 1.5),
    2: (1.0, 1.0, 2.0),
    3: (1.5, 1.5, 2.0),
}


def resolve_speed_mode(mode: Optional[int]) -> int:
    try:
        m = int(mode if mode is not None else 3)
    except (TypeError, ValueError):
        m = 3
    return max(0, min(3, m))


def get_speed_cooldowns(mode: Optional[int]) -> Tuple[float, float, float]:
    return SPEED_MODE_COOLDOWNS.get(resolve_speed_mode(mode), SPEED_MODE_COOLDOWNS[3])


class ShareRateLimitedError(Exception):
    """115 WAF 405 / 访问被阻断。"""


class ShareReceiveLimitedError(Exception):
    """115 share_receive 限制接收 errno 4200041。"""


_WAF_405_MARKERS = ("405", "访问被阻断", "安全威胁", "Method Not Allowed")


def is_waf_405(exc: BaseException) -> bool:
    text = str(exc)
    if any(m in text for m in _WAF_405_MARKERS):
        return True
    for arg in getattr(exc, "args", ()) or ():
        if isinstance(arg, dict):
            text += str(arg)
        else:
            text += str(arg)
    return any(m in text for m in _WAF_405_MARKERS)


def is_receive_limited(exc: BaseException) -> bool:
    text = str(exc)
    return "4200041" in text or "限制接收" in text


class ShareApiMetrics:
    """115 接口调用指标。"""

    def __init__(self) -> None:
        self._lock = Lock()
        self.snap_calls_total = 0
        self.snap_calls_last_task = 0
        self.waf_405_count = 0
        self.scan_cache_hits = 0
        self.last_405_at: Optional[float] = None
        self.last_task_snap_calls = 0

    def reset_task_counters(self) -> None:
        with self._lock:
            self.snap_calls_last_task = 0

    def record_snap_call(self) -> None:
        with self._lock:
            self.snap_calls_total += 1
            self.snap_calls_last_task += 1

    def record_405(self) -> None:
        with self._lock:
            self.waf_405_count += 1
            self.last_405_at = time()

    def record_cache_hit(self) -> None:
        with self._lock:
            self.scan_cache_hits += 1

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "snap_calls_total": self.snap_calls_total,
                "snap_calls_last_task": self.snap_calls_last_task,
                "waf_405_count": self.waf_405_count,
                "scan_cache_hits": self.scan_cache_hits,
                "last_405_at": self.last_405_at,
            }


_global_api_lock = Lock()
_waf_backoff_attempt = 0
api_metrics = ShareApiMetrics()


@contextmanager
def global_api_guard() -> Iterator[None]:
    """主 worker 与字幕后台线程串行访问 115 API。"""
    _global_api_lock.acquire()
    try:
        yield
    finally:
        _global_api_lock.release()


def handle_waf_405() -> None:
    global _waf_backoff_attempt
    api_metrics.record_405()
    _waf_backoff_attempt += 1
    wait = min(600, 60 * (2 ** min(_waf_backoff_attempt - 1, 3)))
    logger.warning(
        f"【P115ShareStrm】检测到 115 WAF 405 风控，退避 {wait}s 后中止本次请求 "
        f"(累计 405: {api_metrics.waf_405_count})"
    )
    sleep(wait)
    raise ShareRateLimitedError("115 访问被阻断 (405)，请稍后重试")


def reset_waf_backoff() -> None:
    global _waf_backoff_attempt
    _waf_backoff_attempt = 0


def call_protected_api(func: Callable, *args, **kwargs):
    """在全局锁内调用 115 API，405 时长退避。"""
    with global_api_guard():
        try:
            result = func(*args, **kwargs)
            reset_waf_backoff()
            return result
        except Exception as e:
            if is_waf_405(e):
                handle_waf_405()
            raise
