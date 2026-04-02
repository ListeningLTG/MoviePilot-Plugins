from pathlib import Path
from time import sleep, time
from threading import Thread, Lock
from typing import Dict, Any, Optional, Iterator, Callable
from queue import Queue, Empty

from p115client import P115Client, check_response
from p115client.util import complete_url
from p115client.tool.attr import normalize_attr

from app.log import logger
from app.chain.transfer import TransferChain
from app.schemas import FileItem

from .config import configer
from .utils import StrmUrlTemplateResolver


class ShareP115Client(P115Client):
    """
    分享专用 115 客户端，扩展 share/snap 接口
    """

    def share_snap_cookie(
        self,
        payload: dict,
        base_url: str = "https://webapi.115.com",
        **kwargs,
    ) -> dict:
        """
        通过 Cookie 接口获取分享目录列表
        """
        api = complete_url("/share/snap", base_url=base_url)
        payload = {"cid": 0, "limit": 32, "offset": 0, **payload}
        return self.request(url=api, params=payload, **kwargs)


def iter_share_files(
    client: ShareP115Client,
    share_code: str,
    receive_code: str = "",
    cid: int = 0,
    path_prefix: str = "",
) -> Iterator[dict]:
    """
    递归遍历分享链接下的所有文件，附带完整路径前缀
    """
    payload = {
        "share_code": share_code,
        "receive_code": receive_code,
        "cid": cid,
        "limit": 1000,
        "offset": 0,
    }

    resp = client.share_snap_cookie(payload)
    data = check_response(resp).get("data", {})
    items = data.get("list", [])

    for item in items:
        item = normalize_attr(item)
        name = item.get("name", "")
        current_path = f"{path_prefix}/{name}" if path_prefix else f"/{name}"
        if item.get("is_dir"):
            yield from iter_share_files(
                client, share_code, receive_code, int(item["id"]), current_path
            )
        else:
            item["_full_path"] = current_path
            yield item


def process_share_strm(
    share_code: str,
    receive_code: str,
    tmdbid: Optional[int] = None,
    mtype: Optional[str] = None,
    notify: Optional[Callable[[str], None]] = None,
) -> Dict[str, Any]:
    """
    实际执行 STRM 生成逻辑：遍历分享、生成文件、触发 MP 整理
    """
    if not configer.cookies:
        return {"status": False, "msg": "未配置 115 Cookie"}

    if not configer.strm_save_path:
        return {"status": False, "msg": "未配置 STRM 保存路径"}

    try:
        client = ShareP115Client(configer.cookies)
        save_path_obj = Path(configer.strm_save_path)
        save_path_obj.mkdir(parents=True, exist_ok=True)

        media_exts = {
            f".{ext.strip().lower()}"
            for ext in configer.user_rmt_mediaext.split(",")
            if ext.strip()
        }

        resolver = None
        if configer.strm_url_template_enabled and configer.strm_url_template:
            resolver = StrmUrlTemplateResolver(configer.strm_url_template)

        redirect_base = (
            f"{configer.moviepilot_address}/api/v1/plugin/P115StrmHelper/redirect_url"
        )

        strm_count = 0
        total_files = 0
        transfer_chain = TransferChain() if configer.moviepilot_transfer else None

        # 如果提供了 tmdbid 并且开启了整理，尝试提前获取媒体信息以提高入库准确度
        mediainfo = None
        if tmdbid and transfer_chain:
            for i in range(3):
                try:
                    from app.chain.media import MediaChain
                    from app.schemas.types import MediaType
                    # 处理媒体类型
                    media_type = MediaType.from_agent(mtype) if mtype else None
                    mediainfo = MediaChain().recognize_media(tmdbid=tmdbid, mtype=media_type)
                    if mediainfo:
                        logger.info(f"【P115ShareStrm】通过 TMDB ID {tmdbid} ({mtype or '未知类型'}) 提取媒体信息成功: {mediainfo.title_year}")
                        break
                    else:
                        logger.warning(f"【P115ShareStrm】未找到 TMDB ID {tmdbid} ({mtype or '未知类型'}) 对应的媒体信息 (尝试 {i+1}/3)")
                except Exception as e:
                    logger.warning(f"【P115ShareStrm】获取 TMDB 媒体信息时发生异常 (尝试 {i+1}/3): {e}")

                if i < 2:
                    sleep(2)

        for item in iter_share_files(client, share_code, receive_code):
            total_files += 1
            filename = item.get("name", "")
            file_ext = Path(filename).suffix.lower()

            if file_ext not in media_exts:
                continue

            # 保持目录结构
            full_path = item.get("_full_path", f"/{filename}")
            relative_path = Path(full_path.lstrip("/"))
            strm_relative = relative_path.with_suffix(".strm")
            strm_file_path = save_path_obj / strm_relative
            strm_file_path.parent.mkdir(parents=True, exist_ok=True)

            # 生成 STRM URL
            if resolver:
                strm_url = resolver.render(
                    share_code=share_code,
                    receive_code=receive_code,
                    file_id=item["id"],
                    file_name=filename,
                    # file_path: 文件在分享目录内的完整路径，如 /剧名/S01/S01E01.mkv
                    file_path=full_path,
                    base_url=redirect_base,
                )
            else:
                strm_url = (
                    f"{redirect_base}"
                    f"?share_code={share_code}"
                    f"&receive_code={receive_code}"
                    f"&id={item['id']}"
                )

            if not strm_url:
                continue

            strm_file_path.write_text(strm_url, encoding="utf-8")
            strm_count += 1

            if transfer_chain:
                stat = strm_file_path.stat()
                transfer_chain.do_transfer(
                    fileitem=FileItem(
                        storage="local",
                        type="file",
                        path=strm_file_path.as_posix(),
                        name=strm_file_path.name,
                        basename=strm_file_path.stem,
                        extension="strm",
                        size=stat.st_size,
                        modify_time=stat.st_mtime,
                    ),
                    mediainfo=mediainfo
                )

        return {"status": True, "strm_count": strm_count, "total_files": total_files}

    except Exception as e:
        logger.error(f"【P115ShareStrm】逻辑执行异常: {e}", exc_info=True)
        return {"status": False, "msg": str(e)}


class ShareTaskQueue:
    """
    异步任务队列管理器：通过单线程串行处理，避免并发风控
    """

    # 去重时间窗口（秒）：同一 share_code 在此时间内只入队一次
    _DEDUP_WINDOW: float = 10.0

    def __init__(self):
        self._queue: Queue = Queue()
        self._worker_thread: Optional[Thread] = None
        self._lock = Lock()
        self._running = False
        self._notify_callback: Optional[Callable[[Optional[str], str, str], None]] = None
        # 去重缓存：{share_code: 最后入队时间戳}
        self._recent_tasks: Dict[str, float] = {}

    def set_notify_callback(
        self, callback: Callable[[Optional[str], str, str], None]
    ):
        """
        设置通知回调，由 __init__.py 注入，签名: (user_id, title, text) -> None
        """
        self._notify_callback = callback

    def start(self):
        """
        启动工作线程（幂等）
        """
        with self._lock:
            if not self._running:
                self._running = True
                self._worker_thread = Thread(target=self._worker, daemon=True)
                self._worker_thread.start()
                logger.info("【P115ShareStrm】任务队列工作线程已启动")

    def stop(self):
        """
        停止工作线程
        """
        self._running = False

    def add_task(
        self, share_code: str, receive_code: str, user_id: Optional[str] = None, tmdbid: Optional[int] = None, mtype: Optional[str] = None
    ) -> bool:
        """
        向队列添加分享处理任务

        内置去重：同一 share_code 在 _DEDUP_WINDOW 秒内重复入队时直接忽略，
        防止插件热重载导致事件处理器多次注册引发重复处理。

        :return: True 表示成功入队，False 表示被去重过滤
        """
        now = time()
        with self._lock:
            last_time = self._recent_tasks.get(share_code)
            if last_time is not None and now - last_time < self._DEDUP_WINDOW:
                logger.warning(
                    f"【P115ShareStrm】重复任务已忽略（去重窗口 {self._DEDUP_WINDOW}s）: {share_code}"
                )
                return False
            self._recent_tasks[share_code] = now

        self._queue.put((share_code, receive_code, user_id, tmdbid, mtype))
        logger.info(f"【P115ShareStrm】新任务已入队: {share_code}")
        return True

    def _worker(self):
        while self._running:
            try:
                task = self._queue.get(timeout=10)
                share_code, receive_code, user_id, tmdbid, mtype = task

                # 稍作等待，确保主线程"已入队"通知先到达用户，再发"开始处理"
                sleep(0.8)
                self._notify(user_id, "【115分享STRM】", f"🚀 开始处理分享: {share_code}")

                result = process_share_strm(share_code, receive_code, tmdbid=tmdbid, mtype=mtype)

                if result.get("status"):
                    msg = (
                        f"✅ 处理完成: {share_code}\n"
                        f"生成 STRM: {result.get('strm_count')} 个\n"
                        f"遍历文件: {result.get('total_files')} 个"
                    )
                    self._notify(user_id, "【115分享STRM】完成", msg)
                else:
                    self._notify(
                        user_id,
                        "【115分享STRM】失败",
                        f"❌ {share_code}\n原因: {result.get('msg')}",
                    )

                self._queue.task_done()
                sleep(2)

            except Empty:
                continue
            except Exception as e:
                logger.error(f"【P115ShareStrm】工作线程异常: {e}", exc_info=True)
                sleep(5)

    def _notify(self, user_id: Optional[str], title: str, text: str):
        if self._notify_callback:
            try:
                self._notify_callback(user_id, title, text)
            except Exception as e:
                logger.warning(f"【P115ShareStrm】通知发送失败: {e}")


# 全局单例
task_queue = ShareTaskQueue()
