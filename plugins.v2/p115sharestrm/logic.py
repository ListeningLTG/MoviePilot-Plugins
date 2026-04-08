from pathlib import Path
from urllib.parse import quote
from time import sleep, time
from threading import Thread, Lock, Event
from typing import Dict, Any, List, Optional, Iterator, Callable, Tuple
from queue import Queue, Empty
from uuid import uuid4
import json

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


class _EndpointPool:
    """
    轮询多个 115 API 端点，单次请求依次尝试所有端点，成功后自动推进轮询位置。
    """

    def __init__(self, endpoints: List[Tuple[str, Callable]]) -> None:
        # endpoints: list of (name, callable(payload) -> resp)
        self._endpoints = endpoints
        self._idx = 0

    def fetch_page(self, payload: dict, max_retries: int = 5) -> list:
        """
        获取一页数据，轮询端点并在全部失败时退避重试。

        :return: 文件/目录列表
        :raises: 最后一次异常（当所有端点在所有重试中均失败时）
        """
        n = len(self._endpoints)
        last_exc: Optional[Exception] = None

        for attempt in range(max_retries):
            for i in range(n):
                ep_idx = (self._idx + i) % n
                ep_name, ep_func = self._endpoints[ep_idx]
                try:
                    resp = ep_func(payload)
                    data = check_response(resp).get("data", {})
                    # 成功：推进到下一个端点，保持轮询效果
                    self._idx = (ep_idx + 1) % n
                    last_exc = None
                    return data.get("list", [])
                except Exception as e:
                    last_exc = e
                    logger.warning(
                        f"【P115ShareStrm】端点 [{ep_name}] 失败，切换到下一个: {e}"
                    )

            # 当前轮次所有端点均失败，等待后重试
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 2 + (time() % 2)
                logger.warning(
                    f"【P115ShareStrm】所有端点均失败 (cid={payload.get('cid')}, "
                    f"offset={payload.get('offset')})，"
                    f"第 {attempt + 1}/{max_retries} 次重试 (等待 {wait_time:.1f}s)"
                )
                sleep(wait_time)

        raise last_exc


def _make_endpoint_pool(client: ShareP115Client) -> _EndpointPool:
    """
    构建三端点轮询池：
      1. https://proapi.115.com  (App 接口)
      2. http://pro.api.115.com  (App 接口，HTTP)
      3. https://webapi.115.com  (Cookie 接口)
    """
    return _EndpointPool([
        (
            "share_snap_app_https",
            lambda p: client.share_snap_app(p, base_url="https://proapi.115.com", timeout=(10, 60)),
        ),
        (
            "share_snap_app_http",
            lambda p: client.share_snap_app(p, base_url="http://pro.api.115.com", timeout=(10, 60)),
        ),
        (
            "share_snap_cookie",
            lambda p: client.share_snap_cookie(p, timeout=(10, 60)),
        ),
    ])


def iter_share_files(
    client: ShareP115Client,
    share_code: str,
    receive_code: str = "",
    cid: int = 0,
    path_prefix: str = "",
    max_retries: int = 5,
    _pool: Optional[_EndpointPool] = None,
) -> Iterator[dict]:
    """
    递归遍历分享链接下的所有文件，轮询三个 115 API 端点并自动容错切换。

    端点优先级（轮询）：
      proapi.115.com (App/HTTPS) → pro.api.115.com (App/HTTP) → webapi.115.com (Cookie)
    """
    if _pool is None:
        _pool = _make_endpoint_pool(client)

    offset = 0
    limit = 1000
    while True:
        payload = {
            "share_code": share_code,
            "receive_code": receive_code,
            "cid": cid,
            "limit": limit,
            "offset": offset,
        }

        try:
            items = _pool.fetch_page(payload, max_retries=max_retries)
        except Exception as e:
            logger.error(f"【P115ShareStrm】请求分享列表失败，已达最大重试次数: {e}")
            raise

        if not items:
            break

        for item in items:
            item = normalize_attr(item)
            name = item.get("name", "")
            current_path = f"{path_prefix}/{name}" if path_prefix else f"/{name}"
            if item.get("is_dir"):
                # 递归进入子目录，共享同一轮询池
                yield from iter_share_files(
                    client, share_code, receive_code, int(item["id"]), current_path,
                    max_retries, _pool,
                )
            else:
                item["_full_path"] = current_path
                yield item

        offset += limit
        # 如果当前页返回的数据少于 limit，说明已经是最后一页
        if len(items) < limit:
            break


def _download_subtitles_from_share(
    client: ShareP115Client,
    share_code: str,
    receive_code: str,
    subtitle_items: List[dict],
    save_path_obj: Path,
) -> Tuple[List[Path], int]:
    """
    直接通过 share_download_url_app 获取分享字幕文件的下载链接并下载到本地。
    不需要转存到网盘，避免 fs_video_subtitle 只适用于视频文件的问题。

    :return: (成功下载的本地路径列表, 失败数量)
    """
    import httpx

    _download_headers = {
        "User-Agent": (
            "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) "
            "AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148"
        ),
    }

    downloaded_paths: List[Path] = []
    fail_count = 0

    for item in subtitle_items:
        filename = item.get("name", "")
        full_path = item.get("_full_path", f"/{filename}")
        local_path = save_path_obj / Path(full_path.lstrip("/"))

        _max_retries = 3
        _dl_success = False
        for _attempt in range(_max_retries):
            try:
                url_obj = client.share_download_url(
                    {
                        "file_id": item["id"],
                        "share_code": share_code,
                        "receive_code": receive_code,
                    },
                    timeout=30,
                )
                resp_dl = httpx.get(
                    str(url_obj),
                    headers=_download_headers,
                    timeout=30,
                    follow_redirects=True,
                )
                resp_dl.raise_for_status()
                local_path.parent.mkdir(parents=True, exist_ok=True)
                local_path.write_bytes(resp_dl.content)
                logger.info(f"【P115ShareStrm】字幕下载成功: {filename}")
                downloaded_paths.append(local_path)
                _dl_success = True
                break
            except Exception as e:
                logger.warning(
                    f"【P115ShareStrm】字幕下载失败 ({_attempt + 1}/{_max_retries}): "
                    f"{filename}: {e}"
                )
                if _attempt < _max_retries - 1:
                    sleep(2)

        if not _dl_success:
            fail_count += 1

    return downloaded_paths, fail_count


def _resolve_mtype_by_tmdb_names(tmdbid: int, arg_str: str) -> Optional[str]:
    """
    当只有 tmdbid 但 mtype 未知时，同时查询 TMDB 电影和TV，
    将两者的全部候选名称与消息原文做匹配，以此推断媒体类型。

    返回 "tv" / "movie"，或 None（无法判断时）。
    """
    import re
    try:
        from app.modules.themoviedb.tmdbapi import TmdbApi
        from app.schemas.types import MediaType

        tmdb = TmdbApi()
        movie_info = tmdb.get_info(mtype=MediaType.MOVIE, tmdbid=tmdbid)
        tv_info = tmdb.get_info(mtype=MediaType.TV, tmdbid=tmdbid)

        def _collect_names(info: Optional[dict]) -> List[str]:
            if not info:
                return []
            names = []
            for key in ("title", "original_title", "name", "original_name"):
                v = info.get(key)
                if v and isinstance(v, str):
                    names.append(v)
            aka = info.get("also_known_as") or []
            if isinstance(aka, list):
                names.extend(n for n in aka if n and isinstance(n, str))
            return names

        def _matches(names: List[str]) -> bool:
            for name in names:
                if not name:
                    continue
                try:
                    if re.search(re.escape(name), arg_str, re.IGNORECASE):
                        return True
                except re.error:
                    if name in arg_str:
                        return True
            return False

        movie_hit = _matches(_collect_names(movie_info))
        tv_hit = _matches(_collect_names(tv_info))

        logger.info(
            f"【P115ShareStrm】TMDB名称匹配结果 tmdbid={tmdbid}: movie_hit={movie_hit}, tv_hit={tv_hit}"
        )

        if tv_hit and not movie_hit:
            return "tv"
        if movie_hit and not tv_hit:
            return "movie"
        # 两者都命中或都未命中，无法判断
        return None
    except Exception as e:
        logger.warning(f"【P115ShareStrm】TMDB名称匹配异常: {e}")
        return None


def process_share_strm(
    share_code: str,
    receive_code: str,
    tmdbid: Optional[int] = None,
    mtype: Optional[str] = None,
    arg_str: Optional[str] = None,
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
        if configer.strm_url_template_enabled and (
            configer.strm_url_template or configer.strm_url_template_custom
        ):
            resolver = StrmUrlTemplateResolver(
                base_template=configer.strm_url_template or None,
                custom_rules=configer.strm_url_template_custom or None,
            )

        redirect_base = (
            f"{configer.moviepilot_address}/api/v1/plugin/P115StrmHelper/redirect_url"
        )

        strm_count = 0
        media_files = []

        # 构建字幕文件后缀集合（按需）
        subtitle_exts: set = set()
        if configer.download_subtitle:
            subtitle_exts = {
                f".{ext.strip().lower()}"
                for ext in configer.user_subtitle_ext.split(",")
                if ext.strip()
            }

        # 1. 第一步：递归扫描获取分享中的所有目标媒体文件和字幕文件
        logger.info(f"【P115ShareStrm】正在扫描分享内容: {share_code} ...")
        subtitle_files: List[dict] = []
        for item in iter_share_files(client, share_code, receive_code):
            filename = item.get("name", "")
            file_ext = Path(filename).suffix.lower()
            if file_ext in media_exts:
                media_files.append(item)
            elif subtitle_exts and file_ext in subtitle_exts:
                subtitle_files.append(item)

        total_media = len(media_files)
        logger.info(f"【P115ShareStrm】扫描完成，匹配到 {total_media} 个媒体文件")

        # 2. 第二步：提前识别媒体信息（如有提供 ID）
        mediainfo = None

        # 当有 tmdbid 但 mtype 未知时，尝试通过 TMDB 名称匹配推断类型
        if tmdbid and not mtype and arg_str and configer.moviepilot_transfer:
            inferred = _resolve_mtype_by_tmdb_names(tmdbid, arg_str)
            if inferred:
                logger.info(f"【P115ShareStrm】通过TMDB名称匹配推断媒体类型: {inferred}")
                mtype = inferred

        if tmdbid and mtype and configer.moviepilot_transfer:
            for i in range(3):
                try:
                    from app.chain.media import MediaChain
                    from app.schemas.types import MediaType
                    media_type = MediaType.from_agent(mtype) if mtype else None
                    mediainfo = MediaChain().recognize_media(tmdbid=tmdbid, mtype=media_type)
                    if mediainfo:
                        logger.info(f"【P115ShareStrm】提前识别成功: {mediainfo.title_year}")
                        break
                    else:
                        logger.warning(f"【P115ShareStrm】识别任务未返回信息 (尝试 {i+1}/3)")
                except Exception as e:
                    logger.warning(f"【P115ShareStrm】提前识别异常 (尝试 {i+1}/3): {e}")
                if i < 2:
                    sleep(2)

        # 3. 第三步：生成所有 STRM 文件，收集待整理路径
        generated_strm_paths: List[Path] = []
        for i, item in enumerate(media_files):
            filename = item.get("name", "")
            if (i + 1) % 10 == 0 or (i + 1) == total_media:
                logger.info(f"【P115ShareStrm】处理进度 ({i+1}/{total_media}): {filename}")

            full_path = item.get("_full_path", f"/{filename}")
            relative_path = Path(full_path.lstrip("/"))
            strm_relative = relative_path.with_suffix(".strm")

            # 扩展名特定规则可能指定不同的保存目录
            if resolver:
                path_override = resolver.get_save_path_override(filename)
                effective_save_path = Path(path_override) if path_override else save_path_obj
            else:
                effective_save_path = save_path_obj

            strm_file_path = effective_save_path / strm_relative
            strm_file_path.parent.mkdir(parents=True, exist_ok=True)

            # 生成 STRM URL
            if resolver:
                strm_url = resolver.render(
                    file_name=filename,
                    share_code=share_code,
                    receive_code=receive_code,
                    file_id=item["id"],
                    file_path=full_path,
                    base_url=redirect_base,
                )
            else:
                strm_url = (
                    f"{redirect_base}"
                    f"?share_code={share_code}"
                    f"&receive_code={receive_code}"
                    f"&id={item['id']}"
                    f"&file_name={quote(filename)}"
                )

            if not strm_url:
                continue

            strm_file_path.write_text(strm_url, encoding="utf-8")
            strm_count += 1
            if configer.moviepilot_transfer:
                generated_strm_paths.append(strm_file_path)

        # 4. 第四步：全部生成完毕后，批量交由 MoviePilot 整理 STRM
        transfer_chain: Optional[TransferChain] = None
        if generated_strm_paths and configer.moviepilot_transfer:
            logger.info(f"【P115ShareStrm】开始批量整理 STRM，共 {len(generated_strm_paths)} 个文件")
            transfer_chain = TransferChain()
            for strm_file_path in generated_strm_paths:
                try:
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
                        mediainfo=mediainfo,
                    )
                except Exception as e:
                    logger.warning(f"【P115ShareStrm】STRM 整理失败: {strm_file_path.name}: {e}")
            logger.info("【P115ShareStrm】STRM 批量整理完成")

        # 5. 第五步：下载字幕文件并整理（如开启）
        subtitle_count = 0
        subtitle_fail_count = 0
        if configer.download_subtitle and subtitle_files:
            logger.info(f"【P115ShareStrm】开始下载字幕文件，共 {len(subtitle_files)} 个")
            downloaded_subtitle_paths, subtitle_fail_count = _download_subtitles_from_share(
                client, share_code, receive_code, subtitle_files, save_path_obj
            )
            subtitle_count = len(downloaded_subtitle_paths)
            logger.info(
                f"【P115ShareStrm】字幕下载完成，成功 {subtitle_count} 个，失败 {subtitle_fail_count} 个"
            )
            if downloaded_subtitle_paths and configer.moviepilot_transfer:
                if transfer_chain is None:
                    transfer_chain = TransferChain()
                for subtitle_path in downloaded_subtitle_paths:
                    try:
                        stat = subtitle_path.stat()
                        transfer_chain.do_transfer(
                            fileitem=FileItem(
                                storage="local",
                                type="file",
                                path=subtitle_path.as_posix(),
                                name=subtitle_path.name,
                                basename=subtitle_path.stem,
                                extension=subtitle_path.suffix.lstrip(".").lower(),
                                size=stat.st_size,
                                modify_time=stat.st_mtime,
                            ),
                            mediainfo=mediainfo,
                        )
                    except Exception as e:
                        logger.warning(f"【P115ShareStrm】字幕整理失败: {subtitle_path.name}: {e}")

        return {
            "status": True,
            "strm_count": strm_count,
            "total_files": total_media,
            "subtitle_count": subtitle_count,
            "subtitle_fail_count": subtitle_fail_count,
        }

    except Exception as e:
        logger.error(f"【P115ShareStrm】逻辑执行异常: {e}", exc_info=True)
        return {"status": False, "msg": str(e)}


class ShareTaskQueue:
    """
    异步任务队列管理器：通过单线程串行处理，避免并发风控。
    支持 JSON 文件持久化，MP/插件重启后自动恢复未完成任务。
    """

    # 去重时间窗口（秒）：同一 share_code 在此时间内只入队一次
    _DEDUP_WINDOW: float = 10.0
    # 单任务最大重试次数（超过后从持久化文件移除，不再重试）
    _MAX_RETRY: int = 3

    def __init__(self):
        self._queue: Queue = Queue()
        self._worker_thread: Optional[Thread] = None
        self._lock = Lock()
        self._running = False
        self._processing_count: int = 0  # 当前正在处理中的任务数
        self._notify_callback: Optional[Callable[[Optional[str], str, str], None]] = None
        # 去重缓存：{share_code: 最后入队时间戳}
        self._recent_tasks: Dict[str, float] = {}
        # 持久化文件路径，在 start() 中通过 settings 初始化
        self._persist_path: Optional[Path] = None

    # ── 持久化 ──────────────────────────────────────────────

    def _get_persist_path(self) -> Path:
        """懒加载持久化文件路径（避免模块加载时 settings 尚未就绪）"""
        if self._persist_path is None:
            from app.core.config import settings
            data_dir = Path(settings.PLUGIN_DATA_PATH) / "P115ShareStrm"
            data_dir.mkdir(parents=True, exist_ok=True)
            self._persist_path = data_dir / "pending_tasks.json"
        return self._persist_path

    def _load_tasks(self) -> List[Dict]:
        """从 JSON 文件加载持久化任务列表，文件不存在或损坏时返回空列表"""
        path = self._get_persist_path()
        try:
            if path.exists():
                return json.loads(path.read_text(encoding="utf-8"))
        except Exception as e:
            logger.warning(f"【P115ShareStrm】读取持久化任务失败，将重置: {e}")
        return []

    def _save_tasks(self, tasks: List[Dict]) -> None:
        """原子写入：先写 .tmp 再 rename，防止崩溃导致文件损坏"""
        path = self._get_persist_path()
        tmp = path.with_suffix(".tmp")
        try:
            tmp.write_text(json.dumps(tasks, ensure_ascii=False, indent=2), encoding="utf-8")
            tmp.replace(path)
        except Exception as e:
            logger.warning(f"【P115ShareStrm】写入持久化任务失败: {e}")

    def _persist_add(self, task_id: str, share_code: str, receive_code: str,
                     user_id: Optional[str], tmdbid: Optional[int], mtype: Optional[str],
                     arg_str: Optional[str] = None) -> None:
        """向持久化文件追加一条任务（需在 _lock 内调用）"""
        tasks = self._load_tasks()
        tasks.append({
            "task_id": task_id,
            "share_code": share_code,
            "receive_code": receive_code,
            "user_id": user_id,
            "tmdbid": tmdbid,
            "mtype": mtype,
            "arg_str": arg_str,
            "added_at": time(),
            "retry_count": 0,
        })
        self._save_tasks(tasks)

    def _persist_remove(self, task_id: str) -> None:
        """从持久化文件移除已完成/放弃的任务"""
        with self._lock:
            tasks = self._load_tasks()
            tasks = [t for t in tasks if t.get("task_id") != task_id]
            self._save_tasks(tasks)

    def _persist_increment_retry(self, task_id: str) -> int:
        """将任务重试次数 +1，返回更新后的重试次数"""
        with self._lock:
            tasks = self._load_tasks()
            count = 0
            for t in tasks:
                if t.get("task_id") == task_id:
                    t["retry_count"] = t.get("retry_count", 0) + 1
                    count = t["retry_count"]
                    break
            self._save_tasks(tasks)
        return count

    # ── 公共接口 ─────────────────────────────────────────────

    def set_notify_callback(
        self, callback: Callable[[Optional[str], str, str], None]
    ):
        """设置通知回调，由 __init__.py 注入，签名: (user_id, title, text) -> None"""
        self._notify_callback = callback

    def start(self):
        """
        启动工作线程（幂等）。
        启动前先从持久化文件恢复未完成任务，重新入队。
        """
        with self._lock:
            if not self._running:
                self._running = True
                self._worker_thread = Thread(target=self._worker, daemon=True)
                self._worker_thread.start()
                logger.info("【P115ShareStrm】任务队列工作线程已启动")

        # 恢复持久化任务（在 _lock 外执行，避免死锁）
        self._restore_persisted_tasks()

    def stop(self):
        """停止工作线程"""
        self._running = False

    def add_task(
        self,
        share_code: str,
        receive_code: str,
        user_id: Optional[str] = None,
        tmdbid: Optional[int] = None,
        mtype: Optional[str] = None,
        arg_str: Optional[str] = None,
        _task_id: Optional[str] = None,
        _skip_dedup: bool = False,
    ) -> bool:
        """
        向队列添加分享处理任务。

        内置去重：同一 share_code 在 _DEDUP_WINDOW 秒内重复入队时直接忽略。
        _task_id / _skip_dedup 供内部恢复任务使用，外部调用无需传入。

        :return: True 表示成功入队，False 表示被去重过滤
        """
        now = time()
        task_id = _task_id or str(uuid4())

        with self._lock:
            if not _skip_dedup:
                last_time = self._recent_tasks.get(share_code)
                if last_time is not None and now - last_time < self._DEDUP_WINDOW:
                    logger.warning(
                        f"【P115ShareStrm】重复任务已忽略（去重窗口 {self._DEDUP_WINDOW}s）: {share_code}"
                    )
                    return False
                self._recent_tasks[share_code] = now
                # 新任务才写持久化，恢复任务已在文件中
                self._persist_add(task_id, share_code, receive_code, user_id, tmdbid, mtype, arg_str)

        self._queue.put((task_id, share_code, receive_code, user_id, tmdbid, mtype, arg_str))
        logger.info(f"【P115ShareStrm】新任务已入队: {share_code} (id={task_id})")
        return True

    def _restore_persisted_tasks(self) -> None:
        """从持久化文件恢复未完成任务，重新放入内存队列"""
        tasks = self._load_tasks()
        if not tasks:
            return
        logger.info(f"【P115ShareStrm】从持久化文件恢复 {len(tasks)} 个待处理任务")
        for t in tasks:
            if t.get("retry_count", 0) >= self._MAX_RETRY:
                logger.warning(
                    f"【P115ShareStrm】任务已达最大重试次数，跳过: {t.get('share_code')} (id={t.get('task_id')})"
                )
                self._persist_remove(t["task_id"])
                continue
            self._queue.put((
                t["task_id"],
                t["share_code"],
                t["receive_code"],
                t.get("user_id"),
                t.get("tmdbid"),
                t.get("mtype"),
                t.get("arg_str"),
            ))
            logger.info(f"【P115ShareStrm】已恢复任务: {t['share_code']} (重试次数: {t.get('retry_count', 0)})")

    # ── 工作线程 ──────────────────────────────────────────────

    def _worker(self):
        while self._running:
            try:
                task = self._queue.get(timeout=10)
                task_id, share_code, receive_code, user_id, tmdbid, mtype, arg_str = task

                # 稍作等待，确保"已入队"通知先到达用户
                sleep(0.8)
                self._processing_count += 1
                self._notify(user_id, "【115分享STRM】", f"🚀 开始处理分享: {share_code}")

                result = process_share_strm(share_code, receive_code, tmdbid=tmdbid, mtype=mtype, arg_str=arg_str)

                if result.get("status"):
                    msg = (
                        f"✅ 处理完成: {share_code}\n"
                        f"生成 STRM: {result.get('strm_count')} 个\n"
                        f"遍历文件: {result.get('total_files')} 个"
                    )
                    if result.get("subtitle_count") or result.get("subtitle_fail_count"):
                        msg += f"\n下载字幕: {result.get('subtitle_count', 0)} 个"
                        if result.get("subtitle_fail_count"):
                            msg += f"，失败 {result.get('subtitle_fail_count')} 个"
                    self._notify(user_id, "【115分享STRM】完成", msg)
                    # 成功：从持久化文件移除
                    self._persist_remove(task_id)
                else:
                    retry_count = self._persist_increment_retry(task_id)
                    if retry_count >= self._MAX_RETRY:
                        logger.error(
                            f"【P115ShareStrm】任务失败且已达最大重试次数 ({self._MAX_RETRY})，放弃: {share_code}"
                        )
                        self._persist_remove(task_id)
                        self._notify(
                            user_id,
                            "【115分享STRM】放弃",
                            f"❌ {share_code}\n已重试 {self._MAX_RETRY} 次，放弃处理\n原因: {result.get('msg')}",
                        )
                    else:
                        self._notify(
                            user_id,
                            "【115分享STRM】失败",
                            f"❌ {share_code}\n原因: {result.get('msg')}\n将在重启后自动重试 (第 {retry_count}/{self._MAX_RETRY} 次)",
                        )

                self._queue.task_done()
                self._processing_count = max(0, self._processing_count - 1)
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
