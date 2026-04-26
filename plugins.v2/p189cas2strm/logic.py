import json
import os
import re
import base64
import asyncio
from urllib.parse import quote
from typing import Dict, Any, List, Optional, Callable, Tuple
from time import time as now_time, sleep
from threading import Thread, Lock, Event
from queue import Queue, Empty
from pathlib import Path

from fastapi import Query, Response
from fastapi.responses import RedirectResponse

from app.log import logger
from app.chain.transfer import TransferChain
from app.schemas import FileItem
from .config import configer
from .p189_client import P189ClientWrapper
from .utils import extract_tmdb_info

# --- API Router 定义 ---

async def cas_redirect(c: str = Query(..., description="Base64 encoded CAS info")):
    """
    按需秒传重定向 API
    """
    try:
        logger.info("【P189Cas2Strm】收到 STRM 播控请求，开始解析元数据...")
        raw_c = (c or "").strip().replace(" ", "+")
        padded_c = raw_c + ("=" * ((4 - len(raw_c) % 4) % 4))
        try:
            try:
                cas_json = base64.b64decode(padded_c).decode("utf-8")
            except Exception:
                cas_json = base64.urlsafe_b64decode(padded_c).decode("utf-8")
            cas_data = json.loads(cas_json)
        except Exception as de:
            logger.error(f"【P189Cas2Strm】CAS 参数解码失败: {de}; c_prefix={raw_c[:48]}")
            return Response(content="Invalid CAS payload", status_code=400)

        filename = cas_data.get('name')
        size = cas_data.get('size')
        md5 = cas_data.get('md5')
        slice_md5 = cas_data.get('sliceMd5')

        logger.info(f"【P189Cas2Strm】任务解析: 文件={filename}, 大小={size}, MD5={md5}")

        if not all([filename, size, md5, slice_md5]):
            logger.error(f"【P189Cas2Strm】无效的 CAS 数据块: {cas_data}")
            return Response(content="Invalid CAS data", status_code=400)

        client = P189ClientWrapper(
            configer.username,
            configer.password,
            cookie_store_path=configer.cookie_store_path,
        )
        await client.ensure_logged_in()

        candidate_ids: List[str] = []
        cached_file_id = cas_record_manager.get(str(md5))
        if cached_file_id:
            candidate_ids.append(str(cached_file_id))
            logger.info(f"【P189Cas2Strm】命中复用记录 fileId={cached_file_id} md5={md5}")

        download_url = None
        tried = set()
        for fid in candidate_ids:
            if fid in tried:
                continue
            tried.add(fid)
            download_url = await client.get_media_play_url(fid)
            if download_url:
                logger.info(f"【P189Cas2Strm】复用 fileId 获取下载链接成功 fileId={fid}")
                break

        if not download_url:
            lock = _redirect_upload_locks.setdefault(str(md5), asyncio.Lock())
            async with lock:
                # 再查一次，避免并发请求重复秒传
                latest_file_id = cas_record_manager.get(str(md5))
                if latest_file_id and str(latest_file_id) not in tried:
                    download_url = await client.get_media_play_url(str(latest_file_id))
                    if download_url:
                        logger.info(f"【P189Cas2Strm】并发复用成功 fileId={latest_file_id} md5={md5}")

                if not download_url:
                    target_root_id = await client.fs_get_path_id(configer.p189_target_path)
                    if not target_root_id:
                        logger.error(f"【P189Cas2Strm】无法定位或创建远程路径: {configer.p189_target_path}")
                        return Response(content="Failed to access target root", status_code=500)

                    timestamp = int(now_time() * 1000)
                    timestamp_folder_id = await client.fs_mkdir(target_root_id, str(timestamp))
                    logger.info(f"【P189Cas2Strm】准备在临时目录 {timestamp} 中执行秒传...")

                    success = await client.rapid_upload(timestamp_folder_id, filename, size, md5, slice_md5)
                    if not success:
                        logger.error(f"【P189Cas2Strm】秒传执行失败，请检查网盘空间或账号状态: {filename}")
                        return Response(content="Rapid upload failed", status_code=500)

                    list_file_id = None
                    for attempt in range(1, 6):
                        listing = client.client.fs_list(timestamp_folder_id)
                        files = listing.get('fileListAO', {}).get('fileList', []) or listing.get('fileList', [])
                        for f in files:
                            if f.get('name') == filename:
                                list_file_id = f.get('id') or f.get('fileId')
                                if list_file_id:
                                    break
                        if list_file_id:
                            logger.info(f"【P189Cas2Strm】列表命中上传文件 fileId={list_file_id} attempt={attempt}")
                            break
                        await asyncio.sleep(0.6)

                    if list_file_id:
                        list_file_id = str(list_file_id)
                        cas_record_manager.add(str(md5), list_file_id)
                        if list_file_id not in tried:
                            download_url = await client.get_media_play_url(list_file_id)

        if not download_url:
            logger.error(f"【P189Cas2Strm】获取下载链接失败，candidate_file_ids={candidate_ids}")
            return Response(content="Failed to get download URL", status_code=500)

        redirect_url = download_url
        if getattr(configer, "play_proxy_enabled", False):
            proxy_prefix = str(getattr(configer, "play_proxy_prefix", "") or "").strip()
            if proxy_prefix:
                if not proxy_prefix.startswith("http://") and not proxy_prefix.startswith("https://"):
                    proxy_prefix = f"https://{proxy_prefix}"

                encoded_url = quote(download_url, safe="")
                if "{url}" in proxy_prefix:
                    redirect_url = proxy_prefix.replace("{url}", encoded_url)
                elif "?url=" in proxy_prefix or "&url=" in proxy_prefix:
                    redirect_url = f"{proxy_prefix}{encoded_url}"
                elif proxy_prefix.endswith("/") and "?" not in proxy_prefix:
                    # 兼容 Worker 路径模式: https://worker.domain/<source_url>
                    redirect_url = f"{proxy_prefix}{download_url}"
                else:
                    connector = "&" if "?" in proxy_prefix else "?"
                    redirect_url = f"{proxy_prefix}{connector}url={encoded_url}"

        logger.info(f"【P189Cas2Strm】重定向成功 -> {redirect_url[:50]}...")
        return RedirectResponse(url=redirect_url)
    except Exception as e:
        logger.error(f"【P189Cas2Strm】重定向接口全局异常: {e}", exc_info=True)
        return Response(content=str(e), status_code=500)

# --- 记录管理类 ---
_redirect_upload_locks: Dict[str, asyncio.Lock] = {}

class CasRecordManager:
    def __init__(self, record_path: str):
        self.record_path = record_path
        self.records: Dict[str, str] = {}
        self.load()

    def load(self):
        if os.path.exists(self.record_path):
            try:
                with open(self.record_path, 'r', encoding='utf-8') as f:
                    self.records = json.load(f)
            except Exception as e:
                logger.error(f"【P189Cas2Strm】加载复用记录失败: {e}")

    def save(self):
        try:
            with open(self.record_path, 'w', encoding='utf-8') as f:
                json.dump(self.records, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"【P189Cas2Strm】同步复用记录失败: {e}")

    def get(self, md5: str) -> Optional[str]:
        return self.records.get(md5)

    def add(self, md5: str, file_id: str):
        self.records[md5] = file_id
        self.save()

    def clear(self):
        self.records = {}
        self.save()

class AsyncRunner:
    """独立常驻事件循环运行器。"""

    def __init__(self):
        self._thread: Optional[Thread] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._ready = Event()
        self._lock = Lock()

    def start(self):
        with self._lock:
            if self._thread and self._thread.is_alive() and self._loop and not self._loop.is_closed():
                return

            self._ready.clear()

            def _run_loop():
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                self._loop = loop
                self._ready.set()
                try:
                    loop.run_forever()
                finally:
                    pending = [t for t in asyncio.all_tasks(loop) if not t.done()]
                    for t in pending:
                        t.cancel()
                    if pending:
                        loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
                    loop.close()

            self._thread = Thread(target=_run_loop, daemon=True, name="p189cas2strm-async-runner")
            self._thread.start()

        if not self._ready.wait(timeout=3):
            raise RuntimeError("async runner start timeout")

    def submit(self, coro, timeout: Optional[float] = None):
        self.start()
        loop = self._loop
        if not loop or loop.is_closed():
            raise RuntimeError("async runner loop unavailable")

        fut = asyncio.run_coroutine_threadsafe(coro, loop)
        if timeout is None:
            return fut.result()
        return fut.result(timeout=timeout)

    def stop(self):
        with self._lock:
            loop = self._loop
            thread = self._thread
            self._loop = None
            self._thread = None

        if loop and not loop.is_closed():
            loop.call_soon_threadsafe(loop.stop)
        if thread and thread.is_alive():
            thread.join(timeout=3)


# --- 任务队列类 (Thread 增强版) ---
class ShareTaskQueue:
    _DEDUP_WINDOW = 60

    def __init__(self):
        self._queue = Queue()
        self._running = False
        self._worker_thread = None
        self._lock = Lock()
        self._processing_count = 0
        self._notify_callback = None
        self._recent_tasks = {}
        self._task_file = os.path.join(configer.plugin_data_path, "tasks.json")
        self._runner = AsyncRunner()

    def set_notify_callback(self, callback: Callable):
        self._notify_callback = callback

    def start(self):
        with self._lock:
            if not self._running:
                self._running = True
                self._runner.start()
                self._worker_thread = Thread(target=self._worker, daemon=True)
                self._worker_thread.start()
                logger.info("【P189Cas2Strm】任务队列工作线程已启动 (Thread + AsyncRunner 模式)")
                self._load_pending_tasks()

    def stop(self):
        self._running = False
        self._runner.stop()
        try:
            while True:
                self._queue.get_nowait()
                self._queue.task_done()
        except Empty:
            pass

    @property
    def processing_count(self):
        return self._processing_count

    def add_task(self, share_code: str, access_code: str, user_id: Optional[str] = None, arg_str: str = ""):
        now = now_time()
        with self._lock:
            last_time = self._recent_tasks.get(share_code)
            if last_time and now - last_time < self._DEDUP_WINDOW:
                logger.warning(f"【P189Cas2Strm】忽略重复请求: {share_code}")
                return False
            self._recent_tasks[share_code] = now
            
        task = (share_code, access_code, user_id, arg_str)
        self._queue.put(task)
        self._save_task_to_file(task)
        logger.info(f"【P189Cas2Strm】任务已压入队列: {share_code}，当前排队数: {self._queue.qsize()}")
        return True

    def run_async(self, coro, timeout: Optional[float] = None):
        return self._runner.submit(coro, timeout=timeout)

    def _worker(self):
        while self._running:
            try:
                task = self._queue.get(timeout=10)
                share_code, access_code, user_id, arg_str = task
                self._processing_count += 1
                logger.info(f"【P189Cas2Strm】[工作线程] 提取到新任务: {share_code}")
                self._notify(user_id, "【189分享STRM】", f"🚀 开始处理分享: {share_code}")
                
                try:
                    # 在线程中运行异步逻辑（统一调度到常驻事件循环）
                    res = self._runner.submit(process_share_cas(share_code, access_code, arg_str))
                    if res.get("status"):
                        self._notify(user_id, "【189分享STRM】完成", f"✅ 处理完成，生成 {res.get('count')} 个 STRM")
                    else:
                        self._notify(user_id, "【189分享STRM】失败", f"❌ 处理失败: {res.get('msg')}")
                except Exception as e:
                    logger.error(f"【P189Cas2Strm】[工作线程] 执行异常: {e}", exc_info=True)
                
                self._remove_task_from_file(task)
                self._queue.task_done()
                self._processing_count = max(0, self._processing_count - 1)
                sleep(2) # 避免 API 过频
            except Empty:
                continue
            except Exception as e:
                logger.error(f"【P189Cas2Strm】[工作线程] 循环异常: {e}")
                sleep(5)

    def _notify(self, user_id, title, text):
        if self._notify_callback:
            self._notify_callback(user_id, title, text)

    def _save_task_to_file(self, task):
        try:
            tasks = []
            if os.path.exists(self._task_file):
                with open(self._task_file, 'r', encoding='utf-8') as f:
                    tasks = json.load(f)
            tasks.append(task)
            with open(self._task_file, 'w', encoding='utf-8') as f:
                json.dump(tasks, f, ensure_ascii=False)
        except: pass

    def _remove_task_from_file(self, task):
        try:
            if not os.path.exists(self._task_file): return
            with open(self._task_file, 'r', encoding='utf-8') as f:
                tasks = json.load(f)
            new_tasks = [t for t in tasks if t[0] != task[0]]
            with open(self._task_file, 'w', encoding='utf-8') as f:
                json.dump(new_tasks, f, ensure_ascii=False)
        except: pass

    def _load_pending_tasks(self):
        try:
            if os.path.exists(self._task_file):
                with open(self._task_file, 'r', encoding='utf-8') as f:
                    tasks = json.load(f)
                if tasks:
                    logger.info(f"【P189Cas2Strm】从持久化记录恢复了 {len(tasks)} 个未完成任务")
                    for t in tasks:
                        self._queue.put(tuple(t))
        except Exception as e:
            logger.error(f"【P189Cas2Strm】恢复持久化任务失败: {e}")

async def process_share_cas(share_code: str, access_code: str, arg_str: str = "") -> Dict[str, Any]:
    """核心处理逻辑"""
    logger.info(f"【P189Cas2Strm】[逻辑层] 正在准备客户端...")

    video_exts = {
        ".mkv", ".mp4", ".avi", ".mov", ".wmv", ".flv", ".ts", ".m2ts", ".webm", ".m4v"
    }

    def _is_video_cas(name: str) -> bool:
        n = str(name or "").strip()
        if not n.lower().endswith(".cas"):
            return False
        src_name = n[:-4]
        src_ext = os.path.splitext(src_name)[1].lower()
        return src_ext in video_exts

    client = P189ClientWrapper(
        configer.username,
        configer.password,
        cookie_store_path=configer.cookie_store_path,
    )
    login_ok = await client.login()
    if not login_ok:
        logger.error("【P189Cas2Strm】[逻辑层] 客户端登录失败，请检查配置中的账号密码。")
        return {"status": False, "msg": "登录失败"}

    # 会话有效性预检
    try:
        # 使用轻量级接口验证当前 Cookie 是否真的可用
        await client._protected_client_call("user_info_brief", async_=True)
    except Exception as e:
        logger.warning(f"【P189Cas2Strm】[逻辑层] 会话预检异常: {e}")

    # 1. 验证分享
    logger.info(f"【P189Cas2Strm】[逻辑层] 正在请求分享校验 (share_code={share_code})...")
    info = await client.share_check(share_code, access_code)
    if not info:
        logger.error(f"【P189Cas2Strm】[逻辑层] 获取分享信息失败，可能链接失效或访问码 {access_code} 错误")
        return {"status": False, "msg": "获取分享信息失败"}
    
    share_id = info.get("shareId")
    logger.info(f"【P189Cas2Strm】[逻辑层] 分享校验通过，share_id={share_id}")
    
    # 2. 列出文件
    logger.info("【P189Cas2Strm】[逻辑层] 正在递归列出分享内文件...")
    files = await client.share_list_all(share_id, access_code, share_code=share_code)

    video_exts = {
        ".mkv", ".mp4", ".avi", ".mov", ".wmv", ".flv", ".ts", ".m2ts", ".webm", ".m4v"
    }

    cas_files = []
    skipped_non_video = 0
    for f in files:
        name = str(f.get("name", "") or "").strip()
        if not name.lower().endswith(".cas"):
            continue

        src_name = name[:-4]
        src_ext = os.path.splitext(src_name)[1].lower()
        if src_ext not in video_exts:
            skipped_non_video += 1
            continue

        cas_files.append(f)

    if not cas_files:
        logger.warning(f"【P189Cas2Strm】[逻辑层] 未发现可处理的视频 CAS 文件: {share_code}，跳过非视频 CAS={skipped_non_video}")
        return {"status": True, "count": 0}

    logger.info(
        f"【P189Cas2Strm】[逻辑层] 发现 {len(cas_files)} 个可处理视频 CAS，跳过非视频 CAS={skipped_non_video}，进入处理循环..."
    )
    
    # 准备基础目录
    temp_dir_id = await client.fs_get_path_id(f"/P189CasTemp/{share_code}")
    strm_save_path = configer.strm_save_path
    if not os.path.exists(strm_save_path):
        os.makedirs(strm_save_path, exist_ok=True)

    bulk_enabled = bool(getattr(configer, "bulk_save_enabled", False))
    max_concurrency = int(getattr(configer, "max_concurrency", 2) or 2)
    max_concurrency = max(1, min(max_concurrency, 6))
        
    count = 0
    generated_strm_paths: List[Path] = []
    mediainfo = None

    if configer.moviepilot_transfer and configer.tmdb_extract and arg_str:
        try:
            tmdbid, mtype = extract_tmdb_info(arg_str)
            if tmdbid and mtype in ["movie", "tv"]:
                from app.chain.media import MediaChain
                from app.schemas.types import MediaType
                mediainfo = MediaChain().recognize_media(
                    tmdbid=tmdbid,
                    mtype=MediaType.from_agent(mtype),
                )
                if mediainfo:
                    logger.info(f"【P189Cas2Strm】预识别媒体信息成功: {mediainfo.title_year}")
        except Exception as e:
            logger.warning(f"【P189Cas2Strm】TMDB 提取/识别失败，回退默认整理: {e}")

    async def _list_local_cas(folder_id: str, rel_dir: str = "") -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        page_num = 1
        page_size = 100
        while True:
            listing = await client.client.fs_list(
                {"folderId": str(folder_id), "pageNum": page_num, "pageSize": page_size},
                async_=True,
            )
            files = listing.get("fileListAO", {}).get("fileList", []) or listing.get("fileList", [])
            folders = listing.get("fileListAO", {}).get("folderList", []) or listing.get("folderList", [])

            for file_item in files:
                name = str(file_item.get("name") or file_item.get("fileName") or "").strip()
                fid = file_item.get("id") or file_item.get("fileId")
                if not fid or not _is_video_cas(name):
                    continue
                items.append({
                    "name": name,
                    "id": str(fid),
                    "_rel_dir": rel_dir,
                    "_bulk_local": True,
                })

            for folder in folders:
                sub_id = folder.get("id") or folder.get("folderId") or folder.get("fileId")
                folder_name = str(folder.get("name") or folder.get("folderName") or "").strip()
                if not sub_id:
                    continue
                next_rel = f"{rel_dir}/{folder_name}".strip("/") if folder_name else rel_dir
                items.extend(await _list_local_cas(str(sub_id), next_rel))

            if len(files) + len(folders) < page_size:
                break
            page_num += 1
        return items

    async def _process_one_cas(f_info: Dict[str, Any], sem: asyncio.Semaphore) -> Optional[Path]:
        name = str(f_info.get("name") or "").strip()
        rel_dir = str(f_info.get("_rel_dir") or "").strip().strip("/")

        async with sem:
            try:
                local_fid = None
                if f_info.get("_bulk_local"):
                    local_fid = str(f_info.get("id") or "").strip()
                else:
                    fid = f_info.get("id")
                    if not fid:
                        logger.error(f"【P189Cas2Strm】CAS 缺少文件 ID: {name}")
                        return None
                    logger.info(f"【P189Cas2Strm】-- 正在转换: {name} --")
                    local_fid = await client.share_save_to_local(share_id, share_code, fid, access_code, temp_dir_id, expected_name=name)

                if not local_fid:
                    logger.error(f"【P189Cas2Strm】文件转存失败: {name}")
                    return None

                logger.info(f"【P189Cas2Strm】已就绪临时文件 ID: {local_fid}，正在读取内容...")
                content = await client.fs_read_content(local_fid)

                if not content:
                    logger.error(f"【P189Cas2Strm】文件内容读取为空: {name}")
                    return None

                content = content.strip()
                strm_name = os.path.splitext(name)[0] + ".strm"
                target_dir = os.path.join(strm_save_path, rel_dir) if rel_dir else strm_save_path
                if not os.path.exists(target_dir):
                    os.makedirs(target_dir, exist_ok=True)
                strm_file = os.path.join(target_dir, strm_name)

                redirect_host = configer.moviepilot_address
                strm_content = f"{redirect_host.rstrip('/')}/api/v1/plugin/p189cas2strm/redirect?c={content}"

                with open(strm_file, "w", encoding="utf-8") as sf:
                    sf.write(strm_content)

                logger.info(f"【P189Cas2Strm】✅ STRM 写入成功: {strm_name}")
                return Path(strm_file)
            except Exception as e:
                logger.error(f"【P189Cas2Strm】处理 CAS 异常: {name}, {e}")
                return None

    share_root_name = str(info.get("fileName") or info.get("name") or "").strip().strip("/")

    def _normalize_rel_dir(rel_dir: str) -> str:
        rel = str(rel_dir or "").strip().strip("/")
        if share_root_name:
            if rel == share_root_name:
                rel = ""
            elif rel.startswith(f"{share_root_name}/"):
                rel = rel[len(share_root_name) + 1:]
        return rel

    def _canonical_key(name: str, rel_dir: str) -> str:
        rel = _normalize_rel_dir(rel_dir)
        nm = str(name or "").strip()
        return f"{rel}/{nm}".strip("/").lower()

    def _target_rel_dir(item: Dict[str, Any]) -> str:
        rel = _normalize_rel_dir(item.get("_rel_dir") or "")
        return f"{share_root_name}/{rel}".strip("/") if share_root_name else rel

    target_by_key: Dict[str, Dict[str, Any]] = {}
    for item in cas_files:
        key = _canonical_key(item.get("name") or item.get("fileName") or "", item.get("_rel_dir") or "")
        if key and key not in target_by_key:
            target_by_key[key] = item

    expected_total = len(target_by_key)
    process_items = []
    bulk_mode_active = False

    async def _build_bulk_candidates(folder_id: str) -> Tuple[List[Dict[str, Any]], set]:
        local_items = await _list_local_cas(str(folder_id), "")
        matched_items: List[Dict[str, Any]] = []
        matched_keys = set()
        for local_item in local_items:
            key = _canonical_key(local_item.get("name") or local_item.get("fileName") or "", local_item.get("_rel_dir") or "")
            if not key or key in matched_keys:
                continue
            target_item = target_by_key.get(key)
            if not target_item:
                continue
            local_id = str(local_item.get("id") or local_item.get("fileId") or "").strip()
            if not local_id:
                continue
            matched_keys.add(key)
            matched_items.append(
                {
                    "name": str(target_item.get("name") or target_item.get("fileName") or "").strip(),
                    "id": local_id,
                    "_rel_dir": _target_rel_dir(target_item),
                    "_bulk_local": True,
                    "_cas_key": key,
                }
            )
        return matched_items, matched_keys

    if bulk_enabled:
        try:
            share_root_id = str(info.get("fileId") or info.get("shareDirFileId") or info.get("shareId") or "").strip()
            if share_root_id:
                logger.info(f"【P189Cas2Strm】启用整目录批量转存，shareRootId={share_root_id}")
                local_root_id = await client.share_save_to_local(
                    share_id,
                    share_code,
                    share_root_id,
                    access_code,
                    temp_dir_id,
                    expected_name=share_root_name,
                    is_folder=True,
                )
                if local_root_id:
                    wait_interval = 10
                    max_wait_seconds = 60
                    waited = 0

                    best_items, best_keys = await _build_bulk_candidates(str(temp_dir_id))
                    last_count = len(best_keys)

                    while last_count < expected_total and waited < max_wait_seconds:
                        logger.info(
                            f"【P189Cas2Strm】批量转存可见性检查: {last_count}/{expected_total}，{wait_interval}s 后复查"
                        )
                        await asyncio.sleep(wait_interval)
                        waited += wait_interval

                        current_items, current_keys = await _build_bulk_candidates(str(temp_dir_id))
                        current_count = len(current_keys)

                        if current_count > len(best_keys):
                            best_items, best_keys = current_items, current_keys

                        if current_count <= last_count:
                            logger.info(
                                f"【P189Cas2Strm】批量转存可见性无增长({current_count})，提前结束等待"
                            )
                            break
                        last_count = current_count

                    bulk_items = best_items
                    bulk_keys = best_keys
                    missing_keys = [k for k in target_by_key.keys() if k not in bulk_keys]
                    fill_items = []
                    for key in missing_keys:
                        target_item = target_by_key[key]
                        fill = dict(target_item)
                        fill["_rel_dir"] = _target_rel_dir(target_item)
                        fill["_cas_key"] = key
                        fill_items.append(fill)

                    process_items = bulk_items + fill_items
                    bulk_mode_active = True
                    logger.info(
                        f"【P189Cas2Strm】批量转存完成，目标={expected_total}，本地命中={len(bulk_keys)}，缺口补齐={len(fill_items)}"
                    )
                else:
                    logger.warning("【P189Cas2Strm】批量转存未成功，回退逐文件转存模式")
            else:
                logger.warning("【P189Cas2Strm】未获取到分享根目录 ID，回退逐文件转存模式")
        except Exception as be:
            logger.warning(f"【P189Cas2Strm】批量转存模式异常，回退逐文件: {be}")

    if not bulk_mode_active:
        process_items = []
        for item in cas_files:
            key = _canonical_key(item.get("name") or item.get("fileName") or "", item.get("_rel_dir") or "")
            fallback = dict(item)
            fallback["_rel_dir"] = _target_rel_dir(item)
            fallback["_cas_key"] = key
            process_items.append(fallback)

    deduped_items: List[Dict[str, Any]] = []
    seen_keys = set()
    for item in process_items:
        key = item.get("_cas_key") or _canonical_key(item.get("name") or item.get("fileName") or "", item.get("_rel_dir") or "")
        if not key or key in seen_keys:
            continue
        seen_keys.add(key)
        item["_cas_key"] = key
        deduped_items.append(item)

    if len(deduped_items) != expected_total:
        logger.warning(
            f"【P189Cas2Strm】待处理去重后数量异常: unique={len(deduped_items)} expected={expected_total}"
        )
    process_items = deduped_items

    if bulk_mode_active:
        results = []
        bulk_items = [item for item in process_items if item.get("_bulk_local")]
        fill_items = [item for item in process_items if not item.get("_bulk_local")]

        if bulk_items:
            sem = asyncio.Semaphore(max_concurrency)
            results.extend(await asyncio.gather(*[_process_one_cas(item, sem) for item in bulk_items]))

        if fill_items:
            logger.info(f"【P189Cas2Strm】开始逐文件补齐缺口 CAS={len(fill_items)}")
            for item in fill_items:
                p = await _process_one_cas(item, asyncio.Semaphore(1))
                results.append(p)
    else:
        results = []
        for item in process_items:
            p = await _process_one_cas(item, asyncio.Semaphore(1))
            results.append(p)
    for p in results:
        if p:
            count += 1
            generated_strm_paths.append(p)

    if generated_strm_paths and configer.moviepilot_transfer:
        logger.info(f"【P189Cas2Strm】开始批量整理 STRM，共 {len(generated_strm_paths)} 个文件")
        try:
            from app.db.transferhistory_oper import TransferHistoryOper
            transfer_history_oper = TransferHistoryOper()
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
                        background=True,
                    )
                except Exception as te:
                    logger.warning(f"【P189Cas2Strm】STRM 整理入队失败: {strm_file_path.name}: {te}")

            logger.info("【P189Cas2Strm】STRM 已全部入队，等待整理完成...")

            pending = set(strm_file_path.as_posix() for strm_file_path in generated_strm_paths)
            timeout = 600
            interval = 2
            waited = 0
            while pending and waited < timeout:
                finished = set()
                for src in pending:
                    history = transfer_history_oper.get_by_src(src, "local")
                    if history and history.dest:
                        finished.add(src)
                if finished:
                    logger.info(f"【P189Cas2Strm】已完成 {len(finished)} 个 STRM 整理")
                pending -= finished
                if pending:
                    sleep(interval)
                    waited += interval

            if pending:
                logger.warning(f"【P189Cas2Strm】部分 STRM 整理超时未完成: {pending}")
            else:
                logger.info("【P189Cas2Strm】STRM 批量整理完成")
        except Exception as te:
            logger.warning(f"【P189Cas2Strm】STRM 整理阶段异常，已跳过: {te}")

    try:
        if temp_dir_id and str(temp_dir_id) not in ("", "-11"):
            logger.info(f"【P189Cas2Strm】提交分享码目录删除任务: {share_code}")
            deleted = await client.fs_delete(temp_dir_id, is_folder=True)
            if not deleted:
                await asyncio.sleep(1.5)
                logger.warning(f"【P189Cas2Strm】分享码目录删除首次失败，准备重试: {share_code}")
                deleted = await client.fs_delete(temp_dir_id, is_folder=True)
            if deleted:
                logger.info(f"【P189Cas2Strm】已删除分享码临时目录: {share_code}")
            else:
                logger.warning(f"【P189Cas2Strm】分享码目录删除失败，等待定时清理兜底: {share_code}")
    except Exception as e:
        logger.warning(f"【P189Cas2Strm】删除分享码临时目录失败 share_code={share_code}: {e}")

    logger.info(f"【P189Cas2Strm】🎉 任务圆满完成，共生成 {count} 个文件")
    return {"status": True, "count": count}

# 全局单例
task_queue = ShareTaskQueue()
cas_record_manager = CasRecordManager(configer.cas_record_path)
