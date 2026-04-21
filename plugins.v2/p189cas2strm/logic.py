import json
import os
import re
import base64
import asyncio
from typing import Dict, Any, List, Optional, Callable, Tuple
from time import time as now_time, sleep
from threading import Thread, Lock
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
        # 1. 解析 CAS 信息（兼容 URL 传参导致的空格/缺失补位/urlsafe 变体）
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
            
        # 2. 初始化天翼云客户端（优先复用本地持久化会话）
        client = P189ClientWrapper(
            configer.username,
            configer.password,
            cookie_store_path=configer.cookie_store_path,
        )
        await client.ensure_logged_in()
        
        # 3. 确定秒传目标目录
        target_root_id = await client.fs_get_path_id(configer.p189_target_path)
        if not target_root_id:
            logger.error(f"【P189Cas2Strm】无法定位或创建远程路径: {configer.p189_target_path}")
            return Response(content="Failed to access target root", status_code=500)
        
        # 4. 执行秒传（带时间戳文件夹防止冲突）
        timestamp = int(now_time() * 1000)
        timestamp_folder_id = await client.fs_mkdir(target_root_id, str(timestamp))
        logger.info(f"【P189Cas2Strm】准备在临时目录 {timestamp} 中执行秒传...")
        
        success = await client.rapid_upload(timestamp_folder_id, filename, size, md5, slice_md5)
        if not success:
            logger.error(f"【P189Cas2Strm】秒传执行失败，请检查网盘空间或账号状态: {filename}")
            return Response(content="Rapid upload failed", status_code=500)
            
        # 获取新生成的直链
        listing = client.client.fs_list(timestamp_folder_id)
        files = listing.get('fileListAO', {}).get('fileList', []) or listing.get('fileList', [])
        new_file_id = None
        for f in files:
            if f.get('name') == filename:
                new_file_id = f.get('id') or f.get('fileId')
                break
        
        if not new_file_id:
            logger.error("【P189Cas2Strm】秒传成功但列表轮询未发现文件，可能是 API 延迟")
            return Response(content="Upload success but file not found", status_code=500)

        download_url = await client.get_download_url(new_file_id)
        if not download_url:
            logger.error(f"【P189Cas2Strm】获取下载链接失败，fileId={new_file_id}")
            return Response(content="Failed to get download URL", status_code=500)
            
        logger.info(f"【P189Cas2Strm】重定向成功 -> {download_url[:50]}...")
        return RedirectResponse(url=download_url)
    except Exception as e:
        logger.error(f"【P189Cas2Strm】重定向接口全局异常: {e}", exc_info=True)
        return Response(content=str(e), status_code=500)

# --- 记录管理类 ---
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

    def set_notify_callback(self, callback: Callable):
        self._notify_callback = callback

    def start(self):
        with self._lock:
            if not self._running:
                self._running = True
                self._worker_thread = Thread(target=self._worker, daemon=True)
                self._worker_thread.start()
                logger.info("【P189Cas2Strm】任务队列工作线程已启动 (Thread 模式)")
                self._load_pending_tasks()

    def stop(self):
        self._running = False

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

    def _worker(self):
        while self._running:
            try:
                task = self._queue.get(timeout=10)
                share_code, access_code, user_id, arg_str = task
                self._processing_count += 1
                logger.info(f"【P189Cas2Strm】[工作线程] 提取到新任务: {share_code}")
                self._notify(user_id, "【189分享STRM】", f"🚀 开始处理分享: {share_code}")
                
                try:
                    # 在线程中运行异步逻辑
                    res = asyncio.run(process_share_cas(share_code, access_code, arg_str))
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
    
    client = P189ClientWrapper(
        configer.username,
        configer.password,
        cookie_store_path=configer.cookie_store_path,
    )
    login_ok = await client.login()
    if not login_ok:
        logger.error("【P189Cas2Strm】[逻辑层] 客户端登录失败，请检查配置中的账号密码。")
        return {"status": False, "msg": "登录失败"}

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
    cas_files = [f for f in files if f.get("name", "").endswith(".cas")]
    
    if not cas_files:
        logger.warning(f"【P189Cas2Strm】[逻辑层] 未在分享中发现任何 .cas 描述文件: {share_code}")
        return {"status": True, "count": 0}

    logger.info(f"【P189Cas2Strm】[逻辑层] 发现 {len(cas_files)} 个 .cas 文件，进入处理循环...")
    
    # 准备基础目录
    temp_dir_id = await client.fs_get_path_id(f"/P189CasTemp/{share_code}")
    strm_save_path = configer.strm_save_path
    if not os.path.exists(strm_save_path):
        os.makedirs(strm_save_path, exist_ok=True)
        
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

    for f_info in cas_files:
        name = f_info["name"]
        fid = f_info["id"]
        
        # 转存 -> 读取 -> 删除
        logger.info(f"【P189Cas2Strm】-- 正在转换: {name} --")
        local_fid = await client.share_save_to_local(share_id, fid, access_code, temp_dir_id, expected_name=name)
        if not local_fid:
            logger.error(f"【P189Cas2Strm】文件转存失败: {name}")
            continue
            
        logger.info(f"【P189Cas2Strm】已成功转存至本地临时 ID: {local_fid}，正在读取内容...")
        content = await client.fs_read_content(local_fid)
        
        logger.info(f"【P189Cas2Strm】正在清理网盘临时文件 (ID: {local_fid})...")
        await client.fs_delete(local_fid)
        
        if not content:
            logger.error(f"【P189Cas2Strm】文件内容读取为空: {name}")
            continue
            
        try:
            content = content.strip()
            # 生成 STRM
            strm_name = os.path.splitext(name)[0] + ".strm"
            strm_file = os.path.join(strm_save_path, strm_name)
            
            redirect_host = configer.moviepilot_address
            strm_content = f"{redirect_host.rstrip('/')}/api/v1/plugin/p189cas2strm/redirect?c={content}"
            
            with open(strm_file, "w", encoding="utf-8") as sf:
                sf.write(strm_content)
            
            count += 1
            generated_strm_paths.append(Path(strm_file))
            logger.info(f"【P189Cas2Strm】✅ STRM 写入成功: {strm_name}")
        except Exception as e:
            logger.error(f"【P189Cas2Strm】文件写入 IO 异常: {name}, {e}")

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

    logger.info(f"【P189Cas2Strm】🎉 任务圆满完成，共生成 {count} 个文件")
    return {"status": True, "count": count}

# 全局单例
task_queue = ShareTaskQueue()
cas_record_manager = CasRecordManager(configer.cas_record_path)
