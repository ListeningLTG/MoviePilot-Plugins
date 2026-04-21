import json
import os
import re
import base64
import time
from pathlib import Path
from threading import Thread, Lock
from queue import Queue, Empty
from time import sleep, time as now_time
from typing import Dict, Any, List, Optional, Callable, Tuple
from uuid import uuid4

from fastapi import APIRouter, Query, Response
from fastapi.responses import RedirectResponse

from app.log import logger
from app.chain.transfer import TransferChain
from app.schemas import FileItem
from app.core.config import settings

from .config import configer
from .utils import extract_tmdb_info
from .p189_client import P189ClientWrapper

# --- API Router 定义 ---
router = APIRouter()

@router.get("/api/v1/plugin/p189cas2strm/redirect")
async def cas_redirect(c: str = Query(..., description="Base64 encoded CAS info")):
    """
    按需秒传重定向 API
    """
    try:
        # 1. 解析 CAS 信息
        cas_json = base64.b64decode(c).decode('utf-8')
        cas_data = json.loads(cas_json)
        
        filename = cas_data.get('name')
        size = cas_data.get('size')
        md5 = cas_data.get('md5')
        slice_md5 = cas_data.get('sliceMd5')
        
        if not all([filename, size, md5, slice_md5]):
            return Response(content="Invalid CAS data", status_code=400)
            
        # 2. 初始化天翼云客户端
        client = P189ClientWrapper(configer.username, configer.password)
        await client.ensure_logged_in()
        
        # 3. 确定秒传目标目录（带时间戳防止并发冲突）
        timestamp = int(now_time() * 1000)
        target_root_id = await client.fs_get_path_id(configer.p189_target_path)
        if not target_root_id:
            return Response(content="Failed to create target root directory", status_code=500)
            
        timestamp_folder_id = await client.fs_mkdir(target_root_id, str(timestamp))
        if not timestamp_folder_id:
            return Response(content="Failed to create timestamp directory", status_code=500)
            
        # 4. 检查是否有记录可以复用
        existing_file_id = cas_record_manager.get(md5)
        new_file_id = None
        
        if existing_file_id:
            logger.info(f"【P189Cas2Strm】发现 CAS 记录复用: {filename} (ID: {existing_file_id})")
            # 通过复制实现复用
            success = await client.fs_copy(existing_file_id, timestamp_folder_id, filename)
            if success:
                # 复制成功后，重新获取一下文件 ID
                listing = client.client.fs_list(timestamp_folder_id)
                files = listing.get('fileListAO', {}).get('fileList', []) or listing.get('fileList', [])
                for f in files:
                    if f.get('name') == filename:
                        new_file_id = f.get('id') or f.get('fileId')
                        break
        
        if not new_file_id:
            # 执行秒传
            logger.info(f"【P189Cas2Strm】执行秒传上传: {filename}")
            success = await client.rapid_upload(timestamp_folder_id, filename, size, md5, slice_md5)
            if success:
                listing = client.client.fs_list(timestamp_folder_id)
                files = listing.get('fileListAO', {}).get('fileList', []) or listing.get('fileList', [])
                for f in files:
                    if f.get('name') == filename:
                        new_file_id = f.get('id') or f.get('fileId')
                        cas_record_manager.add(md5, new_file_id)
                        break
        
        if not new_file_id:
            logger.error(f"【P189Cas2Strm】秒传/复用失败: {filename}")
            return Response(content="Rapid upload failed", status_code=500)
            
        # 5. 获取 直链
        download_url = await client.get_download_url(new_file_id)
        if not download_url:
            return Response(content="Failed to get download URL", status_code=500)
            
        return RedirectResponse(url=download_url)
    except Exception as e:
        logger.error(f"【P189Cas2Strm】重定向接口异常: {e}")
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
                logger.error(f"【P189Cas2Strm】加载 CAS 记录失败: {e}")

    def save(self):
        try:
            with open(self.record_path, 'w', encoding='utf-8') as f:
                json.dump(self.records, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"【P189Cas2Strm】同步 CAS 记录失败: {e}")

    def get(self, md5: str) -> Optional[str]:
        return self.records.get(md5)

    def add(self, md5: str, file_id: str):
        self.records[md5] = file_id
        self.save()

    def clear(self):
        self.records = {}
        if os.path.exists(self.record_path):
            os.remove(self.record_path)

# --- 任务队列类 ---
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

    def set_notify_callback(self, callback: Callable):
        self._notify_callback = callback

    def start(self):
        with self._lock:
            if not self._running:
                self._running = True
                self._worker_thread = Thread(target=self._worker, daemon=True)
                self._worker_thread.start()
                logger.info("【P189Cas2Strm】任务队列工作线程已启动")

    def stop(self):
        self._running = False

    def add_task(self, share_code: str, access_code: str, user_id: Optional[str] = None, arg_str: str = ""):
        now = now_time()
        with self._lock:
            last_time = self._recent_tasks.get(share_code)
            if last_time and now - last_time < self._DEDUP_WINDOW:
                return False
            self._recent_tasks[share_code] = now
        self._queue.put((share_code, access_code, user_id, arg_str))
        return True

    def _worker(self):
        while self._running:
            try:
                task = self._queue.get(timeout=10)
                share_code, access_code, user_id, arg_str = task
                self._processing_count += 1
                self._notify(user_id, "【189分享STRM】", f"🚀 开始处理分享: {share_code}")
                try:
                    res = process_share_cas(share_code, access_code, arg_str)
                    if res.get("status"):
                        self._notify(user_id, "【189分享STRM】完成", f"✅ 处理完成，生成 {res.get('count')} 个 STRM")
                    else:
                        self._notify(user_id, "【189分享STRM】失败", f"❌ 处理失败: {res.get('msg')}")
                except Exception as e:
                    logger.error(f"【P189Cas2Strm】任务处理异常: {e}")
                self._processing_count = max(0, self._processing_count - 1)
                self._queue.task_done()
            except Empty:
                continue

    def _notify(self, user_id, title, text):
        if self._notify_callback:
            self._notify_callback(user_id, title, text)

def process_share_cas(share_code: str, access_code: str, arg_str: str = "") -> Dict[str, Any]:
    """核心处理逻辑"""
    # 模拟处理过程
    return {"status": True, "count": 0}

# 全局单例
task_queue = ShareTaskQueue()
cas_record_manager = CasRecordManager(configer.cas_record_path)
