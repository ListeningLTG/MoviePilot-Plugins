import asyncio
import base64
import json
from typing import Optional, List, Dict, Any, Union
from app.log import logger
from p189client import P189Client, check_response
from p189client.exception import P189OSError

class P189ClientWrapper:
    """
    天翼云盘客户端封装类 v1.0.4
    """
    def __init__(self, username: str = "", password: str = "", cookies: str = ""):
        self.username = username
        self.password = password
        cookie_text = (cookies or "").strip()
        if not cookie_text and password and ("UID=" in password or "SEID=" in password or "COOKIE_LOGIN_USER" in password):
            cookie_text = password.strip()
        self.client = P189Client(cookies=cookie_text)
        self.session_key: Optional[str] = None

    @property
    def is_logged_in(self) -> bool:
        return bool(self.client.cookies_str)

    async def login(self) -> bool:
        """
        登录 189
        """
        try:
            if self.is_logged_in:
                return True
            logger.info(f"【P189Client】正在尝试登录账号: {self.username}")
            self.client.login(self.username, self.password)
            logger.info("【P189Client】账号登录成功。")
            return True
        except Exception as e:
            logger.error(f"【P189Client】登录发生异常: {e}")
            return False

    async def ensure_logged_in(self):
        """
        确保已登录
        """
        if not self.is_logged_in:
            ok = await self.login()
            if not ok:
                raise RuntimeError("P189 login failed")

    async def share_check(self, share_code: str, access_code: str) -> Optional[dict]:
        """
        校验分享链接并获取信息
        """
        try:
            logger.info(f"【P189Client】正在获取分享信息: shareCode={share_code}")
            params = {"shareCode": share_code, "accessCode": access_code}
            res = self.client.share_info_by_code(params)
            check_response(res)
            logger.info(f"【P189Client】分享详情获取成功: {res.get('shareId')}")
            return res
        except Exception as e:
            logger.error(f"【P189Client】分享校验失败: {e}")
            return None

    async def share_list_all(self, share_id: str, access_code: str) -> List[dict]:
        """
        获取分享内的所有文件列表（递归平铺）
        """
        try:
            logger.info(f"【P189Client】正在列出分享文件，shareId={share_id}")
            params = {
                "shareId": share_id,
                "accessCode": access_code,
                "fileId": -11,
                "isMaster": 0
            }
            res = self.client.share_fs_list(params)
            check_response(res)
            
            data = res.get("fileListAO", {}).get("fileList", []) or res.get("fileList", [])
            logger.info(f"【P189Client】列表拉取完成，共发现 {len(data)} 个项目。")
            return data
        except Exception as e:
            logger.error(f"【P189Client】获取分享列表异常: {e}")
            return []

    async def share_save_to_local(self, share_id: str, file_id: str, access_code: str, target_folder_id: str) -> Optional[str]:
        """
        将分享文件转存到指定目录，返回新文件 ID
        """
        try:
            logger.info(f"【P189Client】正在发起转存任务: fileId={file_id} -> folderId={target_folder_id}")
            payload = {
                "shareId": share_id,
                "fileIds": [file_id],
                "targetFolderId": str(target_folder_id),
                "accessCode": access_code
            }
            res = self.client.share_save(payload)
            check_response(res)
            
            await asyncio.sleep(1.5) # 给 API 同步时间
            listing = self.client.fs_list(target_folder_id)
            files = listing.get('fileListAO', {}).get('fileList', []) or listing.get('fileList', [])
            for f in files:
                if str(f.get('oldFileId') or '') == str(file_id) or f.get('fileId') == file_id:
                     return f.get('id') or f.get('fileId')
            
            if files:
                return files[0].get('id') or files[0].get('fileId')
            return None
        except Exception as e:
            logger.error(f"【P189Client】转存操作失败: {e}")
            return None

    async def fs_read_content(self, file_id: str) -> Optional[str]:
        """
        读取文件文本内容 (CAS 文件都是 Base64)
        """
        try:
            logger.info(f"【P189Client】正在读取文件内容: fileId={file_id}")
            url = await self.get_download_url(file_id)
            if not url:
                return None
            
            import httpx
            async with httpx.AsyncClient(timeout=30) as client:
                res = await client.get(url)
                res.raise_for_status()
                logger.info(f"【P189Client】文件内容读取成功，长度: {len(res.text)}")
                return res.text
        except Exception as e:
            logger.error(f"【P189Client】文件内容读取失败: {e}")
            return None

    async def fs_mkdir(self, parent_id: str, name: str) -> Optional[str]:
        """
        创建目录
        """
        try:
            exist_id = await self.fs_get_path_id_child(parent_id, name)
            if exist_id:
                return exist_id
                
            logger.info(f"【P189Client】正在创建云盘目录: {name} (parent={parent_id})")
            payload = {
                "parentFolderId": str(parent_id),
                "folderName": name
            }
            res = self.client.fs_mkdir(payload)
            check_response(res)
            return str(res.get("id") or res.get("folderId"))
        except Exception as e:
            logger.error(f"【P189Client】创建目录失败: {e}")
            return None

    async def fs_get_path_id_child(self, parent_id: str, name: str) -> Optional[str]:
        """
        获取子目录 ID
        """
        try:
            listing = self.client.fs_list(parent_id)
            folders = listing.get('fileListAO', {}).get('folderList', []) or listing.get('folderList', [])
            for folder in folders:
                if folder.get("name") == name or folder.get("folderName") == name:
                    return str(folder.get("id") or folder.get("folderId"))
        except: pass
        return None

    async def fs_get_path_id(self, path: str) -> Optional[str]:
        """
        通过路径获取目录 ID
        """
        if not path or path == "/":
            return "-11" 
        
        parts = [p for p in path.split("/") if p]
        curr_id = "-11"
        for part in parts:
            found_id = await self.fs_get_path_id_child(curr_id, part)
            if found_id:
                curr_id = str(found_id)
            else:
                curr_id = await self.fs_mkdir(curr_id, part)
                if not curr_id:
                    return None
        return str(curr_id)

    async def fs_delete(self, file_id: str, is_folder: bool = False) -> bool:
        """
        物理删除
        """
        try:
            logger.info(f"【P189Client】正在执行物理删除: ID={file_id}")
            payload = {
                "type": "DELETE",
                "taskInfos": [{
                    "fileId": str(file_id),
                    "isFolder": 1 if is_folder else 0
                }]
            }
            res = self.client.fs_batch(payload)
            check_response(res)
            return True
        except Exception as e:
            logger.error(f"【P189Client】删除失败: {e}")
            return False

    async def get_download_url(self, file_id: str) -> Optional[str]:
        """
        获取 302 下载链接
        """
        try:
            resp = self.client.download_url_info({"fileId": str(file_id)})
            check_response(resp)
            return resp.get("fileDownloadUrl") or resp.get("downloadUrl")
        except Exception as e:
            logger.error(f"【P189Client】获取下载链接失败: {e}")
            return None

    async def rapid_upload(self, parent_id: str, filename: str, size: int, md5: str, slice_md5: str) -> bool:
        """
        执行秒传校验与初始化
        """
        try:
            logger.info(f"【P189Client】执行秒传校验: {filename} (MD5={md5})")
            payload = {
                "parentFolderId": str(parent_id),
                "fileName": filename,
                "size": int(size),
                "md5": md5,
                "sliceMd5": slice_md5
            }
            res = self.client.upload_init(payload)
            check_response(res)
            return res.get("isExist") == 1
        except Exception as e:
            logger.error(f"【P189Client】秒传校验过程异常: {e}")
            return False

    async def fs_empty_recycle(self) -> bool:
        """
        清空回收站
        """
        try:
            res = self.client.recyclebin_clear({})
            check_response(res)
            return True
        except Exception as e:
            logger.error(f"【P189Client】清空回收站失败: {e}")
            return False
