import re
from urllib.parse import quote
from typing import Tuple, Optional

from app.core.event import eventmanager, Event
from app.schemas.types import EventType
from app.log import logger


class AliTo115Mixin:

    @eventmanager.register(EventType.PluginAction)
    def handle_ali_to_115(self, event: Event):
        """远程命令触发：阿里云盘分享秒传到115"""
        if not event:
            return
        event_data = event.event_data
        if not event_data or event_data.get("action") != "mh_ali_to_115":
            return

        # 检查功能是否启用
        if not self._ali2115_enabled:
            self.post_message(
                channel=event_data.get("channel"),
                title="阿里云盘秒传功能未启用",
                text="请先在插件配置中启用阿里云盘秒传功能",
                userid=event_data.get("user")
            )
            return

        # 检查阿里云盘 Token 是否配置
        if not self._ali2115_token:
            self.post_message(
                channel=event_data.get("channel"),
                title="阿里云盘 Token 未配置",
                text="请先在插件配置中填写阿里云盘 Refresh Token",
                userid=event_data.get("user")
            )
            return

        # 检查115 Cookie 是否配置
        if not self._p115_cookie:
            self.post_message(
                channel=event_data.get("channel"),
                title="115 Cookie 未配置",
                text="请先在插件配置中填写115 Cookie",
                userid=event_data.get("user")
            )
            return

        # 获取分享链接
        share_url = event_data.get("arg_str")
        if not share_url or not share_url.strip():
            self.post_message(
                channel=event_data.get("channel"),
                title="参数错误",
                text="用法: /mhaly2115 <阿里云盘分享链接>\n例如: /mhaly2115 https://www.alipan.com/s/xxxxx",
                userid=event_data.get("user")
            )
            return

        share_url = share_url.strip()
        logger.info(f"mhnotify: 收到阿里云盘秒传命令: {share_url}")

        # 发送确认消息
        self.post_message(
            channel=event_data.get("channel"),
            title="⏳ 阿里云盘秒传任务开始",
            text=f"正在处理分享链接...\n{share_url[:60]}...",
            userid=event_data.get("user")
        )

        # 在后台线程执行秒传
        try:
            import threading
            threading.Thread(
                target=self._execute_ali_to_115,
                args=(share_url, event_data.get("channel"), event_data.get("user")),
                daemon=True
            ).start()
        except Exception as e:
            logger.error(f"mhnotify: 启动阿里云盘秒传线程失败: {e}")
            self.post_message(
                channel=event_data.get("channel"),
                title="❌ 阿里云盘秒传失败",
                text=f"启动秒传任务失败: {str(e)}",
                userid=event_data.get("user")
            )

    def _execute_ali_to_115(self, share_url: str, channel: str = None, userid: str = None):
        """
        执行阿里云盘分享秒传到115
        :param share_url: 阿里云盘分享链接
        :param channel: 消息通道
        :param userid: 用户ID
        """
        from hashlib import sha1
        from time import sleep
        from urllib.parse import urlparse
        from pathlib import Path
        
        try:
            # 导入依赖
            try:
                from p115client import P115Client
            except ImportError:
                logger.error("mhnotify: p115client 未安装")
                self.post_message(
                    channel=channel,
                    title="❌ 阿里云盘秒传失败",
                    text="p115client 未安装，请先安装依赖",
                    userid=userid
                )
                return

            try:
                from aligo import Aligo
            except ImportError:
                logger.error("mhnotify: aligo 未安装")
                self.post_message(
                    channel=channel,
                    title="❌ 阿里云盘秒传失败",
                    text="aligo 未安装，请先安装依赖",
                    userid=userid
                )
                return
            
            # 提取分享码
            share_id, share_pwd = self._extract_ali_share_code(share_url)
            if not share_id:
                logger.error(f"mhnotify: 无法从链接中提取分享码: {share_url}")
                self.post_message(
                    channel=channel,
                    title="❌ 阿里云盘秒传失败",
                    text=f"无法从链接中提取分享码，请检查链接格式\n链接: {share_url}",
                    userid=userid
                )
                return
            
            logger.info(f"mhnotify: 提取到分享码: {share_id}")
            
            # 创建阿里云盘客户端
            try:
                ali_client = Aligo(refresh_token=self._ali2115_token)
                logger.info("mhnotify: 阿里云盘客户端创建成功")
            except Exception as e:
                logger.error(f"mhnotify: 创建阿里云盘客户端失败: {e}")
                self.post_message(
                    channel=channel,
                    title="❌ 阿里云盘秒传失败",
                    text=f"阿里云盘登录失败: {str(e)}\n请检查 Refresh Token 是否有效",
                    userid=userid
                )
                return
            
            # 创建115客户端
            try:
                p115_client = P115Client(self._p115_cookie, app="web")
                logger.info("mhnotify: 115客户端创建成功")
            except Exception as e:
                logger.error(f"mhnotify: 创建115客户端失败: {e}")
                self.post_message(
                    channel=channel,
                    title="❌ 阿里云盘秒传失败",
                    text=f"115登录失败: {str(e)}\n请检查 Cookie 是否有效",
                    userid=userid
                )
                return
            
            # 获取分享 Token
            try:
                share_token = ali_client.get_share_token(share_id, share_pwd=share_pwd)
                if not getattr(share_token, 'share_token', None):
                     raise Exception(f"获取Token异常，请检查提取码或链接是否有效: {share_token}")
                logger.info(f"mhnotify: 获取分享 Token 成功")
            except Exception as e:
                logger.error(f"mhnotify: 获取分享 Token 失败: {e}")
                self.post_message(
                    channel=channel,
                    title="❌ 阿里云盘秒传失败",
                    text=f"获取分享信息失败: {str(e)}\n分享链接可能已失效",
                    userid=userid
                )
                return
            
            # 获取或创建阿里云盘临时文件夹
            ali_folder_path = self._ali2115_ali_folder.strip() or "/秒传转存"
            if not ali_folder_path.startswith('/'):
                ali_folder_path = '/' + ali_folder_path
            ali_folder_name = ali_folder_path.lstrip('/').split('/')[0]
            
            try:
                folder_info = ali_client.get_folder_by_path(path=ali_folder_name)
                if not folder_info:
                    folder_info = ali_client.create_folder(name=ali_folder_name, check_name_mode="overwrite")
                ali_folder_id = folder_info.file_id
                logger.info(f"mhnotify: 阿里云盘临时文件夹 ID: {ali_folder_id}")
            except Exception as e:
                logger.error(f"mhnotify: 获取/创建阿里云盘临时文件夹失败: {e}")
                self.post_message(
                    channel=channel,
                    title="❌ 阿里云盘秒传失败",
                    text=f"创建阿里云盘临时文件夹失败: {str(e)}",
                    userid=userid
                )
                return
            
            # 获取115目标文件夹ID
            target_path = self._ali2115_115_folder.strip() or "/秒传接收"
            if not target_path.startswith('/'):
                target_path = '/' + target_path
            target_path = target_path.rstrip('/')
            
            try:
                resp = p115_client.fs_dir_getid(target_path)
                if resp and resp.get("id"):
                    target_cid = int(resp.get("id"))
                else:
                    # 目录不存在，创建它
                    mkdir_resp = p115_client.fs_makedirs_app(target_path, pid=0)
                    target_cid = int(mkdir_resp.get("cid", 0))
                logger.info(f"mhnotify: 115目标文件夹 ID: {target_cid}")
            except Exception as e:
                logger.error(f"mhnotify: 获取/创建115目标文件夹失败: {e}")
                self.post_message(
                    channel=channel,
                    title="❌ 阿里云盘秒传失败",
                    text=f"创建115目标文件夹失败: {str(e)}",
                    userid=userid
                )
                return
            
            # 保存分享到阿里云盘
            try:
                logger.info("mhnotify: 开始保存分享到阿里云盘...")
                ali_client.share_file_save_all_to_drive(
                    share_token=share_token,
                    to_parent_file_id=ali_folder_id,
                )
                logger.info("mhnotify: 分享保存成功，等待同步...")
                sleep(3)
            except Exception as e:
                logger.error(f"mhnotify: 保存分享失败: {e}")
                self.post_message(
                    channel=channel,
                    title="❌ 阿里云盘秒传失败",
                    text=f"保存分享到阿里云盘失败: {str(e)}",
                    userid=userid
                )
                return
            
            # 获取分享文件列表（递归）
            media_exts = ['.mp4', '.mkv', '.avi', '.wmv', '.mov', '.flv', '.rmvb', '.rm', '.ts', '.m2ts', '.webm', '.mpg', '.mpeg', '.m4v', '.3gp', '.iso', '.srt', '.ass', '.sup']
            
            def get_share_files(share_token, parent_file_id="root", current_path=""):
                """递归获取分享文件列表（包含相对路径）"""
                files = []
                try:
                    file_list = ali_client.get_share_file_list(share_token, parent_file_id=parent_file_id)
                    for file in file_list:
                        new_path = f"{current_path}/{file.name}" if current_path else file.name
                        if file.type == "folder":
                            files.extend(get_share_files(share_token, file.file_id, new_path))
                        else:
                            suffix = Path(file.name).suffix.lower()
                            if suffix in media_exts:
                                files.append({"name": file.name, "rel_path": current_path})
                            else:
                                logger.debug(f"mhnotify: 跳过无关文件: {file.name}")
                except Exception as e:
                    logger.warning(f"mhnotify: 获取分享文件列表异常: {e}")
                return files
            
            share_files = get_share_files(share_token)
            if not share_files:
                logger.warning("mhnotify: 分享中没有找到媒体文件")
                self.post_message(
                    channel=channel,
                    title="⚠️ 阿里云盘秒传完成",
                    text="分享中没有找到媒体或字幕文件 (mp4, mkv, srt 等)",
                    userid=userid
                )
                return
            
            logger.info(f"mhnotify: 找到 {len(share_files)} 个媒体/字幕文件待秒传")
            
            # 获取已转存文件的下载链接和SHA1
            download_url_list = []
            remove_list = []
            
            def walk_files(parent_file_id, callback, current_rel_path=""):
                """遍历阿里云盘目录"""
                try:
                    file_list = ali_client.get_file_list(parent_file_id=parent_file_id)
                    for file in file_list:
                        callback(file, current_rel_path)
                        if file.type == "folder":
                            new_rel_path = f"{current_rel_path}/{file.name}" if current_rel_path else file.name
                            walk_files(file.file_id, callback, new_rel_path)
                except Exception as e:
                    logger.warning(f"mhnotify: 遍历文件夹异常: {e}")
            
            file_info_list = list(share_files)
            
            def collect_file_info(file, rel_path):
                nonlocal file_info_list
                # 记录所有遍历到的文件ID，以便后续删除
                if file.file_id not in remove_list:
                    remove_list.append(file.file_id)
                
                # 检查是否是我们需要的目标文件
                if file.type == "file":
                    # 匹配文件名与相对路径
                    matched_info = None
                    for info in file_info_list:
                        if info.get("name") == file.name and info.get("rel_path") == rel_path:
                            matched_info = info
                            break
                    
                    if matched_info:
                        try:
                            url_info = ali_client.get_download_url(file_id=file.file_id)
                            if url_info and url_info.url:
                                info = {
                                    "url": url_info.url,
                                    "size": url_info.size,
                                    "name": matched_info.get("name"),
                                    "rel_path": rel_path,
                                    "sha1": str(url_info.content_hash).upper(),
                                    "file_id": file.file_id
                                }
                                download_url_list.append(info)
                                file_info_list = [i for i in file_info_list if i != matched_info]
                        except Exception as e:
                            logger.warning(f"mhnotify: 获取文件 {file.name} 下载链接失败: {e}")

            # 第一次全量刷新目录缓存，确保能获取到最新转存的文件
            # 注意：ali_client.get_file_list 默认可能有缓存，尝试强制遍历
            logger.info("mhnotify: 正在获取转存文件列表...")
            
            # 最多尝试10次获取所有文件信息 (增加重试次数)
            for attempt in range(10):
                if not file_info_list:
                    break
                walk_files(ali_folder_id, collect_file_info)
                if file_info_list:
                    logger.info(f"mhnotify: 尚有 {len(file_info_list)} 个文件未获取到下载信息，等待重试 ({attempt+1}/10)...")
                    sleep(3)
            
            if not download_url_list:
                logger.error("mhnotify: 未能获取任何文件的下载信息")
                # 尝试列出当前目录下的文件，辅助排查
                try:
                    logger.info("mhnotify: 当前阿里云盘临时目录下的文件列表:")
                    def log_file(f):
                        logger.info(f"  - {f.name} ({f.type})")
                    walk_files(ali_folder_id, log_file)
                except:
                    pass
                
                self.post_message(
                    channel=channel,
                    title="❌ 阿里云盘秒传失败",
                    text="未能获取文件下载信息，可能是阿里云盘转存尚未完成同步，请重试",
                    userid=userid
                )
                return
            
            logger.info(f"mhnotify: 获取到 {len(download_url_list)} 个文件的下载信息")
            
            # 执行秒传到115
            success_count = 0
            fail_count = 0
            
            # 创建用于二次校验的辅助函数
            def calculate_sha1_range(url: str, sign_check: str) -> str:
                """计算指定范围的 sha1"""
                import httpx
                headers = {
                    "Range": f"bytes={sign_check}",
                    "Referer": "https://www.aliyundrive.com/",
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                }
                with httpx.stream("GET", url, headers=headers, follow_redirects=True, timeout=120) as r:
                    r.raise_for_status()
                    _sha1 = sha1()
                    for chunk in r.iter_bytes(chunk_size=8192):
                        _sha1.update(chunk)
                    return _sha1.hexdigest().upper()
            
            def make_read_range_func(file_id: str, fallback_url: str):
                """创建一个读取范围数据的函数，每次调用时实时获取新的下载链接"""
                def read_range_bytes_or_hash(sign_check: str) -> str:
                    # 每次二次校验时都重新获取下载链接（阿里云盘链接过期很快）
                    try:
                        url_info = ali_client.get_download_url(file_id=file_id)
                        url = url_info.url
                        logger.debug(f"mhnotify: 获取新的下载链接进行二次校验")
                    except Exception as e:
                        logger.warning(f"mhnotify: 二次校验时获取下载链接失败: {e}，使用备用链接")
                        url = fallback_url
                    return calculate_sha1_range(url, sign_check)
                return read_range_bytes_or_hash
            
            # 115 目录缓存 (rel_path -> cid)
            cid_cache = {"": target_cid}

            def upload_to_115(file_info: dict):
                """上传文件到115"""
                file_id = file_info.get("file_id")
                file_name = file_info.get("name")
                fallback_url = file_info.get("url")
                rel_path = file_info.get("rel_path", "")
                
                # 获取或创建115目标子目录
                dest_cid = target_cid
                if rel_path:
                    if rel_path in cid_cache:
                        dest_cid = cid_cache[rel_path]
                    else:
                        try:
                            full_dest_path = f"{target_path}/{rel_path}".replace('//', '/')
                            mkdir_resp = p115_client.fs_makedirs_app(full_dest_path, pid=0)
                            if mkdir_resp and mkdir_resp.get("cid"):
                                dest_cid = int(mkdir_resp.get("cid"))
                                cid_cache[rel_path] = dest_cid
                        except Exception as e:
                            logger.warning(f"mhnotify: 确保115目录 {rel_path} 存在失败: {e}")
                
                # 创建专属于这个文件的读取函数
                read_range_func = make_read_range_func(file_id, fallback_url)
                
                return p115_client.upload_file_init(
                    filename=file_name,
                    filesize=file_info.get("size"),
                    filesha1=file_info.get("sha1"),
                    pid=dest_cid,
                    read_range_bytes_or_hash=read_range_func,
                )
            
            # 顺序上传（避免并发时下载链接混乱）
            for file_info in download_url_list:
                file_name = file_info.get("name")
                retries = 0
                max_retries = 3
                
                while retries <= max_retries:
                    try:
                        result = upload_to_115(file_info)
                        if result and isinstance(result, dict) and result.get("status") == 2:
                            logger.info(f"mhnotify: 文件 '{file_name}' 秒传成功")
                            success_count += 1
                            break
                        else:
                            status_code = result.get("status", "N/A") if isinstance(result, dict) else "N/A"
                            logger.warning(f"mhnotify: 文件 '{file_name}' 上传状态异常 (status: {status_code})")
                    except Exception as exc:
                        logger.warning(f"mhnotify: 文件 '{file_name}' 上传异常: {exc}")
                    
                    retries += 1
                    if retries <= max_retries:
                        delay = 2 * (2 ** (retries - 1))
                        logger.info(f"mhnotify: 文件 '{file_name}' 将在 {delay} 秒后进行第 {retries} 次重试...")
                        sleep(delay)
                    else:
                        logger.error(f"mhnotify: 文件 '{file_name}' 已达到最大重试次数，放弃上传")
                        fail_count += 1
            # 清理阿里云盘临时文件
            try:
                logger.info("mhnotify: 开始清理阿里云盘临时文件...")
                if remove_list:
                    # 使用 aligo 的 move_file_to_trash 或直接删除
                    for file_id in remove_list:
                        try:
                            ali_client.move_file_to_trash(file_id=file_id)
                        except Exception as del_err:
                            logger.debug(f"mhnotify: 删除文件 {file_id} 失败: {del_err}")
                logger.info(f"mhnotify: 已清理 {len(remove_list)} 个临时文件")
            except Exception as e:
                logger.warning(f"mhnotify: 清理阿里云盘临时文件失败: {e}")
            
            # 发送结果通知
            result_text = f"📦 分享链接: {share_url[:50]}...\n\n"
            result_text += f"✅ 秒传成功: {success_count} 个文件\n"
            if fail_count > 0:
                result_text += f"❌ 秒传失败: {fail_count} 个文件\n"
            result_text += f"\n📂 保存路径: {target_path}"
            
            title = "✅ 阿里云盘秒传完成" if fail_count == 0 else f"⚠️ 阿里云盘秒传完成（{fail_count}个失败）"
            
            self.post_message(
                channel=channel,
                title=title,
                text=result_text,
                userid=userid
            )
            
            logger.info(f"mhnotify: 阿里云盘秒传完成，成功 {success_count} 个，失败 {fail_count} 个")
            
            # 如果开启了移动整理，执行整理
            if self._ali2115_organize and success_count > 0:
                logger.info("mhnotify: 开始执行秒传后移动整理...")
                try:
                    access_token = self._get_mh_access_token()
                    if access_token:
                        self._organize_cloud_download(access_token, target_path)
                        self.post_message(
                            channel=channel,
                            title="📁 移动整理已启动",
                            text=f"正在整理 {target_path} 目录中的文件...",
                            userid=userid
                        )
                    else:
                        logger.error("mhnotify: 无法获取MH access token，跳过移动整理")
                except Exception as e:
                    logger.error(f"mhnotify: 秒传后移动整理失败: {e}")
            
        except Exception as e:
            logger.error(f"mhnotify: 阿里云盘秒传异常: {e}", exc_info=True)
            self.post_message(
                channel=channel,
                title="❌ 阿里云盘秒传失败",
                text=f"秒传过程中发生错误: {str(e)}",
                userid=userid
            )

    @staticmethod
    def _extract_ali_share_code(url: str) -> Tuple[Optional[str], Optional[str]]:
        """
        从阿里云盘分享链接中提取分享码和提取码
        支持格式:
        - https://www.alipan.com/s/xxxxx
        - https://www.aliyundrive.com/s/xxxxx
        - 链接后跟提取码/pwd/password
        - 链接后直接跟4位提取码（空格分隔）
        """
        import re

        share_code = None
        share_pwd = None

        try:
            # 1. 提取分享ID (优先使用正则匹配)
            match_id = re.search(r'/s/([a-zA-Z0-9]+)', url)
            if match_id:
                share_code = match_id.group(1)
            else:
                # 尝试简单分割（兼容性保留）
                clean_url = url.split()[0].strip()
                if "/s/" in clean_url:
                    parts = clean_url.split("/s/")[-1].split("/")
                    if parts:
                        share_code = parts[0].split("?")[0]

            # 2. 提取提取码
            # 2.1 显式提取码 (提取码: xxxx, pwd=xxxx)
            match_pwd_explicit = re.search(r'(?:提取码|pwd|password|code)[:=：\s]*([a-zA-Z0-9]{4})', url, re.IGNORECASE)
            if match_pwd_explicit:
                share_pwd = match_pwd_explicit.group(1)
            
            # 2.2 隐式提取码 (紧跟在链接后的4位字符)
            # 仅当未找到显式提取码，且找到了share_code时尝试
            if not share_pwd and share_code:
                # 构造正则：匹配链接(包含share_code) + 可能的尾部字符 + 空白 + 4位字符 + (分隔符或结尾)
                # 排除 http 开头以防匹配到下一个链接
                pattern_implicit = rf'/s/{re.escape(share_code)}[/?]*[\s\u3000]+([a-zA-Z0-9]{{4}})(?:[;\s\n，。]|$)'
                match_pwd_implicit = re.search(pattern_implicit, url)
                if match_pwd_implicit:
                    candidate = match_pwd_implicit.group(1)
                    # 简单过滤：不能是 http 或 www 开头，不能纯数字(通常提取码含字母，但也可能是纯数字，这里不做强限制，只防常见单词)
                    if not candidate.lower().startswith(('http', 'www', 'com', 'org', 'net')):
                        share_pwd = candidate

        except Exception as e:
            logger.warning(f"mhnotify: 提取分享码异常: {e}")
        return share_code, share_pwd

