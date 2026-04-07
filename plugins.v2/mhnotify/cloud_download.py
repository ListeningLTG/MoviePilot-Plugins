from typing import List, Tuple, Dict, Any, Optional, Union

from app.core.config import settings
from app.core.event import Event
from app.schemas.types import NotificationType
from app.log import logger


class CloudDownloadMixin:

    def _btl_get_video_detail(self, access_token: str, douban_id: Union[str, int]) -> List[Dict[str, Any]]:
        try:
            base = "https://web5.mukaku.com/prod/api/v1/getVideoDetail"
            params = {
                "id": str(douban_id),
                "app_id": "83768d9ad4",
                "identity": "23734adac0301bccdcb107c4aa21f96c"
            }
            logger.info(f"111mhnotify: 获取BTL视频详情 GET {base} id={douban_id} app_id={params['app_id']} identity={params['identity'][:8]}***")
            import requests, time
            seeds = []
            for i in range(3):
                try:
                    resp = requests.get(base, params=params, headers={
                        "Accept": "application/json, text/plain, */*",
                        "Accept-Language": "zh_CN",
                        "Content-Type": "application/json;charset=UTF-8",
                        "Referer": "https://web5.mukaku.com/search",
                        "User-Agent": "Mozilla/5.0"
                    }, timeout=20)
                    try:
                        text = resp.text or ""
                        logger.info(f"mhnotify: BTL详情响应 status={resp.status_code} len={len(text)}")
                        logger.info(f"mhnotify: BTL详情响应内容: {text[:1000]}")
                    except Exception:
                        pass
                    if resp.status_code != 200:
                        time.sleep(1 + i)
                        continue
                    data = resp.json() or {}
                    if not isinstance(data, dict) or str(data.get("code")) != "200":
                        logger.info(f"mhnotify: BTL详情返回错误 code={data.get('code')} message={data.get('message')}")
                        time.sleep(1 + i)
                        continue
                    core = data.get("data") if isinstance(data.get("data"), dict) else {}
                    if core and "all_seeds" in core and isinstance(core["all_seeds"], list):
                        seeds = core["all_seeds"]
                        logger.info(f"mhnotify: BTL详情使用 all_seeds 资源条数={len(seeds)}")
                    else:
                        seeds = (core.get("resources") or core.get("list") or
                                 data.get("resources") or data.get("list") or [])
                        logger.info(f"mhnotify: BTL详情使用兼容资源字段，条数={len(seeds) if isinstance(seeds, list) else 0}")
                    break
                except Exception:
                    time.sleep(1 + i)
            return seeds if isinstance(seeds, list) else []
        except Exception:
            logger.info("mhnotify: BTL详情调用异常")
            return []

    def _btl_get_video_list(self, title: str, page: int = 1, limit: int = 24, app_id:str= "83768d9ad4" , identity: str="23734adac0301bccdcb107c4aa21f96c") -> List[Dict[str, Any]]:
        try:
            base = "https://web5.mukaku.com/prod/api/v1/getVideoList"
            params = {
                "sb": title,
                "page": page,
                "limit": limit,
                "app_id": app_id,
                "identity": identity
            }
            # params["app_id"] = (app_id or self._btl_app_id or "").strip()
            # params["identity"] = (identity or self._btl_identity or "").strip()
            logger.info(f"mhnotify: BTL getVideoList GET {base} sb={title} page={page} limit={limit} app_id={params['app_id']} identity={params['identity'][:8]}***")
            import requests, time
            inner = []
            for i in range(3):
                try:
                    resp = requests.get(base, params=params, headers={
                        "Accept": "application/json, text/plain, */*",
                        "Accept-Language": "zh_CN",
                        "Content-Type": "application/json;charset=UTF-8",
                        "Referer": "https://web5.mukaku.com/search",
                        "User-Agent": "Mozilla/5.0"
                    }, timeout=20)
                    if resp.status_code != 200:
                        time.sleep(1 + i)
                        continue
                    data = resp.json() or {}
                    if not isinstance(data, dict) or str(data.get("code")) != "200":
                        logger.info(f"mhnotify: BTL getVideoList 返回错误 code={data.get('code')} message={data.get('message')}")
                        time.sleep(1 + i)
                        continue
                    inner = (data.get("data") or {}).get("data") or []
                    logger.info(f"mhnotify: BTL getVideoList 返回条目数={len(inner) if isinstance(inner, list) else 0}")
                    break
                except Exception:
                    time.sleep(1 + i)
            return inner if isinstance(inner, list) else []
        except Exception:
            logger.info("mhnotify: BTL getVideoList 调用异常")
            return []

    def _get_quality_priority(self, access_token: str) -> Dict[str, Any]:
        try:
            url = f"{self._mh_domain}/api/v1/task_rules/subscription"
            headers = self._auth_headers(access_token)
            logger.info(f"mhnotify: 获取质量优先级配置 GET {url}")
            res = RequestUtils(headers=headers).get_res(url)
            if not res or res.status_code != 200:
                logger.error(f"mhnotify: 获取质量优先级失败 status={getattr(res, 'status_code', 'N/A')} body={getattr(res, 'text', '')[:200]}")
                return {}
            data = res.json() or {}
            qp = (data.get("data") or {}).get("quality_priority") or {}
            rp = qp.get("resolution_priority") or []
            hp = qp.get("hdr_priority") or []
            cp = qp.get("codec_priority") or []
            logger.info(f"mhnotify: 质量优先级摘要 resolution={rp} hdr={hp} codec={cp}")
            return qp
        except Exception:
            logger.error("mhnotify: 获取质量优先级异常", exc_info=True)
            return {}

    def _select_btl_resources_by_priority(self, resources: List[Dict[str, Any]], quality_pref: Optional[str]) -> List[Dict[str, Any]]:
        def get_group(x: Dict[str, Any]) -> str:
            g = str(x.get("definition_group") or "").lower()
            n = str(x.get("zname") or x.get("name") or "").lower()
            if not g and n:
                if "remux" in n:
                    return "remux"
                if "web-dl" in n or "webdl" in n or "webrip" in n:
                    return "web-dl"
                if "hdtv" in n:
                    return "hdtv"
                if "blu" in n or "蓝光" in n:
                    return "blu-ray"
            if "3d" in g:
                return "3d"
            if "remux" in n or "remux" in g:
                return "remux"
            if "蓝光原盘" in g or "blu" in g or "蓝光" in g:
                return "blu-ray"
            if "web-dl" in g or "webdl" in g or "webrip" in g:
                return "web-dl"
            if "hdtv" in g:
                return "hdtv"
            return g or "other"
        def is_valid(x: Dict[str, Any]) -> bool:
            g = str(x.get("definition_group") or "")
            n = str(x.get("zname") or x.get("name") or "")
            if g.strip().lower() == "3d" or "3D" in g:
                return False
            if ("蓝光原盘" in g) and ("REMUX" not in n.upper()):
                return False
            return True
        filtered = [r for r in resources if is_valid(r)]
        pref = str(quality_pref or "").lower()
        if not pref or pref == "auto":
            order = ["remux", "blu-ray", "web-dl", "hdtv", "other"]
        else:
            order = []
            p = pref.replace(">", ",").replace("|", ",").replace("/", ",")
            for seg in [s.strip().lower() for s in p.split(",") if s.strip()]:
                if "remux" in seg:
                    order.append("remux")
                elif "blu" in seg or "蓝光" in seg:
                    order.append("blu-ray")
                elif "web" in seg:
                    order.append("web-dl")
                elif "hdtv" in seg:
                    order.append("hdtv")
            order += [x for x in ["remux", "blu-ray", "web-dl", "hdtv", "other"] if x not in order]
        grouped: Dict[str, List[Dict[str, Any]]] = {}
        for r in filtered:
            k = get_group(r)
            grouped.setdefault(k, []).append(r)
        def parse_size_bytes(v: Any) -> int:
            try:
                # 优先 zsize，如 "39.76 GB"
                s = str(v or "").strip()
                if not s:
                    return 0
                # 若传入的是资源项字典，提取 zsize 或其他字段
                if isinstance(v, dict):
                    s = str(v.get("zsize") or v.get("size") or v.get("file_size") or v.get("filesize") or "").strip()
                    if not s:
                        return 0
                # 纯数字（字节）
                if s.isdigit():
                    return int(s)
                import re
                m = re.match(r"^\s*([\d\.]+)\s*([a-zA-Z]+)\s*$", s)
                if not m:
                    # 尝试从中文单位中解析
                    m2 = re.match(r"^\s*([\d\.]+)\s*(字节|KB|MB|GB|TB)\s*$", s, re.IGNORECASE)
                    if not m2:
                        return 0
                    num = float(m2.group(1))
                    unit = m2.group(2).upper()
                else:
                    num = float(m.group(1))
                    unit = m.group(2).upper()
                # 标准单位换算（按 1024）
                if unit in ("B", "BYTE", "BYTES", "字节"):
                    factor = 1
                elif unit in ("KB", "K", "KIB"):
                    factor = 1024
                elif unit in ("MB", "M", "MIB"):
                    factor = 1024 ** 2
                elif unit in ("GB", "G", "GIB"):
                    factor = 1024 ** 3
                elif unit in ("TB", "T", "TIB"):
                    factor = 1024 ** 4
                else:
                    factor = 1
                return int(num * factor)
            except Exception:
                return 0
        result: List[Dict[str, Any]] = []
        for k in order:
            group_list = grouped.get(k, [])
            if group_list:
                try:
                    group_list = sorted(group_list, key=lambda x: parse_size_bytes(x), reverse=True)
                except Exception:
                    pass
                result += group_list
        return result

    def _select_btl_resources_by_quality_priority(self, resources: List[Dict[str, Any]], qp: Dict[str, Any]) -> List[Dict[str, Any]]:
        try:
            res_pri: List[str] = [str(x).lower() for x in (qp.get("resolution_priority") or [])]
            hdr_pri: List[str] = [str(x).upper() for x in (qp.get("hdr_priority") or [])]
            codec_pri: List[str] = [str(x).upper() for x in (qp.get("codec_priority") or [])]
            exclude_res = set([str(x).lower() for x in (qp.get("exclude_resolutions") or [])])
            exclude_hdr = set([str(x).upper() for x in (qp.get("exclude_hdr_types") or [])])
            exclude_codec = set([str(x).upper() for x in (qp.get("exclude_codecs") or [])])
        except Exception:
            res_pri, hdr_pri, codec_pri = [], [], []
            exclude_res, exclude_hdr, exclude_codec = set(), set(), set()
        def parse_size_bytes(v: Dict[str, Any]) -> int:
            s = str(v.get("zsize") or v.get("size") or v.get("file_size") or v.get("filesize") or "").strip()
            if not s:
                return 0
            if s.isdigit():
                try:
                    return int(s)
                except Exception:
                    return 0
            try:
                import re
                m = re.match(r"^\s*([\d\.]+)\s*([a-zA-Z]+)\s*$", s)
                if not m:
                    return 0
                num = float(m.group(1)); unit = m.group(2).upper()
                if unit in ("B", "BYTE", "BYTES", "字节"):
                    factor = 1
                elif unit in ("KB", "K", "KIB"):
                    factor = 1024
                elif unit in ("MB", "M", "MIB"):
                    factor = 1024 ** 2
                elif unit in ("GB", "G", "GIB"):
                    factor = 1024 ** 3
                elif unit in ("TB", "T", "TIB"):
                    factor = 1024 ** 4
                else:
                    factor = 1
                return int(num * factor)
            except Exception:
                return 0
        def extract_features(x: Dict[str, Any]) -> Tuple[str, str, str]:
            name = str(x.get("zname") or x.get("name") or "")
            name_u = name.upper()
            # resolution
            res = ""
            for r in ["2160P", "1080P", "720P", "480P"]:
                if r in name_u:
                    res = r.lower()
                    break
            # hdr
            hdr = ""
            for h in ["DV", "HDR10+", "HDR10", "HDR", "HDR VIVID"]:
                if h in name_u:
                    hdr = h
                    break
            # codec
            codec = ""
            if any(k in name_u for k in ["H265", "X265", "HEVC"]):
                codec = "H265"
            elif any(k in name_u for k in ["H264", "X264", "AVC"]):
                codec = "H264"
            elif "AV1" in name_u:
                codec = "AV1"
            return res, hdr, codec
        def excluded(x: Dict[str, Any]) -> bool:
            g = str(x.get("definition_group") or "")
            n = str(x.get("zname") or x.get("name") or "")
            if ("无字" in n) or ("無字" in n) or ("无字片源" in n):
                return True
            if g.strip().lower() == "3d" or "3D" in g:
                return True
            if ("蓝光原盘" in g) and ("REMUX" not in n.upper()):
                return True
            res, hdr, codec = extract_features(x)
            if res and res in exclude_res:
                return True
            if hdr and hdr in exclude_hdr:
                return True
            if codec and codec in exclude_codec:
                return True
            return False
        # 过滤
        filtered = [r for r in resources if not excluded(r)]
        # 打分：各维度按列表下标值，越小越优；缺失视为末位
        def pri_index(value: str, lst: List[str], normalize_upper: bool = False) -> int:
            if not value:
                return len(lst) + 1
            val = value.upper() if normalize_upper else value
            try:
                return lst.index(val) if not normalize_upper else lst.index(val)
            except ValueError:
                return len(lst) + 1
        def sort_key(x: Dict[str, Any]) -> Tuple[int, int, int, int]:
            res, hdr, codec = extract_features(x)
            ri = pri_index(res, res_pri)  # res_pri 已为小写
            hi = pri_index(hdr, hdr_pri, normalize_upper=True)
            ci = pri_index(codec, codec_pri, normalize_upper=True)
            size = parse_size_bytes(x)
            return (ri, hi, ci, -size)
        try:
            sorted_list = sorted(filtered, key=sort_key)
        except Exception:
            # 兜底：仅按大小
            sorted_list = sorted(filtered, key=lambda x: parse_size_bytes(x), reverse=True)
        logger.info(f"mhnotify: 质量优先级排序完成，候选={len(sorted_list)}")
        if sorted_list:
            top = sorted_list[0]
            logger.info(f"mhnotify: 首选资源 name={str(top.get('zname') or top.get('name') or '')[:80]} size={str(top.get('zsize') or '')}")
        return sorted_list

    def _try_cloud_download_with_candidates(self, candidates: List[Dict[str, Any]], sid: Union[str, int], mh_uuid: str) -> bool:
        total = len(candidates or [])
        for idx, item in enumerate(candidates or [], start=1):
            url = str(item.get("zlink") or item.get("link") or "")
            if not url:
                continue
            name = str(item.get("zname") or item.get("name") or "")
            size = str(item.get("zsize") or "")
            logger.info(f"mhnotify: 尝试云下载候选 {idx}/{total} name={name[:80]} size={size}")
            ok, msg, info = self._add_offline_download(url, start_monitor=True)
            if ok:
                ih = info.get("info_hash")
                if ih:
                    mapping = self.get_data(self._ASSIST_CLOUD_MAP_KEY) or {}
                    mapping[ih] = {"sid": int(sid), "mh_uuid": mh_uuid}
                    self.save_data(self._ASSIST_CLOUD_MAP_KEY, mapping)
                    logger.info(f"mhnotify: 云下载任务已提交，info_hash={str(ih)[:16]}... 已建立回调映射 sid={sid}")
                return True
            else:
                logger.info(f"mhnotify: 云下载失败，继续尝试下一候选。原因={msg}")
        return False

    def _add_offline_download(self, url: str, start_monitor: bool = True) -> Tuple[bool, str, Dict[str, Any]]:
        """
        添加115离线下载任务
        参考 p115client 官方库的 P115Offline.add 方法实现
        :param url: 下载链接（磁力链接、种子URL等）
        :param start_monitor: 是否启动后台监控线程（批量下载时设为False，统一监控）
        :return: (是否成功, 消息文本, 任务信息字典)
        """
        task_info = {}  # 用于返回任务信息，供批量处理使用
        try:
            # 导入p115client
            try:
                from p115client import P115Client
            except ImportError:
                return False, "p115client 未安装，请先安装依赖", task_info

            # 创建115客户端
            client = P115Client(self._p115_cookie, app="web")
            
            # 获取或创建目标目录ID
            target_path = self._cloud_download_path or "/云下载"
            # 标准化路径
            target_path = target_path.strip()
            if not target_path.startswith('/'):
                target_path = '/' + target_path
            target_path = target_path.rstrip('/')
            if not target_path:
                target_path = "/"
            
            target_cid = 0
            
            try:
                if target_path != "/":
                    # 使用 fs_dir_getid 获取目录ID
                    resp = client.fs_dir_getid(target_path)
                    logger.debug(f"mhnotify: fs_dir_getid 响应: {resp}")
                    if resp and resp.get("id"):
                        target_cid = int(resp.get("id"))
                        logger.info(f"mhnotify: 目标目录 {target_path} ID: {target_cid}")
                    elif resp and resp.get("id") == 0:
                        # 目录不存在，需要创建
                        logger.info(f"mhnotify: 目录 {target_path} 不存在，尝试创建...")
                        mkdir_resp = client.fs_makedirs_app(target_path, pid=0)
                        logger.debug(f"mhnotify: fs_makedirs_app 响应: {mkdir_resp}")
                        if mkdir_resp and mkdir_resp.get("cid"):
                            target_cid = int(mkdir_resp.get("cid"))
                            logger.info(f"mhnotify: 创建目录成功，ID: {target_cid}")
                        else:
                            logger.warning(f"mhnotify: 创建目录失败: {mkdir_resp}")
                            target_cid = 0
                    else:
                        logger.warning(f"mhnotify: 获取目录ID失败: {resp}")
                        target_cid = 0
            except Exception as e:
                logger.warning(f"mhnotify: 获取目录ID异常: {e}", exc_info=True)
                target_cid = 0

            # 构建离线下载payload
            # 参考 p115client 源码：单个URL使用 "url" 键，调用 offline_add_url
            download_url = url.strip()
            payload = {"url": download_url}
            if target_cid:
                payload["wp_path_id"] = target_cid
            
            logger.info(f"mhnotify: 添加离线下载任务，目标目录ID: {target_cid}, URL: {download_url[:80]}...")
            
            # 调用115离线下载API（单个URL用 offline_add_url）
            resp = client.offline_add_url(payload)
            logger.debug(f"mhnotify: offline_add_url 响应: {resp}")
            
            # 检查响应
            if not resp:
                return False, "115 API 响应为空"
            
            # 响应可能是dict或其他类型
            if isinstance(resp, dict):
                state = resp.get('state', False)
                
                # 解析返回的任务信息（无论成功还是失败，data中可能都有info_hash）
                data = resp.get('data', {})
                if isinstance(data, dict):
                    info_hash = data.get('info_hash', '')
                    task_name = data.get('name', '')
                    files_list = data.get('files', [])
                else:
                    info_hash = ''
                    task_name = ''
                    files_list = []
                
                if not state:
                    error_msg = resp.get('error_msg', '') or resp.get('error', '未知错误')
                    error_code = resp.get('errcode', '')
                    
                    # 特殊处理错误码10008：任务已存在
                    if error_code == 10008:
                        logger.warning(f"mhnotify: 离线下载任务已存在: {info_hash}")
                        
                        # 构造详细的提示信息
                        exist_msg = "⚠️ 云下载任务已存在\n"
                        exist_msg += f"错误信息: {error_msg}\n"
                        if info_hash:
                            exist_msg += f"任务Hash: {info_hash[:16]}...\n"
                        if files_list:
                            exist_msg += f"包含文件: {len(files_list)} 个\n"
                            # 显示主要文件名（跳过小图片）
                            main_files = [f for f in files_list if f.get('size', 0) > 10*1024*1024]
                            if main_files:
                                exist_msg += f"主要文件: {main_files[0].get('name', '未知')[:50]}...\n"
                        exist_msg += f"保存路径: {target_path}\n"
                        exist_msg += "\nℹ️ 任务已存在，请在115网盘查看下载进度"
                        
                        # 任务已存在时不启动监控线程，避免重复监控
                        # 用户可以手动在115网盘查看任务状态
                        
                        return True, exist_msg, task_info
                    else:
                        # 其他错误
                        logger.error(f"mhnotify: 离线下载失败，响应: {resp}")
                        fail_msg = f"❌ 添加失败\n"
                        fail_msg += f"错误信息: {error_msg}\n"
                        fail_msg += f"错误码: {error_code}"
                        if info_hash:
                            fail_msg += f"\nHash: {info_hash[:16]}..."
                        return False, fail_msg, task_info
                
                # 成功添加
                # 单个URL返回的结构可能不同
                
                if not task_name:
                    # 尝试从其他字段获取
                    task_name = data.get('file_name', '') or data.get('title', '') or '任务已添加'
                
                success_msg = f"任务已添加到115云下载\n"
                if task_name:
                    success_msg += f"任务名称: {task_name}\n"
                success_msg += f"保存路径: {target_path}"
                if info_hash:
                    success_msg += f"\nHash: {info_hash[:16]}..."
                
                logger.info(f"mhnotify: 115离线下载任务添加成功: {task_name or info_hash or '未知'}")
                
                # 填充任务信息，供批量处理使用
                task_info = {
                    "client": client,
                    "info_hash": info_hash,
                    "target_cid": target_cid,
                    "task_name": task_name,
                    "target_path": target_path
                }
                
                # 如果开启了剔除小文件或移动整理功能，且需要启动监控
                if (self._cloud_download_remove_small_files or self._cloud_download_organize) and info_hash and start_monitor:
                    try:
                        if self._cloud_download_remove_small_files:
                            logger.info(f"mhnotify: 云下载剔除小文件已启用，将等待任务完成后处理...")
                        if self._cloud_download_organize:
                            logger.info(f"mhnotify: 云下载移动整理已启用，将等待任务完成后处理...")
                        
                        # 启动异步任务监控下载完成并处理
                        import threading
                        threading.Thread(
                            target=self._monitor_and_remove_small_files,
                            args=(client, info_hash, target_cid, task_name, target_path),
                            daemon=True
                        ).start()
                    except Exception as e:
                        logger.warning(f"mhnotify: 启动后处理任务失败: {e}")
                
                return True, success_msg, task_info
            else:
                # 可能返回的是其他类型
                logger.info(f"mhnotify: 离线下载响应类型: {type(resp)}, 内容: {resp}")
                return True, f"任务已提交到115云下载\n保存路径: {target_path}", task_info
            
        except ImportError as e:
            logger.error(f"mhnotify: 导入p115client失败: {e}")
            return False, f"依赖库导入失败: {str(e)}", task_info
        except Exception as e:
            logger.error(f"mhnotify: 添加115离线下载任务失败: {e}", exc_info=True)
            return False, f"添加失败: {str(e)}", task_info

    def _monitor_batch_downloads(self, tasks: List[Dict[str, Any]]):
        """
        批量监控多个离线下载任务，等待全部完成后统一清理和整理
        如果某个任务10分钟内仍在下载中，将其独立出去单独监控
        :param tasks: 任务信息列表，每个元素包含 client, info_hash, target_cid, task_name, target_path
        """
        import time
        import threading
        
        if not tasks:
            return
        
        logger.info(f"mhnotify: 开始批量监控 {len(tasks)} 个离线下载任务")
        
        # 等待15秒，让任务进入下载队列
        logger.info(f"mhnotify: 等待15秒，让任务进入下载队列...")
        time.sleep(15)
        
        # 任务状态跟踪
        task_status = {}  # info_hash -> {"completed": bool, "success": bool, "actual_cid": int, "is_directory": bool, "split_out": bool}
        task_first_seen_downloading = {}  # info_hash -> 首次发现在下载中的时间戳
        
        for task in tasks:
            task_status[task["info_hash"]] = {
                "completed": False,
                "success": False,
                "actual_cid": task["target_cid"],
                "is_directory": False,
                "task_name": task["task_name"],
                "split_out": False  # 是否已被独立出去
            }
        
        client = tasks[0]["client"]  # 使用第一个任务的client
        target_path = tasks[0]["target_path"]  # 假设所有任务保存到同一目录
        
        # 超时配置
        split_timeout = 600  # 10分钟后将慢任务独立出去
        check_interval = 30  # 检查间隔：30秒（为了更快检测超时）
        max_checks = 1440  # 最多检查12小时（30秒 * 1440 = 12小时）
        
        # ========== 第一阶段：监控所有任务下载完成 ==========
        logger.info(f"mhnotify: 第一阶段 - 监控所有任务下载状态（10分钟超时后独立慢任务）...")
        
        for check_round in range(max_checks):
            all_done = True  # 所有任务都已完成或被独立出去
            current_time = time.time()
            
            for task in tasks:
                info_hash = task["info_hash"]
                status = task_status[info_hash]
                
                # 已完成或已独立出去的任务跳过
                if status["completed"] or status["split_out"]:
                    continue
                
                all_done = False
                
                try:
                    # 先查正在下载列表
                    downloading_task = self._query_downloading_task_by_hash(client, info_hash)
                    
                    if downloading_task and downloading_task.get('status', 0) == 1:
                        # 仍在下载中
                        percent = downloading_task.get('percentDone', 0)
                        
                        # 记录首次发现下载中的时间
                        if info_hash not in task_first_seen_downloading:
                            task_first_seen_downloading[info_hash] = current_time
                            logger.info(f"mhnotify: 任务开始下载: {task['task_name']}")
                        
                        # 检查是否超过10分钟
                        downloading_duration = current_time - task_first_seen_downloading[info_hash]
                        if downloading_duration >= split_timeout:
                            # 超过10分钟，独立出去单独监控
                            logger.info(f"mhnotify: 任务 {task['task_name']} 下载超过10分钟（{percent:.1f}%），独立出去单独监控")
                            status["split_out"] = True
                            
                            # 启动独立的监控线程
                            threading.Thread(
                                target=self._monitor_and_remove_small_files,
                                args=(client, info_hash, task["target_cid"], task["task_name"], task["target_path"]),
                                daemon=True
                            ).start()
                        else:
                            # 每2分钟记录一次进度
                            if check_round % 4 == 0:
                                remaining = int((split_timeout - downloading_duration) / 60)
                                logger.info(f"mhnotify: 正在下载: {task['task_name']} - {percent:.1f}%（{remaining}分钟后独立）")
                        continue
                    
                    # 不在下载列表，查已完成列表
                    current_task = self._query_offline_task_by_hash(client, info_hash)
                    
                    if current_task and isinstance(current_task, dict):
                        task_api_status = current_task.get('status', 0)
                        if task_api_status == 2:
                            # 已完成
                            status["completed"] = True
                            status["success"] = True
                            actual_cid = current_task.get('file_id', '')
                            if actual_cid:
                                try:
                                    status["actual_cid"] = int(actual_cid)
                                except:
                                    pass
                            file_category = current_task.get('file_category', 1)
                            status["is_directory"] = (file_category == 0)
                            logger.info(f"mhnotify: 任务已完成: {task['task_name']}")
                        elif task_api_status == 1:
                            # 失败
                            status["completed"] = True
                            status["success"] = False
                            logger.warning(f"mhnotify: 任务失败: {task['task_name']}")
                    else:
                        # 两处都找不到，可能被删除
                        status["completed"] = True
                        status["success"] = False
                        logger.warning(f"mhnotify: 任务可能已被删除: {task['task_name']}")
                        
                except Exception as e:
                    logger.warning(f"mhnotify: 查询任务 {task['task_name']} 异常: {e}")
            
            if all_done:
                logger.info(f"mhnotify: 批量监控的任务已全部处理完成")
                break
            
            time.sleep(check_interval)
        
        # ========== 第二阶段：统计结果（只统计未被独立出去的任务） ==========
        batch_tasks = [t for t in tasks if not task_status[t["info_hash"]]["split_out"]]
        success_tasks = [t["info_hash"] for t in batch_tasks if task_status[t["info_hash"]]["success"]]
        failed_tasks = [t["info_hash"] for t in batch_tasks if task_status[t["info_hash"]]["completed"] and not task_status[t["info_hash"]]["success"]]
        split_tasks = [t for t in tasks if task_status[t["info_hash"]]["split_out"]]
        
        logger.info(f"mhnotify: 批量任务统计 - 成功: {len(success_tasks)}, 失败: {len(failed_tasks)}, 独立监控: {len(split_tasks)}")
        
        # 如果没有成功的任务，直接发送通知并结束
        if not success_tasks:
            if split_tasks:
                # 有任务被独立出去，发送部分通知
                self._send_batch_cloud_download_notification(
                    tasks=batch_tasks,
                    task_status=task_status,
                    removed_count=0,
                    removed_size_mb=0,
                    split_count=len(split_tasks)
                )
            logger.info(f"mhnotify: 批量监控无成功任务，结束")
            return
        
        # ========== 第三阶段：统一清理小文件 ==========
        total_removed_count = 0
        total_removed_size = 0
        
        if self._cloud_download_remove_small_files and success_tasks:
            logger.info(f"mhnotify: 开始统一清理小文件...")
            time.sleep(5)  # 等待文件列表同步
            
            for info_hash in success_tasks:
                status = task_status[info_hash]
                if status["is_directory"]:
                    try:
                        removed_count, removed_size = self._remove_small_files_in_directory(client, status["actual_cid"])
                        total_removed_count += removed_count
                        total_removed_size += removed_size
                        if removed_count > 0:
                            logger.info(f"mhnotify: 任务 {status['task_name']} 清理了 {removed_count} 个小文件")
                    except Exception as e:
                        logger.warning(f"mhnotify: 清理任务 {status['task_name']} 小文件异常: {e}")
        
        # ========== 第四阶段：统一执行一次移动整理 ==========
        if self._cloud_download_organize and target_path and success_tasks:
            logger.info(f"mhnotify: 开始统一移动整理...")
            try:
                access_token = self._get_mh_access_token()
                if access_token:
                    self._organize_cloud_download(access_token, target_path)
                else:
                    logger.error(f"mhnotify: 无法获取MH access token，跳过移动整理")
            except Exception as e:
                logger.error(f"mhnotify: 移动整理异常: {e}")
        
        # ========== 第五阶段：发送汇总通知 ==========
        self._send_batch_cloud_download_notification(
            tasks=batch_tasks,
            task_status=task_status,
            removed_count=total_removed_count,
            removed_size_mb=total_removed_size / 1024 / 1024,
            split_count=len(split_tasks)
        )
        
        logger.info(f"mhnotify: 批量离线下载监控任务结束")

    def _send_batch_cloud_download_notification(self, tasks: List[Dict[str, Any]], 
                                                  task_status: Dict[str, Dict],
                                                  removed_count: int, removed_size_mb: float,
                                                  split_count: int = 0):
        """
        发送批量云下载完成的汇总通知
        :param split_count: 被独立出去单独监控的任务数量
        """
        try:
            success_count = sum(1 for s in task_status.values() if s.get("success"))
            fail_count = sum(1 for s in task_status.values() if s.get("completed") and not s.get("success") and not s.get("split_out"))
            
            title = f"✅ 115云下载批量任务完成"
            if fail_count > 0:
                title = f"⚠️ 115云下载批量任务完成（{fail_count}个失败）"
            
            text_parts = [f"📦 共 {len(tasks) + split_count} 个任务"]
            status_line = f"✅ 成功: {success_count} | ❌ 失败: {fail_count}"
            if split_count > 0:
                status_line += f" | ⏳ 独立监控: {split_count}"
            text_parts.append(status_line)
            
            # 列出任务名称
            if tasks:
                text_parts.append("")
                for task in tasks:
                    info_hash = task["info_hash"]
                    status = task_status.get(info_hash, {})
                    if status.get("success"):
                        text_parts.append(f"✅ {task['task_name'][:30]}")
                    elif status.get("split_out"):
                        text_parts.append(f"⏳ {task['task_name'][:30]}")
                    else:
                        text_parts.append(f"❌ {task['task_name'][:30]}")
            
            if split_count > 0:
                text_parts.append("")
                text_parts.append(f"ℹ️ {split_count} 个慢任务已独立监控，完成后将单独通知")
            
            if removed_count > 0:
                text_parts.append("")
                text_parts.append(f"🧹 清理小文件: {removed_count} 个")
                text_parts.append(f"💾 释放空间: {removed_size_mb:.2f} MB")
            
            text = "\n".join(text_parts)
            
            self.post_message(
                mtype=None,
                title=title,
                text=text
            )
            logger.info(f"mhnotify: 批量云下载完成通知已发送")
        except Exception as e:
            logger.error(f"mhnotify: 发送批量云下载通知失败: {e}", exc_info=True)

    def _monitor_and_remove_small_files(self, client, info_hash: str, target_cid: int, task_name: str, target_path: str = ""):
        """
        监控离线下载任务完成后删除小文件
        :param client: P115Client实例
        :param info_hash: 任务hash
        :param target_cid: 目标目录ID
        :param task_name: 任务名称
        :param target_path: 云下载目标路径
        """
        try:
            import time
            logger.info(f"mhnotify: 开始监控离线下载任务: {task_name}")
            
            # 添加任务后等待15秒，让任务有时间出现在下载列表中
            logger.info(f"mhnotify: 等待15秒，让任务进入下载队列...")
            time.sleep(15)
            
            # ========== 第一阶段：监控正在下载 ==========
            # 使用 stat=12 查询正在下载的任务
            logger.info(f"mhnotify: 第一阶段 - 监控正在下载状态...")
            
            normal_check_interval = 120  # 正常检查间隔：2分钟
            max_downloading_checks = 720  # 最多检查24小时
            
            task_found = False  # 标记是否至少找到过一次任务
            not_found_count = 0  # 连续未找到任务的次数
            max_not_found_after_found = 3  # 任务出现后最多容忍3次未找到才认为已完成
            
            for i in range(max_downloading_checks):
                try:
                    # 查询正在下载的任务（stat=12）
                    downloading_task = self._query_downloading_task_by_hash(client, info_hash)
                    
                    if downloading_task:
                        # 找到任务了
                        task_found = True
                        not_found_count = 0  # 重置未找到计数
                        
                        percent = downloading_task.get('percentDone', 0)
                        status = downloading_task.get('status', 0)
                        
                        # status=1 表示正在下载
                        if status == 1:
                            # 使用正常检查间隔（2分钟）
                            if i % 5 == 0:  # 每10分钟记录一次进度
                                logger.info(f"mhnotify: 正在下载: {task_name} - {percent:.1f}%")
                            time.sleep(normal_check_interval)
                            continue
                        else:
                            # status != 1，可能下载完成或失败，跳出循环
                            logger.info(f"mhnotify: 任务状态变化 (status={status})，进入下一阶段...")
                            break
                    else:
                        # 正在下载列表中未找到任务
                        not_found_count += 1
                        
                        if not task_found:
                            # 任务从未在下载列表中找到过，直接进入第二阶段查找已完成任务
                            logger.info(f"mhnotify: 未在下载列表中找到任务，进入已完成检查阶段...")
                            break
                        else:
                            # 任务之前找到过，现在找不到了
                            if not_found_count >= max_not_found_after_found:
                                # 连续多次找不到，说明已完成或失败，进入第二阶段
                                logger.info(f"mhnotify: 任务已不在下载列表中，进入已完成检查阶段...")
                                break
                            else:
                                logger.debug(f"mhnotify: 暂时未找到任务 ({not_found_count}/{max_not_found_after_found})，继续等待...")
                                time.sleep(normal_check_interval)
                                continue
                        
                except Exception as e:
                    logger.warning(f"mhnotify: 查询正在下载任务异常: {e}")
                    # 出现异常直接进入第二阶段
                    logger.info(f"mhnotify: 查询异常，进入已完成检查阶段...")
                    break
            
            # ========== 第二阶段：检查已完成任务 ==========
            logger.info(f"mhnotify: 第二阶段 - 检查已完成任务...")
            
            # 等待3秒，确保任务状态同步
            time.sleep(3)
            
            # 使用 stat=11 查询所有任务（包括已完成）
            max_completed_checks = 5  # 最多检查5次
            completed_check_interval = 10  # 10秒
            consecutive_failures = 0
            max_consecutive_failures = 3
            
            for i in range(max_completed_checks):
                try:
                    # 使用115 Web API查询离线任务列表（stat=11 查询所有任务）
                    current_task = self._query_offline_task_by_hash(client, info_hash)
                    
                    # 类型检查：确保返回的是字典
                    if current_task and not isinstance(current_task, dict):
                        logger.warning(f"mhnotify: 查询任务返回类型错误: {type(current_task)}")
                        current_task = None
                    
                    if not current_task:
                        # 未找到任务
                        consecutive_failures += 1
                        logger.warning(f"mhnotify: 未找到已完成任务 {info_hash[:16]}... (尝试 {consecutive_failures}/{max_consecutive_failures})")
                        
                        if consecutive_failures >= max_consecutive_failures:
                            # 连续多次未找到，进一步检查失败列表
                            failed_task = self._query_offline_failed_task_by_hash(client, info_hash)
                            if failed_task:
                                logger.error(f"mhnotify: 任务 {info_hash[:16]}... 在失败列表中存在，发送失败通知")
                                self._send_cloud_download_failed_notification(task_name)
                            else:
                                logger.error(f"mhnotify: 未找到云下载任务，可能已被删除")
                                self._send_cloud_download_deleted_notification(task_name)
                                try:
                                    mapping = self.get_data(self._ASSIST_CLOUD_MAP_KEY) or {}
                                    info = mapping.get(info_hash)
                                    if info:
                                        sid = info.get("sid")
                                        with SessionFactory() as db:
                                            SubscribeOper(db=db).update(int(sid), {"state": "R"})
                                        mapping.pop(info_hash, None)
                                        self.save_data(self._ASSIST_CLOUD_MAP_KEY, mapping)
                                        logger.info(f"mhnotify: 云下载任务已删除，已恢复MP订阅启用 sid={sid}")
                                except Exception:
                                    pass
                            break
                        
                        time.sleep(completed_check_interval)
                        continue
                    
                    # 查询成功，重置失败计数
                    consecutive_failures = 0
                    
                    # 检查任务状态：2=已完成, 1=失败, 0=下载中
                    status = current_task.get('status', 0)
                    if status == 2:
                        logger.info(f"mhnotify: 离线下载任务已完成: {task_name}")
                        
                        # 从任务信息中获取实际文件/文件夹ID（file_id）
                        # file_id 是下载完成后的文件或文件夹的实际ID
                        actual_cid = current_task.get('file_id', '')
                        if actual_cid:
                            try:
                                actual_cid = int(actual_cid)
                            except:
                                actual_cid = target_cid
                        else:
                            actual_cid = target_cid
                        
                        logger.info(f"mhnotify: 实际文件/文件夹ID: {actual_cid}")
                        
                        # 检查 file_category，只有文件夹才需要清理小文件
                        file_category = current_task.get('file_category', 1)
                        is_directory = (file_category == 0)
                        
                        # 记录清理结果用于通知
                        removed_count = 0
                        removed_size_mb = 0.0
                        
                        # 如果开启了剔除小文件，先删除小文件
                        if self._cloud_download_remove_small_files:
                            if is_directory:
                                logger.info(f"mhnotify: 检测到文件夹，开始清理小文件...")
                                time.sleep(5)  # 等待5秒确保文件列表同步
                                removed_count, removed_size = self._remove_small_files_in_directory(client, actual_cid)
                                removed_size_mb = removed_size / 1024 / 1024
                            else:
                                logger.info(f"mhnotify: 检测到单个文件，跳过小文件清理")
                        
                        # 如果开启了移动整理，执行移动整理
                        if self._cloud_download_organize and target_path:
                            logger.info(f"mhnotify: 开始移动整理...")
                            # 获取MH access token
                            access_token = self._get_mh_access_token()
                            if access_token:
                                self._organize_cloud_download(access_token, target_path)
                            else:
                                logger.error(f"mhnotify: 无法获取MH access token，跳过移动整理")
                        
                        # 发送云下载完成通知
                        self._send_cloud_download_notification(task_name, removed_count, removed_size_mb)
                        
                        try:
                            mapping = self.get_data(self._ASSIST_CLOUD_MAP_KEY) or {}
                            info = mapping.get(info_hash)
                            if info:
                                sid = info.get("sid")
                                mh_uuid = info.get("mh_uuid")
                                # 不在此处删除MH，改为完成MP订阅后由 SubscribeComplete 事件删除MH
                                with SessionFactory() as db:
                                    sub = SubscribeOper(db=db).get(int(sid))
                                if sub:
                                    self._finish_mp_subscribe(sub)
                                mapping.pop(info_hash, None)
                                self.save_data(self._ASSIST_CLOUD_MAP_KEY, mapping)
                                logger.info(f"mhnotify: 云下载辅助完成，已完成MP订阅，MH删除由事件触发 sid={sid}")
                        except Exception:
                            pass
                        
                        break
                    elif status == 1:
                        logger.warning(f"mhnotify: 离线下载任务失败: {task_name}")
                        self._send_cloud_download_failed_notification(task_name)
                        try:
                            mapping = self.get_data(self._ASSIST_CLOUD_MAP_KEY) or {}
                            info = mapping.get(info_hash)
                            if info:
                                sid = info.get("sid")
                                with SessionFactory() as db:
                                    SubscribeOper(db=db).update(int(sid), {"state": "R"})
                                mapping.pop(info_hash, None)
                                self.save_data(self._ASSIST_CLOUD_MAP_KEY, mapping)
                                logger.info(f"mhnotify: 云下载辅助失败，已恢复MP订阅启用 sid={sid}")
                        except Exception:
                            pass
                        break
                    else:
                        # status 不为 2 也不为 1，继续等待
                        logger.info(f"mhnotify: 任务状态: {status}，继续等待...")
                        time.sleep(completed_check_interval)
                        
                except Exception as e:
                    consecutive_failures += 1
                    logger.warning(f"mhnotify: 检查已完成任务异常 ({consecutive_failures}/{max_consecutive_failures}): {e}")
                    
                    if consecutive_failures >= max_consecutive_failures:
                        logger.error(f"mhnotify: 连续{max_consecutive_failures}次检查失败，停止监控任务: {task_name}")
                        break
                    
                    time.sleep(completed_check_interval)
            
            logger.info(f"mhnotify: 离线下载监控任务结束: {task_name} (Hash: {info_hash[:16]}...)")
            
        except Exception as e:
            logger.error(f"mhnotify: 监控离线下载任务异常: {e}", exc_info=True)

    def _query_offline_task_by_hash(self, client, info_hash: str) -> Optional[Dict[str, Any]]:
        """
        使用115 Web API查询离线任务（通过info_hash匹配）
        :param client: P115Client实例
        :param info_hash: 任务hash
        :return: 任务信息字典或None
        """
        try:
            # 构造请求参数
            # 参考 115-ol-list.txt，需要 page, stat, uid, sign, time 参数
            import time as time_module
            import hashlib
            
            # 获取用户ID（从cookie或client中获取）
            uid = None
            try:
                # 尝试从client获取用户信息
                user_info = client.fs_userinfo()
                if user_info and isinstance(user_info, dict):
                    uid = user_info.get('user_id')
            except:
                pass
            
            if not uid:
                # 从cookie中解析UID
                cookie_dict = {}
                for item in self._p115_cookie.split(';'):
                    item = item.strip()
                    if '=' in item:
                        k, v = item.split('=', 1)
                        cookie_dict[k.strip()] = v.strip()
                uid_str = cookie_dict.get('UID', '')
                if uid_str and '_' in uid_str:
                    uid = uid_str.split('_')[0]
            
            if not uid:
                logger.warning(f"mhnotify: 无法获取115用户ID")
                return None
            
            # 构造签名（参考115-ol-list API）
            timestamp = int(time_module.time())
            # 签名算法：md5(uid + time)，实际算法可能不同，这里先尝试简单方式
            sign_str = f"{uid}{timestamp}"
            sign = hashlib.md5(sign_str.encode()).hexdigest()
            
            # 调用离线任务列表API
            # stat=11表示查询所有任务（包括已完成）
            url = "https://115.com/web/lixian/?ct=lixian&ac=task_lists"
            params = {
                'page': 1,
                'stat': 11,  # 11=所有任务
                'uid': uid,
                'sign': sign,
                'time': timestamp
            }
            
            # 使用RequestUtils发送请求
            headers = {
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "Cookie": self._p115_cookie,
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }
            
            response = RequestUtils(headers=headers).post_res(url, data=params)
            if not response or response.status_code != 200:
                logger.debug(f"mhnotify: 查询离线任务列表失败: {response.status_code if response else 'No response'}")
                return None
            
            result = response.json()
            if not result or not result.get('state'):
                logger.debug(f"mhnotify: 离线任务列表响应异常: {result}")
                return None
            
            # 查找匹配的任务
            tasks = result.get('tasks', [])
            for task in tasks:
                if task.get('info_hash', '').lower() == info_hash.lower():
                    return task
            
            return None
        
        except Exception as e:
            logger.debug(f"mhnotify: 查询离线任务异常: {e}")
            return None

    def _query_offline_failed_task_by_hash(self, client, info_hash: str) -> Optional[Dict[str, Any]]:
        try:
            import time as time_module
            import hashlib
            uid = self._get_115_uid()
            if not uid:
                logger.warning(f"mhnotify: 无法获取115用户ID")
                return None
            timestamp = int(time_module.time())
            sign = hashlib.md5(f"{uid}{timestamp}".encode()).hexdigest()
            url = "https://115.com/web/lixian/?ct=lixian&ac=task_lists"
            params = {
                'page': 1,
                'stat': 1,
                'uid': uid,
                'sign': sign,
                'time': timestamp
            }
            headers = {
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "Cookie": self._p115_cookie,
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }
            response = RequestUtils(headers=headers).post_res(url, data=params)
            if not response or response.status_code != 200:
                logger.debug(f"mhnotify: 查询失败任务列表失败: {response.status_code if response else 'No response'}")
                return None
            result = response.json()
            if not result or not result.get('state'):
                logger.debug(f"mhnotify: 失败任务列表响应异常: {result}")
                return None
            tasks = result.get('tasks', [])
            for task in tasks:
                if task.get('info_hash', '').lower() == info_hash.lower():
                    return task
            return None
        except Exception as e:
            logger.debug(f"mhnotify: 查询失败任务异常: {e}")
            return None

    def _remove_small_files_in_directory(self, client, cid: int) -> Tuple[int, int]:
        """
        删除目录中小于10MB的文件（递归遍历子目录）
        :param client: P115Client实例
        :param cid: 目录ID (文件夹的file_id)
        :return: (删除文件数量, 删除文件总大小字节数)
        """
        try:
            logger.info(f"mhnotify: 开始递归清理小文件，根目录cid={cid}")
            
            min_size = 10 * 1024 * 1024  # 10MB
            removed_count = 0
            removed_size = 0
            
            # 使用 p115client.tool.iterdir 的 iter_files 递归遍历所有文件
            try:
                from p115client.tool.iterdir import iter_files  # type: ignore
                logger.info("mhnotify: 使用 iter_files 递归遍历目录...")
                
                # iter_files 会递归返回所有文件（不含目录）
                for attr in iter_files(client, cid):
                    try:
                        # attr 是一个 dict，包含 id, parent_id, name, size, is_dir 等
                        if not isinstance(attr, dict):
                            continue
                        
                        # iter_files 只返回文件，但还是检查一下
                        is_dir = attr.get('is_dir', False)
                        if is_dir:
                            continue
                        
                        file_id = attr.get('id') or attr.get('fid') or attr.get('file_id')
                        file_name = attr.get('name') or attr.get('n') or attr.get('fn') or ''
                        file_size = attr.get('size') or attr.get('fs') or attr.get('s') or 0
                        
                        if isinstance(file_size, str):
                            try:
                                file_size = int(file_size)
                            except:
                                file_size = 0
                        
                        if not file_id:
                            logger.debug(f"mhnotify: 文件无ID，跳过: {file_name}")
                            continue
                        
                        logger.debug(f"mhnotify: 检查文件: {file_name}, 大小: {file_size/1024/1024:.2f}MB")
                        
                        # 如果文件小于10MB，且不是字幕文件（srt, ass, sup），则删除
                        if file_size < min_size:
                            # 检查扩展名，忽略字幕文件
                            if any(file_name.lower().endswith(ext) for ext in ('.srt', '.ass', '.sup')):
                                logger.debug(f"mhnotify: 跳过字幕文件: {file_name}")
                                continue

                            try:
                                logger.info(f"mhnotify: 准备删除小文件: {file_name} ({file_size/1024/1024:.2f}MB)")
                                client.fs_delete(file_id)
                                removed_count += 1
                                removed_size += file_size
                                logger.info(f"mhnotify: 成功删除小文件: {file_name}")
                            except Exception as e:
                                logger.warning(f"mhnotify: 删除文件失败 {file_name}: {e}")
                    except Exception as e:
                        logger.debug(f"mhnotify: 处理文件项异常: {e}")
                        continue
                        
            except ImportError:
                logger.warning("mhnotify: iter_files 导入失败，使用备用方案...")
                # 备用方案：手动递归遍历
                removed_count, removed_size = self._remove_small_files_recursive(client, cid, min_size)
            except Exception as e:
                logger.warning(f"mhnotify: iter_files 调用失败: {e}，使用备用方案...")
                removed_count, removed_size = self._remove_small_files_recursive(client, cid, min_size)
            
            if removed_count > 0:
                logger.info(f"mhnotify: 云下载小文件清理完成，共删除 {removed_count} 个文件，释放空间 {removed_size/1024/1024:.2f}MB")
            else:
                logger.info(f"mhnotify: 云下载目录中没有小于10MB的文件需要删除")
            
            return removed_count, removed_size
                
        except Exception as e:
            logger.error(f"mhnotify: 删除小文件异常: {e}", exc_info=True)
            return 0, 0

    def _remove_small_files_recursive(self, client, cid: int, min_size: int) -> Tuple[int, int]:
        """
        备用方案：手动递归遍历目录删除小文件
        :param client: P115Client实例
        :param cid: 目录ID
        :param min_size: 最小文件大小阈值（字节）
        :return: (删除数量, 删除大小)
        """
        removed_count = 0
        removed_size = 0
        
        # 使用栈实现非递归遍历，避免深层递归导致栈溢出
        dir_stack = [cid]
        
        while dir_stack:
            current_cid = dir_stack.pop()
            logger.debug(f"mhnotify: 遍历目录 cid={current_cid}")
            
            offset = 0
            limit = 1000
            
            while True:
                try:
                    # 调用 fs_files 获取目录内容
                    resp = client.fs_files(cid=current_cid, limit=limit, offset=offset)
                except Exception as e:
                    logger.warning(f"mhnotify: fs_files 调用失败 (cid={current_cid}): {e}")
                    break
                
                # 处理响应
                items = []
                if isinstance(resp, dict):
                    items = resp.get('data', []) or resp.get('files', [])
                elif hasattr(resp, '__iter__'):
                    try:
                        items = list(resp)
                    except:
                        break
                
                if not items:
                    break
                
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    
                    # 判断是文件还是目录
                    # 根据 p115client 的逻辑：
                    # - 如果有 'n' 字段: 没有 'fid' 的是目录
                    # - 如果有 'fn' 字段: fc == "0" 是目录，fc == "1" 是文件
                    is_dir = False
                    if 'n' in item:
                        # 新格式：没有 fid 的是目录
                        is_dir = 'fid' not in item
                    elif 'fn' in item:
                        # 老格式：fc 字段
                        fc = item.get('fc')
                        is_dir = (fc == '0' or fc == 0)
                    else:
                        # 其他格式：检查 file_category
                        fc = item.get('file_category')
                        is_dir = (fc == 0 or fc == '0')
                    
                    if is_dir:
                        # 是目录，获取目录ID并加入栈
                        sub_cid = item.get('cid') or item.get('id') or item.get('category_id')
                        if sub_cid:
                            try:
                                sub_cid = int(sub_cid)
                                dir_stack.append(sub_cid)
                                dir_name = item.get('n') or item.get('fn') or item.get('name') or item.get('category_name') or ''
                                logger.debug(f"mhnotify: 发现子目录: {dir_name} (cid={sub_cid})")
                            except:
                                pass
                    else:
                        # 是文件，检查大小
                        file_id = item.get('fid') or item.get('file_id') or item.get('id')
                        file_name = item.get('n') or item.get('fn') or item.get('name') or item.get('file_name') or ''
                        file_size = item.get('s') or item.get('fs') or item.get('size') or item.get('file_size') or 0
                        
                        if isinstance(file_size, str):
                            try:
                                file_size = int(file_size)
                            except:
                                file_size = 0
                        
                        if not file_id:
                            continue
                        
                        logger.debug(f"mhnotify: 检查文件: {file_name}, 大小: {file_size/1024/1024:.2f}MB")
                        
                        if file_size < min_size:
                            # 检查扩展名，忽略字幕文件
                            if any(file_name.lower().endswith(ext) for ext in ('.srt', '.ass', '.sup')):
                                logger.debug(f"mhnotify: 跳过字幕文件: {file_name}")
                                continue

                            try:
                                logger.info(f"mhnotify: 准备删除小文件: {file_name} ({file_size/1024/1024:.2f}MB)")
                                client.fs_delete(file_id)
                                removed_count += 1
                                removed_size += file_size
                                logger.info(f"mhnotify: 成功删除小文件: {file_name}")
                            except Exception as e:
                                logger.warning(f"mhnotify: 删除文件失败 {file_name}: {e}")
                
                if len(items) < limit:
                    break
                offset += limit
        
        return removed_count, removed_size

    def _send_cloud_download_notification(self, task_name: str, removed_count: int, removed_size_mb: float):
        """
        发送云下载完成通知
        :param task_name: 任务名称
        :param removed_count: 删除的小文件数量
        :param removed_size_mb: 删除的文件总大小(MB)
        """
        try:
            # 构建通知消息
            title = "✅ 115云下载完成"
            text_parts = [f"📦 任务: {task_name}"]
            
            if removed_count > 0:
                text_parts.append(f"🧹 清理小文件: {removed_count} 个")
                text_parts.append(f"💾 释放空间: {removed_size_mb:.2f} MB")
            
            text = "\n".join(text_parts)
            
            # 发送通知
            self.post_message(
                mtype=None,
                title=title,
                text=text
            )
            logger.info(f"mhnotify: 云下载完成通知已发送: {task_name}")
        except Exception as e:
            logger.error(f"mhnotify: 发送云下载通知失败: {e}", exc_info=True)

    def _get_mh_access_token(self, max_retries: int = 5) -> Optional[str]:
        """
        获取MH access token，支持重试
        :param max_retries: 最大重试次数（默认5次）
        :return: access token或None
        """
        for attempt in range(1, max_retries + 1):
            try:
                login_url = f"{self._mh_domain}/api/v1/auth/login"
                login_payload = {
                    "username": self._mh_username,
                    "password": self._mh_password
                }
                headers = {
                    "Accept": "application/json, text/plain, */*",
                    "Content-Type": "application/json;charset=UTF-8",
                    "Origin": self._mh_domain,
                    "Accept-Language": "zh-CN",
                    "User-Agent": "MoviePilot/Plugin MHNotify"
                }
                login_res = RequestUtils(headers=headers).post(login_url, json=login_payload)
                if not login_res or login_res.status_code != 200:
                    if attempt < max_retries:
                        wait_time = 2 ** (attempt - 1)
                        logger.warning(f"mhnotify: MH登录失败（第{attempt}/{max_retries}次），{wait_time}秒后重试...")
                        time.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"mhnotify: MH登录失败，已重试{max_retries}次")
                        return None
                
                try:
                    login_data = login_res.json()
                    access_token = login_data.get("data", {}).get("access_token")
                    if access_token:
                        if attempt > 1:
                            logger.info(f"mhnotify: MH登录成功（第{attempt}次尝试）")
                        return access_token
                    else:
                        if attempt < max_retries:
                            wait_time = 2 ** (attempt - 1)
                            logger.warning(f"mhnotify: 未获取到 access token（第{attempt}/{max_retries}次），{wait_time}秒后重试...")
                            time.sleep(wait_time)
                            continue
                        else:
                            logger.error(f"mhnotify: 未获取到 access token，已重试{max_retries}次")
                            return None
                except Exception as parse_err:
                    if attempt < max_retries:
                        wait_time = 2 ** (attempt - 1)
                        logger.warning(f"mhnotify: 解析MH登录响应失败（第{attempt}/{max_retries}次）: {parse_err}，{wait_time}秒后重试...")
                        time.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"mhnotify: 解析MH登录响应失败，已重试{max_retries}次")
                        return None
            except Exception as e:
                if attempt < max_retries:
                    wait_time = 2 ** (attempt - 1)
                    logger.warning(f"mhnotify: 获取MH access token异常（第{attempt}/{max_retries}次）: {e}，{wait_time}秒后重试...")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"mhnotify: 获取MH access token异常，已重试{max_retries}次: {e}")
                    return None
        
        logger.error(f"mhnotify: MH登录失败，已达到最大重试次数{max_retries}")
        return None

    def _organize_cloud_download(self, access_token: str, target_path: str):
        """
        云下载完成后移动到MH默认目录并整理
        :param access_token: MH access token
        :param target_path: 云下载目标路径
        """
        try:
            logger.info(f"mhnotify: 开始云下载移动整理流程，目标路径: {target_path}")
            
            # 1. 获取115网盘账户信息
            cloud_url = f"{self._mh_domain}/api/v1/cloud-accounts?active_only=true"
            headers = {
                "Accept": "application/json, text/plain, */*",
                "Authorization": f"Bearer {access_token}",
                "User-Agent": "MoviePilot/Plugin MHNotify",
                "Accept-Language": "zh-CN"
            }
            
            logger.info(f"mhnotify: 正在获取云账户列表...")
            cloud_res = RequestUtils(headers=headers).get_res(cloud_url)
            
            if not cloud_res:
                logger.error(f"mhnotify: 获取云账户列表失败 - 响应为空")
                return
            
            if cloud_res.status_code != 200:
                logger.error(f"mhnotify: 获取云账户列表失败 - 状态码: {cloud_res.status_code}, 响应: {cloud_res.text[:500]}")
                return
            
            try:
                cloud_data = cloud_res.json()
                logger.debug(f"mhnotify: 云账户响应数据: {cloud_data}")
            except Exception as e:
                logger.error(f"mhnotify: 解析云账户响应失败 - {e}, 原始响应: {cloud_res.text[:500]}")
                return
            
            # 查找第一个115网盘账户
            accounts = cloud_data.get("data", {}).get("accounts", [])
            logger.info(f"mhnotify: 找到 {len(accounts)} 个云账户")
            
            drive115_account = None
            for account in accounts:
                account_type = account.get("cloud_type")
                account_name = account.get("name")
                logger.debug(f"mhnotify: 检查账户: {account_name} (类型: {account_type})")
                if account_type == "drive115":
                    drive115_account = account
                    break
            
            if not drive115_account:
                logger.error(f"mhnotify: 未找到115网盘账户，可用账户类型: {[a.get('cloud_type') for a in accounts]}")
                return
            
            account_identifier = drive115_account.get("external_id")
            logger.info(f"mhnotify: 找到115网盘账户: {drive115_account.get('name')}, ID: {account_identifier}")
            
            # 2. 提交网盘目录分析任务
            analyze_url = f"{self._mh_domain}/api/v1/library-tool/analyze-cloud-directory-async"
            analyze_payload = {
                "cloud_type": "drive115",
                "account_identifier": account_identifier,
                "cloud_path": target_path
            }
            
            logger.info(f"mhnotify: 正在提交网盘分析任务...")
            logger.debug(f"mhnotify: 分析任务参数: {analyze_payload}")
            
            analyze_res = RequestUtils(headers=headers).post_res(analyze_url, json=analyze_payload)
            
            if not analyze_res:
                logger.error(f"mhnotify: 提交网盘分析任务失败 - 响应为空")
                return
            
            if analyze_res.status_code != 200:
                logger.error(f"mhnotify: 提交网盘分析任务失败 - 状态码: {analyze_res.status_code}, 响应: {analyze_res.text[:500]}")
                return
            
            try:
                analyze_data = analyze_res.json()
                logger.debug(f"mhnotify: 分析任务响应数据: {analyze_data}")
            except Exception as e:
                logger.error(f"mhnotify: 解析分析任务响应失败 - {e}, 原始响应: {analyze_res.text[:500]}")
                return
            
            task_id = analyze_data.get("data", {}).get("task_id")
            if not task_id:
                message = analyze_data.get("message", "")
                logger.error(f"mhnotify: 未获取到分析任务ID - message: {message}, 完整响应: {analyze_data}")
                return
            
            logger.info(f"mhnotify: 网盘分析任务已提交，task_id: {task_id}")
            
            # 3. 循环查询进度直到完成
            import time
            max_wait = 300  # 最多等待5分钟
            elapsed = 0
            
            while elapsed < max_wait:
                time.sleep(2)
                elapsed += 2
                
                progress_url = f"{self._mh_domain}/api/v1/library-tool/analysis-task/{task_id}/progress"
                progress_res = RequestUtils(headers=headers).get_res(progress_url)
                
                if not progress_res or progress_res.status_code != 200:
                    logger.warning(f"mhnotify: 查询分析进度失败，继续等待...")
                    continue
                
                try:
                    progress_data = progress_res.json()
                except Exception:
                    logger.warning(f"mhnotify: 解析进度响应失败，继续等待...")
                    continue
                
                task_info = progress_data.get("data", {})
                progress = task_info.get("progress", 0)
                status = task_info.get("status", "")
                current_step = task_info.get("current_step", "")
                
                logger.debug(f"mhnotify: 分析进度 {progress}% - {current_step}")
                
                if progress >= 100 and status == "completed":
                    logger.info(f"mhnotify: 网盘分析任务完成")
                    break
                elif status == "failed":
                    error = task_info.get("error", "未知错误")
                    logger.error(f"mhnotify: 网盘分析任务失败: {error}")
                    return
            
            if elapsed >= max_wait:
                logger.warning(f"mhnotify: 网盘分析任务超时，跳过移动整理")
                return
            
            # 等待3秒后再进行下一步，确保后端处理完成
            logger.info(f"mhnotify: 网盘分析完成，等待3秒后继续...")
            time.sleep(3)
            
            # 4. 获取默认目录配置
            defaults_url = f"{self._mh_domain}/api/v1/subscription/config/cloud-defaults"
            logger.info(f"mhnotify: 正在获取默认目录配置...")
            defaults_res = RequestUtils(headers=headers).get_res(defaults_url)
            
            if not defaults_res:
                logger.error(f"mhnotify: 获取默认目录配置失败 - 响应为空")
                return
            
            if defaults_res.status_code != 200:
                logger.error(f"mhnotify: 获取默认目录配置失败 - 状态码: {defaults_res.status_code}, 响应: {defaults_res.text[:500]}")
                return
            
            try:
                defaults_data = defaults_res.json()
                logger.debug(f"mhnotify: 默认目录配置数据: {defaults_data}")
            except Exception as e:
                logger.error(f"mhnotify: 解析默认目录配置失败 - {e}, 原始响应: {defaults_res.text[:500]}")
                return
            
            account_configs = defaults_data.get("data", {}).get("account_configs", {})
            account_config = account_configs.get(account_identifier, {})
            default_directory = account_config.get("default_directory", "/影视")

            
            logger.info(f"m2hnotify: 获取到默认目录: {default_directory}")
            logger.debug(f"mhnotify: 账户配置: {account_config}")
            
            # 5. 提交文件整理任务
            logger.info(f"mhnotify: 等待3秒后提交文件整理任务...")
            time.sleep(3)
            
            organize_url = f"{self._mh_domain}/api/v1/library-tool/organize-files-async"
            organize_payload = {
                "task_id": task_id,  # 使用网盘分析任务的task_id
                "cloud_type": "drive115",
                "source_path": target_path,
                "account_identifier": account_identifier,
                "target_folder_path": default_directory,
                "is_share_link": False,
                "operation_mode": "move",
                "include_series": True,
                "include_movies": True
            }
            
            logger.info(f"mhnotify: 准备提交文件整理任务")
            logger.info(f"mhnotify: 请求URL: {organize_url}")
            logger.info(f"mhnotify: 请求参数: {organize_payload}")
            logger.info(f"mhnotify: 请求头Authorization: Bearer {access_token[:20]}...")
            
            try:
                organize_res = RequestUtils(headers=headers).post_res(organize_url, json=organize_payload)
                
                # 检查响应对象
                if organize_res is None:
                    logger.error(f"mhnotify: 提交文件整理任务失败 - RequestUtils返回None")
                    logger.error(f"mhnotify: 这可能是网络错误或请求超时")
                    return
                
                # 打印响应的基本信息
                logger.info(f"mhnotify: 响应对象类型: {type(organize_res)}")
                logger.info(f"mhnotify: 响应对象属性: {dir(organize_res)}")
                
                # 尝试获取状态码
                try:
                    status_code = organize_res.status_code
                    logger.info(f"mhnotify: 文件整理任务响应状态码: {status_code}")
                except Exception as e:
                    logger.error(f"mhnotify: 无法获取响应状态码: {e}")
                    return
                
                # 尝试获取响应内容
                try:
                    response_text = organize_res.text
                    logger.info(f"mhnotify: 响应内容长度: {len(response_text)} 字节")
                    logger.info(f"mhnotify: 响应内容: {response_text[:1000]}")
                except Exception as e:
                    logger.error(f"mhnotify: 无法获取响应内容: {e}")
                    return
                
            except Exception as e:
                logger.error(f"mhnotify: 提交文件整理任务请求异常: {e}", exc_info=True)
                return
            
            if status_code != 200:
                try:
                    error_text = response_text
                    logger.error(f"mhnotify: 提交文件整理任务失败 - 状态码: {status_code}")
                    logger.error(f"mhnotify: 错误响应内容: {error_text}")
                except:
                    logger.error(f"mhnotify: 提交文件整理任务失败 - 状态码: {status_code}, 无法读取响应内容")
                return
            
            try:
                organize_data = organize_res.json()
                logger.info(f"mhnotify: 整理任务响应JSON: {organize_data}")
            except Exception as e:
                logger.error(f"mhnotify: 解析整理任务响应失败 - {e}")
                logger.error(f"mhnotify: 原始响应: {response_text}")
                return
            
            # 检查响应状态
            # 接口可能返回 success 字段，也可能返回 code 字段表示成功
            success = organize_data.get("success", False)
            code = organize_data.get("code", "")
            message = organize_data.get("message", "")
            
            # code == 200 或 code == "200" 也视为成功
            if not success and str(code) != "200":
                logger.error(f"mhnotify: 文件整理任务提交失败 - code: {code}, message: {message}")
                return
            
            organize_task_id = organize_data.get("data", {}).get("task_id")
            
            if not organize_task_id:
                logger.info(f"mhnotify: 文件整理任务已创建: {message}")
            else:
                logger.info(f"mhnotify: 文件整理任务已提交，task_id: {organize_task_id}, message: {message}")
            
        except Exception as e:
            logger.error(f"mhnotify: 云下载移动整理异常: {e}", exc_info=True)

    def _query_downloading_task_by_hash(self, client, info_hash: str) -> Optional[Dict[str, Any]]:
        """
        使用115 Web API查询正在下载的离线任务（通过info_hash匹配）
        :param client: P115Client实例
        :param info_hash: 任务hash
        :return: 任务信息字典或None
        """
        try:
            # 构造请求参数
            # 参考 115-downing.txt，stat=12 表示查询正在下载的任务
            import time as time_module
            import hashlib
            
            # 获取用户ID
            uid = self._get_115_uid()
            if not uid:
                logger.warning(f"mhnotify: 无法获取115用户ID")
                return None
            
            # 构造签名
            timestamp = int(time_module.time())
            sign_str = f"{uid}{timestamp}"
            sign = hashlib.md5(sign_str.encode()).hexdigest()
            
            # 调用离线任务列表API（stat=12 表示正在下载）
            url = "https://115.com/web/lixian/?ct=lixian&ac=task_lists"
            params = {
                'page': 1,
                'stat': 12,  # 12=正在下载
                'uid': uid,
                'sign': sign,
                'time': timestamp
            }
            
            # 使用RequestUtils发送请求
            headers = {
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "Cookie": self._p115_cookie,
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }
            
            response = RequestUtils(headers=headers).post_res(url, data=params)
            if not response or response.status_code != 200:
                logger.debug(f"mhnotify: 查询正在下载任务失败: {response.status_code if response else 'No response'}")
                return None
            
            result = response.json()
            if not result or not result.get('state'):
                logger.debug(f"mhnotify: 正在下载任务列表响应异常: {result}")
                return None
            
            # 查找匹配的任务
            tasks = result.get('tasks', [])
            for task in tasks:
                if task.get('info_hash', '').lower() == info_hash.lower():
                    return task
            
            return None
            
        except Exception as e:
            logger.debug(f"mhnotify: 查询正在下载任务异常: {e}")
            return None

    def _get_115_uid(self) -> Optional[str]:
        """
        从 cookie 中获取 115 用户 ID
        :return: 用户ID或None
        """
        try:
            cookie_dict = {}
            for item in self._p115_cookie.split(';'):
                item = item.strip()
                if '=' in item:
                    k, v = item.split('=', 1)
                    cookie_dict[k.strip()] = v.strip()
            
            uid_str = cookie_dict.get('UID', '')
            if uid_str and '_' in uid_str:
                return uid_str.split('_')[0]
            
            return None
        except Exception as e:
            logger.warning(f"mhnotify: 解析UID失败: {e}")
            return None

    def _send_cloud_download_deleted_notification(self, task_name: str):
        """
        发送云下载任务被删除通知
        :param task_name: 任务名称
        """
        try:
            title = "⚠️ 未找到云下载任务"
            text = f"📦 任务: {task_name}\n\n未找到云下载任务，可能已被删除。"
            
            self.post_message(
                mtype=None,
                title=title,
                text=text
            )
            logger.info(f"mhnotify: 云下载任务删除通知已发送: {task_name}")
        except Exception as e:
            logger.error(f"mhnotify: 发送云下载删除通知失败: {e}", exc_info=True)

    def _send_cloud_download_failed_notification(self, task_name: str):
        """
        发送云下载任务失败通知
        :param task_name: 任务名称
        """
        try:
            title = "❌ 115云下载失败"
            text = f"📦 任务: {task_name}\n\n下载过程中出现错误，请检查115网盘。"
            
            self.post_message(
                mtype=None,
                title=title,
                text=text
            )
            logger.info(f"mhnotify: 云下载失败通知已发送: {task_name}")
        except Exception as e:
            logger.error(f"mhnotify: 发送云下载失败通知失败: {e}", exc_info=True)

    def handle_cloud_download(self, event: Event):
        """远程命令触发：添加115云下载任务"""
        if not event:
            return
        event_data = event.event_data
        if not event_data or event_data.get("action") != "mh_add_offline":
            return

        # 检查功能是否启用
        if not self._cloud_download_enabled:
            self.post_message(
                channel=event_data.get("channel"),
                title="云下载功能未启用",
                text="请先在插件配置中启用115云下载功能",
                userid=event_data.get("user")
            )
            return

        # 检查115 Cookie是否配置
        if not self._p115_cookie:
            self.post_message(
                channel=event_data.get("channel"),
                title="115 Cookie未配置",
                text="请先在插件配置中填写115 Cookie",
                userid=event_data.get("user")
            )
            return

        # 获取下载链接（支持多个链接，用逗号、空格或换行分隔）
        download_urls_raw = event_data.get("arg_str")
        if not download_urls_raw or not download_urls_raw.strip():
            self.post_message(
                channel=event_data.get("channel"),
                title="参数错误",
                text="用法: /mhol <下载链接>\n支持多个链接，用逗号分隔: /mhol url1,url2,url3",
                userid=event_data.get("user")
            )
            return

        # 解析多个URL（支持逗号、空格、换行分隔）
        import re
        download_urls = re.split(r'[,\s]+', download_urls_raw.strip())
        download_urls = [url.strip() for url in download_urls if url.strip()]
        
        if not download_urls:
            self.post_message(
                channel=event_data.get("channel"),
                title="参数错误",
                text="未解析到有效的下载链接",
                userid=event_data.get("user")
            )
            return

        logger.info(f"mhnotify: 收到云下载命令，共 {len(download_urls)} 个链接")

        # 判断是否需要批量监控
        is_batch = len(download_urls) > 1
        need_monitor = self._cloud_download_remove_small_files or self._cloud_download_organize

        # 执行云下载
        success_count = 0
        fail_count = 0
        results = []
        batch_tasks = []  # 用于批量监控的任务列表
        last_message = ""
        
        for idx, download_url in enumerate(download_urls, 1):
            logger.info(f"mhnotify: 处理第 {idx}/{len(download_urls)} 个链接: {download_url[:80]}...")
            # 批量模式下不启动单独的监控线程，由统一的批量监控处理
            success, message, task_info = self._add_offline_download(download_url, start_monitor=(not is_batch))
            last_message = message
            if success:
                success_count += 1
                results.append(f"✅ 链接{idx}: 成功")
                # 收集任务信息用于批量监控
                if is_batch and need_monitor and task_info.get("info_hash"):
                    batch_tasks.append(task_info)
            else:
                fail_count += 1
                results.append(f"❌ 链接{idx}: {message}")

        # 发送结果消息
        if len(download_urls) == 1:
            # 单个链接，保持原有格式
            if success_count == 1:
                self.post_message(
                    channel=event_data.get("channel"),
                    title="云下载任务添加成功",
                    text=last_message,
                    userid=event_data.get("user")
                )
            else:
                self.post_message(
                    channel=event_data.get("channel"),
                    title="云下载任务添加失败",
                    text=last_message,
                    userid=event_data.get("user")
                )
        else:
            # 多个链接，汇总结果
            summary = f"共 {len(download_urls)} 个链接\n成功: {success_count} | 失败: {fail_count}\n\n" + "\n".join(results)
            if need_monitor and batch_tasks:
                summary += f"\n\n⏳ 将统一监控 {len(batch_tasks)} 个任务，完成后执行清理和整理"
            title = "云下载批量任务已提交" if fail_count == 0 else f"云下载批量任务已提交（{fail_count}个失败）"
            self.post_message(
                channel=event_data.get("channel"),
                title=title,
                text=summary,
                userid=event_data.get("user")
            )
            
            # 启动批量监控线程
            if need_monitor and batch_tasks:
                try:
                    import threading
                    logger.info(f"mhnotify: 启动批量监控线程，监控 {len(batch_tasks)} 个任务")
                    threading.Thread(
                        target=self._monitor_batch_downloads,
                        args=(batch_tasks,),
                        daemon=True
                    ).start()
                except Exception as e:
                    logger.warning(f"mhnotify: 启动批量监控线程失败: {e}")

