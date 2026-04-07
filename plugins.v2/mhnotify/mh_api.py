import time
from typing import List, Dict, Any, Optional, Union

from app.utils.http import RequestUtils
from app.log import logger


class MHApiMixin:

    def _mh_login(self) -> Optional[str]:
        """登录 MH 获取 access_token"""
        try:
            # 使用缓存token，避免每分钟重复登录
            now_ts = int(time.time())
            if self._mh_token and now_ts < self._mh_token_expire_ts:
                logger.debug("mhnotify: 使用缓存的MH access_token")
                return self._mh_token
            logger.info(f"mhnotify: 准备登录MH，domain={self._mh_domain}, username={self._mh_username}")
            if not self._mh_domain or not self._mh_username or not self._mh_password:
                logger.error("mhnotify: 登录MH失败，缺少域名或用户名或密码配置")
                return None
            login_url = f"{self._mh_domain}/api/v1/auth/login"
            payload = {"username": self._mh_username, "password": self._mh_password}
            headers = {
                "Accept": "application/json, text/plain, */*",
                "Content-Type": "application/json;charset=UTF-8",
                "Origin": self._mh_domain,
                "Accept-Language": "zh-CN",
                "User-Agent": "MoviePilot/Plugin MHNotify"
            }
            res = RequestUtils(headers=headers).post(login_url, json=payload)
            if res is None:
                logger.error("mhnotify: 登录MH未获取到任何响应")
            else:
                logger.info(f"mhnotify: 登录MH响应 status={res.status_code}")
            if not res or res.status_code != 200:
                return None
            data = res.json() or {}
            token = (data.get("data") or {}).get("access_token")
            logger.info(f"mhnotify: 登录MH成功，access_token获取={'yes' if token else 'no'}")
            if token:
                # 写入缓存
                self._mh_token = token
                self._mh_token_expire_ts = now_ts + max(60, self._mh_token_ttl_seconds)
            return token
        except Exception:
            logger.error("mhnotify: 登录MH出现异常", exc_info=True)
            return None

    def _auth_headers(self, access_token: str) -> Dict[str, str]:
        return {
            "Accept": "application/json, text/plain, */*",
            "Authorization": f"Bearer {access_token}",
            "User-Agent": "MoviePilot/Plugin MHNotify",
            "Accept-Language": "zh-CN"
        }

    def _mh_get_defaults(self, access_token: str) -> Dict[str, Any]:
        try:
            url = f"{self._mh_domain}/api/v1/subscription/config/defaults"
            logger.info(f"mhnotify: 获取MH默认配置 GET {url}")
            res = RequestUtils(headers=self._auth_headers(access_token)).get_res(url)
            if res is None:
                logger.error("mhnotify: 获取MH默认配置未返回响应")
            elif res.status_code != 200:
                logger.error(f"mhnotify: 获取MH默认配置失败 status={res.status_code} body={getattr(res, 'text', '')[:200]}")
            else:
                data = res.json() or {}
                core = (data or {}).get("data") or {}
                logger.info(
                    "mhnotify: 默认配置摘要 cloud_type=%s account=%s target_directory=%s",
                    core.get("cloud_type"), core.get("account_identifier"), core.get("target_directory")
                )
                return data
        except Exception:
            logger.error("mhnotify: 获取MH默认配置异常", exc_info=True)
            pass
        return {}

    def _normalize_media_type(self, sub_type: Optional[str], info_type: Optional[str]) -> str:
        try:
            st = (sub_type or "").strip().lower()
            it = (info_type or "").strip().lower() if isinstance(info_type, str) else ""
            movie_alias = {"movie", "mov", "影片", "电影"}
            tv_alias = {"tv", "television", "电视剧", "剧集", "series"}
            if st in movie_alias or it in movie_alias:
                return "movie"
            if st in tv_alias or it in tv_alias:
                return "tv"
            # 兜底：优先按 info_type，其次按 sub_type
            if it in {"movie", "tv"}:
                return it
            return "movie"
        except Exception:
            return "movie"

    def _mh_create_subscription(self, access_token: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        try:
            url = f"{self._mh_domain}/api/v1/subscription/create"
            headers = self._auth_headers(access_token)
            headers.update({"Content-Type": "application/json;charset=UTF-8", "Origin": self._mh_domain})
            logger.info(f"mhnotify: 创建MH订阅 POST {url} media_type={payload.get('media_type')} tmdb_id={payload.get('tmdb_id')} title={str(payload.get('title'))[:50]}")
            # 增加显式超时与小次数重试，缓解瞬时网络抖动
            timeout_seconds = 30
            max_retries = 2  # 总共尝试 1+2 次
            for attempt in range(1, max_retries + 2):
                res = RequestUtils(headers=headers, timeout=timeout_seconds).post(url, json=payload)
                if res is None:
                    logger.error(f"mhnotify: 创建MH订阅未返回响应（第{attempt}次，可能超时{timeout_seconds}s）")
                elif res.status_code not in (200, 204):
                    body_text = getattr(res, 'text', '')
                    logger.error(f"mhnotify: 创建MH订阅失败（第{attempt}次） status={res.status_code} body={body_text[:200]}")
                    # 如果已存在相同配置的订阅，尝试查询并复用
                    try:
                        if res.status_code == 400 and ('已存在相同配置' in body_text or 'already exists' in body_text.lower()):
                            lst = self._mh_list_subscriptions(access_token)
                            subs = (lst.get("data") or {}).get("subscriptions") or []
                            cand_uuid = None
                            want_tmdb = payload.get('tmdb_id')
                            want_type = (payload.get('media_type') or '').lower()
                            want_seasons = set(payload.get('selected_seasons') or [])
                            for rec in subs:
                                params = rec.get('params') or {}
                                if params.get('tmdb_id') == want_tmdb and (params.get('media_type') or '').lower() == want_type:
                                    try:
                                        cur_seasons = set(int(x) for x in (params.get('selected_seasons') or []))
                                    except Exception:
                                        cur_seasons = set()
                                    if not want_seasons or cur_seasons == want_seasons:
                                        cand_uuid = rec.get('uuid') or rec.get('task', {}).get('uuid')
                                        break
                            if cand_uuid:
                                logger.info(f"mhnotify: 复用已存在的MH订阅 uuid={cand_uuid}")
                                return {"data": {"subscription_id": cand_uuid, "task": {"uuid": cand_uuid}}}
                    except Exception:
                        logger.warning("mhnotify: 检索已存在的MH订阅失败", exc_info=True)
                else:
                    data = res.json() or {}
                    uuid = (data.get("data") or {}).get("subscription_id") or (data.get("data") or {}).get("task", {}).get("uuid")
                    logger.info(f"mhnotify: 创建MH订阅成功 uuid={uuid}")
                    return data
                # 还有重试次数时，进行指数级短暂停顿
                if attempt <= max_retries:
                    time.sleep(2 * attempt)
        except Exception:
            logger.error("mhnotify: 创建MH订阅异常", exc_info=True)
            pass
        return {}

    def _mh_list_subscriptions(self, access_token: str, status: Optional[str] = None, search: Optional[str] = None, page_size: int = 2000) -> Dict[str, Any]:
        try:
            url = f"{self._mh_domain}/api/v1/subscription/list?page=1&page_size={page_size}"
            if status:
                url += f"&status={status}"
            if search:
                try:
                    url += f"&search={quote(search)}"
                except Exception:
                    url += f"&search={search}"
            logger.info(f"mhnotify: 查询MH订阅列表 GET {url}")
            res = RequestUtils(headers=self._auth_headers(access_token)).get_res(url)
            if res is None:
                logger.error("mhnotify: 查询MH订阅列表未返回响应")
            elif res.status_code != 200:
                logger.error(f"mhnotify: 查询MH订阅列表失败 status={res.status_code} body={getattr(res, 'text', '')[:200]}")
            else:
                data = res.json() or {}
                subs = (data.get("data") or {}).get("subscriptions") or []
                logger.info(f"mhnotify: 查询MH订阅列表成功 count={len(subs)}")
                return data
        except Exception:
            logger.error("mhnotify: 查询MH订阅列表异常", exc_info=True)
            pass
        return {}

    def _mh_delete_subscription(self, access_token: str, uuid: str) -> bool:
        try:
            url = f"{self._mh_domain}/api/v1/subscription/{uuid}"
            headers = self._auth_headers(access_token)
            headers.update({"Origin": self._mh_domain})
            logger.info(f"mhnotify: 删除MH订阅 DELETE {url}")
            res = RequestUtils(headers=headers).delete_res(url)
            ok = bool(res and res.status_code in (200, 204))
            if res is None:
                logger.error("mhnotify: 删除MH订阅未返回响应")
            else:
                logger.info(f"mhnotify: 删除MH订阅响应 status={res.status_code} ok={ok}")
            return ok
        except Exception:
            logger.error("mhnotify: 删除MH订阅异常", exc_info=True)
            return False

    def _mh_delete_by_title(self, access_token: str, title: str) -> int:
        try:
            if not title:
                return 0
            lst = self._mh_list_subscriptions(access_token, status="active", search=title, page_size=2000)
            subs = (lst.get("data") or {}).get("subscriptions") or []
            count = 0
            t_norm = str(title).strip().lower()
            for rec in subs:
                params = rec.get("params") or {}
                cloud = str(params.get("cloud_type") or "").strip().lower()
                name = (rec.get("name") or rec.get("task", {}).get("name") or params.get("title") or "").strip().lower()
                if cloud == "drive115" and name and name == t_norm:
                    uuid = rec.get("uuid") or rec.get("task", {}).get("uuid")
                    if uuid and self._mh_delete_subscription(access_token, uuid):
                        count += 1
            logger.info(f"mhnotify: 按标题删除MH订阅 title={title} 已删除数量={count}")
            return count
        except Exception:
            logger.error("mhnotify: 按标题删除MH订阅异常", exc_info=True)
            return 0

    def _mh_delete_by_tmdb(self, access_token: str, tmdb_id: Union[str, int], media_type: Optional[str] = None, season: Optional[int] = None) -> int:
        try:
            if not tmdb_id:
                return 0
            
            # 使用 TMDB ID 锁，防止并发操作同一订阅
            with self._get_tmdb_lock(tmdb_id):
                lst = self._mh_list_subscriptions(access_token, status="active", page_size=2000)
                subs = (lst.get("data") or {}).get("subscriptions") or []
                count = 0
                tmdb_norm = str(tmdb_id)
                mtype_norm = (str(media_type or "").lower().strip() or None)
                for rec in subs:
                    params = rec.get("params") or {}
                    cloud = str(params.get("cloud_type") or "").strip().lower()
                    ptmdb = str(params.get("tmdb_id") or "")
                    pmtype = str(params.get("media_type") or "").lower().strip()
                    if cloud == "drive115" and ptmdb == tmdb_norm and (not mtype_norm or pmtype == mtype_norm):
                        uuid = rec.get("uuid") or rec.get("task", {}).get("uuid")
                        if not uuid:
                            continue
                        
                        # 检查是否指定了删除特定季
                        if season is not None:
                            # 获取MH订阅当前的季列表
                            current_seasons = params.get("selected_seasons") or []
                            # 转换为int列表以便比较
                            try:
                                current_seasons_int = [int(s) for s in current_seasons]
                            except:
                                current_seasons_int = []
                            
                            target_season = int(season)
                            
                            if target_season in current_seasons_int:
                                # 如果包含该季，则移除
                                new_seasons = [s for s in current_seasons_int if s != target_season]
                                
                                if not new_seasons:
                                    # 如果移除后为空，则删除整个订阅
                                    logger.info(f"mhnotify: 订阅 {rec.get('name')} 移除季 {target_season} 后为空，执行删除")
                                    if self._mh_delete_subscription(access_token, uuid):
                                        count += 1
                                else:
                                    # 如果还有其他季，则更新订阅
                                    logger.info(f"mhnotify: 订阅 {rec.get('name')} 移除季 {target_season}，剩余 {new_seasons}")
                                    update_payload = {"selected_seasons": new_seasons}
                                    self._mh_update_subscription(access_token, uuid, update_payload)
                                    # 这里不算作删除数量，但已经处理了
                            else:
                                logger.info(f"mhnotify: 订阅 {rec.get('name')} 不包含季 {target_season}，跳过处理")
                        else:
                            if self._mh_delete_subscription(access_token, uuid):
                                count += 1
                logger.info(f"mhnotify: 按tmdb_id删除MH订阅 tmdb_id={tmdb_norm} type={mtype_norm or '*'} season={season} 已删除数量={count}")
                return count
        except Exception:
            logger.error("mhnotify: 按tmdb_id删除MH订阅异常", exc_info=True)
            return 0

    def _mh_update_subscription(self, access_token: str, uuid: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """更新MH订阅（修改季集合等参数）
        兼容示例：PUT /api/v1/subscription/{uuid}，body 包含 name/cron/params
        params 中包含 selected_seasons 与 episode_ranges 以及其他字段
        """
        try:
            url = f"{self._mh_domain}/api/v1/subscription/{uuid}"
            headers = self._auth_headers(access_token)
            headers.update({"Content-Type": "application/json;charset=UTF-8", "Origin": self._mh_domain})
            # 组装更新体：仅更新提供的字段，避免覆盖已有 name/cron
            update_body: Dict[str, Any] = {
                "params": payload
            }
            # 仅当显式传入 name 或 title 时才更新 name
            if payload.get("name") or payload.get("title"):
                update_body["name"] = payload.get("name") or f"[订阅] {payload.get('title')}"
            # 仅当显式传入 cron 时才更新 cron
            if "cron" in payload:
                update_body["cron"] = payload.get("cron")
            logger.info(f"mhnotify: 更新MH订阅 PUT {url} seasons={payload.get('selected_seasons')}")
            res = RequestUtils(headers=headers, timeout=30).put_res(url, json=update_body)
            if res is None:
                logger.error("mhnotify: 更新MH订阅未返回响应")
            elif res.status_code not in (200, 204):
                logger.error(f"mhnotify: 更新MH订阅失败 status={res.status_code} body={getattr(res, 'text', '')[:200]}")
            else:
                data = res.json() or {}
                logger.info("mhnotify: 更新MH订阅成功")
                return data
        except Exception:
            logger.error("mhnotify: 更新MH订阅异常", exc_info=True)
        return {}

    def _mh_execute_subscription(self, access_token: str, uuid: str) -> bool:
        """触发MH订阅立即执行查询
        POST /api/v1/subscription/{uuid}/execute
        """
        try:
            url = f"{self._mh_domain}/api/v1/subscription/{uuid}/execute"
            headers = self._auth_headers(access_token)
            headers.update({"Content-Length": "0", "Origin": self._mh_domain})
            logger.info(f"mhnotify: 触发MH订阅执行 POST {url}")
            res = RequestUtils(headers=headers, timeout=30).post_res(url)
            if res is None:
                logger.error("mhnotify: 触发MH订阅执行未返回响应")
                return False
            elif res.status_code != 200:
                logger.error(f"mhnotify: 触发MH订阅执行失败 status={res.status_code} body={getattr(res, 'text', '')[:200]}")
                return False
            else:
                data = res.json() or {}
                logger.info(f"mhnotify: 触发MH订阅执行成功：{data.get('message', '')}")
                return True
        except Exception:
            logger.error("mhnotify: 触发MH订阅执行异常", exc_info=True)
        return False

    def _mh_get_listeners(self, access_token: str) -> List[Dict[str, Any]]:
        """获取TG转发监听配置列表
        GET /api/v1/tg-forwarder/listeners
        """
        try:
            url = f"{self._mh_domain}/api/v1/tg-forwarder/listeners"
            res = RequestUtils(headers=self._auth_headers(access_token), timeout=15).get_res(url=url)
            if res and res.status_code == 200:
                return (res.json().get("data") or {}).get("listeners") or []
            logger.warning(f"mhnotify: 获取转发监听列表失败 status={getattr(res, 'status_code', None)}")
        except Exception:
            logger.warning("mhnotify: 获取转发监听列表异常", exc_info=True)
        return []

    def _mh_sync_listener_keyword(self, access_token: str, listener_id: str, keyword: str,
                                   field: str = "keywords", remove: bool = False) -> bool:
        """向监听配置的指定字段（keywords/blacklist_keywords）添加或移除关键词
        PUT /api/v1/tg-forwarder/listeners/{id}
        """
        try:
            import copy
            listeners = self._mh_get_listeners(access_token)
            target = next((l for l in listeners if l.get("id") == listener_id), None)
            if not target:
                logger.warning(f"mhnotify: 未找到转发监听配置 id={listener_id}")
                return False
            listener = copy.deepcopy(target)
            current_kws = list((listener.get("filter") or {}).get(field) or [])
            if remove:
                if keyword not in current_kws:
                    return True
                current_kws = [k for k in current_kws if k != keyword]
            else:
                if keyword in current_kws:
                    return True
                current_kws.append(keyword)
            listener.setdefault("filter", {})[field] = current_kws
            url = f"{self._mh_domain}/api/v1/tg-forwarder/listeners/{listener_id}"
            res = RequestUtils(
                headers=self._auth_headers(access_token),
                content_type="application/json"
            ).put_res(url=url, json=listener)
            ok = bool(res and res.status_code == 200)
            action = "移除" if remove else "添加"
            field_name = "白名单" if field == "keywords" else "黑名单"
            if ok:
                logger.info(f"mhnotify: 已{action}转发{field_name}关键词 '{keyword}' -> 监听[{listener_id}]")
            else:
                logger.warning(f"mhnotify: {action}转发{field_name}关键词失败 '{keyword}' -> 监听[{listener_id}]")
            return ok
        except Exception:
            logger.warning("mhnotify: 同步转发监听关键词异常", exc_info=True)
        return False

