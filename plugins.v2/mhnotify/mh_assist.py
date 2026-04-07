import re
import time
import threading
from datetime import datetime
from typing import List, Tuple, Dict, Any, Optional, Union

from apscheduler.triggers.cron import CronTrigger
from app.core.config import settings
from app.core.event import eventmanager, Event
from app.schemas.types import EventType, MediaType
from app.core.context import MediaInfo
from app.modules.themoviedb.tmdbapi import TmdbApi
from app.chain.download import DownloadChain
from app.log import logger
from app.db import SessionFactory
from app.db.subscribe_oper import SubscribeOper


class MHAssistMixin:

    @eventmanager.register(EventType.SubscribeAdded)
    def _on_subscribe_added(self, event: Event):
        """
        mh订阅辅助：仅对新订阅生效
        - 暂停该订阅（state='S'，不改动已有订阅）
        - 登录MH并读取默认配置
        - 按媒体类型在MH创建订阅
        - 记录mh_uuid并在5分钟后查询进度，按规则处理（删除或恢复MP订阅）
        """
        try:
            if not event or not self._mh_assist_enabled:
                return
            event_data = event.event_data or {}
            try:
                mid = (event_data.get("mediainfo") or {}).get("tmdb_id") or (event_data.get("mediainfo") or {}).get("tmdbid")
                mtitle = (event_data.get("mediainfo") or {}).get("title") or (event_data.get("mediainfo") or {}).get("name")
                mseason = (event_data.get("mediainfo") or {}).get("season")
                mdouban = (event_data.get("mediainfo") or {}).get("douban_id") or (event_data.get("mediainfo") or {}).get("doubanid")
                logger.info(f"mhnotify: SubscribeAdded 事件: sub_id={event_data.get('subscribe_id')} tmdb_id={mid} douban_id={mdouban} title={mtitle} event.season={mseason}")
            except Exception:
                pass
            sub_id = event_data.get("subscribe_id")
            mediainfo_dict = event_data.get("mediainfo") or {}
            if not sub_id:
                return
            # 暂停该订阅，仅针对新订阅
            with SessionFactory() as db:
                subscribe = SubscribeOper(db=db).get(sub_id)
                if not subscribe:
                    return
                SubscribeOper(db=db).update(sub_id, {"state": "S"})
                # 重新获取，确保季号等字段已正确加载
                subscribe = SubscribeOper(db=db).get(sub_id)
                try:
                    logger.info(f"mhnotify: 订阅暂停完成 id={sub_id} type={getattr(subscribe,'type',None)} season={getattr(subscribe,'season',None)}")
                except Exception:
                    pass
            # 登录 MH 拿 token
            access_token = self._mh_login()
            if not access_token:
                logger.error("mhnotify: 登录MediaHelper失败，无法创建订阅")
                return
            # 读取默认配置
            defaults = self._mh_get_defaults(access_token)

            # 提取 tmdb_id 并加锁，防止多季并发添加导致重复创建 MH 订阅
            tmdb_id = getattr(subscribe, 'tmdbid', None) or mediainfo_dict.get('tmdb_id') or mediainfo_dict.get('tmdbid')

            with self._get_tmdb_lock(tmdb_id):
                # 若为剧集，聚合同一 TMDB 的多季订阅（电影不需要聚合季）
                aggregate_seasons: Optional[List[int]] = None
                # 判断媒体类型
                sub_type = (getattr(subscribe, 'type', '') or '').strip().lower()
                is_tv = sub_type in ('tv', '电视剧')
                aggregate_seasons = []  # 初始化，电影时为空
                
                # 只有电视剧才进行聚合季逻辑
                if is_tv:
                    try:
                        # 取 tmdb_id (已在上方提取)
                        # tmdb_id = getattr(subscribe, 'tmdbid', None) or mediainfo_dict.get('tmdb_id') or mediainfo_dict.get('tmdbid')
                        # 查询 MP 内相同 tmdb 的订阅，聚合季
                        if tmdb_id:
                            logger.info(f"mhnotify: 聚合季开始，tmdb_id={tmdb_id}")
                            with SessionFactory() as db:
                                all_subs = SubscribeOper(db=db).list_by_tmdbid(tmdb_id)
                                logger.info(f"mhnotify: MP内同tmdb订阅数={len(all_subs or [])}")
                                seasons = []
                                for s in all_subs or []:
                                    try:
                                        stype = (getattr(s, 'type', '') or '').strip()
                                        stype_lower = (stype or '').lower()
                                        if stype_lower == 'tv' or stype in {'电视剧'}:
                                            # 优先使用订阅中的 season，其次从标题解析
                                            s_season = getattr(s, 'season', None)
                                            if s_season is None:
                                                s_season = self._extract_season_from_text(getattr(s, 'name', '') or '')
                                            seasons.append(s_season)
                                            logger.info(f"mhnotify: 订阅聚合候选 id={getattr(s,'id',None)} type={stype} season={getattr(s,'season',None)} parsed={s_season}")
                                    except Exception:
                                        pass
                            # 转换季为整数（支持字符串数字）
                            for x in seasons:
                                if isinstance(x, int):
                                    aggregate_seasons.append(x)
                                elif isinstance(x, str) and x.isdigit():
                                    aggregate_seasons.append(int(x))
                            # 过滤无效季号（None/0/负数）并去重排序
                            aggregate_seasons = sorted({s for s in aggregate_seasons if isinstance(s, int) and s > 0})
                            logger.info(f"mhnotify: 聚合季（转换后）={aggregate_seasons}")
                            if aggregate_seasons:
                                logger.info(f"mhnotify: 检测到该剧存在多季订阅，聚合季：{aggregate_seasons}")
                            else:
                                logger.info("mhnotify: 未聚合到季信息，将回退使用事件或订阅中的季")
                    except Exception:
                        logger.warning("mhnotify: 聚合季信息失败", exc_info=True)
                else:
                    # 电影类型不需要聚合季
                    logger.debug(f"mhnotify: 媒体类型为电影，跳过聚合季逻辑")
                # 构建创建参数（若为TV将带入聚合季）
                create_payload = self._build_mh_create_payload(subscribe, mediainfo_dict, defaults, aggregate_seasons=aggregate_seasons)
                if not create_payload:
                    logger.error("mhnotify: 构建MH订阅创建参数失败")
                    return
                # 若已存在相同 tmdb_id 的 MH 订阅，则复用或重建（以聚合季为准）
                existing_uuid: Optional[str] = None
                existing_selected: List[int] = []
                existing_custom_links: List[str] = []  # 保留现有订阅的自定义链接
                try:
                    lst = self._mh_list_subscriptions(access_token)
                    subs = (lst.get("data") or {}).get("subscriptions") or []
                    for rec in subs:
                        params = rec.get("params") or {}
                        if (params.get("cloud_type") or "").strip().lower() != "drive115":
                            continue
                        if params.get("tmdb_id") == create_payload.get("tmdb_id") and (params.get("media_type") or '').lower() == (create_payload.get("media_type") or '').lower():
                            existing_uuid = rec.get("uuid") or rec.get("task", {}).get("uuid")
                            try:
                                existing_selected = [int(x) for x in (params.get("selected_seasons") or [])]
                            except Exception:
                                existing_selected = []
                            # 获取现有订阅的自定义链接
                            existing_custom_links = params.get("user_custom_links") or []
                            if existing_custom_links:
                                logger.info(f"mhnotify: 现有MH订阅已有 {len(existing_custom_links)} 个自定义链接")
                            logger.info(f"mhnotify: 现有MH订阅命中 tmdb_id={params.get('tmdb_id')} uuid={existing_uuid} seasons={existing_selected}")
                            break
                    if existing_uuid:
                        agg_set = set(create_payload.get("selected_seasons") or [])
                        exist_set = set(existing_selected or [])
                        if agg_set and agg_set != exist_set:
                            # 需要包含更多季：优先尝试更新订阅季集合；失败则重建
                            # 更新时保留现有的自定义链接
                            if existing_custom_links:
                                create_payload["user_custom_links"] = existing_custom_links
                            logger.info(f"mhnotify: 发现现有MH订阅 {existing_uuid}，季集合不一致，尝试更新为 {sorted(agg_set)}")
                            upd = self._mh_update_subscription(access_token, existing_uuid, create_payload)
                            if upd:
                                logger.info(f"mhnotify: 已更新现有订阅 {existing_uuid} 为聚合季 {sorted(agg_set)}")
                            else:
                                logger.info(f"mhnotify: 更新失败，改为重建订阅为聚合季 {sorted(agg_set)}")
                                self._mh_delete_subscription(access_token, existing_uuid)
                                existing_uuid = None
                        else:
                            # 完全一致：直接复用（按媒体类型输出提示）
                            if is_tv:
                                logger.info(f"mhnotify: 发现现有MH订阅 {existing_uuid}，季集合一致，复用该订阅")
                            else:
                                logger.info(f"mhnotify: 发现现有MH订阅 {existing_uuid}，电影信息一致，复用该订阅")
                except Exception:
                    logger.warning("mhnotify: 检查现有MH订阅失败", exc_info=True)
                # 创建订阅（或复用现有）
                mh_uuid = None
                if existing_uuid:
                    mh_uuid = existing_uuid
                else:
                    resp = self._mh_create_subscription(access_token, create_payload)
                    mh_uuid = (resp or {}).get("data", {}).get("subscription_id") or (resp or {}).get("data", {}).get("task", {}).get("uuid")
                if not mh_uuid:
                    logger.error(f"mhnotify: MH订阅创建失败：{resp}")
                    return
                # 按媒体类型决定首次查询延迟：电影3分钟，电视剧6分钟
                _mtype = (create_payload.get("media_type") or "movie").lower()
                initial_delay = 180 if _mtype == "movie" else 360
                delay_mins = initial_delay // 60
                if existing_uuid:
                    logger.info(f"mhnotify: 复用现有MH订阅，uuid={mh_uuid}；{delay_mins}分钟后查询进度")
                    # 复用现有订阅时，触发立即执行查询（MH不会自动触发）
                    try:
                        access_token = self._mh_login()
                        if access_token and self._mh_execute_subscription(access_token, mh_uuid):
                            logger.info(f"mhnotify: 已触发复用订阅 {mh_uuid} 立即执行查询")
                        else:
                            logger.warning(f"mhnotify: 触发复用订阅执行失败")
                    except Exception as e:
                        logger.warning(f"mhnotify: 触发复用订阅执行异常: {e}")
                else:
                    logger.info(f"mhnotify: 已在MH创建订阅，uuid={mh_uuid}；{delay_mins}分钟后查询进度")
            
            # 记录待检查项
            pending: Dict[str, dict] = self.get_data(self._ASSIST_PENDING_KEY) or {}
            pending[str(sub_id)] = {
                "mh_uuid": mh_uuid,
                "created_at": int(time.time()),
                "type": (create_payload.get("media_type") or mediainfo_dict.get("type") or "movie"),
                "douban_id": (mediainfo_dict.get("douban_id") or mediainfo_dict.get("doubanid") or getattr(subscribe, 'doubanid', None))
            }
            self.save_data(self._ASSIST_PENDING_KEY, pending)
            # 同步订阅名称到转发白名单/黑名单
            try:
                _sub_name = getattr(subscribe, 'name', '') or mediainfo_dict.get('title') or ''
                self._sync_forwarder_keywords(_sub_name, remove=False)
            except Exception:
                logger.warning("mhnotify: 新订阅同步转发关键词失败", exc_info=True)
            try:
                import threading
                def _delayed_check():
                    try:
                        # 执行完成后从定时器字典中移除
                        with self._timers_lock:
                            self._pending_timers.pop(str(sub_id), None)
                        
                        p = self.get_data(self._ASSIST_PENDING_KEY) or {}
                        info = p.get(str(sub_id))
                        if not info:
                            return
                        info["created_at"] = int(time.time()) - initial_delay - 1
                        p[str(sub_id)] = info
                        self.save_data(self._ASSIST_PENDING_KEY, p)
                        self._assist_scheduler()
                    except Exception:
                        logger.warning("mhnotify: 新订阅延迟检查异常", exc_info=True)
                
                # 创建定时器并存储引用
                timer = threading.Timer(initial_delay, _delayed_check)
                with self._timers_lock:
                    self._pending_timers[str(sub_id)] = timer
                timer.start()
                logger.info(f"mhnotify: 已安排新订阅在 {delay_mins} 分钟后立即检查进度（不受全局调度频率影响）")
            except Exception:
                logger.warning("mhnotify: 安排新订阅延迟检查失败", exc_info=True)
        except Exception as e:
            logger.error(f"mhnotify: 处理新增订阅事件失败: {e}")

    # 旧屏蔽逻辑移除

    # 旧屏蔽逻辑移除

    def _get_tmdb_lock(self, tmdb_id: Union[int, str]) -> threading.Lock:
        """获取指定tmdb_id的锁"""
        if not tmdb_id:
            return self._locks_lock
        try:
            tid = int(tmdb_id)
        except:
            return self._locks_lock
            
        with self._locks_lock:
            if tid not in self._tmdb_locks:
                self._tmdb_locks[tid] = threading.Lock()
            return self._tmdb_locks[tid]

    def _build_mh_create_payload(self, subscribe, mediainfo_dict: Dict[str, Any], defaults: Dict[str, Any], aggregate_seasons: Optional[List[int]] = None) -> Optional[Dict[str, Any]]:
        try:
            data = (defaults or {}).get("data") or {}
            quality_pref = "auto"
            target_dir = data.get("target_directory") or "/影视"
            logger.info(f"m1hnotify: 目标目录: {target_dir}")
            cron = self._mh_assist_cron or data.get("cron") or "0 */6 * * *"
            cloud_type = data.get("cloud_type") or "drive115"
            account_identifier = data.get("account_identifier") or ""
            # 取订阅字段（兼容对象或字典）
            def _get(field: str):
                try:
                    if hasattr(subscribe, field):
                        return getattr(subscribe, field)
                    if isinstance(subscribe, dict):
                        return subscribe.get(field)
                except Exception:
                    return None
                return None
            # 媒体信息
            tmdb_id = _get('tmdbid') or mediainfo_dict.get('tmdb_id') or mediainfo_dict.get('tmdbid')
            title = _get('name') or mediainfo_dict.get('title')
            sub_type = _get('type')
            info_type = mediainfo_dict.get('type')
            mtype_norm = self._normalize_media_type(sub_type, info_type)
            release_date = mediainfo_dict.get('release_date')
            overview = mediainfo_dict.get('overview')
            poster_path = mediainfo_dict.get('poster_path')
            vote_average = mediainfo_dict.get('vote_average')
            search_keywords = _get('keyword') or mediainfo_dict.get('search_keywords') or title
            if not title:
                title = mediainfo_dict.get('original_title') or mediainfo_dict.get('name') or "未知标题"
            payload: Dict[str, Any] = {
                "tmdb_id": tmdb_id,
                "title": title,
                "original_title": mediainfo_dict.get('original_title'),
                "media_type": mtype_norm,
                "release_date": release_date,
                "overview": overview,
                "poster_path": poster_path,
                "vote_average": vote_average,
                "search_keywords": search_keywords,
                "quality_preference": quality_pref,
                "target_directory": target_dir,
                "target_dir_id": "",
                "target_path": "",
                "cron": cron,
                "cloud_type": cloud_type,
                "account_identifier": account_identifier,
                "custom_name": title,
                "user_custom_links": []
            }
            if payload["media_type"] == "tv":
                logger.info(f"mhnotify: 解析季信息: event.season={mediainfo_dict.get('season')} subscribe.season={_get('season')}")
                # 聚合季信息：若提供 aggregate_seasons，则使用其作为订阅的季集合
                if aggregate_seasons:
                    # 去重并排序
                    seasons = sorted({int(s) for s in aggregate_seasons if s is not None}) or [1]
                    src = "聚合"
                else:
                    # 从事件或订阅中解析季号（支持字符串数字）；失败则从标题解析；仍失败则默认1
                    raw_season = mediainfo_dict.get('season') or _get('season')
                    def _to_int(v):
                        if isinstance(v, int):
                            return v
                        if isinstance(v, str) and v.isdigit():
                            return int(v)
                        return None
                    season_num = _to_int(raw_season)
                    src = "事件/订阅"
                    if not season_num:
                        season_num = self._extract_season_from_text(title or '')
                        src = "标题解析" if season_num else "默认1"
                    season_num = season_num or 1
                    seasons = [season_num]
                payload["selected_seasons"] = seasons
                payload["episode_ranges"] = {str(s): {"min_episode": None, "max_episode": None, "exclude_episodes": [], "exclude_text": ""} for s in seasons}
                logger.info(f"mhnotify: TV订阅季选定: {seasons}; 来源={src}")
            else:
                payload["selected_seasons"] = []
            # 日志摘要
            logger.info(
                "mhnotify: 构建MH订阅创建参数 tmdb_id=%s title=%s media_type=%s target_dir=%s cloud_type=%s account=%s",
                payload.get("tmdb_id"), payload.get("title"), payload.get("media_type"), target_dir, cloud_type, account_identifier
            )
            return payload
        except Exception:
            logger.error("mhnotify: __build_mh_create_payload 异常，subscribe或mediainfo缺失关键字段")
            return None

    def _extract_season_from_text(self, text: str) -> Optional[int]:
        """从标题/文本中解析季号，支持中文与英文常见格式
        例："第二季"、"第2季"、"Season 2"、"S02"、"2季"、"第十季"、"第十一季"
        返回正整数；无法解析返回 None
        """
        if not text:
            return None
        try:
            t = text.strip()
            # 英文格式 Season X / SXX
            m = re.search(r"(?:Season\s*)(\d{1,2})", t, re.IGNORECASE)
            if m:
                return int(m.group(1))
            m = re.search(r"\bS(\d{1,2})\b", t, re.IGNORECASE)
            if m:
                return int(m.group(1))
            # 中文格式 第X季 / X季
            m = re.search(r"第([一二三四五六七八九十百零〇两\d]{1,3})季", t)
            if m:
                num = m.group(1)
                return self._parse_chinese_numeral(num)
            m = re.search(r"([一二三四五六七八九十百零〇两\d]{1,3})季", t)
            if m:
                num = m.group(1)
                return self._parse_chinese_numeral(num)
            # 其它：第X期/部 有时也指季（尽量解析但不强制使用）
            m = re.search(r"第([一二三四五六七八九十百零〇两\d]{1,3})(?:期|部)", t)
            if m:
                num = m.group(1)
                val = self._parse_chinese_numeral(num)
                return val if val and val > 0 else None
        except Exception:
            pass
        return None

    def _parse_chinese_numeral(self, s: str) -> Optional[int]:
        """解析中文数字到整数，支持到 99 左右；也支持纯数字字符串"""
        if not s:
            return None
        try:
            if s.isdigit():
                return int(s)
            mapping = {
                '零': 0, '〇': 0,
                '一': 1, '二': 2, '两': 2, '三': 3, '四': 4, '五': 5,
                '六': 6, '七': 7, '八': 8, '九': 9,
                '十': 10
            }
            total = 0
            # 处理像 "十一"、"二十"、"二十一"
            if '十' in s:
                parts = s.split('十')
                if parts[0] == '':
                    total += 10
                else:
                    total += mapping.get(parts[0], 0) * 10
                if len(parts) > 1 and parts[1] != '':
                    total += mapping.get(parts[1], 0)
                return total if total > 0 else None
            # 单字数字
            return mapping.get(s, None)
        except Exception:
            return None

    def _compute_progress(self, sub_rec: Dict[str, Any]) -> Tuple[str, int, int]:
        """返回 (media_type, saved, expected_total)"""
        params = (sub_rec or {}).get("params") or {}
        mtype = (params.get("media_type") or (sub_rec.get("subscription_info") or {}).get("media_type") or "movie").lower()
        
        saved = 0
        expected_total = 1 if mtype == 'movie' else 0
        
        try:
            episodes = (sub_rec.get("episodes") or [])
            if episodes:
                # 1. 获取订阅的季
                selected_seasons = params.get("selected_seasons") or []
                selected_set = set(str(x) for x in selected_seasons)
                
                # 2. 获取已保存的集数 (episodes_arr)
                # 结构: episodes[0].episodes_arr = {"1": [1,2,3], "2": [1,2]}
                episodes_arr = (episodes[0] or {}).get("episodes_arr") or {}
                
                # 3. 获取所有季集数信息 (episodes_count)
                counts = (episodes[0] or {}).get("episodes_count") or {}
                
                if mtype == 'tv':
                    for season_str, season_info in counts.items():
                        # 如果指定了 selected_seasons，则只计算选中的季
                        if selected_set and str(season_str) not in selected_set:
                            continue
                        
                        # 计算总集数
                        expected_total += int(season_info.get("count") or 0)
                        
                        # 计算已保存集数 (该季下已有的集数列表长度)
                        saved_list = episodes_arr.get(str(season_str)) or []
                        saved += len(saved_list)
                else:
                    # movie: 检查 episodes_arr 是否有内容，或者 params 里 saved_resources 是否 > 0
                    if episodes_arr or int(params.get("saved_resources") or 0) > 0:
                        saved = 1
                    expected_total = 1
        except Exception:
            pass
        return mtype, saved, expected_total

    def _is_mh_subscription_completed(self, rec: dict) -> Tuple[bool, int, int, str]:
        try:
            if not isinstance(rec, dict):
                logger.warning("mhnotify: 订阅记录类型异常，跳过完成判定")
                return False, 0, 0, ""
            mtype, saved, expected = self._compute_progress(rec)
            name = rec.get("name") or (rec.get("task") or {}).get("name")
            logger.info(f"mhnotify: 订阅进度 name={str(name or '')[:50]} type={mtype} saved={saved}/{expected}")
            return expected > 0 and saved >= expected, saved, expected, mtype
        except Exception:
            logger.warning("mhnotify: 订阅完成判定异常，视为未完成")
            return False, 0, 0, ""

    def _is_plugin_created_mh_subscription(self, rec: dict) -> bool:
        try:
            if not isinstance(rec, dict):
                return False
            
            # 获取MH订阅的TMDB ID
            params = rec.get("params") or {}
            mh_tmdb_id = params.get("tmdb_id")
            if not mh_tmdb_id:
                return False
                
            # 获取插件记录的watch列表
            watch: Dict[str, dict] = self.get_data(self._ASSIST_WATCH_KEY) or {}
            if not watch:
                return False
                
            # 检查MH订阅的TMDB ID是否在插件记录的watch列表中
            for sid, info in watch.items():
                watch_tmdb_id = info.get("tmdb_id")
                if watch_tmdb_id and str(watch_tmdb_id) == str(mh_tmdb_id):
                    return True
                    
            return False
        except Exception:
            return False

    def _assist_scheduler(self):
        """每分钟执行：先等待2分钟进行首次查询；未查询到则每1分钟重试，直到查询到；并处理MP完成监听"""
        try:
            # 处理待检查
            pending: Dict[str, dict] = self.get_data(self._ASSIST_PENDING_KEY) or {}
            if pending:
                now_ts = int(time.time())
                # 收集已到查询时间的条目（首次查询延迟）
                matured_items = {sid: info for sid, info in pending.items() if now_ts - int(info.get("created_at") or 0) >= self._assist_initial_delay_seconds}
                if matured_items:
                    logger.info(f"mhnotify: 助手到期查询条目数={len(matured_items)}")
                    token = self._mh_login()
                    if not token:
                        logger.error("mhnotify: 登录MH失败，无法查询订阅进度")
                    else:
                        lst = self._mh_list_subscriptions(token)
                        subs = (lst.get("data") or {}).get("subscriptions") or []
                        subs_map = {}
                        for rec in subs:
                            uid = rec.get("uuid") or rec.get("task", {}).get("uuid")
                            if uid:
                                subs_map[uid] = rec
                        for sid, info in list(matured_items.items()):
                            mh_uuid = info.get("mh_uuid")
                            logger.info(f"mhnotify: 到期处理 sid={sid} mh_uuid={mh_uuid} type={info.get('type')} douban_id={info.get('douban_id')}")
                            target = subs_map.get(mh_uuid)
                            if not target:
                                # 未找到，记录重试次数，超过5次则移除记录
                                attempts = int(info.get("attempts") or 0) + 1
                                info["attempts"] = attempts
                                info["last_attempt"] = now_ts
                                if attempts >= 5:
                                    logger.warning(f"mhnotify: 订阅 {mh_uuid} 未在MH列表中找到，已重试{attempts}次，移除记录")
                                    pending.pop(sid, None)
                                    self.save_data(self._ASSIST_PENDING_KEY, pending)
                                    continue
                                else:
                                    retry_mins = max(1, int(self._assist_retry_interval_seconds / 60))
                                    logger.warning(f"mhnotify: 未在MH列表中找到订阅 {mh_uuid}，第{attempts}次重试，{retry_mins}分钟后继续")
                                    pending[str(sid)] = info
                                    self.save_data(self._ASSIST_PENDING_KEY, pending)
                                    continue
                            mtype, saved, expected = self._compute_progress(target)
                            logger.info(f"mhnotify: 订阅 {mh_uuid} 进度 saved={saved}/{expected} type={mtype}")
                            with SessionFactory() as db:
                                subscribe = SubscribeOper(db=db).get(int(sid))
                            if not subscribe:
                                # MP订阅已不存在（可能为取消单季）
                                # 优先尝试：按同 TMDB 的剩余季更新 MH 订阅；若无剩余季则删除 MH
                                try:
                                    del_token = self._mh_login()
                                except Exception:
                                    del_token = None
                                if del_token and mh_uuid:
                                    try:
                                        lst2 = self._mh_list_subscriptions(del_token)
                                        subs2 = (lst2.get("data") or {}).get("subscriptions") or []
                                        rec2 = None
                                        for r in subs2:
                                            uid2 = r.get("uuid") or (r.get("task") or {}).get("uuid")
                                            if uid2 == mh_uuid:
                                                rec2 = r
                                                break
                                        tmdb_id = None
                                        if rec2:
                                            params2 = rec2.get("params") or {}
                                            tmdb_id = params2.get("tmdb_id")
                                        remaining_seasons: List[int] = []
                                        if tmdb_id:
                                            try:
                                                with SessionFactory() as db2:
                                                    all_subs = SubscribeOper(db=db2).list_by_tmdbid(tmdb_id)
                                                seasons = []
                                                for s in all_subs or []:
                                                    try:
                                                        stype = (getattr(s, 'type', '') or '').strip()
                                                        stype_lower = (stype or '').lower()
                                                        if stype_lower == 'tv' or stype in {'电视剧'}:
                                                            s_season = getattr(s, 'season', None)
                                                            if s_season is None:
                                                                s_season = self._extract_season_from_text(getattr(s, 'name', '') or '')
                                                            seasons.append(s_season)
                                                    except Exception:
                                                        pass
                                                tmp: List[int] = []
                                                for x in seasons:
                                                    if isinstance(x, int):
                                                        tmp.append(x)
                                                    elif isinstance(x, str) and x.isdigit():
                                                        tmp.append(int(x))
                                                remaining_seasons = sorted({s for s in tmp if isinstance(s, int) and s > 0})
                                            except Exception:
                                                remaining_seasons = []
                                        if remaining_seasons:
                                            # 更新 MH 订阅季集合为剩余季
                                            try:
                                                base_params = (rec2 or {}).get("params") or {}
                                                base_params["selected_seasons"] = remaining_seasons
                                                base_params["episode_ranges"] = {str(s): {"min_episode": None, "max_episode": None, "exclude_episodes": [], "exclude_text": ""} for s in remaining_seasons}
                                                self._mh_update_subscription(del_token, mh_uuid, base_params)
                                                logger.info(f"mhnotify: 取消单季后更新MH订阅 seasons={remaining_seasons}")
                                            except Exception:
                                                logger.warning("mhnotify: 更新MH订阅季集合失败，降级为删除", exc_info=True)
                                                self._mh_delete_subscription(del_token, mh_uuid)
                                        else:
                                            # 无剩余季，删除 MH 订阅
                                            self._mh_delete_subscription(del_token, mh_uuid)
                                    except Exception:
                                        # 降级策略：出现异常则尽量删除对应 MH 订阅，避免遗留无主订阅
                                        try:
                                            self._mh_delete_subscription(del_token, mh_uuid)
                                        except Exception:
                                            logger.warning("mhnotify: 处理剩余季时异常且删除失败", exc_info=True)
                                pending.pop(sid, None)
                                self.save_data(self._ASSIST_PENDING_KEY, pending)
                                continue
                            if mtype == 'movie':
                                if expected <= 1 and saved >= 1:
                                    # 完成：直接完成MP订阅，MH删除交由 SubscribeComplete 事件处理
                                    self._finish_mp_subscribe(subscribe)
                                    pending.pop(sid, None)
                                    self.save_data(self._ASSIST_PENDING_KEY, pending)
                                else:
                                    if self._cloud_download_assist:
                                        try:
                                            logger.info(f"mhnotify: 进入云下载辅助分支 sid={sid} mh_uuid={mh_uuid}")
                                            qp_dict = self._get_quality_priority(token)
                                        except Exception:
                                            qp_dict = {}
                                        douban_id = info.get("douban_id")
                                        # 若缺少豆瓣ID，先按标题搜索回退
                                        search_douban_id = douban_id
                                        if not search_douban_id:
                                            try:
                                                title_src = subscribe.name or mediainfo_dict.get("title") or ""
                                                search_list = self._btl_get_video_list(title_src)
                                                pick = None
                                                for rec in search_list or []:
                                                    r_type = int(rec.get("type") or 0)
                                                    r_title = str(rec.get("title") or "").strip()
                                                    if r_type == 1 and r_title and r_title == title_src:
                                                        pick = rec
                                                        break
                                                if pick:
                                                    search_douban_id = pick.get("doub_id") or pick.get("douban_id") or pick.get("id") or pick.get("db_id")
                                                    logger.info(f"mhnotify: BTL getVideoList 匹配到豆瓣ID={search_douban_id}")
                                                else:
                                                    logger.info("mhnotify: BTL getVideoList 未匹配到电影条目或标题不一致")
                                            except Exception:
                                                search_douban_id = None
                                        if search_douban_id:
                                            details = self._btl_get_video_detail(token, search_douban_id)
                                            logger.info(f"mhnotify: 云下载辅助：质量优先级已获取 keys={list(qp_dict.keys()) if isinstance(qp_dict, dict) else []}")
                                            logger.info(f"mhnotify: 云下载辅助：BTL资源条数={len(details) if isinstance(details, list) else 0}")
                                            if not details:
                                                logger.info("mhnotify: BTL详情查询失败或无资源，恢复订阅启用")
                                                # with SessionFactory() as db:
                                                
                                                with SessionFactory() as db:
                                                    # 获取原始订阅信息
                                                    origin_sub = SubscribeOper(db=db).get(subscribe.id)
                                                    if origin_sub:
                                                        # 恢复订阅状态为 R
                                                        SubscribeOper(db=db).update(subscribe.id, {"state": "R"})
                                                pending.pop(sid, None)
                                                self.save_data(self._ASSIST_PENDING_KEY, pending)
                                            else:
                                                candidates = self._select_btl_resources_by_quality_priority(details, qp_dict)
                                                logger.info(f"mhnotify: 云下载辅助：候选条数（排序后）={len(candidates)}")
                                                started = self._try_cloud_download_with_candidates(candidates, sid, mh_uuid)
                                                if started:
                                                    logger.info(f"mhnotify: 已触发云下载（优先级首选），等待下载完成后回调处理订阅 sid={sid}")
                                                    # 为后续取消事件提供映射
                                                    watch: Dict[str, dict] = self.get_data(self._ASSIST_WATCH_KEY) or {}
                                                    try:
                                                        tmdb_id = getattr(subscribe, 'tmdbid', None)
                                                        sub_type = (getattr(subscribe, 'type', '') or '').lower()
                                                    except Exception:
                                                        tmdb_id = None
                                                        sub_type = ''
                                                    watch[str(sid)] = {"mh_uuid": mh_uuid, "tmdb_id": tmdb_id, "type": sub_type or 'movie'}
                                                    self.save_data(self._ASSIST_WATCH_KEY, watch)
                                                    pending.pop(sid, None)
                                                    self.save_data(self._ASSIST_PENDING_KEY, pending)
                                                else:
                                                    logger.info("mhnotify: 云下载辅助未匹配到可用资源或全部失败，恢复订阅启用")
                                                    with SessionFactory() as db:
                                                        SubscribeOper(db=db).update(subscribe.id, {"state": "R"})
                                                    # 恢复启用后，加入watch映射，用于取消事件快速删除MH
                                                    watch: Dict[str, dict] = self.get_data(self._ASSIST_WATCH_KEY) or {}
                                                    watch[str(sid)] = {"mh_uuid": mh_uuid}
                                                    self.save_data(self._ASSIST_WATCH_KEY, watch)
                                                    pending.pop(sid, None)
                                                    self.save_data(self._ASSIST_PENDING_KEY, pending)
                                        else:
                                            logger.info("mhnotify: 云下载辅助跳过：缺少豆瓣ID")
                                            with SessionFactory() as db:
                                                SubscribeOper(db=db).update(subscribe.id, {"state": "R"})
                                            # 加入watch映射，用于取消事件快速删除MH
                                            watch: Dict[str, dict] = self.get_data(self._ASSIST_WATCH_KEY) or {}
                                            watch[str(sid)] = {"mh_uuid": mh_uuid}
                                            self.save_data(self._ASSIST_WATCH_KEY, watch)
                                            pending.pop(sid, None)
                                            self.save_data(self._ASSIST_PENDING_KEY, pending)
                                    else:
                                        with SessionFactory() as db:
                                            SubscribeOper(db=db).update(subscribe.id, {"state": "R"})
                                        watch: Dict[str, dict] = self.get_data(self._ASSIST_WATCH_KEY) or {}
                                        try:
                                            tmdb_id = getattr(subscribe, 'tmdbid', None)
                                            sub_type = (getattr(subscribe, 'type', '') or '').lower()
                                        except Exception:
                                            tmdb_id = None
                                            sub_type = ''
                                        watch[str(sid)] = {"mh_uuid": mh_uuid, "tmdb_id": tmdb_id, "type": sub_type or 'movie'}
                                        self.save_data(self._ASSIST_WATCH_KEY, watch)
                                        pending.pop(sid, None)
                                        self.save_data(self._ASSIST_PENDING_KEY, pending)
                            else:
                                # TV
                                if expected > 0 and saved >= expected:
                                    # 完成：直接完成MP订阅，MH删除交由 SubscribeComplete 事件处理
                                    self._finish_mp_subscribe(subscribe)
                                    pending.pop(sid, None)
                                    self.save_data(self._ASSIST_PENDING_KEY, pending)
                                else:
                                    # 未完成：不删除MH，启用MP订阅，并加入watch等待MP完成/取消后删除MH
                                    # 注意：对于多季订阅，如果部分季已完成，不应在此处标记整个订阅为完成
                                    # 因为MH订阅是聚合的，这里的 saved 和 expected 是针对聚合后的所有季
                                    # 如果当前MP订阅对应的季已经下载完了，但MH里还有其他季没下载完，这里也是 saved < expected
                                    # 所以这里应该进一步检查：当前MP订阅对应的季是否已经全部保存了
                                    
                                    current_sub_completed = False
                                    try:
                                        # 获取当前MP订阅的季
                                        mp_season = getattr(subscribe, 'season', None)
                                        if mp_season is not None:
                                            # 获取MH订阅中该季的保存情况
                                            episodes = (target.get("episodes") or [])
                                            if episodes:
                                                counts = (episodes[0] or {}).get("episodes_count") or {}
                                                episodes_arr = (episodes[0] or {}).get("episodes_arr") or {}
                                                
                                                season_str = str(mp_season)
                                                
                                                # 该季总集数
                                                season_total = int((counts.get(season_str) or {}).get("count") or 0)
                                                # 该季已保存集数
                                                season_saved = len(episodes_arr.get(season_str) or [])
                                                
                                                if season_total > 0 and season_saved >= season_total:
                                                    current_sub_completed = True
                                                    logger.info(f"mhnotify: 订阅 {mh_uuid} 整体未完成({saved}/{expected})，但MP订阅对应的第{mp_season}季已完成({season_saved}/{season_total})")
                                    except Exception:
                                        pass
                                    
                                    if current_sub_completed:
                                        # 仅完成当前MP订阅
                                        self._finish_mp_subscribe(subscribe)
                                        pending.pop(sid, None)
                                        self.save_data(self._ASSIST_PENDING_KEY, pending)
                                    else:
                                        # 确实未完成
                                        with SessionFactory() as db:
                                            SubscribeOper(db=db).update(subscribe.id, {"state": "R"})
                                        watch: Dict[str, dict] = self.get_data(self._ASSIST_WATCH_KEY) or {}
                                        try:
                                            tmdb_id = getattr(subscribe, 'tmdbid', None)
                                            sub_type = (getattr(subscribe, 'type', '') or '').lower()
                                        except Exception:
                                            tmdb_id = None
                                            sub_type = ''
                                        watch[str(sid)] = {"mh_uuid": mh_uuid, "tmdb_id": tmdb_id, "type": sub_type or 'movie'}
                                        self.save_data(self._ASSIST_WATCH_KEY, watch)
                                        pending.pop(sid, None)
                                        self.save_data(self._ASSIST_PENDING_KEY, pending)
            # 监听MP完成后删除MH（可选）
            watch: Dict[str, dict] = self.get_data(self._ASSIST_WATCH_KEY) or {}
            if watch and self._mh_assist_auto_delete:
                for sid, info in list(watch.items()):
                    with SessionFactory() as db:
                        sub = SubscribeOper(db=db).get(int(sid))
                    if not sub:
                        # MP订阅不存在（取消/完成），处理对应MH：优先更新剩余季，否则删除
                        mh_uuid = info.get("mh_uuid")
                        try:
                            del_token = self._mh_login()
                        except Exception:
                            del_token = None
                        if mh_uuid and del_token:
                            try:
                                lst2 = self._mh_list_subscriptions(del_token)
                                subs2 = (lst2.get("data") or {}).get("subscriptions") or []
                                rec2 = None
                                for r in subs2:
                                    uid2 = r.get("uuid") or (r.get("task") or {}).get("uuid")
                                    if uid2 == mh_uuid:
                                        rec2 = r
                                        break
                                tmdb_id = None
                                if rec2:
                                    params2 = rec2.get("params") or {}
                                    tmdb_id = params2.get("tmdb_id")
                                remaining_seasons: List[int] = []
                                if tmdb_id:
                                    try:
                                        with SessionFactory() as db2:
                                            all_subs = SubscribeOper(db=db2).list_by_tmdbid(tmdb_id)
                                        seasons = []
                                        for s in all_subs or []:
                                            try:
                                                stype = (getattr(s, 'type', '') or '').strip()
                                                stype_lower = (stype or '').lower()
                                                if stype_lower == 'tv' or stype in {'电视剧'}:
                                                    s_season = getattr(s, 'season', None)
                                                    if s_season is None:
                                                        s_season = self._extract_season_from_text(getattr(s, 'name', '') or '')
                                                    seasons.append(s_season)
                                            except Exception:
                                                pass
                                        tmp: List[int] = []
                                        for x in seasons:
                                            if isinstance(x, int):
                                                tmp.append(x)
                                            elif isinstance(x, str) and x.isdigit():
                                                tmp.append(int(x))
                                        remaining_seasons = sorted({s for s in tmp if isinstance(s, int) and s > 0})
                                    except Exception:
                                        remaining_seasons = []
                                if remaining_seasons:
                                    try:
                                        base_params = (rec2 or {}).get("params") or {}
                                        base_params["selected_seasons"] = remaining_seasons
                                        base_params["episode_ranges"] = {str(s): {"min_episode": None, "max_episode": None, "exclude_episodes": [], "exclude_text": ""} for s in remaining_seasons}
                                        self._mh_update_subscription(del_token, mh_uuid, base_params)
                                        logger.info(f"mhnotify: 取消单季后更新MH订阅 seasons={remaining_seasons}")
                                    except Exception:
                                        logger.warning("mhnotify: 更新MH订阅季集合失败，降级为删除", exc_info=True)
                                        self._mh_delete_subscription(del_token, mh_uuid)
                                else:
                                    self._mh_delete_subscription(del_token, mh_uuid)
                            except Exception:
                                # 降级策略：出现异常则尽量删除对应 MH 订阅，避免遗留无主订阅
                                try:
                                    self._mh_delete_subscription(del_token, mh_uuid)
                                except Exception:
                                    logger.warning("mhnotify: watch 分支处理剩余季时异常且删除失败", exc_info=True)
                        # 清理当前监听项
                        watch.pop(sid, None)
                        self.save_data(self._ASSIST_WATCH_KEY, watch)
        except Exception as e:
            logger.error(f"mhnotify: 助手调度异常: {e}")

    def _sync_forwarder_keywords(self, subscribe_name: str, remove: bool = False) -> None:
        """同步订阅名称到/从MH TG转发监听白名单/黑名单"""
        if not subscribe_name:
            return
        _has_whitelist = self._mh_forwarder_whitelist_enabled and self._mh_forwarder_whitelist_listeners
        _has_blacklist = self._mh_forwarder_blacklist_enabled and self._mh_forwarder_blacklist_listeners
        if not _has_whitelist and not _has_blacklist:
            return
        try:
            token = self._mh_login()
            if not token:
                logger.warning("mhnotify: 同步转发关键词失败：MH登录失败")
                return
            action = "移除" if remove else "添加"
            if _has_whitelist:
                for lid in self._mh_forwarder_whitelist_listeners:
                    self._mh_sync_listener_keyword(token, lid, subscribe_name, "keywords", remove)
            if _has_blacklist:
                for lid in self._mh_forwarder_blacklist_listeners:
                    self._mh_sync_listener_keyword(token, lid, subscribe_name, "blacklist_keywords", remove)
            logger.info(f"mhnotify: 转发关键词同步完成 '{subscribe_name}' {action}")
        except Exception:
            logger.warning("mhnotify: 同步转发监听关键词异常", exc_info=True)

    @eventmanager.register(EventType.SubscribeDeleted)
    def _on_subscribe_deleted(self, event: Event):
        """
        监听 MP 订阅取消事件，按 tmdb_id 删除对应的 MH 订阅（drive115）
        """
        try:
            if not event or not event.event_data:
                return
            _auto_delete = self._mh_assist_auto_delete
            _forwarder_active = (
                (self._mh_forwarder_whitelist_enabled and bool(self._mh_forwarder_whitelist_listeners)) or
                (self._mh_forwarder_blacklist_enabled and bool(self._mh_forwarder_blacklist_listeners))
            )
            if not _auto_delete and not _forwarder_active:
                return
            data = event.event_data
            sid = str(data.get("subscribe_id") or data.get("id") or "")
            if not sid:
                return
            logger.info(f"mhnotify: SubscribeDeleted 事件收到 sid={sid}")
            
            # 取消待处理的定时器（如果存在）
            try:
                with self._timers_lock:
                    timer = self._pending_timers.pop(sid, None)
                    if timer:
                        timer.cancel()
                        logger.info(f"mhnotify: 已取消订阅 {sid} 的待检查定时器")
            except Exception as e:
                logger.warning(f"mhnotify: 取消定时器失败: {e}")
            
            # 从待检查队列中移除（如果存在）
            try:
                pending = self.get_data(self._ASSIST_PENDING_KEY) or {}
                if sid in pending:
                    pending.pop(sid, None)
                    self.save_data(self._ASSIST_PENDING_KEY, pending)
                    logger.info(f"mhnotify: 已从待检查队列中移除订阅 {sid}")
            except Exception as e:
                logger.warning(f"mhnotify: 从待检查队列移除失败: {e}")
            
            try:
                import json
                logger.info(f"mhnotify: SubscribeDeleted 全量事件 event_data={str(json.dumps(data, ensure_ascii=False, default=str))[:2000]}")
            except Exception:
                try:
                    logger.info(f"mhnotify: SubscribeDeleted 全量事件（fallback） event_data={str(data)[:2000]}")
                except Exception:
                    pass
            # 获取 tmdb_id、媒体类型、订阅名称
            tmdb_id = None
            mtype = None
            season = None
            subscribe_name = ""
            # 优先从事件 subscribe_info 读取
            try:
                si = (data.get("subscribe_info") or data.get("subscribe") or {}) or {}
                tmdb_id = si.get("tmdbid") or si.get("tmdb_id")
                mtype = self._normalize_media_type(si.get("type"), si.get("type"))
                season = si.get("season")
                subscribe_name = si.get("name") or ""
                logger.info(f"mhnotify: SubscribeDeleted subscribe_info 提取 name={si.get('name')} tmdbid={tmdb_id} type={si.get('type')} season={season}")
            except Exception:
                pass
            try:
                with SessionFactory() as db:
                    sub = SubscribeOper(db=db).get(int(sid))
                if tmdb_id is None:
                    tmdb_id = getattr(sub, 'tmdbid', None)
                if not mtype:
                    mtype = (getattr(sub, 'type', '') or '').lower()
                if season is None:
                    season = getattr(sub, 'season', None)
                if not subscribe_name:
                    subscribe_name = getattr(sub, 'name', '') or ""
            except Exception:
                pass
            if not tmdb_id:
                tmdb_id = (data.get("mediainfo") or {}).get("tmdb_id") or (data.get("mediainfo") or {}).get("tmdbid")
            if not mtype:
                mtype = (data.get("mediainfo") or {}).get("type")
            # 同步转发关键词（独立于 auto_delete 开关）
            if _forwarder_active and subscribe_name:
                try:
                    self._sync_forwarder_keywords(subscribe_name, remove=True)
                except Exception:
                    logger.warning("mhnotify: 取消订阅同步转发关键词失败", exc_info=True)
            if not _auto_delete:
                return
            if not tmdb_id:
                logger.info("mhnotify: SubscribeDeleted 未获取到tmdb_id，跳过删除")
                return
            token = self._mh_login()
            if not token:
                logger.warning("mhnotify: 登录MH失败，无法按tmdb_id删除订阅")
                return
            deleted = self._mh_delete_by_tmdb(token, tmdb_id, media_type=mtype, season=season)
            if deleted <= 0:
                logger.info("mhnotify: 未按tmdb_id删除到任何MH订阅（drive115），可能未创建或类型不一致")
        except Exception:
            logger.error("mhnotify: 处理SubscribeDeleted事件异常", exc_info=True)

    @eventmanager.register(EventType.SubscribeComplete)
    def _on_subscribe_complete(self, event: Event):
        try:
            if not event or not event.event_data:
                return
            _auto_delete = self._mh_assist_auto_delete
            _forwarder_active = (
                (self._mh_forwarder_whitelist_enabled and bool(self._mh_forwarder_whitelist_listeners)) or
                (self._mh_forwarder_blacklist_enabled and bool(self._mh_forwarder_blacklist_listeners))
            )
            if not _auto_delete and not _forwarder_active:
                return
            data = event.event_data
            sid = str(data.get("subscribe_id") or data.get("id") or "")
            if not sid:
                return
            logger.info(f"mhnotify: SubscribeComplete 事件收到 sid={sid}")
            try:
                import json
                logger.info(f"mhnotify: SubscribeComplete 全量事件 event_data={str(json.dumps(data, ensure_ascii=False, default=str))[:2000]}")
            except Exception:
                try:
                    logger.info(f"mhnotify: SubscribeComplete 全量事件（fallback） event_data={str(data)[:2000]}")
                except Exception:
                    pass
            tmdb_id = None
            mtype = None
            season = None
            subscribe_name = ""
            try:
                si = (data.get("subscribe_info") or data.get("subscribe") or {}) or {}
                tmdb_id = si.get("tmdbid") or si.get("tmdb_id")
                mtype = self._normalize_media_type(si.get("type"), si.get("type"))
                season = si.get("season")
                subscribe_name = si.get("name") or ""
                logger.info(f"mhnotify: SubscribeComplete subscribe_info 提取 name={si.get('name')} tmdbid={tmdb_id} type={si.get('type')} season={season}")
            except Exception:
                pass
            try:
                with SessionFactory() as db:
                    sub = SubscribeOper(db=db).get(int(sid))
                if tmdb_id is None:
                    tmdb_id = getattr(sub, 'tmdbid', None)
                if not mtype:
                    mtype = (getattr(sub, 'type', '') or '').lower()
                if season is None:
                    season = getattr(sub, 'season', None)
                if not subscribe_name:
                    subscribe_name = getattr(sub, 'name', '') or ""
            except Exception:
                pass
            if not tmdb_id:
                tmdb_id = (data.get("mediainfo") or {}).get("tmdb_id") or (data.get("mediainfo") or {}).get("tmdbid")
            if not mtype:
                mtype = (data.get("mediainfo") or {}).get("type")
            # 同步转发关键词（独立于 auto_delete 开关）
            if _forwarder_active and subscribe_name:
                try:
                    self._sync_forwarder_keywords(subscribe_name, remove=True)
                except Exception:
                    logger.warning("mhnotify: 完成订阅同步转发关键词失败", exc_info=True)
            if not _auto_delete:
                return
            if not tmdb_id:
                logger.info("mhnotify: SubscribeComplete 未获取到tmdb_id，跳过删除")
                return
            token = self._mh_login()
            if not token:
                logger.warning("mhnotify: 登录MH失败，无法按tmdb_id删除订阅")
                return
            deleted = self._mh_delete_by_tmdb(token, tmdb_id, media_type=mtype, season=season)
            if deleted <= 0:
                logger.info("mhnotify: 未按tmdb_id删除到任何MH订阅（drive115），可能未创建或类型不一致")
        except Exception:
            logger.error("mhnotify: 处理SubscribeComplete事件异常", exc_info=True)

    def _finish_mp_subscribe(self, subscribe):
        try:
            # 恢复订阅状态为 R，不修改 sites，保留原有的站点配置以便洗版
            with SessionFactory() as db:
                # 检查是否配置了忽略MP洗版订阅
                if self._ignore_mp_wash:
                    logger.info(f"mhnotify: MP订阅 {subscribe.name} 忽略洗版")
                else:
                    SubscribeOper(db=db).update(subscribe.id, {"state": "R"})
                    logger.info(f"mhnotify: MP订阅 {subscribe.name} 已恢复为运行状态(R)")
            
            # 生成元数据
            from app.core.metainfo import MetaInfo
            from app.schemas.types import MediaType
            from app.chain.subscribe import SubscribeChain
            from app.core.context import MediaInfo
            meta = MetaInfo(subscribe.name)
            meta.year = subscribe.year
            meta.begin_season = subscribe.season or None
            try:
                meta.type = MediaType(subscribe.type)
            except Exception:
                pass
            # 构造最小可用的 mediainfo（用于完成订阅日志与通知）
            mediainfo = MediaInfo()
            try:
                # 类型映射
                st = (subscribe.type or "").strip().lower()
                if st in {"电影", "movie", "movies"}:
                    mediainfo.type = MediaType.MOVIE
                elif st in {"电视剧", "tv", "series"}:
                    mediainfo.type = MediaType.TV
                else:
                    mediainfo.type = meta.type or MediaType.MOVIE
                mediainfo.title = subscribe.name
                mediainfo.year = subscribe.year
                mediainfo.tmdb_id = getattr(subscribe, 'tmdbid', None)
                mediainfo.poster_path = getattr(subscribe, 'poster', None)
                mediainfo.backdrop_path = getattr(subscribe, 'backdrop', None)
                mediainfo.overview = getattr(subscribe, 'description', None)
                mediainfo.vote_average = getattr(subscribe, 'vote', None)
            except Exception:
                pass
            # 完成订阅
            chain = SubscribeChain()
            if self._ignore_mp_wash:
                try:
                    original_best = getattr(subscribe, "best_version", False)
                    setattr(subscribe, "best_version", False)
                except Exception:
                    pass
            chain.finish_subscribe_or_not(
                subscribe=subscribe,
                meta=meta,
                mediainfo=mediainfo,
                downloads=None,
                lefts={},
                force=True
            )
        except Exception as e:
            logger.error(f"mhnotify: 完成MP订阅失败: {e}")

