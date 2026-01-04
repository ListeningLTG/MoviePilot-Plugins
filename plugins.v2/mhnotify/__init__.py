import time

from typing import List, Tuple, Dict, Any, Optional
from apscheduler.triggers.cron import CronTrigger

from app.core.config import settings
from app.core.event import eventmanager, Event
from app.schemas.types import EventType
from app.utils.http import RequestUtils
from app.log import logger
from app.plugins import _PluginBase
from app.db import SessionFactory
from app.db.subscribe_oper import SubscribeOper


class MHNotify(_PluginBase):
    # 插件名称
    plugin_name = "MediaHelper增强"
    # 插件描述
    plugin_desc = "整理完媒体后，通知MediaHelper执行strm生成任务；并提供mh订阅辅助"
    # 插件图标
    plugin_icon = "https://raw.githubusercontent.com/JieWSOFT/MediaHelp/main/frontend/apps/web-antd/public/icon.png"
    # 插件版本
    plugin_version = "0.9"
    # 插件作者
    plugin_author = "ListeningLTG"
    # 作者主页
    author_url = "https://github.com/ListeningLTG"
    # 插件配置项ID前缀
    plugin_config_prefix = "mhnotify_"
    # 加载顺序
    plugin_order = 1
    # 可使用的用户级别
    auth_level = 1

    # 私有属性
    _mh_domain = None
    _mh_username = None
    _mh_password = None
    _mh_job_names = None
    _enabled = False
    _last_event_time = 0
    # 下一次允许通知的时间戳（用于等待窗口）
    _next_notify_time = 0
    # 等待通知数量
    _wait_notify_count = 0
    # 延迟分钟数（存在运行中整理任务时的等待窗口）
    _wait_minutes = 5
    # mh订阅辅助开关
    _mh_assist_enabled: bool = False
    # 助手：待检查的mh订阅映射（mp_sub_id -> {mh_uuid, created_at, type}）
    _ASSIST_PENDING_KEY = "mhnotify_assist_pending"
    # 助手：等待MP完成后删除mh订阅的监听映射（mp_sub_id -> {mh_uuid}）
    _ASSIST_WATCH_KEY = "mhnotify_assist_watch"

    def init_plugin(self, config: dict = None):
        if config:
            self._enabled = config.get("enabled")
            self._mh_domain = config.get("mh_domain")
            self._mh_username = config.get('mh_username')
            self._mh_password = config.get('mh_password')
            self._mh_job_names = config.get('mh_job_names') or ""
            try:
                self._wait_minutes = int(config.get('wait_minutes') or 5)
            except Exception:
                self._wait_minutes = 5
            # mh订阅辅助开关
            self._mh_assist_enabled = bool(config.get("mh_assist", False))

            # 清除助手记录（运行一次）
            try:
                if bool(config.get("clear_once", False)):
                    logger.info("mhnotify: 检测到清除助手记录（运行一次）开关已开启，开始清理...")
                    self._clear_all_records()
                    # 复位为关闭，并更新配置
                    config["clear_once"] = False
                    self.update_config(config)
                    logger.info("mhnotify: 助手记录清理完成，已自动复位为关闭")
            except Exception:
                logger.error("mhnotify: 执行清理助手记录失败", exc_info=True)

    def get_state(self) -> bool:
        return self._enabled

    def get_service(self) -> List[Dict[str, Any]]:
        """
        注册插件公共服务
        [{
            "id": "服务ID",
            "name": "服务名称",
            "trigger": "触发器：cron/interval/date/CronTrigger.from_crontab()",
            "func": self.xxx,
            "kwargs": {} # 定时器参数
        }]
        """
        services = []
        if self._enabled:
            services.append({
                "id": "MHNotify",
                "name": "MediaHelper增强",
                "trigger": CronTrigger.from_crontab("* * * * *"),
                "func": self.__notify_mh,
                "kwargs": {}
            })
        # mh订阅辅助调度
        if self._mh_assist_enabled:
            services.append({
                "id": "MHAssist",
                "name": "mh订阅辅助",
                "trigger": CronTrigger.from_crontab("* * * * *"),
                "func": self.__assist_scheduler,
                "kwargs": {}
            })
        return services

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """定义远程控制命令"""
        return [{
            "cmd": "/mhnotify_clear",
            "event": EventType.PluginAction,
            "desc": "清除订阅记录（移除脏数据）",
            "category": "维护",
            "data": {
                "action": "mhnotify_clear"
            }
        }]

    def get_api(self) -> List[Dict[str, Any]]:
        # 不需要前端即时API
        return []

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """
        拼装插件配置页面，需要返回两块数据：1、页面配置；2、数据结构
        """
        # 不在渲染阶段请求后端，改为名称输入方案

        return [
            {
                'component': 'VForm',
                'content': [
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 6
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'enabled',
                                            'label': '启用插件',
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 6
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'mh_assist',
                                            'label': 'mh订阅辅助（仅新订阅生效）',
                                            'hint': '开启后，新添加的订阅将默认在MP中暂停，并由插件在MH创建订阅、延时查询进度、按规则删除或恢复MP订阅；不影响已有订阅'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 6
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'clear_once',
                                            'label': '清除助手记录（运行一次）',
                                            'hint': '开启后点保存立即清除所有助手记录（pending/watch），随后自动复位为关闭，移除脏数据'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 6
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'mh_domain',
                                            'label': 'MediaHelper地址'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 6
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'mh_username',
                                            'label': 'MediaHelper_用户名'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 6
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'mh_password',
                                            'label': 'MediaHelper_密码',
                                            'type': 'password'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 12
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'mh_job_names',
                                            'label': 'strm任务名称（英文逗号分隔）',
                                            'placeholder': '例如：115网盘1,115网盘2',
                                            'hint': '填写strm生成任务名称；留空则默认匹配名称含“115网盘”'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'wait_minutes',
                                            'label': '延迟分钟数',
                                            'type': 'number',
                                            'placeholder': '默认 5',
                                            'hint': '检测到仍有整理运行时，延迟等待该分钟数；等待期间如有新整理完成将滚动延长'
                                        }
                                    },
                                    {
                                        'component': 'VAlert',
                                        'props': {
                                            'type': 'info',
                                            'variant': 'tonal',
                                            'text': '当MP整理或刮削媒体后，将通知MediaHelper执行strm生成任务（无运行任务则立即触发）'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                },
                                'content': [
                                    {
                                        'component': 'VAlert',
                                        'props': {
                                            'type': 'info',
                                            'variant': 'tonal',
                                            'text': '为避免频繁触发：若检测到仍有整理运行，将延迟等待（可配置，默认5分钟）；等待期间如有新整理完成将滚动延长，直到无运行任务再触发'
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ], {
            "enabled": False,
            "mh_username": "",
            "mh_password": "",
            "mh_job_names": "",
            "mh_domain": "",
            "wait_minutes": 5,
            "mh_assist": False,
            "clear_once": False
        }

    def get_page(self) -> List[dict]:
        pass

    @eventmanager.register(EventType)
    def send(self, event):
        """
        向第三方Webhook发送请求
        """
        if not self._enabled or not self._mh_domain or not self._mh_username or not self._mh_password:
            return

        if not event or not event.event_type:
            return

        def __to_dict(_event):
            """
            递归将对象转换为字典
            """
            if isinstance(_event, dict):
                for k, v in _event.items():
                    _event[k] = __to_dict(v)
                return _event
            elif isinstance(_event, list):
                for i in range(len(_event)):
                    _event[i] = __to_dict(_event[i])
                return _event
            elif isinstance(_event, tuple):
                return tuple(__to_dict(list(_event)))
            elif isinstance(_event, set):
                return set(__to_dict(list(_event)))
            elif hasattr(_event, 'to_dict'):
                return __to_dict(_event.to_dict())
            elif hasattr(_event, '__dict__'):
                return __to_dict(_event.__dict__)
            elif isinstance(_event, (int, float, str, bool, type(None))):
                return _event
            else:
                return str(_event)

        version = getattr(settings, "VERSION_FLAG", "v1")
        event_type = event.event_type if version == "v1" else event.event_type.value
        if event_type not in ["transfer.complete", "metadata.scrape"]:
            return
        event_data = __to_dict(event.event_data)

        # logger.info(f"event_data: {event_data}")
        if event_type == "transfer.complete":
            transferinfo = event_data["transferinfo"]
            success = transferinfo["success"]
            if success:
                name = transferinfo["target_item"]["name"]
                logger.info(f"整理完成：{name}")
                self._wait_notify_count += 1
                self._last_event_time = self.__get_time()
        elif event_type == "metadata.scrape":
            name = event_data.get("name")
            logger.info(f"刮削完成：{name}")
            self._wait_notify_count += 1
            self._last_event_time = self.__get_time()

    def __get_time(self):
        return int(time.time())

    def __has_running_transfers(self) -> bool:
        """
        检测是否有正在运行的整理任务
        """
        try:
            from app.chain.transfer import TransferChain
            # 与前端一致，使用 get_queue_tasks()
            jobs = TransferChain().get_queue_tasks()
            if not jobs:
                logger.debug("mhnotify: 当前整理队列为空 []")
                return False
            for job in jobs:
                tasks = getattr(job, 'tasks', [])
                if any((getattr(t, 'state', '') == 'running') for t in tasks):
                    logger.debug("mhnotify: 发现 running 任务，判定为正在整理")
                    return True
            logger.debug("mhnotify: 队列非空但无 running 任务，判定为不在整理")
            return False
        except Exception as e:
            # 记录异常并返回不在整理，避免误报
            logger.warning(f"mhnotify: 检测整理任务状态异常：{e}，按无运行处理")
            return False

    def __notify_mh(self):
        try:
            # 当有待通知时，根据是否存在运行中整理任务决定立即触发或进入等待窗口
            now_ts = self.__get_time()
            if self._wait_notify_count > 0:
                if self.__has_running_transfers():
                    # 若存在运行中任务：设置或延长等待窗口（单位：分钟）
                    delay_seconds = max(int(self._wait_minutes) * 60, 0)
                    if self._next_notify_time == 0 or now_ts >= self._next_notify_time:
                        self._next_notify_time = now_ts + delay_seconds
                    # 在等待窗口期间不触发通知
                    logger.info(f"检测到正在运行的整理任务，延迟 {self._next_notify_time - now_ts}s 后再触发")
                    return
                else:
                    # 无运行中任务：若设置了等待窗口但未到期，继续等待；否则立即触发
                    if self._next_notify_time and now_ts < self._next_notify_time:
                        logger.info(f"等待窗口未到期（{self._next_notify_time - now_ts}s），暂不触发通知")
                        return
                    # 立即触发通知，重置等待窗口
                    self._next_notify_time = 0
                # 登录获取 access_token
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
                    logger.error(f"MediaHelper 登录失败：{getattr(login_res, 'status_code', 'N/A')} - {getattr(login_res, 'text', '')}")
                    return
                try:
                    login_data = login_res.json()
                    access_token = (login_data or {}).get("data", {}).get("access_token")
                except Exception:
                    access_token = None
                if not access_token:
                    logger.error("MediaHelper 登录成功但未获取到 access_token")
                    return
                # 获取任务列表并筛选 strm 任务
                tasks_url = f"{self._mh_domain}/api/v1/scheduled/tasks"
                list_headers = {
                    "Accept": "application/json, text/plain, */*",
                    "Authorization": f"Bearer {access_token}",
                    "User-Agent": "MoviePilot/Plugin MHNotify",
                    "Accept-Language": "zh-CN"
                }
                list_res = RequestUtils(headers=list_headers).get_res(tasks_url)
                if not list_res or list_res.status_code != 200:
                    logger.error(f"获取 MediaHelper 任务列表失败：{getattr(list_res, 'status_code', 'N/A')} - {getattr(list_res, 'text', '')}")
                    return
                try:
                    list_data = list_res.json() or {}
                    tasks = list_data.get("data", [])
                except Exception:
                    tasks = []
                # 过滤 cloud_strm_sync 任务
                strm_tasks = [t for t in tasks if t.get('task') == 'cloud_strm_sync' and t.get('enabled')]
                # 根据名称匹配（英文逗号分隔），否则默认名称包含“115网盘”
                selected_uuids = []
                name_filters = []
                if self._mh_job_names:
                    name_filters = [n.strip() for n in self._mh_job_names.split(',') if n.strip()]
                if name_filters:
                    selected_uuids = [t.get('uuid') for t in strm_tasks if (t.get('name') or '') in name_filters]
                else:
                    selected_uuids = [t.get('uuid') for t in strm_tasks if '115网盘' in (t.get('name') or '')]
                if not selected_uuids:
                    logger.warning("未找到可执行的 strm 任务（cloud_strm_sync），请检查任务名称或在配置中填写任务UUID列表")
                    return
                # 逐个触发，间隔5秒
                exec_headers = {
                    "Accept": "application/json, text/plain, */*",
                    "Content-Type": "application/json;charset=UTF-8",
                    "Authorization": f"Bearer {access_token}",
                    "Origin": self._mh_domain,
                    "Accept-Language": "zh-CN",
                    "User-Agent": "MoviePilot/Plugin MHNotify"
                }
                success_any = False
                for uuid in selected_uuids:
                    exec_url = f"{self._mh_domain}/api/v1/scheduled/execute/{uuid}"
                    exec_res = RequestUtils(headers=exec_headers).post(exec_url, json={})
                    if exec_res and exec_res.status_code in (200, 204):
                        logger.info(f"已触发 MediaHelper 计划任务：{uuid}")
                        success_any = True
                    elif exec_res is not None:
                        logger.error(f"触发任务失败：{uuid} - {exec_res.status_code} - {exec_res.text}")
                    else:
                        logger.error(f"触发任务失败：{uuid} - 未获取到返回信息")
                    time.sleep(5)
                if success_any:
                    self._wait_notify_count = 0
            else:
                if self._wait_notify_count > 0:
                    logger.info(
                        f"等待通知数量：{self._wait_notify_count}，最后事件时间：{self._last_event_time}")
        except Exception as e:
            logger.error(f"通知MediaHelper发生异常：{e}")

    def stop_service(self):
        """
        退出插件
        """
        pass

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
            sub_id = event_data.get("subscribe_id")
            mediainfo_dict = event_data.get("mediainfo") or {}
            if not sub_id:
                return
            # 暂停该订阅，仅针对新订阅
            with SessionFactory() as db:
                subscribe = SubscribeOper(db=db).get(sub_id)
                if not subscribe:
                    return
                SubscribeOper(db=db).update(sub_id, {"state": "S", "sites": [-1]})
            # 登录 MH 拿 token
            access_token = self.__mh_login()
            if not access_token:
                logger.error("mhnotify: 登录MediaHelper失败，无法创建订阅")
                return
            # 读取默认配置
            defaults = self.__mh_get_defaults(access_token)
            # 构建创建参数
            create_payload = self.__build_mh_create_payload(subscribe, mediainfo_dict, defaults)
            if not create_payload:
                logger.error("mhnotify: 构建MH订阅创建参数失败")
                return
            # 创建订阅
            resp = self.__mh_create_subscription(access_token, create_payload)
            mh_uuid = (resp or {}).get("data", {}).get("subscription_id") or (resp or {}).get("data", {}).get("task", {}).get("uuid")
            if not mh_uuid:
                logger.error(f"mhnotify: MH订阅创建失败：{resp}")
                return
            logger.info(f"mhnotify: 已在MH创建订阅，uuid={mh_uuid}；5分钟后查询进度")
            # 记录待检查项
            pending: Dict[str, dict] = self.get_data(self._ASSIST_PENDING_KEY) or {}
            pending[str(sub_id)] = {
                "mh_uuid": mh_uuid,
                "created_at": int(time.time()),
                "type": (create_payload.get("media_type") or mediainfo_dict.get("type") or "movie")
            }
            self.save_data(self._ASSIST_PENDING_KEY, pending)
        except Exception as e:
            logger.error(f"mhnotify: 处理新增订阅事件失败: {e}")

    # 旧屏蔽逻辑移除

    # 旧屏蔽逻辑移除

    def __mh_login(self) -> Optional[str]:
        """登录 MH 获取 access_token"""
        try:
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
            return token
        except Exception:
            logger.error("mhnotify: 登录MH出现异常", exc_info=True)
            return None

    def __auth_headers(self, access_token: str) -> Dict[str, str]:
        return {
            "Accept": "application/json, text/plain, */*",
            "Authorization": f"Bearer {access_token}",
            "User-Agent": "MoviePilot/Plugin MHNotify",
            "Accept-Language": "zh-CN"
        }

    def __mh_get_defaults(self, access_token: str) -> Dict[str, Any]:
        try:
            url = f"{self._mh_domain}/api/v1/subscription/config/defaults"
            logger.info(f"mhnotify: 获取MH默认配置 GET {url}")
            res = RequestUtils(headers=self.__auth_headers(access_token)).get_res(url)
            if res is None:
                logger.error("mhnotify: 获取MH默认配置未返回响应")
            elif res.status_code != 200:
                logger.error(f"mhnotify: 获取MH默认配置失败 status={res.status_code} body={getattr(res, 'text', '')[:200]}")
            else:
                data = res.json() or {}
                core = (data or {}).get("data") or {}
                logger.info(
                    "mhnotify: 默认配置摘要 cloud_type=%s account=%s target_directory=%s quality_preference=%s",
                    core.get("cloud_type"), core.get("account_identifier"), core.get("target_directory"), core.get("quality_preference")
                )
                return data
        except Exception:
            logger.error("mhnotify: 获取MH默认配置异常", exc_info=True)
            pass
        return {}

    def __normalize_media_type(self, sub_type: Optional[str], info_type: Optional[str]) -> str:
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

    def __build_mh_create_payload(self, subscribe, mediainfo_dict: Dict[str, Any], defaults: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            data = (defaults or {}).get("data") or {}
            quality_pref = data.get("quality_preference") or "auto"
            target_dir = data.get("target_directory") or "/影视"
            cron = data.get("cron") or "0 */6 * * *"
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
            mtype_norm = self.__normalize_media_type(sub_type, info_type)
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
                season = _get('season') or 1
                payload["selected_seasons"] = [season]
                payload["episode_ranges"] = {str(season): {"min_episode": None, "max_episode": None, "exclude_episodes": [], "exclude_text": ""}}
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

    def __mh_create_subscription(self, access_token: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        try:
            url = f"{self._mh_domain}/api/v1/subscription/create"
            headers = self.__auth_headers(access_token)
            headers.update({"Content-Type": "application/json;charset=UTF-8", "Origin": self._mh_domain})
            logger.info(f"mhnotify: 创建MH订阅 POST {url} media_type={payload.get('media_type')} tmdb_id={payload.get('tmdb_id')} title={str(payload.get('title'))[:50]}")
            res = RequestUtils(headers=headers).post(url, json=payload)
            if res is None:
                logger.error("mhnotify: 创建MH订阅未返回响应")
            elif res.status_code not in (200, 204):
                logger.error(f"mhnotify: 创建MH订阅失败 status={res.status_code} body={getattr(res, 'text', '')[:200]}")
            else:
                data = res.json() or {}
                uuid = (data.get("data") or {}).get("subscription_id") or (data.get("data") or {}).get("task", {}).get("uuid")
                logger.info(f"mhnotify: 创建MH订阅成功 uuid={uuid}")
                return data
        except Exception:
            logger.error("mhnotify: 创建MH订阅异常", exc_info=True)
            pass
        return {}

    def __mh_list_subscriptions(self, access_token: str) -> Dict[str, Any]:
        try:
            url = f"{self._mh_domain}/api/v1/subscription/list?page=1&page_size=2000"
            logger.info(f"mhnotify: 查询MH订阅列表 GET {url}")
            res = RequestUtils(headers=self.__auth_headers(access_token)).get_res(url)
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

    def __mh_delete_subscription(self, access_token: str, uuid: str) -> bool:
        try:
            url = f"{self._mh_domain}/api/v1/subscription/{uuid}"
            headers = self.__auth_headers(access_token)
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

    def __compute_progress(self, sub_rec: Dict[str, Any]) -> Tuple[str, int, int]:
        """返回 (media_type, saved, expected_total)"""
        params = (sub_rec or {}).get("params") or {}
        mtype = (params.get("media_type") or (sub_rec.get("subscription_info") or {}).get("media_type") or "movie").lower()
        saved = int(params.get("saved_resources") or (sub_rec.get("params") or {}).get("saved_resources") or (sub_rec.get("saved_resources") if isinstance(sub_rec.get("saved_resources"), int) else 0))
        # episodes_count 在 episodes[0].episodes_count
        expected_total = 1 if mtype == 'movie' else 0
        try:
            episodes = (sub_rec.get("episodes") or [])
            if episodes:
                counts = (episodes[0] or {}).get("episodes_count") or {}
                if mtype == 'tv':
                    for s in counts.values():
                        expected_total += int(s.get("count") or 0)
                else:
                    # movie: 如果存在也按1处理
                    expected_total = 1
        except Exception:
            pass
        return mtype, saved, expected_total

    def __assist_scheduler(self):
        """每分钟执行：先等待2分钟进行首次查询；未查询到则每1分钟重试，直到查询到；并处理MP完成监听"""
        try:
            # 处理待检查
            pending: Dict[str, dict] = self.get_data(self._ASSIST_PENDING_KEY) or {}
            if pending:
                now_ts = int(time.time())
                for sid, info in list(pending.items()):
                    created_at = int(info.get("created_at") or 0)
                    # 首次查询延迟 2 分钟
                    if now_ts - created_at < 120:
                        continue
                    mh_uuid = info.get("mh_uuid")
                    # 查询进度
                    token = self.__mh_login()
                    if not token:
                        logger.error("mhnotify: 登录MH失败，无法查询订阅进度")
                        continue
                    lst = self.__mh_list_subscriptions(token)
                    subs = (lst.get("data") or {}).get("subscriptions") or []
                    target = None
                    for rec in subs:
                        if (rec.get("uuid") or rec.get("task", {}).get("uuid")) == mh_uuid:
                            target = rec
                            break
                    if not target:
                        # 未找到，记录重试次数，超过30次则移除记录
                        attempts = int(info.get("attempts") or 0) + 1
                        info["attempts"] = attempts
                        info["last_attempt"] = now_ts
                        if attempts >= 30:
                            logger.warning(f"mhnotify: 订阅 {mh_uuid} 未在MH列表中找到，已重试{attempts}次，移除记录")
                            pending.pop(sid, None)
                            self.save_data(self._ASSIST_PENDING_KEY, pending)
                            continue
                        else:
                            logger.warning(f"mhnotify: 未在MH列表中找到订阅 {mh_uuid}，第{attempts}次重试，1分钟后继续")
                            pending[str(sid)] = info
                            self.save_data(self._ASSIST_PENDING_KEY, pending)
                            continue
                    mtype, saved, expected = self.__compute_progress(target)
                    logger.info(f"mhnotify: 订阅 {mh_uuid} 进度 saved={saved}/{expected} type={mtype}")
                    with SessionFactory() as db:
                        subscribe = SubscribeOper(db=db).get(int(sid))
                    if not subscribe:
                        # MP订阅已不存在，清理并删除MH订阅
                        if token and mh_uuid:
                            self.__mh_delete_subscription(token, mh_uuid)
                        pending.pop(sid, None)
                        continue
                    if mtype == 'movie':
                        if expected <= 1 and saved >= 1:
                            # 完成：删除MH，完成MP订阅
                            self.__mh_delete_subscription(token, mh_uuid)
                            self.__finish_mp_subscribe(subscribe)
                            pending.pop(sid, None)
                        else:
                            # 未完成：恢复MP订阅并监听MP完成后删除MH
                            with SessionFactory() as db:
                                SubscribeOper(db=db).update(subscribe.id, {"state": "R", "sites": []})
                            watch: Dict[str, dict] = self.get_data(self._ASSIST_WATCH_KEY) or {}
                            watch[sid] = {"mh_uuid": mh_uuid}
                            self.save_data(self._ASSIST_WATCH_KEY, watch)
                            pending.pop(sid, None)
                    else:
                        # TV
                        if expected > 0 and saved >= expected:
                            # 完成：删除MH，完成MP订阅
                            self.__mh_delete_subscription(token, mh_uuid)
                            self.__finish_mp_subscribe(subscribe)
                            pending.pop(sid, None)
                        else:
                            # 未完成：删除MH并启用MP订阅
                            self.__mh_delete_subscription(token, mh_uuid)
                            with SessionFactory() as db:
                                SubscribeOper(db=db).update(subscribe.id, {"state": "R", "sites": []})
                            pending.pop(sid, None)
            # 监听MP完成后删除MH
            watch: Dict[str, dict] = self.get_data(self._ASSIST_WATCH_KEY) or {}
            if watch:
                token = self.__mh_login()
                for sid, info in list(watch.items()):
                    with SessionFactory() as db:
                        sub = SubscribeOper(db=db).get(int(sid))
                    if not sub:
                        # MP订阅已完成（被删除），删除MH并清理监听
                        mh_uuid = info.get("mh_uuid")
                        if token and mh_uuid:
                            self.__mh_delete_subscription(token, mh_uuid)
                        watch.pop(sid, None)
                        self.save_data(self._ASSIST_WATCH_KEY, watch)
        except Exception as e:
            logger.error(f"mhnotify: 助手调度异常: {e}")

    def _clear_all_records(self) -> Dict[str, Any]:
        """清除助手记录（pending/watch），移除脏数据"""
        try:
            self.save_data(self._ASSIST_PENDING_KEY, {})
            self.save_data(self._ASSIST_WATCH_KEY, {})
            logger.info("mhnotify: 已清除助手记录（pending/watch）")
            return {"success": True}
        except Exception as e:
            logger.error(f"mhnotify: 清除助手记录失败: {e}")
            return {"success": False, "error": str(e)}

    @eventmanager.register(EventType.PluginAction)
    def remote_clear_records(self, event: Event):
        """远程命令触发：清除订阅记录"""
        if not event:
            return
        event_data = event.event_data
        if not event_data or event_data.get("action") != "mhnotify_clear":
            return

        logger.info("收到命令，开始清除 mhnotify 助手记录...")
        self.post_message(
            channel=event_data.get("channel"),
            title="开始清除 mhnotify 助手记录...",
            userid=event_data.get("user")
        )

        result = self._clear_all_records()

        title = "mhnotify 助手记录清除完成" if result.get("success") else f"mhnotify 助手记录清除失败：{result.get('error')}"
        self.post_message(
            channel=event_data.get("channel"),
            title=title,
            userid=event_data.get("user")
        )

    def __finish_mp_subscribe(self, subscribe):
        try:
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
                mediainfo.poster_path = None
                mediainfo.backdrop_path = None
                mediainfo.overview = None
            except Exception:
                pass
            # 完成订阅
            SubscribeChain().finish_subscribe_or_not(
                subscribe=subscribe,
                meta=meta,
                mediainfo=mediainfo,
                downloads=None,
                lefts={},
                force=True
            )
        except Exception as e:
            logger.error(f"mhnotify: 完成MP订阅失败: {e}")
