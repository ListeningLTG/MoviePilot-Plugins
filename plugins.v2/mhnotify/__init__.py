import time
import re

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
    plugin_desc = "监听115生活事件和MP整理/刮削事件后，通知MediaHelper执行strm生成任务；提供mh订阅辅助；支持115云下载（/mhol命令）、自动删除小文件及移动整理"
    # 插件图标
    plugin_icon = "https://raw.githubusercontent.com/ListeningLTG/MoviePilot-Plugins/refs/heads/main/icons/mh2.jpg"
    # 插件版本
    plugin_version = "1.5.3"
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
    #（已废弃）
    _wait_minutes = 5
    # mh订阅辅助开关
    _mh_assist_enabled: bool = False
    # mh订阅辅助：MP订阅完成后自动删除MH订阅
    _mh_assist_auto_delete: bool = False
    # 助手：待检查的mh订阅映射（mp_sub_id -> {mh_uuid, created_at, type}）
    _ASSIST_PENDING_KEY = "mhnotify_assist_pending"
    # 助手：等待MP完成后删除mh订阅的监听映射（mp_sub_id -> {mh_uuid}）
    _ASSIST_WATCH_KEY = "mhnotify_assist_watch"
    # HDHive 配置
    _hdhive_enabled: bool = False
    _hdhive_query_mode: str = "playwright"  # playwright/api
    _hdhive_username: str = ""
    _hdhive_password: str = ""
    _hdhive_cookie: str = ""
    _hdhive_auto_refresh: bool = False
    _hdhive_refresh_before: int = 3600
    # MH登录缓存
    _mh_token: Optional[str] = None
    _mh_token_expire_ts: int = 0
    _mh_token_ttl_seconds: int = 600  # 默认缓存10分钟
    # 助手调度延迟/重试常量（首次查询2分钟，之后每1分钟重试）
    _assist_initial_delay_seconds: int = 120
    _assist_retry_interval_seconds: int = 60
    # 115 生活事件监听
    _p115_life_enabled: bool = False
    _p115_cookie: str = ""
    _p115_events: List[str] = []  # 可选：upload/move/receive/create/copy/delete
    _p115_poll_cron: str = "* * * * *"  # 每分钟
    _P115_LAST_TS_KEY = "mhnotify_p115_life_last_ts"
    _P115_LAST_ID_KEY = "mhnotify_p115_life_last_id"
    _p115_watch_dirs: List[str] = []  # 仅当文件路径命中这些目录前缀时触发
    _p115_watch_rules: List[Dict[str, Any]] = []  # [{path: '/目录', events: ['upload', ...]}]
    _p115_wait_minutes: int = 5  # 生活事件静默窗口（分钟）
    _p115_next_notify_time: int = 0  # 生活事件下一次允许触发的时间戳
    _p115_dir_cache: Dict[int, str] = {}  # parent_id -> dir path 缓存
    _rule_count: int = 3  # 规则行数（表单动态显示）
    #（已废弃）是否检测 MP 整理运行
    _check_mp_transfer_enabled: bool = False
    # MP 整理/刮削事件触发开关
    _mp_event_enabled: bool = False
    # MP 事件等待时间（分钟）
    _mp_event_wait_minutes: int = 5
    # MP 事件监听的存储类型（多选）
    _mp_event_storages: List[str] = []
    # 可用存储列表缓存
    _available_storages: List[Dict[str, str]] = []
    # 云下载开关
    _cloud_download_enabled: bool = False
    # 云下载保存路径
    _cloud_download_path: str = "/云下载"
    # 云下载剔除小文件开关
    _cloud_download_remove_small_files: bool = False
    # 云下载移动整理开关
    _cloud_download_organize: bool = False

    def init_plugin(self, config: dict = None):
        if config:
            self._enabled = config.get("enabled")
            self._mh_domain = config.get("mh_domain")
            self._mh_username = config.get('mh_username')
            self._mh_password = config.get('mh_password')
            self._mh_job_names = config.get('mh_job_names') or ""
            # 移除 MP 整理延迟窗口配置（保留占位不生效）
            try:
                _ = int(config.get('wait_minutes') or 5)
            except Exception:
                pass
            # mh订阅辅助开关
            self._mh_assist_enabled = bool(config.get("mh_assist", False))
            # mh订阅辅助：MP订阅完成后自动删除MH订阅（默认关闭）
            self._mh_assist_auto_delete = bool(config.get("mh_assist_auto_delete", False))

            # HDHive 设置
            self._hdhive_enabled = bool(config.get("hdhive_enabled", False))
            self._hdhive_query_mode = config.get("hdhive_query_mode", "api") or "api"
            self._hdhive_username = config.get("hdhive_username", "") or ""
            self._hdhive_password = config.get("hdhive_password", "") or ""
            self._hdhive_cookie = config.get("hdhive_cookie", "") or ""
            self._hdhive_auto_refresh = bool(config.get("hdhive_auto_refresh", False))
            try:
                self._hdhive_refresh_before = int(config.get("hdhive_refresh_before", 3600) or 3600)
            except Exception:
                self._hdhive_refresh_before = 3600

            # 清理助手订阅记录（运行一次）
            try:
                if bool(config.get("clear_once", False)):
                    logger.info("mhnotify: 检测到清理助手订阅记录（运行一次）开关已开启，开始清理...")
                    self._clear_all_records()
                    # 复位为关闭，并更新配置
                    config["clear_once"] = False
                    self.update_config(config)
                    logger.info("mhnotify: 助手订阅记录清理完成，已自动复位为关闭")
            except Exception:
                logger.error("mhnotify: 执行清理助手订阅记录失败", exc_info=True)
            
            # 清理助手云下载记录（运行一次）
            try:
                if bool(config.get("clear_cloud_download_once", False)):
                    logger.info("mhnotify: 检测到清理助手云下载记录（运行一次）开关已开启，开始清理...")
                    self._clear_cloud_download_records()
                    # 复位为关闭，并更新配置
                    config["clear_cloud_download_once"] = False
                    self.update_config(config)
                    logger.info("mhnotify: 助手云下载记录清理完成，已自动复位为关闭")
            except Exception:
                logger.error("mhnotify: 执行清理助手云下载记录失败", exc_info=True)

            # 115 生活事件
            self._p115_life_enabled = bool(config.get("p115_life_enabled", False))
            self._p115_cookie = config.get("p115_cookie", "") or ""
            self._p115_events = config.get("p115_life_events", []) or []
            # 兼容字符串逗号分隔
            if isinstance(self._p115_events, str):
                self._p115_events = [x.strip() for x in self._p115_events.split(',') if x.strip()]
            # 轮询频率（保留为 cron，暂仅支持每分钟）
            self._p115_poll_cron = config.get("p115_life_cron", "* * * * *") or "* * * * *"
            # 目录前缀过滤（兼容旧配置）
            watch_dirs = config.get("p115_watch_dirs", []) or []
            if isinstance(watch_dirs, str):
                watch_dirs = [x.strip() for x in watch_dirs.split(',') if x.strip()]
            # 规范化为以 '/' 开头的 Posix 路径
            norm_dirs: List[str] = []
            for d in watch_dirs:
                d = d.replace('\\', '/').strip()
                if not d:
                    continue
                if not d.startswith('/'):
                    d = '/' + d
                # 去除尾随 '/'
                d = d.rstrip('/')
                norm_dirs.append(d)
            self._p115_watch_dirs = norm_dirs
            
            # 目录事件规则：优先从 rule_path_X / rule_events_X 字段解析（新表单格式）
            norm_rules: List[Dict[str, Any]] = []
            max_rules = 10
            
            # 从新格式解析：rule_path_0, rule_events_0, ...
            for i in range(max_rules):
                path_key = f'rule_path_{i}'
                events_key = f'rule_events_{i}'
                p = (config.get(path_key) or '').replace('\\', '/').strip()
                if not p:
                    continue
                if not p.startswith('/'):
                    p = '/' + p
                p = p.rstrip('/')
                evs = config.get(events_key) or []
                if isinstance(evs, str):
                    evs = [x.strip().lower() for x in evs.split(',') if x.strip()]
                elif isinstance(evs, list):
                    evs = [str(x).strip().lower() for x in evs if str(x).strip()]
                norm_rules.append({'path': p, 'events': evs})
            
            # 若新格式为空，尝试从旧的 JSON 列表解析（兼容旧配置）
            if not norm_rules:
                rules = config.get("p115_watch_rules", []) or []
                if isinstance(rules, list):
                    for r in rules:
                        try:
                            p = (r.get('path') or '').replace('\\', '/').strip()
                            if not p:
                                continue
                            if not p.startswith('/'):
                                p = '/' + p
                            p = p.rstrip('/')
                            evs = r.get('events') or []
                            if isinstance(evs, str):
                                evs = [x.strip().lower() for x in evs.split(',') if x.strip()]
                            elif isinstance(evs, list):
                                evs = [str(x).strip().lower() for x in evs if str(x).strip()]
                            norm_rules.append({'path': p, 'events': evs})
                        except Exception:
                            continue
            
            self._p115_watch_rules = norm_rules
            # 同步更新 p115_watch_rules 配置（供 API 使用）
            config['p115_watch_rules'] = norm_rules
            
            # 规则行数（用于表单动态显示）
            try:
                self._rule_count = int(config.get('rule_count', 3) or 3)
                if self._rule_count < 1:
                    self._rule_count = 1
                if self._rule_count > 10:
                    self._rule_count = 10
            except Exception:
                self._rule_count = 3
            
            try:
                self._p115_wait_minutes = int(config.get('p115_wait_minutes', 5) or 5)
            except Exception:
                self._p115_wait_minutes = 5
            # 移除 MP 整理检测开关（不再生效）
            self._check_mp_transfer_enabled = False
            
            # MP 整理/刮削事件触发开关
            self._mp_event_enabled = bool(config.get("mp_event_enabled", False))
            try:
                self._mp_event_wait_minutes = int(config.get('mp_event_wait_minutes', 5) or 5)
            except Exception:
                self._mp_event_wait_minutes = 5
            
            # MP 事件监听的存储类型
            self._mp_event_storages = config.get("mp_event_storages", []) or []
            if isinstance(self._mp_event_storages, str):
                self._mp_event_storages = [x.strip() for x in self._mp_event_storages.split(',') if x.strip()]
            
            # 初始化时获取可用存储列表
            self._available_storages = self.__get_available_storages()
            
            # 云下载配置
            self._cloud_download_enabled = bool(config.get("cloud_download_enabled", False))
            self._cloud_download_path = config.get("cloud_download_path", "/云下载") or "/云下载"
            self._cloud_download_remove_small_files = bool(config.get("cloud_download_remove_small_files", False))
            self._cloud_download_organize = bool(config.get("cloud_download_organize", False))

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
        # 115 生活事件监听
        if self._p115_life_enabled and (self._p115_cookie or "").strip():
            try:
                services.append({
                    "id": "P115LifeWatch",
                    "name": "115生活事件监听",
                    "trigger": CronTrigger.from_crontab(self._p115_poll_cron),
                    "func": self.__watch_115_life,
                    "kwargs": {}
                })
            except Exception:
                # 若 cron 非法，回退每分钟
                services.append({
                    "id": "P115LifeWatch",
                    "name": "115生活事件监听",
                    "trigger": CronTrigger.from_crontab("* * * * *"),
                    "func": self.__watch_115_life,
                    "kwargs": {}
                })
        return services

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """定义远程控制命令"""
        return [
            {
                "cmd": "/mhol",
                "event": EventType.PluginAction,
                "desc": "添加115云下载任务",
                "category": "下载",
                "data": {
                    "action": "mh_add_offline"
                }
            }
        ]

    def get_api(self) -> List[Dict[str, Any]]:
        # 提供 115 目录浏览 API，便于做目录选择器
        return [
            {
                "path": "/p115/list_directories",
                "endpoint": self.api_p115_list_directories,
                "methods": ["GET"],
                "summary": "列出115网盘指定路径下的目录"
            },
            {
                "path": "/p115/watch_rules",
                "endpoint": self.api_p115_watch_rules,
                "methods": ["GET"],
                "summary": "获取当前目录事件规则"
            },
            {
                "path": "/p115/add_watch_rule",
                "endpoint": self.api_p115_add_watch_rule,
                "methods": ["POST"],
                "summary": "添加目录事件规则（path, events）"
            },
            {
                "path": "/p115/remove_watch_rule",
                "endpoint": self.api_p115_remove_watch_rule,
                "methods": ["POST"],
                "summary": "移除目录事件规则（path）"
            }
        ]

    def api_p115_list_directories(self, path: str = "/", apikey: str = "") -> dict:
        try:
            if apikey != settings.API_TOKEN:
                return {"success": False, "error": "API密钥错误"}
            if not self._p115_cookie:
                return {"success": False, "error": "未配置 115 Cookie"}
            # 复用现有的 P115 客户端封装
            try:
                from app.plugins.p115strgmsub.clients.p115 import P115ClientManager  # type: ignore
            except Exception:
                P115ClientManager = None
            if not P115ClientManager:
                return {"success": False, "error": "缺少 P115 客户端依赖（p115strgmsub）"}
            mgr = P115ClientManager(cookies=self._p115_cookie)
            if not mgr.check_login():
                return {"success": False, "error": "115 登录失败，Cookie 可能已过期"}
            # 规范化路径
            path = (path or "/").replace("\\", "/")
            if not path.startswith("/"):
                path = "/" + path
            directories = mgr.list_directories(path)
            # 构建面包屑
            breadcrumbs = []
            if path and path != "/":
                parts = [p for p in path.split("/") if p]
                current_path = ""
                breadcrumbs.append({"name": "根目录", "path": "/"})
                for part in parts:
                    current_path = f"{current_path}/{part}"
                    breadcrumbs.append({"name": part, "path": current_path})
            else:
                breadcrumbs.append({"name": "根目录", "path": "/"})
            return {
                "success": True,
                "path": path,
                "breadcrumbs": breadcrumbs,
                "directories": directories
            }
        except Exception as e:
            logger.error(f"mhnotify: 列出115目录失败: {e}")
            return {"success": False, "error": str(e)}

    def _rules_to_text(self, rules: List[Dict[str, Any]]) -> str:
        """将规则列表转换为文本格式"""
        lines = []
        for rule in rules:
            path = rule.get('path', '')
            events = rule.get('events', [])
            if path:
                if events:
                    lines.append(f"{path}:{','.join(events)}")
                else:
                    lines.append(path)
        return '\n'.join(lines)

    def api_p115_watch_rules(self, apikey: str = "") -> dict:
        try:
            if apikey != settings.API_TOKEN:
                return {"success": False, "error": "API密钥错误"}
            return {"success": True, "rules": self._p115_watch_rules}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def api_p115_add_watch_rule(self, path: str = "/", events: Any = None, apikey: str = "") -> dict:
        try:
            if apikey != settings.API_TOKEN:
                return {"success": False, "error": "API密钥错误"}
            if not path or path == "":
                return {"success": False, "error": "缺少目录路径"}
            p = path.replace('\\', '/').strip()
            if not p.startswith('/'):
                p = '/' + p
            p = p.rstrip('/')
            evs: List[str] = []
            if events:
                if isinstance(events, str):
                    evs = [x.strip().lower() for x in events.split(',') if x.strip()]
                elif isinstance(events, list):
                    evs = [str(x).strip().lower() for x in events if str(x).strip()]
            # 更新内存与配置
            rules = [r for r in (self._p115_watch_rules or []) if r.get('path') != p]
            rules.append({'path': p, 'events': evs})
            self._p115_watch_rules = rules
            cfg = self.get_config()
            if isinstance(cfg, dict):
                cfg['p115_watch_rules'] = rules
                cfg['p115_watch_rules_text'] = self._rules_to_text(rules)
                self.update_config(cfg)
            return {"success": True, "rules": rules}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def api_p115_remove_watch_rule(self, path: str = "/", apikey: str = "") -> dict:
        try:
            if apikey != settings.API_TOKEN:
                return {"success": False, "error": "API密钥错误"}
            p = path.replace('\\', '/').strip()
            if not p.startswith('/'):
                p = '/' + p
            p = p.rstrip('/')
            rules = [r for r in (self._p115_watch_rules or []) if r.get('path') != p]
            self._p115_watch_rules = rules
            cfg = self.get_config()
            if isinstance(cfg, dict):
                cfg['p115_watch_rules'] = rules
                cfg['p115_watch_rules_text'] = self._rules_to_text(rules)
                self.update_config(cfg)
            return {"success": True, "rules": rules}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def _build_rule_row(self, index: int) -> dict:
        """构建单条目录规则的表单行"""
        return {
            'component': 'VRow',
            'props': {'class': 'align-center'},
            'content': [
                {
                    'component': 'VCol',
                    'props': {'cols': 12, 'md': 6},
                    'content': [
                        {
                            'component': 'VTextField',
                            'props': {
                                'model': f'rule_path_{index}',
                                'label': f'目录 {index + 1}',
                                'placeholder': '/我的接收/电影',
                                'density': 'compact',
                                'hide-details': True
                            }
                        }
                    ]
                },
                {
                    'component': 'VCol',
                    'props': {'cols': 12, 'md': 6},
                    'content': [
                        {
                            'component': 'VSelect',
                            'props': {
                                'model': f'rule_events_{index}',
                                'label': '监听事件',
                                'items': [
                                    {'title': '上传', 'value': 'upload'},
                                    {'title': '移动', 'value': 'move'},
                                    {'title': '接收', 'value': 'receive'},
                                    {'title': '新建', 'value': 'create'},
                                    {'title': '复制', 'value': 'copy'},
                                    {'title': '删除', 'value': 'delete'}
                                ],
                                'multiple': True,
                                'chips': True,
                                'closable-chips': True,
                                'clearable': True,
                                'density': 'compact',
                                'hide-details': True,
                                'hint': '留空监听全部事件'
                            }
                        }
                    ]
                }
            ]
        }

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """
        拼装插件配置页面，需要返回两块数据：1、页面配置；2、数据结构
        """
        # 如果存储列表为空，尝试获取一次
        if not self._available_storages:
            self._available_storages = self.__get_available_storages()
        
        # 预设最多10条规则
        max_rules = 10
        
        # 获取当前配置的规则行数（默认3行）
        current_rule_count = getattr(self, '_rule_count', 3)
        if current_rule_count < 1:
            current_rule_count = 1
        if current_rule_count > max_rules:
            current_rule_count = max_rules
        
        # 构建规则行（只显示 current_rule_count 行）
        rule_rows = []
        for i in range(current_rule_count):
            rule_rows.append(self._build_rule_row(i))
        
        # 构建默认值字典，包含现有规则
        defaults = {
            "enabled": False,
            "mh_username": "",
            "mh_password": "",
            "mh_job_names": "",
            "mh_domain": "",
            "wait_minutes": 5,
            "mh_assist": False,
            "mh_assist_auto_delete": False,
            "clear_once": False,
            "clear_cloud_download_once": False,
            "hdhive_enabled": False,
            "hdhive_query_mode": "api",
            "hdhive_username": "",
            "hdhive_password": "",
            "hdhive_cookie": "",
            "hdhive_auto_refresh": False,
            "hdhive_refresh_before": 3600,
            "p115_life_enabled": False,
            "p115_cookie": "",
            "p115_life_events": [],
            "p115_life_cron": "* * * * *",
            "p115_watch_dirs": [],
            "p115_watch_rules": [],
            "p115_wait_minutes": 5,
            "check_mp_transfer": False,
            "rule_count": current_rule_count,
            "mp_event_enabled": False,
            "mp_event_wait_minutes": 5,
            "mp_event_storages": [],
            "cloud_download_enabled": False,
            "cloud_download_path": "/云下载"
        }
        
        # 将现有规则填充到对应的 rule_path_X 和 rule_events_X
        for i in range(max_rules):
            defaults[f'rule_path_{i}'] = ""
            defaults[f'rule_events_{i}'] = []
        
        if self._p115_watch_rules:
            for i, rule in enumerate(self._p115_watch_rules[:max_rules]):
                defaults[f'rule_path_{i}'] = rule.get('path', '')
                defaults[f'rule_events_{i}'] = rule.get('events', [])

        return [
            {
                'component': 'VForm',
                'content': [
                    # 启用插件
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
                            },
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
                    # MP完成后删除MH订阅
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
                                            'model': 'mh_assist_auto_delete',
                                            'label': 'MP订阅完成后自动删除MH订阅',
                                            'hint': '开启后，当MP订阅完成或取消时，自动删除或更新对应的MH订阅。关闭则保留MH订阅'
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
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'mp_event_enabled',
                                            'label': 'MP事件触发（整理/刮削完成）',
                                            'hint': '开启后，当MP整理或刮削媒体完成时，自动通知MH执行strm生成任务（无运行任务则立即触发）'
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
                                            'model': 'mp_event_wait_minutes',
                                            'label': 'MP事件等待分钟数',
                                            'type': 'number',
                                            'placeholder': '默认 5',
                                            'hint': 'MP整理完成后，等待该分钟数以确保所有整理任务完成后再触发MH任务'
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
                                        'component': 'VSelect',
                                        'props': {
                                            'model': 'mp_event_storages',
                                            'label': '监听的存储类型',
                                            'items': self._available_storages or [
                                                {'title': '本地', 'value': 'local'},
                                                {'title': '115网盘', 'value': 'u115'},
                                                {'title': '阿里云盘', 'value': 'alipan'},
                                                {'title': 'RClone', 'value': 'rclone'},
                                                {'title': 'OpenList', 'value': 'alist'}
                                            ],
                                            'multiple': True,
                                            'chips': True,
                                            'closable-chips': True,
                                            'clearable': True,
                                            'density': 'compact',
                                            'hint': '留空则监听所有存储类型的整理/刮削事件'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    # 云下载配置
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
                                            'model': 'cloud_download_enabled',
                                            'label': '启用115云下载功能',
                                            'hint': '开启后，可使用 /mhol 命令添加115离线下载任务'
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
                                            'model': 'cloud_download_path',
                                            'label': '115云下载保存路径',
                                            'placeholder': '/云下载',
                                            'hint': '115网盘中保存离线下载文件的目录路径'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'cloud_download_remove_small_files',
                                            'label': '剔除小文件',
                                            'hint': '云下载完成后自动删除小于10MB的文件',
                                            'persistent-hint': True
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'cloud_download_organize',
                                            'label': '移动整理',
                                            'hint': '云下载完成后自动移动到MH默认目录并整理',
                                            'persistent-hint': True
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    # 115 Cookie
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
                                            'model': 'p115_cookie',
                                            'label': '115 Cookie',
                                            'type': 'password',
                                            'placeholder': 'UID=...; CID=...; SEID=...（粘贴完整 Cookie）',
                                            'hint': '从 115 网页版复制完整 Cookie；仅本地使用，不会对外发送'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    # 分隔线
                    {
                        'component': 'VRow',
                        'props': {'class': 'mt-4'},
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12},
                                'content': [
                                    {
                                        'component': 'VDivider'
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
                                            'text': '可选：监听 115 生活事件（上传/移动/接收/新建/复制/删除）以触发 MH 的 strm 任务。'
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
                                    'md': 3
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'p115_life_enabled',
                                            'label': '监听 115 生活事件'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 9
                                },
                                'content': [
                                    {
                                        'component': 'VAlert',
                                        'props': {
                                            'type': 'warning',
                                            'variant': 'tonal',
                                            'density': 'compact',
                                            'text': '下方可配置最多10条目录规则，每条规则包含目录路径和要监听的事件类型。事件留空表示监听该目录的所有事件。'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    # 目录规则标题
                    {
                        'component': 'VRow',
                        'props': {'class': 'mt-4'},
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12},
                                'content': [
                                    {
                                        'component': 'VDivider'
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
                                'props': {'cols': 12, 'md': 6},
                                'content': [
                                    {
                                        'component': 'span',
                                        'props': {'class': 'text-subtitle-1 font-weight-bold'},
                                        'text': '📁 目录监听规则'
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 6},
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'rule_count',
                                            'label': '规则行数',
                                            'type': 'number',
                                            'min': 1,
                                            'max': 10,
                                            'density': 'compact',
                                            'hint': '修改后保存即可增减规则行（1-10）'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    # 规则行
                    *rule_rows,
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
                                            'model': 'p115_wait_minutes',
                                            'label': '115 事件等待分钟数',
                                            'type': 'number',
                                            'placeholder': '默认 5',
                                            'hint': '检测到 115 生活事件后，等待该分钟数；等待期间如有新生活事件将滚动延长，静默后才触发生成任务'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            
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
                                            'text': 'HDHive资源查询：支持 Playwright/API 两种模式，获取免费 115 分享链接并自动作为自定义链接随订阅传入'
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
                                    'md': 3
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'hdhive_enabled',
                                            'label': '启用 HDHive'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 3
                                },
                                'content': [
                                    {
                                        'component': 'VSelect',
                                        'props': {
                                            'model': 'hdhive_query_mode',
                                            'label': 'HDHive 查询模式',
                                            'items': [
                                                { 'title': 'Playwright', 'value': 'playwright' },
                                                { 'title': 'API', 'value': 'api' }
                                            ],
                                            'clearable': False
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 3
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'hdhive_username',
                                            'label': 'HDHive 用户名'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 3
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'hdhive_password',
                                            'label': 'HDHive 密码',
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
                                    'md': 6
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'hdhive_cookie',
                                            'label': 'HDHive Cookie（API 模式）',
                                            'type': 'password'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 3
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'hdhive_auto_refresh',
                                            'label': '自动刷新 Cookie'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 3
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'hdhive_refresh_before',
                                            'label': 'Cookie提前刷新秒数',
                                            'type': 'number',
                                            'placeholder': '默认 3600'
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
                                    'cols': 12
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'clear_once',
                                            'label': '清理助手订阅记录（运行一次）',
                                            'hint': '⚠️ 开启后点保存立即清除本助手里的MH订阅监听记录（pending/watch），清理后将无法再监听之前添加的MH订阅记录。用于移除脏数据或重置助手状态，操作后自动复位为关闭'
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
                                    'cols': 12
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'clear_cloud_download_once',
                                            'label': '清理助手云下载记录（运行一次）',
                                            'hint': '⚠️ 开启后点保存立即清除本助手里的云下载监控记录，清理后将无法再监听之前添加的云下载任务记录。当前版本云下载使用实时线程监控（预留接口），操作后自动复位为关闭'
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
                                        'component': 'VAlert',
                                        'props': {
                                            'type': 'info',
                                            'variant': 'tonal',
                                            'text': '当检测到匹配的 115 生活事件后，将在静默期结束时触发 MediaHelper 的 strm 任务'
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
                                            'text': '为避免频繁触发：启用生活事件静默窗口（默认5分钟）；窗口期间如有新事件将滚动延长，静默结束后再触发'
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ], defaults

    def get_page(self) -> List[dict]:
        pass

    @eventmanager.register(EventType.TransferComplete)
    @eventmanager.register(EventType.DownloadAdded)
    def send(self, event):
        """
        监听 MP 整理完成和刮削完成事件，触发 MH 生成 strm 任务
        需要在配置中开启 'MP事件触发' 开关
        支持按存储类型过滤
        """
        if not self._enabled or not self._mp_event_enabled:
            return
        
        if not event or not event.event_type:
            return
        
        # 辅助函数：将事件对象递归转换为字典
        def __to_dict(_event):
            if _event is None:
                return None
            elif isinstance(_event, dict):
                return {k: __to_dict(v) for k, v in _event.items()}
            elif isinstance(_event, list):
                return [__to_dict(item) for item in _event]
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
        
        # 获取事件类型
        version = getattr(settings, "VERSION_FLAG", "v1")
        event_type = event.event_type if version == "v1" else event.event_type.value
        
        # 只处理整理完成和刮削完成事件
        if event_type not in ["transfer.complete", "metadata.scrape", EventType.TransferComplete, EventType.DownloadAdded]:
            return
        
        # 解析事件数据
        event_data = __to_dict(event.event_data)
        storage = None
        name = None
        
        try:
            # 整理完成事件
            if event_type in ["transfer.complete", EventType.TransferComplete]:
                transferinfo = event_data.get("transferinfo", {})
                success = transferinfo.get("success", False)
                if not success:
                    return
                
                target_diritem = transferinfo.get("target_diritem", {})
                target_item = transferinfo.get("target_item", {})
                storage = target_diritem.get("storage")
                name = target_item.get("name")
            
            # 刮削完成事件
            elif event_type in ["metadata.scrape", EventType.DownloadAdded]:
                fileitem = event_data.get("fileitem", {})
                storage = fileitem.get("storage") if isinstance(fileitem, dict) else None
                name = event_data.get("name")
        
        except Exception as e:
            logger.error(f"mhnotify: 解析事件数据失败: {e}")
            return
        
        # 检查存储类型过滤
        if self._mp_event_storages:
            if not storage or storage not in self._mp_event_storages:
                logger.debug(f"mhnotify: 存储类型 [{storage}] 不在监听列表中，忽略事件")
                return
        
        logger.info(f"mhnotify: 收到 MP 事件 [{event_type}]，存储: [{storage}]，文件: [{name}]")
        
        # 增加待通知计数
        self._wait_notify_count += 1
        self._last_event_time = self.__get_time()
        
        # 检查是否有正在运行的整理任务
        if self.__has_running_transfers():
            logger.info("mhnotify: 检测到正在运行的整理任务，延迟触发")
            # 设置等待窗口
            now_ts = self.__get_time()
            wait_seconds = self._mp_event_wait_minutes * 60
            self._next_notify_time = now_ts + wait_seconds
        else:
            logger.info("mhnotify: 无运行中的整理任务，将在下次调度时立即触发")
            # 清零等待时间，下次调度立即触发
            self._next_notify_time = 0

    def __get_time(self):
        return int(time.time())
    
    def __get_available_storages(self) -> List[Dict[str, str]]:
        """
        从MP系统获取可用的存储列表
        """
        try:
            from app.helper.storage import StorageHelper
            from app.db.systemconfig_oper import SystemConfigOper
            from app.schemas.types import SystemConfigKey
            
            # 直接从数据库读取存储配置
            storage_confs = SystemConfigOper().get(SystemConfigKey.Storages)
            if storage_confs:
                storage_list = []
                for storage in storage_confs:
                    storage_type = storage.get("type", "")
                    storage_name = storage.get("name", storage_type)
                    if storage_type:
                        storage_list.append({
                            "title": storage_name,
                            "value": storage_type
                        })
                logger.info(f"mhnotify: 成功获取存储列表，共 {len(storage_list)} 个")
                return storage_list
            logger.debug("mhnotify: 未配置存储，使用默认列表")
        except Exception as e:
            logger.error(f"mhnotify: 获取存储列表异常: {e}")
        
        # 返回默认存储列表
        return [
            {"title": "本地", "value": "local"},
            {"title": "115网盘", "value": "u115"},
            {"title": "阿里云盘", "value": "alipan"},
            {"title": "RClone", "value": "rclone"},
            {"title": "OpenList", "value": "alist"}
        ]

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
                # 若启用 115 生活事件监听，则先检查生活事件静默窗口
                if self._p115_life_enabled and self._p115_next_notify_time:
                    if now_ts < self._p115_next_notify_time:
                        logger.info(f"115 生活事件静默窗口未到期（{self._p115_next_notify_time - now_ts}s），暂不触发通知")
                        return
                    else:
                        # 到期后清零窗口
                        self._p115_next_notify_time = 0
                
                # 若启用 MP 事件触发，检查 MP 事件等待窗口
                if self._mp_event_enabled and self._next_notify_time:
                    if now_ts < self._next_notify_time:
                        # 如果仍有运行中的整理任务，延长等待时间
                        if self.__has_running_transfers():
                            wait_seconds = self._mp_event_wait_minutes * 60
                            self._next_notify_time = now_ts + wait_seconds
                            logger.info(f"MP整理任务仍在运行，延长等待窗口 {self._mp_event_wait_minutes} 分钟")
                        else:
                            logger.info(f"MP事件等待窗口未到期（{self._next_notify_time - now_ts}s），暂不触发通知")
                        return
                    else:
                        # 到期后清零窗口
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
                try:
                    list_data = list_res.json() or {}
                    tasks = list_data.get("data", [])
                except Exception:
                    tasks = []
                # 过滤 cloud_strm_sync 任务
                strm_tasks = [t for t in tasks if t.get('task') == 'cloud_strm_sync' and t.get('enabled')]
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

    def __watch_115_life(self):
        """监听 115 生活事件，满足筛选时触发待通知计数"""
        try:
            if not self._p115_life_enabled:
                return
            cookie = (self._p115_cookie or "").strip()
            if not cookie:
                return
            # 读取上次指针
            last_ts = int(self.get_data(self._P115_LAST_TS_KEY) or 0)
            last_id_raw = self.get_data(self._P115_LAST_ID_KEY)
            try:
                last_id = int(last_id_raw) if last_id_raw is not None else 0
            except Exception:
                last_id = 0
            
            # 首次启用时，从当前时间开始监听，避免拉取所有历史事件
            if last_ts == 0:
                current_ts = int(time.time())
                logger.info(f"mhnotify: 115生活事件首次启用，从当前时间开始监听 (ts={current_ts})")
                self.save_data(self._P115_LAST_TS_KEY, current_ts)
                self.save_data(self._P115_LAST_ID_KEY, 0)
                return

            # 优先使用 p115client 的 life API（与 p115strmhelper 保持一致）
            try:
                from p115client import P115Client  # type: ignore
                from p115client.tool.life import iter_life_behavior_once, life_show  # type: ignore
                client = P115Client(cookie, app="web")
                # 确认生活事件已开启
                try:
                    resp = life_show(client)
                    if not (isinstance(resp, dict) and resp.get("state")):
                        logger.warning("mhnotify: 115 生活事件未开启或获取失败，跳过本轮")
                        return
                except Exception:
                    # life_show 失败不致命，继续尝试拉取
                    pass

                # 拉取一次（从上次指针开始）
                events_iter = iter_life_behavior_once(
                    client=client,
                    from_time=last_ts,
                    from_id=last_id,
                    app="web",
                    cooldown=1,
                )
                # 收集到内存（限制一定数量避免过大）
                events: List[Dict[str, Any]] = []
                max_collect = 200
                for idx, ev in enumerate(events_iter):
                    if idx >= max_collect:
                        break
                    events.append(ev)

                if not events:
                    return

                # 将事件类型映射到简化类别，供 UI 选择匹配
                def map_type_to_simple(t: int) -> str:
                    """
                    115生活事件类型映射（参考 p115strmhelper）
                    已知类型：
                    - type 1,2 → upload (上传)
                    - type 5,6 → move (移动)
                    - type 14 → receive (接收)
                    - type 17 → create (新建)
                    - type 18 → copy (复制)
                    - type 22 → delete (删除)
                    如遇未映射类型，将在日志中记录警告
                    """
                    if t in (1, 2):
                        return "upload"
                    if t in (5, 6):
                        return "move"
                    if t == 14:
                        return "receive"
                    if t == 17:
                        return "create"
                    if t == 18:
                        return "copy"
                    if t == 22:
                        return "delete"
                    return ""

                selected = set([x.lower() for x in (self._p115_events or [])])
                def _match_rules(full_path: str, ev_simple: str) -> bool:
                    rules = self._p115_watch_rules or []
                    if not rules:
                        return False
                    try:
                        for r in rules:
                            rp = (r.get('path') or '').strip()
                            evs = [str(x).strip().lower() for x in (r.get('events') or [])]
                            if not rp:
                                continue
                            if full_path.startswith(rp + '/') or full_path == rp:
                                if not evs:
                                    return True
                                return bool(ev_simple) and (ev_simple in evs)
                        return False
                    except Exception:
                        return False
                has_new = False
                new_last_ts = last_ts
                new_last_id = last_id
                triggered_events = []  # 收集触发的事件信息
                
                # 只处理最近10分钟内的事件
                current_time = int(time.time())
                time_window = 10 * 60  # 10分钟
                cutoff_time = current_time - time_window
                
                # p115strmhelper 在 once_pull 中最终以最新事件更新指针；这里按时间/ID取最大
                for it in events:
                    try:
                        t = int(it.get("type", 0))
                        ut = int(it.get("update_time", 0))
                        eid = int(it.get("id", 0))
                        pid = int(it.get("parent_id", 0))
                        fname = str(it.get("file_name", "") or "")
                    except Exception:
                        continue
                    # 跳过旧事件
                    if ut < last_ts or (ut == last_ts and eid <= last_id):
                        continue
                    
                    # 跳过超过10分钟的旧事件
                    if ut < cutoff_time:
                        logger.debug(f"mhnotify: 跳过10分钟前的旧事件: {fname}, 时间: {ut}")
                        # 更新指针但不触发
                        if ut > new_last_ts or (ut == new_last_ts and eid > new_last_id):
                            new_last_ts = ut
                            new_last_id = eid
                        continue
                    
                    # 输出原始事件数据用于调试（仅记录新事件）
                    logger.debug(f"mhnotify: 115生活事件原始数据 type={t}, id={eid}, file={fname}, parent_id={pid}, update_time={ut}, 完整数据={it}")
                    
                    simple = map_type_to_simple(t)
                    # 如果事件类型未能映射，记录警告
                    if not simple:
                        logger.warning(f"mhnotify: 115生活事件未映射类型 type={t}, file={fname}, 原始数据={it}")
                    
                    # 类型匹配
                    type_ok = (not selected) or (simple and simple in selected)
                    dir_ok = True
                    full_path = ""
                    # 目录事件规则优先（若配置了）
                    if type_ok and (self._p115_watch_rules or self._p115_watch_dirs):
                        try:
                            full_dir = self._p115_dir_cache.get(pid)
                            if not full_dir:
                                from p115client.tool.attr import get_path  # type: ignore
                                full_dir = get_path(client=client, attr=pid, root_id=None) or ''
                                if full_dir.startswith('根目录'):
                                    full_dir = full_dir[3:]
                                full_dir = full_dir.replace('\\', '/').strip()
                                if not full_dir.startswith('/'):
                                    full_dir = '/' + full_dir
                                full_dir = full_dir.rstrip('/')
                                self._p115_dir_cache[pid] = full_dir
                            full_path = (full_dir + '/' + fname).replace('\\', '/')
                            if self._p115_watch_rules:
                                dir_ok = _match_rules(full_path=full_path, ev_simple=simple)
                            elif self._p115_watch_dirs:
                                dir_ok = any(full_path.startswith(d + '/') or full_path == d for d in self._p115_watch_dirs)
                        except Exception:
                            dir_ok = False
                    if type_ok and dir_ok:
                        has_new = True
                        # 记录触发的事件详情
                        event_name_map = {
                            "upload": "上传",
                            "move": "移动",
                            "receive": "接收",
                            "create": "新建",
                            "copy": "复制",
                            "delete": "删除"
                        }
                        event_name = event_name_map.get(simple, simple or f"type_{t}")
                        triggered_events.append({"path": full_path or fname, "event": event_name, "type": t, "time": ut})
                    if ut > new_last_ts or (ut == new_last_ts and eid > new_last_id):
                        new_last_ts = ut
                        new_last_id = eid

                if has_new:
                    self._wait_notify_count += 1
                    self._last_event_time = int(time.time())
                    # 输出详细的触发信息（包含事件发生时间）
                    from datetime import datetime
                    for evt in triggered_events:
                        evt_time = datetime.fromtimestamp(evt.get('time', 0)).strftime('%Y-%m-%d %H:%M:%S') if evt.get('time') else '未知'
                        logger.info(f"mhnotify: 115生活事件触发 - 目录: {evt['path']} | 事件: {evt['event']} | 发生时间: {evt_time}")
                    logger.info(f"mhnotify: 115生活事件触发（p115client.life），共 {len(triggered_events)} 个事件，计入一次strm触发信号")
                    # 设置/延长生活事件静默窗口
                    try:
                        delay_seconds = max(int(self._p115_wait_minutes) * 60, 0)
                    except Exception:
                        delay_seconds = 300
                    self._p115_next_notify_time = int(time.time()) + delay_seconds

                # 保存指针
                if new_last_ts:
                    self.save_data(self._P115_LAST_TS_KEY, int(new_last_ts))
                if new_last_id:
                    self.save_data(self._P115_LAST_ID_KEY, int(new_last_id))
                return
            except Exception:
                # 若 p115client 不可用或异常，退回到简易 HTTP 方案
                pass

            # 回退：HTTP 方案（兼容性较差，仅作为兜底）
            headers = {
                "Accept": "application/json, text/plain, */*",
                "Cookie": cookie,
                "User-Agent": "MoviePilot/Plugin MHNotify",
                "Referer": "https://115.com/"
            }
            candidate_urls = [
                "https://webapi.115.com/life/events?limit=50",
                "https://webapi.115.com/files/new?aid=1&cid=0&show_dir=1&offset=0&limit=50",
            ]
            hit_url = None
            items: List[Dict[str, Any]] = []
            for url in candidate_urls:
                try:
                    res = RequestUtils(headers=headers, timeout=20).get_res(url)
                    if not res or res.status_code != 200:
                        continue
                    data = res.json()
                    if "events" in data:
                        items = data.get("events") or []
                    elif "data" in data and isinstance(data.get("data"), dict) and ("list" in data["data"]):
                        items = data.get("data", {}).get("list", [])
                    elif "list" in data:
                        items = data.get("list") or []
                    else:
                        items = []
                    hit_url = url
                    if items:
                        break
                except Exception:
                    continue
            if not items:
                return

            def normalize_event_name(item: Dict[str, Any]) -> str:
                name = (item.get("action") or item.get("event") or item.get("type") or "").lower()
                text = (item.get("action_text") or item.get("event_text") or item.get("name") or "").lower()
                m = {
                    "上传": "upload", "upload": "upload",
                    "移动": "move", "move": "move",
                    "接收": "receive", "receive": "receive",
                    "新建": "create", "创建": "create", "create": "create",
                    "复制": "copy", "copy": "copy",
                    "删除": "delete", "移到回收站": "delete", "delete": "delete",
                }
                for k, v in m.items():
                    if k in name or k in text:
                        return v
                return name or text or ""

            def extract_ts(item: Dict[str, Any]) -> int:
                for key in ("update_time", "utime", "time", "ctime", "created_time"):
                    val = item.get(key)
                    if isinstance(val, (int, float)):
                        return int(val)
                    if isinstance(val, str) and val.isdigit():
                        return int(val)
                return 0

            def extract_id(item: Dict[str, Any]) -> int:
                for key in ("id", "eid", "event_id"):
                    val = item.get(key)
                    if val is not None and str(val).isdigit():
                        return int(val)
                return 0

            selected = set([x.lower() for x in (self._p115_events or [])])
            def _match_rules(full_path: str, ev_simple: str) -> bool:
                rules = self._p115_watch_rules or []
                if not rules:
                    return False
                try:
                    for r in rules:
                        rp = (r.get('path') or '').strip()
                        evs = [str(x).strip().lower() for x in (r.get('events') or [])]
                        if not rp:
                            continue
                        if full_path.startswith(rp + '/') or full_path == rp:
                            if not evs:
                                return True
                            return bool(ev_simple) and (ev_simple in evs)
                    return False
                except Exception:
                    return False
            has_new = False
            new_last_ts = last_ts
            new_last_id = last_id
            triggered_events = []  # 收集触发的事件信息
            for it in items:
                # 输出原始事件数据用于调试
                logger.debug(f"mhnotify: 115生活事件HTTP原始数据={it}")
                
                ev = normalize_event_name(it)
                ts = extract_ts(it)
                eid = extract_id(it)
                if ts < last_ts or (ts == last_ts and eid <= last_id):
                    continue
                
                # 如果事件类型未能识别，记录警告
                if not ev:
                    logger.warning(f"mhnotify: 115生活事件HTTP未识别类型，原始数据={it}")
                
                type_ok = (not selected) or (ev and ev in selected)
                dir_ok = True
                full_path = ""
                # 目录事件规则优先（HTTP 兜底下尽力获取路径，可能不完整）
                if type_ok and (self._p115_watch_rules or self._p115_watch_dirs):
                    try:
                        pid = int(it.get('parent_id') or 0)
                        fname = str(it.get('file_name') or it.get('name') or '')
                        full_dir = self._p115_dir_cache.get(pid)
                        if not full_dir:
                            full_dir = ''
                        full_path = (full_dir + '/' + fname).replace('\\', '/')
                        if self._p115_watch_rules:
                            dir_ok = _match_rules(full_path=full_path, ev_simple=ev)
                        elif self._p115_watch_dirs:
                            dir_ok = any(full_path.startswith(d + '/') or full_path == d for d in self._p115_watch_dirs)
                    except Exception:
                        dir_ok = False
                if type_ok and dir_ok:
                    has_new = True
                    # 记录触发的事件详情
                    event_name_map = {
                        "upload": "上传",
                        "move": "移动",
                        "receive": "接收",
                        "create": "新建",
                        "copy": "复制",
                        "delete": "删除"
                    }
                    event_name = event_name_map.get(ev, ev or "未知")
                    fname = str(it.get('file_name') or it.get('name') or '')
                    triggered_events.append({"path": full_path or fname, "event": event_name})
                if ts > new_last_ts or (ts == new_last_ts and eid > new_last_id):
                    new_last_ts = ts
                    new_last_id = eid

            if has_new:
                self._wait_notify_count += 1
                self._last_event_time = int(time.time())
                # 输出详细的触发信息
                for evt in triggered_events:
                    logger.info(f"mhnotify: 115生活事件触发 - 目录: {evt['path']} | 事件: {evt['event']}")
                logger.info(f"mhnotify: 115生活事件触发（{hit_url}），共 {len(triggered_events)} 个事件，计入一次strm触发信号")
                try:
                    delay_seconds = max(int(self._p115_wait_minutes) * 60, 0)
                except Exception:
                    delay_seconds = 300
                self._p115_next_notify_time = int(time.time()) + delay_seconds
            if new_last_ts:
                self.save_data(self._P115_LAST_TS_KEY, int(new_last_ts))
            if new_last_id:
                self.save_data(self._P115_LAST_ID_KEY, int(new_last_id))
        except Exception:
            logger.warning("mhnotify: 监听115生活事件异常", exc_info=True)

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
                logger.info(f"mhnotify: SubscribeAdded 事件: sub_id={event_data.get('subscribe_id')} tmdb_id={mid} title={mtitle} event.season={mseason}")
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
                SubscribeOper(db=db).update(sub_id, {"state": "S", "sites": [-1]})
                # 重新获取，确保季号等字段已正确加载
                subscribe = SubscribeOper(db=db).get(sub_id)
                try:
                    logger.info(f"mhnotify: 订阅暂停完成 id={sub_id} type={getattr(subscribe,'type',None)} season={getattr(subscribe,'season',None)}")
                except Exception:
                    pass
            # 登录 MH 拿 token
            access_token = self.__mh_login()
            if not access_token:
                logger.error("mhnotify: 登录MediaHelper失败，无法创建订阅")
                return
            # 读取默认配置
            defaults = self.__mh_get_defaults(access_token)
            # 若为剧集，聚合同一 TMDB 的多季订阅（电影不需要聚合季）
            aggregate_seasons: Optional[List[int]] = None
            # 判断媒体类型
            sub_type = (getattr(subscribe, 'type', '') or '').strip().lower()
            is_tv = sub_type in ('tv', '电视剧')
            aggregate_seasons = []  # 初始化，电影时为空
            
            # 只有电视剧才进行聚合季逻辑
            if is_tv:
                try:
                    # 取 tmdb_id
                    tmdb_id = getattr(subscribe, 'tmdbid', None) or mediainfo_dict.get('tmdb_id') or mediainfo_dict.get('tmdbid')
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
                                            s_season = self.__extract_season_from_text(getattr(s, 'name', '') or '')
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
            create_payload = self.__build_mh_create_payload(subscribe, mediainfo_dict, defaults, aggregate_seasons=aggregate_seasons)
            if not create_payload:
                logger.error("mhnotify: 构建MH订阅创建参数失败")
                return
            # 若已存在相同 tmdb_id 的 MH 订阅，则复用或重建（以聚合季为准）
            existing_uuid: Optional[str] = None
            existing_selected: List[int] = []
            existing_custom_links: List[str] = []  # 保留现有订阅的自定义链接
            try:
                lst = self.__mh_list_subscriptions(access_token)
                subs = (lst.get("data") or {}).get("subscriptions") or []
                for rec in subs:
                    params = rec.get("params") or {}
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
                        upd = self.__mh_update_subscription(access_token, existing_uuid, create_payload)
                        if upd:
                            logger.info(f"mhnotify: 已更新现有订阅 {existing_uuid} 为聚合季 {sorted(agg_set)}")
                        else:
                            logger.info(f"mhnotify: 更新失败，改为重建订阅为聚合季 {sorted(agg_set)}")
                            self.__mh_delete_subscription(access_token, existing_uuid)
                            existing_uuid = None
                    else:
                        # 完全一致：直接复用
                        logger.info(f"mhnotify: 发现现有MH订阅 {existing_uuid}，季集合一致，复用该订阅")
            except Exception:
                logger.warning("mhnotify: 检查现有MH订阅失败", exc_info=True)
            # HDHive 查询自定义链接
            links: List[str] = []
            try:
                links = self.__fetch_hdhive_links(
                    tmdb_id=create_payload.get("tmdb_id"),
                    media_type=create_payload.get("media_type")
                )
                if links:
                    logger.info(f"mhnotify: HDHive 获取到 {len(links)} 个免费115链接")
            except Exception:
                logger.error("mhnotify: HDHive 查询链接失败", exc_info=True)
            
            # 合并现有自定义链接与新查询的 HDHive 链接（去重）
            merged_links: List[str] = list(existing_custom_links)  # 保留现有链接
            if links:
                # 提取链接的核心标识用于去重（去除协议前缀和尾部参数差异）
                def extract_link_key(link: str) -> str:
                    """提取链接的核心部分用于去重比较"""
                    # 移除协议前缀
                    key = link.replace("https://", "").replace("http://", "")
                    # 移除尾部的 & 或 空格
                    key = key.rstrip("& ")
                    return key.lower()
                
                existing_keys = set(extract_link_key(l) for l in existing_custom_links)
                new_count = 0
                for link in links:
                    link_key = extract_link_key(link)
                    if link_key not in existing_keys:
                        merged_links.append(link)
                        existing_keys.add(link_key)
                        new_count += 1
                if new_count > 0:
                    logger.info(f"mhnotify: 合并后共 {len(merged_links)} 个自定义链接（新增 {new_count} 个）")
                else:
                    logger.info(f"mhnotify: HDHive 链接已存在于现有自定义链接中，无需添加")
            
            # 设置 create_payload 的自定义链接（用于新建订阅）
            if merged_links:
                create_payload["user_custom_links"] = merged_links
            
            # 创建订阅（或复用现有）
            mh_uuid = None
            if existing_uuid:
                mh_uuid = existing_uuid
                # 如果有新的 HDHive 链接需要添加，更新现有订阅
                if links and len(merged_links) > len(existing_custom_links):
                    try:
                        update_payload = {"user_custom_links": merged_links}
                        upd_resp = self.__mh_update_subscription(access_token, existing_uuid, update_payload)
                        if upd_resp:
                            logger.info(f"mhnotify: 已将自定义链接更新到现有订阅 {existing_uuid}（共 {len(merged_links)} 个）")
                        else:
                            logger.warning(f"mhnotify: 更新现有订阅的自定义链接失败")
                    except Exception as e:
                        logger.warning(f"mhnotify: 更新现有订阅的自定义链接异常: {e}")
            else:
                resp = self.__mh_create_subscription(access_token, create_payload)
                mh_uuid = (resp or {}).get("data", {}).get("subscription_id") or (resp or {}).get("data", {}).get("task", {}).get("uuid")
            if not mh_uuid:
                logger.error(f"mhnotify: MH订阅创建失败：{resp}")
                return
            # 与调度保持一致：首次查询延迟（默认2分钟）
            delay_mins = max(1, int(self._assist_initial_delay_seconds / 60))
            if existing_uuid:
                logger.info(f"mhnotify: 复用现有MH订阅，uuid={mh_uuid}；{delay_mins}分钟后查询进度")
                # 复用现有订阅时，触发立即执行查询（MH不会自动触发）
                try:
                    access_token = self.__mh_login()
                    if access_token and self.__mh_execute_subscription(access_token, mh_uuid):
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

    def __build_mh_create_payload(self, subscribe, mediainfo_dict: Dict[str, Any], defaults: Dict[str, Any], aggregate_seasons: Optional[List[int]] = None) -> Optional[Dict[str, Any]]:
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
                        season_num = self.__extract_season_from_text(title or '')
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

    def __extract_season_from_text(self, text: str) -> Optional[int]:
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
                return self.__parse_chinese_numeral(num)
            m = re.search(r"([一二三四五六七八九十百零〇两\d]{1,3})季", t)
            if m:
                num = m.group(1)
                return self.__parse_chinese_numeral(num)
            # 其它：第X期/部 有时也指季（尽量解析但不强制使用）
            m = re.search(r"第([一二三四五六七八九十百零〇两\d]{1,3})(?:期|部)", t)
            if m:
                num = m.group(1)
                val = self.__parse_chinese_numeral(num)
                return val if val and val > 0 else None
        except Exception:
            pass
        return None

    def __parse_chinese_numeral(self, s: str) -> Optional[int]:
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

    def __mh_create_subscription(self, access_token: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        try:
            url = f"{self._mh_domain}/api/v1/subscription/create"
            headers = self.__auth_headers(access_token)
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
                            lst = self.__mh_list_subscriptions(access_token)
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

    def __mh_update_subscription(self, access_token: str, uuid: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """更新MH订阅（修改季集合等参数）
        兼容示例：PUT /api/v1/subscription/{uuid}，body 包含 name/cron/params
        params 中包含 selected_seasons 与 episode_ranges 以及其他字段
        """
        try:
            url = f"{self._mh_domain}/api/v1/subscription/{uuid}"
            headers = self.__auth_headers(access_token)
            headers.update({"Content-Type": "application/json;charset=UTF-8", "Origin": self._mh_domain})
            # 组装更新体：尽量复用创建参数作为 params，确保字段完整
            update_body = {
                "name": f"[订阅] {payload.get('title')}",
                "cron": payload.get("cron") or "0 */6 * * *",
                "params": payload
            }
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

    def __mh_execute_subscription(self, access_token: str, uuid: str) -> bool:
        """触发MH订阅立即执行查询
        POST /api/v1/subscription/{uuid}/execute
        """
        try:
            url = f"{self._mh_domain}/api/v1/subscription/{uuid}/execute"
            headers = self.__auth_headers(access_token)
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
                # 收集已到查询时间的条目（首次查询延迟）
                matured_items = {sid: info for sid, info in pending.items() if now_ts - int(info.get("created_at") or 0) >= self._assist_initial_delay_seconds}
                if matured_items:
                    token = self.__mh_login()
                    if not token:
                        logger.error("mhnotify: 登录MH失败，无法查询订阅进度")
                    else:
                        lst = self.__mh_list_subscriptions(token)
                        subs = (lst.get("data") or {}).get("subscriptions") or []
                        subs_map = {}
                        for rec in subs:
                            uid = rec.get("uuid") or rec.get("task", {}).get("uuid")
                            if uid:
                                subs_map[uid] = rec
                        for sid, info in list(matured_items.items()):
                            mh_uuid = info.get("mh_uuid")
                            target = subs_map.get(mh_uuid)
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
                                    retry_mins = max(1, int(self._assist_retry_interval_seconds / 60))
                                    logger.warning(f"mhnotify: 未在MH列表中找到订阅 {mh_uuid}，第{attempts}次重试，{retry_mins}分钟后继续")
                                    pending[str(sid)] = info
                                    self.save_data(self._ASSIST_PENDING_KEY, pending)
                                    continue
                            mtype, saved, expected = self.__compute_progress(target)
                            logger.info(f"mhnotify: 订阅 {mh_uuid} 进度 saved={saved}/{expected} type={mtype}")
                            with SessionFactory() as db:
                                subscribe = SubscribeOper(db=db).get(int(sid))
                            if not subscribe:
                                # MP订阅已不存在（可能为取消单季）
                                # 优先尝试：按同 TMDB 的剩余季更新 MH 订阅；若无剩余季则删除 MH
                                try:
                                    del_token = self.__mh_login()
                                except Exception:
                                    del_token = None
                                if del_token and mh_uuid:
                                    try:
                                        lst2 = self.__mh_list_subscriptions(del_token)
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
                                                                s_season = self.__extract_season_from_text(getattr(s, 'name', '') or '')
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
                                                self.__mh_update_subscription(del_token, mh_uuid, base_params)
                                                logger.info(f"mhnotify: 取消单季后更新MH订阅 seasons={remaining_seasons}")
                                            except Exception:
                                                logger.warning("mhnotify: 更新MH订阅季集合失败，降级为删除", exc_info=True)
                                                self.__mh_delete_subscription(del_token, mh_uuid)
                                        else:
                                            # 无剩余季，删除 MH 订阅
                                            self.__mh_delete_subscription(del_token, mh_uuid)
                                    except Exception:
                                        # 降级策略：出现异常则尽量删除对应 MH 订阅，避免遗留无主订阅
                                        try:
                                            self.__mh_delete_subscription(del_token, mh_uuid)
                                        except Exception:
                                            logger.warning("mhnotify: 处理剩余季时异常且删除失败", exc_info=True)
                                pending.pop(sid, None)
                                self.save_data(self._ASSIST_PENDING_KEY, pending)
                                continue
                            if mtype == 'movie':
                                if expected <= 1 and saved >= 1:
                                    # 完成：删除MH，完成MP订阅
                                    if token:
                                        self.__mh_delete_subscription(token, mh_uuid)
                                    self.__finish_mp_subscribe(subscribe)
                                    pending.pop(sid, None)
                                    self.save_data(self._ASSIST_PENDING_KEY, pending)
                                else:
                                    # 未完成：恢复MP订阅并监听MP完成后删除MH
                                    with SessionFactory() as db:
                                        SubscribeOper(db=db).update(subscribe.id, {"state": "R", "sites": []})
                                    watch: Dict[str, dict] = self.get_data(self._ASSIST_WATCH_KEY) or {}
                                    watch[sid] = {"mh_uuid": mh_uuid}
                                    self.save_data(self._ASSIST_WATCH_KEY, watch)
                                    pending.pop(sid, None)
                                    self.save_data(self._ASSIST_PENDING_KEY, pending)
                            else:
                                # TV
                                if expected > 0 and saved >= expected:
                                    # 完成：删除MH，完成MP订阅
                                    if token:
                                        self.__mh_delete_subscription(token, mh_uuid)
                                    self.__finish_mp_subscribe(subscribe)
                                    pending.pop(sid, None)
                                    self.save_data(self._ASSIST_PENDING_KEY, pending)
                                else:
                                    # 未完成：不删除MH，启用MP订阅，并加入watch等待MP完成/取消后删除MH
                                    with SessionFactory() as db:
                                        SubscribeOper(db=db).update(subscribe.id, {"state": "R", "sites": []})
                                    watch: Dict[str, dict] = self.get_data(self._ASSIST_WATCH_KEY) or {}
                                    watch[sid] = {"mh_uuid": mh_uuid}
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
                            del_token = self.__mh_login()
                        except Exception:
                            del_token = None
                        if mh_uuid and del_token:
                            try:
                                lst2 = self.__mh_list_subscriptions(del_token)
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
                                                        s_season = self.__extract_season_from_text(getattr(s, 'name', '') or '')
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
                                        self.__mh_update_subscription(del_token, mh_uuid, base_params)
                                        logger.info(f"mhnotify: 取消单季后更新MH订阅 seasons={remaining_seasons}")
                                    except Exception:
                                        logger.warning("mhnotify: 更新MH订阅季集合失败，降级为删除", exc_info=True)
                                        self.__mh_delete_subscription(del_token, mh_uuid)
                                else:
                                    self.__mh_delete_subscription(del_token, mh_uuid)
                            except Exception:
                                # 降级策略：出现异常则尽量删除对应 MH 订阅，避免遗留无主订阅
                                try:
                                    self.__mh_delete_subscription(del_token, mh_uuid)
                                except Exception:
                                    logger.warning("mhnotify: watch 分支处理剩余季时异常且删除失败", exc_info=True)
                        # 清理当前监听项
                        watch.pop(sid, None)
                        self.save_data(self._ASSIST_WATCH_KEY, watch)
        except Exception as e:
            logger.error(f"mhnotify: 助手调度异常: {e}")

    def _clear_all_records(self) -> Dict[str, Any]:
        """清理助手订阅记录（pending/watch），移除脏数据"""
        try:
            self.save_data(self._ASSIST_PENDING_KEY, {})
            self.save_data(self._ASSIST_WATCH_KEY, {})
            logger.info("mhnotify: 已清理助手订阅记录（pending/watch）")
            return {"success": True}
        except Exception as e:
            logger.error(f"mhnotify: 清理助手订阅记录失败: {e}")
            return {"success": False, "error": str(e)}
    
    def _clear_cloud_download_records(self) -> Dict[str, Any]:
        """清理助手云下载记录（预留接口）"""
        try:
            # 当前版本云下载使用daemon线程，无持久化数据需要清理
            # 此方法为将来可能的云下载记录功能预留接口
            logger.info("mhnotify: 云下载记录清理完成（当前版本无需清理）")
            return {"success": True}
        except Exception as e:
            logger.error(f"mhnotify: 清理云下载记录失败: {e}")
            return {"success": False, "error": str(e)}

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
                            # 连续多次未找到，可能被删除了
                            logger.error(f"mhnotify: 任务 {info_hash[:16]}... 可能已被删除")
                            self._send_cloud_download_deleted_notification(task_name)
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
                        
                        break
                    elif status == 1:
                        logger.warning(f"mhnotify: 离线下载任务失败: {task_name}")
                        self._send_cloud_download_failed_notification(task_name)
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
                        
                        # 如果文件小于10MB，删除
                        if file_size < min_size:
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

    def _get_mh_access_token(self) -> Optional[str]:
        """
        获取MH access token
        :return: access token或None
        """
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
                logger.error(f"mhnotify: MH登录失败")
                return None
            
            try:
                login_data = login_res.json()
                access_token = login_data.get("data", {}).get("access_token")
                return access_token
            except Exception:
                logger.error(f"mhnotify: 解析MH登录响应失败")
                return None
        except Exception as e:
            logger.error(f"mhnotify: 获取MH access token异常: {e}")
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
            
            logger.info(f"mhnotify: 获取到默认目录: {default_directory}")
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
            title = "⚠️ 115云下载任务已被删除"
            text = f"📦 任务: {task_name}\n\n任务在监控期间被删除，已停止监控。"
            
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

    @eventmanager.register(EventType.PluginAction)
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
                mediainfo.poster_path = getattr(subscribe, 'poster', None)
                mediainfo.backdrop_path = getattr(subscribe, 'backdrop', None)
                mediainfo.overview = getattr(subscribe, 'description', None)
                mediainfo.vote_average = getattr(subscribe, 'vote', None)
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

    # HDHive 模块缓存
    _hdhive_module: Any = None
    _hdhive_module_checked: bool = False
    
    def __get_hdhive_extension_filename(self) -> Optional[str]:
        """
        根据当前平台获取 hdhive 扩展模块的文件名
        
        :return: 文件名，如果平台不支持则返回 None
        """
        import platform
        machine = platform.machine().lower()
        system = platform.system().lower()
        
        # 映射架构名称
        arch_map = {
            "x86_64": "x86_64",
            "amd64": "x86_64",
            "aarch64": "aarch64",
            "arm64": "aarch64",
        }
        arch = arch_map.get(machine, machine)
        
        if system == "windows":
            return f"hdhive.cp312-win_{arch}.pyd"
        elif system == "darwin":
            return "hdhive.cpython-312-darwin.so"
        elif system == "linux":
            return f"hdhive.cpython-312-{arch}-linux-gnu.so"
        else:
            return None
    
    def __download_hdhive_module(self) -> bool:
        """
        检查并下载 hdhive 扩展模块
        
        :return: 是否成功获取模块
        """
        import platform
        import os
        from pathlib import Path
        import urllib.request
        import urllib.error
        
        # 获取插件目录下的 lib 文件夹
        plugin_dir = Path(__file__).parent
        lib_dir = plugin_dir / "lib"
        lib_dir.mkdir(parents=True, exist_ok=True)
        
        system = platform.system().lower()
        machine = platform.machine().lower()
        
        ext_filename = self.__get_hdhive_extension_filename()
        if not ext_filename:
            logger.warning(f"mhnotify: 不支持的平台: {system}/{machine}，HDHive 模块功能无法使用")
            return False
        
        target_path = lib_dir / ext_filename
        
        # 本地文件已存在
        if target_path.exists():
            logger.debug(f"mhnotify: hdhive 扩展模块已存在: {target_path}")
            return True
        
        # 从 GitHub 下载
        base_url = "https://raw.githubusercontent.com/mrtian2016/hdhive_resource/main"
        download_url = f"{base_url}/{ext_filename}"
        
        logger.info(f"mhnotify: 本地未找到 hdhive 扩展模块，尝试下载: {download_url}")
        
        try:
            proxy = getattr(settings, "PROXY", None)
            if proxy:
                if isinstance(proxy, dict):
                    proxy_handler = urllib.request.ProxyHandler(proxy)
                else:
                    proxy_handler = urllib.request.ProxyHandler({"http": proxy, "https": proxy})
                opener = urllib.request.build_opener(proxy_handler)
                logger.debug(f"mhnotify: 下载 hdhive 使用代理: {proxy}")
                response = opener.open(download_url, timeout=120)
            else:
                response = urllib.request.urlopen(download_url, timeout=120)
            
            with response:
                content = response.read()
            
            with open(target_path, "wb") as f:
                f.write(content)
            
            # 非 Windows 平台设置可执行权限
            if system != "windows":
                os.chmod(target_path, 0o755)
            
            logger.info(f"mhnotify: ✓ hdhive 扩展模块下载成功: {target_path}")
            return True
            
        except urllib.error.HTTPError as e:
            if e.code == 404:
                logger.warning(f"mhnotify: ⚠️ hdhive 扩展模块暂不支持当前平台 ({system}/{machine})，将使用 HTTP API 模式")
            else:
                logger.error(f"mhnotify: 下载 hdhive 扩展模块失败 (HTTP {e.code}): {e}")
            return False
        except urllib.error.URLError as e:
            logger.error(f"mhnotify: 下载 hdhive 扩展模块失败（网络错误）: {e}")
            return False
        except Exception as e:
            logger.error(f"mhnotify: 下载 hdhive 扩展模块失败: {e}")
            return False
    
    def __load_hdhive_module(self) -> Optional[Any]:
        """
        加载 hdhive 模块，优先使用本地下载的模块
        
        :return: hdhive 模块对象，失败返回 None
        """
        if self._hdhive_module_checked:
            return self._hdhive_module
        
        self._hdhive_module_checked = True
        
        # 尝试下载模块
        if not self.__download_hdhive_module():
            return None
        
        # 动态加载模块
        import sys
        import importlib.util
        from pathlib import Path
        
        plugin_dir = Path(__file__).parent
        lib_dir = plugin_dir / "lib"
        
        ext_filename = self.__get_hdhive_extension_filename()
        if not ext_filename:
            return None
        
        module_path = lib_dir / ext_filename
        if not module_path.exists():
            return None
        
        try:
            # 将 lib 目录添加到 sys.path
            lib_dir_str = str(lib_dir)
            if lib_dir_str not in sys.path:
                sys.path.insert(0, lib_dir_str)
            
            # 加载模块
            spec = importlib.util.spec_from_file_location("hdhive", str(module_path))
            if spec and spec.loader:
                module = importlib.util.module_from_spec(spec)
                sys.modules["hdhive"] = module
                spec.loader.exec_module(module)
                self._hdhive_module = module
                logger.info(f"mhnotify: ✓ hdhive 模块加载成功")
                return module
            else:
                logger.error("mhnotify: 无法创建 hdhive 模块 spec")
                return None
        except Exception as e:
            logger.error(f"mhnotify: 加载 hdhive 模块失败: {type(e).__name__}: {e}")
            return None
    
    def __fetch_hdhive_links(self, tmdb_id: Optional[int], media_type: Optional[str]) -> List[str]:
        """
        根据配置从 HDHive 查询免费115分享链接，返回 URL 列表
        优先使用 hdhive 模块（Linux/macOS），不可用时回退到 HTTP API 模式
        """
        results: List[str] = []
        try:
            logger.debug(f"mhnotify: HDHive 查询开始 tmdb_id={tmdb_id} media_type={media_type} enabled={self._hdhive_enabled}")
            if not self._hdhive_enabled:
                logger.debug("mhnotify: HDHive 未启用，跳过查询")
                return results
            if not tmdb_id:
                logger.warning("mhnotify: 缺少 TMDB ID，无法使用 HDHive 查询")
                return results
            
            # 尝试加载 hdhive 模块
            hdhive_mod = self.__load_hdhive_module()
            
            if hdhive_mod:
                # 使用 hdhive 模块查询
                return self.__fetch_hdhive_links_with_module(tmdb_id, media_type, hdhive_mod)
            else:
                # 回退到 HTTP API 模式
                logger.debug("mhnotify: hdhive 模块不可用，使用 HTTP API 模式")
                return self.__fetch_hdhive_links_with_http(tmdb_id, media_type)
                
        except Exception as e:
            logger.error(f"mhnotify: __fetch_hdhive_links 异常: {type(e).__name__}: {e}", exc_info=True)
            return []
    
    def __fetch_hdhive_links_with_module(self, tmdb_id: int, media_type: str, hdhive_mod: Any) -> List[str]:
        """
        使用 hdhive 模块查询 HDHive 资源
        """
        results: List[str] = []
        try:
            h_type_str = "movie" if (media_type or "movie").lower() == "movie" else "tv"
            
            # 获取 MediaType 枚举
            MediaType = getattr(hdhive_mod, 'MediaType', None)
            if MediaType is None:
                logger.error("mhnotify: hdhive 模块缺少 MediaType 类")
                return self.__fetch_hdhive_links_with_http(tmdb_id, media_type)
            
            h_type = MediaType.MOVIE if h_type_str == "movie" else MediaType.TV
            
            cookie = self._hdhive_cookie or ""
            
            # Cookie 有效性检查和刷新
            if self._hdhive_auto_refresh and self._hdhive_username and self._hdhive_password:
                is_valid, reason = self.__check_hdhive_cookie_valid(cookie, self._hdhive_refresh_before)
                if not cookie or not is_valid:
                    logger.info(f"HDHive: Cookie 需要刷新 - {reason}")
                    new_cookie = self.__refresh_hdhive_cookie(self._hdhive_username, self._hdhive_password)
                    if new_cookie:
                        cookie = new_cookie
                        self._hdhive_cookie = new_cookie
                        cfg = self.get_config()
                        if isinstance(cfg, dict):
                            cfg["hdhive_cookie"] = new_cookie
                            self.update_config(cfg)
                        logger.info("HDHive: Cookie 刷新成功")
            
            if not cookie:
                if self._hdhive_username and self._hdhive_password:
                    new_cookie = self.__refresh_hdhive_cookie(self._hdhive_username, self._hdhive_password)
                    if new_cookie:
                        cookie = new_cookie
                        self._hdhive_cookie = new_cookie
                        cfg = self.get_config()
                        if isinstance(cfg, dict):
                            cfg["hdhive_cookie"] = new_cookie
                            self.update_config(cfg)
                    else:
                        logger.warning("HDHive: 无法获取有效 Cookie")
                        return results
                else:
                    logger.warning("HDHive: 需要配置 Cookie 或用户名密码")
                    return results
            
            proxy = getattr(settings, "PROXY", None)
            create_client = getattr(hdhive_mod, 'create_client', None)
            
            if create_client is None:
                logger.error("mhnotify: hdhive 模块缺少 create_client 函数")
                return self.__fetch_hdhive_links_with_http(tmdb_id, media_type)
            
            logger.debug(f"mhnotify: 使用 hdhive 模块查询 tmdb_id={tmdb_id}")
            
            with create_client(cookie=cookie, proxy=proxy) as client:
                media = client.get_media_by_tmdb_id(tmdb_id, h_type)
                if not media:
                    logger.info(f"mhnotify: HDHive 未找到媒体 tmdb_id={tmdb_id}")
                    return results
                
                logger.debug(f"mhnotify: HDHive 找到媒体 slug={getattr(media, 'slug', None)}")
                
                res = client.get_resources(media.slug, h_type, media_id=media.id)
                if not res or not res.success:
                    logger.info(f"mhnotify: HDHive 获取资源失败")
                    return results
                
                logger.debug(f"mhnotify: HDHive 获取到资源数量={len(res.resources) if hasattr(res, 'resources') else 0}")
                
                for item in res.resources:
                    website_val = getattr(item.website, 'value', '') if hasattr(item, 'website') else ''
                    is_free = getattr(item, 'is_free', False)
                    
                    if website_val == '115' and is_free:
                        share = client.get_share_url(item.slug)
                        if share and share.url:
                            logger.info(f"mhnotify: HDHive 获取到免费分享链接: {share.url}")
                            results.append(share.url)
            
            return results
            
        except Exception as e:
            logger.error(f"mhnotify: hdhive 模块查询失败: {type(e).__name__}: {e}", exc_info=True)
            # 回退到 HTTP API
            return self.__fetch_hdhive_links_with_http(tmdb_id, media_type)
    
    def __fetch_hdhive_links_with_http(self, tmdb_id: int, media_type: str) -> List[str]:
        """
        使用 HTTP API 直接查询 HDHive 资源（无需 hdhive 模块）
        """
        results: List[str] = []
        try:
            import requests
            
            base_url = "https://hdhive.com"
            h_type = "movie" if (media_type or "movie").lower() == "movie" else "tv"
            logger.debug(f"mhnotify: HDHive HTTP API 查询 tmdb_id={tmdb_id} h_type={h_type}")

            # API 模式需要 Cookie
            query_mode = (self._hdhive_query_mode or "api").lower()
            logger.debug(f"mhnotify: HDHive 查询模式: {query_mode}")
            
            cookie = self._hdhive_cookie or ""
            
            # 自动刷新 Cookie（若开启且 Cookie 无效）
            if self._hdhive_auto_refresh and self._hdhive_username and self._hdhive_password:
                is_valid, reason = self.__check_hdhive_cookie_valid(cookie, self._hdhive_refresh_before)
                logger.debug(f"mhnotify: Cookie 有效性检查: valid={is_valid}, reason={reason}")
                if not cookie or not is_valid:
                    logger.info(f"HDHive: Cookie 需要刷新 - {reason}")
                    new_cookie = self.__refresh_hdhive_cookie(self._hdhive_username, self._hdhive_password)
                    if new_cookie:
                        cookie = new_cookie
                        self._hdhive_cookie = new_cookie
                        cfg = self.get_config()
                        if isinstance(cfg, dict):
                            cfg["hdhive_cookie"] = new_cookie
                            self.update_config(cfg)
                        logger.info("HDHive: Cookie 刷新成功并已保存到配置")
                    else:
                        logger.warning("mhnotify: Cookie 刷新失败")
            
            if not cookie:
                # 尝试 Playwright 模式登录获取 Cookie
                if query_mode == "playwright" and self._hdhive_username and self._hdhive_password:
                    logger.info("mhnotify: HDHive 无有效 Cookie，尝试 Playwright 登录...")
                    new_cookie = self.__refresh_hdhive_cookie(self._hdhive_username, self._hdhive_password)
                    if new_cookie:
                        cookie = new_cookie
                        self._hdhive_cookie = new_cookie
                        cfg = self.get_config()
                        if isinstance(cfg, dict):
                            cfg["hdhive_cookie"] = new_cookie
                            self.update_config(cfg)
                        logger.info("HDHive: Playwright 登录成功，Cookie 已保存")
                    else:
                        logger.warning("HDHive: Playwright 登录失败")
                        return results
                else:
                    logger.warning("HDHive: 需要有效的 Cookie 或配置用户名密码使用 Playwright 模式")
                    return results
            
            # 设置请求头
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                "Referer": base_url,
                "Cookie": cookie,
            }
            
            # 从 Cookie 中提取 csrf_access_token（用于 API 请求）
            csrf_token = ""
            for part in cookie.split(';'):
                part = part.strip()
                if part.startswith('csrf_access_token='):
                    csrf_token = part.split('=', 1)[1]
                    break
            if csrf_token:
                headers["X-CSRF-TOKEN"] = csrf_token
            
            proxy = getattr(settings, "PROXY", None)
            proxies = None
            if proxy:
                if isinstance(proxy, dict):
                    proxies = proxy
                else:
                    proxies = {"http": proxy, "https": proxy}
            
            session = requests.Session()
            session.headers.update(headers)
            if proxies:
                session.proxies.update(proxies)
            
            try:
                # 1. 通过 TMDB ID 查询媒体信息
                search_url = f"{base_url}/api/media/tmdb/{tmdb_id}?type={h_type}"
                logger.debug(f"mhnotify: HDHive 查询媒体 GET {search_url}")
                
                resp = session.get(search_url, timeout=30)
                if resp.status_code != 200:
                    logger.info(f"mhnotify: HDHive 查询媒体失败，状态码={resp.status_code}")
                    return results
                
                media_data = resp.json()
                if not media_data:
                    logger.info(f"mhnotify: HDHive 未找到媒体 tmdb_id={tmdb_id}")
                    return results
                
                media_slug = media_data.get("slug")
                media_id = media_data.get("id")
                logger.debug(f"mhnotify: HDHive 找到媒体 slug={media_slug} id={media_id}")
                
                if not media_slug:
                    logger.info(f"mhnotify: HDHive 媒体数据无 slug")
                    return results
                
                # 2. 获取资源列表
                resources_url = f"{base_url}/api/resource/{h_type}/{media_slug}"
                if media_id:
                    resources_url += f"?media_id={media_id}"
                logger.debug(f"mhnotify: HDHive 获取资源 GET {resources_url}")
                
                resp = session.get(resources_url, timeout=30)
                if resp.status_code != 200:
                    logger.info(f"mhnotify: HDHive 获取资源失败，状态码={resp.status_code}")
                    return results
                
                resources_data = resp.json()
                if not resources_data or not resources_data.get("success"):
                    logger.info(f"mhnotify: HDHive 获取资源返回 success=False")
                    return results
                
                resources = resources_data.get("resources", [])
                logger.debug(f"mhnotify: HDHive 获取到资源数量={len(resources)}")
                
                # 3. 筛选免费的 115 资源并获取分享链接
                for item in resources:
                    website = item.get("website", "")
                    is_free = item.get("is_free", False)
                    res_slug = item.get("slug", "")
                    logger.debug(f"mhnotify: HDHive 资源项 slug={res_slug} website={website} is_free={is_free}")
                    
                    if website == "115" and is_free and res_slug:
                        # 获取分享链接
                        share_url_api = f"{base_url}/api/resource/{res_slug}/share"
                        logger.debug(f"mhnotify: HDHive 获取分享链接 GET {share_url_api}")
                        
                        try:
                            share_resp = session.get(share_url_api, timeout=30)
                            if share_resp.status_code == 200:
                                share_data = share_resp.json()
                                share_link = share_data.get("url") or share_data.get("share_url")
                                if share_link:
                                    logger.info(f"mhnotify: HDHive 获取到免费分享链接: {share_link}")
                                    results.append(share_link)
                                else:
                                    logger.debug(f"mhnotify: HDHive 分享链接响应无 url 字段: {share_data}")
                            else:
                                logger.debug(f"mhnotify: HDHive 获取分享链接失败 status={share_resp.status_code}")
                        except Exception as e:
                            logger.debug(f"mhnotify: HDHive 获取分享链接异常: {e}")
                
                logger.debug(f"mhnotify: HDHive 查询结束，结果数量={len(results)}")
                return results
                
            except requests.exceptions.RequestException as e:
                logger.error(f"HDHive API 请求失败: {type(e).__name__}: {e}")
                return results
                
        except Exception as e:
            logger.error(f"mhnotify: __fetch_hdhive_links 异常: {type(e).__name__}: {e}", exc_info=True)
            return []
    
    def __check_hdhive_cookie_valid(self, cookie: str, refresh_before: int = 3600) -> Tuple[bool, str]:
        """
        检查 HDHive Cookie 是否有效
        
        :param cookie: Cookie 字符串
        :param refresh_before: 在过期前多少秒视为需要刷新
        :return: (是否有效, 原因说明)
        """
        import base64
        import json
        
        if not cookie:
            return False, "Cookie 为空"
        
        # 从 Cookie 中提取 token
        token = None
        for part in cookie.split(';'):
            part = part.strip()
            if part.startswith('token='):
                token = part.split('=', 1)[1]
                break
        
        if not token:
            return False, "Cookie 中无 token"
        
        try:
            # JWT 格式: header.payload.signature
            parts = token.split('.')
            if len(parts) != 3:
                return False, "token 格式错误"
            
            # 解码 payload（第二部分）
            payload = parts[1]
            # 补齐 base64 padding
            padding = 4 - len(payload) % 4
            if padding != 4:
                payload += '=' * padding
            
            decoded = base64.urlsafe_b64decode(payload)
            payload_data = json.loads(decoded)
            
            exp = payload_data.get('exp')
            if not exp:
                return False, "token 无过期时间"
            
            import time
            now = time.time()
            time_left = exp - now
            
            if time_left <= 0:
                return False, "Cookie 已过期"
            
            if time_left < refresh_before:
                hours_left = time_left / 3600
                return False, f"Cookie 将在 {hours_left:.1f} 小时后过期"
            
            return True, f"Cookie 有效，还有 {time_left / 3600:.1f} 小时"
            
        except Exception as e:
            logger.debug(f"mhnotify: 解析 HDHive Cookie 失败: {e}")
            return False, f"解析失败: {e}"
    
    def __refresh_hdhive_cookie(self, username: str, password: str) -> Optional[str]:
        """
        使用 Playwright 登录 HDHive 获取新 Cookie
        
        :param username: HDHive 用户名
        :param password: HDHive 密码
        :return: 新的 Cookie 字符串，失败返回 None
        """
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            logger.error("Playwright 未安装，无法自动刷新 HDHive Cookie")
            logger.info("请运行: pip install playwright && playwright install chromium")
            return None
        
        try:
            base_url = "https://hdhive.com"
            login_url = f"{base_url}/login"
            proxy = getattr(settings, "PROXY", None)
            
            with sync_playwright() as pw:
                launch_options = {"headless": True}
                context_options = {}
                
                if proxy:
                    if isinstance(proxy, dict):
                        proxy_url = proxy.get("http") or proxy.get("https")
                    else:
                        proxy_url = proxy
                    if proxy_url:
                        context_options["proxy"] = {"server": proxy_url}
                        logger.debug(f"HDHive Cookie 刷新使用代理: {proxy_url}")
                
                browser = pw.chromium.launch(**launch_options)
                context = browser.new_context(**context_options)
                page = context.new_page()
                
                logger.info("HDHive: 访问登录页...")
                page.goto(login_url, wait_until="domcontentloaded", timeout=30000)
                page.wait_for_timeout(2000)
                
                # 填写用户名
                username_selectors = ['#username', 'input[name="username"]', 'input[name="email"]', 'input[type="email"]']
                username_filled = False
                for sel in username_selectors:
                    try:
                        if page.query_selector(sel):
                            page.fill(sel, username)
                            logger.debug("HDHive: ✓ 填写用户名")
                            username_filled = True
                            break
                    except Exception:
                        continue
                
                if not username_filled:
                    logger.error("HDHive: 未找到用户名输入框")
                    context.close()
                    browser.close()
                    return None
                
                # 填写密码
                password_selectors = ['#password', 'input[name="password"]', 'input[type="password"]']
                password_filled = False
                for sel in password_selectors:
                    try:
                        if page.query_selector(sel):
                            page.fill(sel, password)
                            logger.debug("HDHive: ✓ 填写密码")
                            password_filled = True
                            break
                    except Exception:
                        continue
                
                if not password_filled:
                    logger.error("HDHive: 未找到密码输入框")
                    context.close()
                    browser.close()
                    return None
                
                # 提交登录
                page.wait_for_timeout(500)
                try:
                    btn = page.query_selector('button[type="submit"]')
                    if btn:
                        btn.click()
                        logger.debug("HDHive: ✓ 点击登录按钮")
                    else:
                        page.keyboard.press("Enter")
                        logger.debug("HDHive: ✓ 按 Enter 键提交")
                except Exception:
                    page.keyboard.press("Enter")
                
                # 等待登录完成
                try:
                    page.wait_for_load_state("domcontentloaded", timeout=15000)
                except Exception:
                    pass
                
                page.wait_for_timeout(3000)
                
                # 检查登录结果
                current_url = page.url
                logger.debug(f"HDHive: 登录后 URL: {current_url}")
                
                if "/login" in current_url:
                    logger.warning("HDHive: 登录可能失败，仍在登录页面")
                
                # 获取 Cookie
                cookies = context.cookies()
                cookie_parts = []
                for c in cookies:
                    if c.get("domain", "").endswith("hdhive.com"):
                        cookie_parts.append(f"{c['name']}={c['value']}")
                
                context.close()
                browser.close()
                
                if cookie_parts:
                    cookie_str = "; ".join(cookie_parts)
                    logger.info(f"HDHive: 获取 Cookie 成功，长度={len(cookie_str)}")
                    return cookie_str
                else:
                    logger.warning("HDHive: 未获取到有效 Cookie")
                    return None
                    
        except Exception as e:
            logger.error(f"HDHive Playwright 登录失败: {type(e).__name__}: {e}", exc_info=True)
            return None
