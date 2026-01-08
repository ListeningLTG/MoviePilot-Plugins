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
    plugin_desc = "监听115生活事件和MP整理/刮削事件后，通知MediaHelper执行strm生成任务；提供mh订阅辅助；支持115云下载（/mhol命令）"
    # 插件图标
    plugin_icon = "https://raw.githubusercontent.com/JieWSOFT/MediaHelp/main/frontend/apps/web-antd/public/icon.png"
    # 插件版本
    plugin_version = "1.3.7"
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
    _hdhive_refresh_before: int = 86400
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
                self._hdhive_refresh_before = int(config.get("hdhive_refresh_before", 86400) or 86400)
            except Exception:
                self._hdhive_refresh_before = 86400

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
                "cmd": "/mhnotify_clear",
                "event": EventType.PluginAction,
                "desc": "清除订阅记录（移除脏数据）",
                "category": "维护",
                "data": {
                    "action": "mhnotify_clear"
                }
            },
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
            "hdhive_enabled": False,
            "hdhive_query_mode": "api",
            "hdhive_username": "",
            "hdhive_password": "",
            "hdhive_cookie": "",
            "hdhive_auto_refresh": False,
            "hdhive_refresh_before": 86400,
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
                                            'label': 'HDHive Cookie（API 模式）'
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
                                            'placeholder': '默认 86400'
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
                        triggered_events.append({"path": full_path or fname, "event": event_name, "type": t})
                    if ut > new_last_ts or (ut == new_last_ts and eid > new_last_id):
                        new_last_ts = ut
                        new_last_id = eid

                if has_new:
                    self._wait_notify_count += 1
                    self._last_event_time = int(time.time())
                    # 输出详细的触发信息
                    for evt in triggered_events:
                        logger.info(f"mhnotify: 115生活事件触发 - 目录: {evt['path']} | 事件: {evt['event']} (type={evt['type']})")
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
            # 若为剧集，聚合同一 TMDB 的多季订阅
            aggregate_seasons: Optional[List[int]] = None
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
                        aggregate_seasons = []
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
            # 构建创建参数（若为TV将带入聚合季）
            create_payload = self.__build_mh_create_payload(subscribe, mediainfo_dict, defaults, aggregate_seasons=aggregate_seasons)
            if not create_payload:
                logger.error("mhnotify: 构建MH订阅创建参数失败")
                return
            # 若已存在相同 tmdb_id 的 MH 订阅，则复用或重建（以聚合季为准）
            existing_uuid: Optional[str] = None
            existing_selected: List[int] = []
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
                        logger.info(f"mhnotify: 现有MH订阅命中 tmdb_id={params.get('tmdb_id')} uuid={existing_uuid} seasons={existing_selected}")
                        break
                if existing_uuid:
                    agg_set = set(create_payload.get("selected_seasons") or [])
                    exist_set = set(existing_selected or [])
                    if agg_set and agg_set != exist_set:
                        # 需要包含更多季：优先尝试更新订阅季集合；失败则重建
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
            try:
                links = self.__fetch_hdhive_links(
                    tmdb_id=create_payload.get("tmdb_id"),
                    media_type=create_payload.get("media_type")
                )
                if links:
                    create_payload["user_custom_links"] = links
                    logger.info(f"mhnotify: HDHive 获取到 {len(links)} 个免费115链接，已加入自定义链接")
            except Exception:
                logger.error("mhnotify: HDHive 查询链接失败", exc_info=True)
            # 创建订阅（或复用现有）
            mh_uuid = None
            if existing_uuid:
                mh_uuid = existing_uuid
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
        """清除助手记录（pending/watch），移除脏数据"""
        try:
            self.save_data(self._ASSIST_PENDING_KEY, {})
            self.save_data(self._ASSIST_WATCH_KEY, {})
            logger.info("mhnotify: 已清除助手记录（pending/watch）")
            return {"success": True}
        except Exception as e:
            logger.error(f"mhnotify: 清除助手记录失败: {e}")
            return {"success": False, "error": str(e)}

    def _add_offline_download(self, url: str) -> Tuple[bool, str]:
        """
        添加115离线下载任务
        :param url: 下载链接（磁力链接、种子URL等）
        :return: (是否成功, 消息文本)
        """
        try:
            # 导入p115client
            try:
                from p115client import P115Client
            except ImportError:
                return False, "p115client 未安装，请先安装依赖"

            # 创建115客户端
            client = P115Client(self._p115_cookie, app="web")
            
            # 获取或创建目标目录ID
            target_path = self._cloud_download_path or "/云下载"
            target_cid = 0
            
            try:
                # 使用p115client的工具函数获取目录ID
                # 参考p115strmhelper的实现
                def get_cid_by_path(client, path):
                    """根据路径获取目录ID"""
                    if not path or path == '/':
                        return 0
                    
                    # 标准化路径
                    path = path.strip()
                    if not path.startswith('/'):
                        path = '/' + path
                    path = path.rstrip('/')
                    
                    # 分割路径
                    parts = [p for p in path.split('/') if p]
                    if not parts:
                        return 0
                    
                    # 从根目录开始逐级查找
                    current_cid = 0
                    for part in parts:
                        # 获取当前目录下的文件列表
                        resp = client.fs_files(cid=current_cid, limit=1150)
                        if not resp or not resp.get('state'):
                            return None
                        
                        # 查找匹配的子目录
                        found = False
                        for item in resp.get('data', []):
                            if item.get('name') == part and item.get('is_directory'):
                                current_cid = item.get('cid')
                                found = True
                                break
                        
                        if not found:
                            # 目录不存在，创建它
                            mkdir_resp = client.fs_mkdir(part, pid=current_cid)
                            if mkdir_resp and mkdir_resp.get('state'):
                                current_cid = mkdir_resp.get('cid')
                            else:
                                logger.warning(f"mhnotify: 创建目录 {part} 失败")
                                return None
                    
                    return current_cid
                
                target_cid = get_cid_by_path(client, target_path)
                if target_cid is None:
                    logger.warning(f"mhnotify: 获取目录ID失败，使用根目录")
                    target_cid = 0
                else:
                    logger.info(f"mhnotify: 目标目录ID: {target_cid}")
                    
            except Exception as e:
                logger.warning(f"mhnotify: 获取目录ID异常，使用根目录: {e}")
                target_cid = 0

            # 添加离线下载任务
            # 构建请求payload
            payload = {
                'url[0]': url,
                'wp_path_id': target_cid
            }
            
            # 调用115离线下载API
            resp = client.offline_add_urls(payload)
            
            # 检查响应
            if not resp:
                return False, "115 API 响应为空"
            
            state = resp.get('state', False)
            if not state:
                error_msg = resp.get('error', '未知错误')
                error_code = resp.get('errcode', '')
                return False, f"添加失败: {error_msg} (错误码: {error_code})"
            
            # 解析返回的任务信息
            data = resp.get('data', {})
            result = data.get('result', [])
            
            if not result:
                return False, "任务添加成功但未返回任务信息"
            
            # 获取第一个任务信息
            task = result[0] if isinstance(result, list) else result
            task_name = task.get('name', '未知任务')
            info_hash = task.get('info_hash', '')
            
            success_msg = f"任务已添加到115云下载\n"
            success_msg += f"任务名称: {task_name}\n"
            success_msg += f"保存路径: {target_path}"
            if info_hash:
                success_msg += f"\nHash: {info_hash[:16]}..."
            
            logger.info(f"mhnotify: 115离线下载任务添加成功: {task_name}")
            return True, success_msg
            
        except ImportError as e:
            logger.error(f"mhnotify: 导入p115client失败: {e}")
            return False, f"依赖库导入失败: {str(e)}"
        except Exception as e:
            logger.error(f"mhnotify: 添加115离线下载任务失败: {e}", exc_info=True)
            return False, f"添加失败: {str(e)}"


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

        # 获取下载链接
        download_url = event_data.get("arg_str")
        if not download_url or not download_url.strip():
            self.post_message(
                channel=event_data.get("channel"),
                title="参数错误",
                text="用法: /mhol <下载链接>",
                userid=event_data.get("user")
            )
            return

        download_url = download_url.strip()
        logger.info(f"mhnotify: 收到云下载命令，链接: {download_url}")

        # 执行云下载
        success, message = self._add_offline_download(download_url)

        # 发送结果消息
        if success:
            self.post_message(
                channel=event_data.get("channel"),
                title="云下载任务添加成功",
                text=message,
                userid=event_data.get("user")
            )
        else:
            self.post_message(
                channel=event_data.get("channel"),
                title="云下载任务添加失败",
                text=message,
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

    def __fetch_hdhive_links(self, tmdb_id: Optional[int], media_type: Optional[str]) -> List[str]:
        """根据配置从 HDHive 查询免费115分享链接，返回 URL 列表"""
        results: List[str] = []
        try:
            if not self._hdhive_enabled:
                return results
            if not tmdb_id:
                logger.warning("mhnotify: 缺少 TMDB ID，无法使用 HDHive 查询")
                return results
            # 延迟导入 HDHive 库
            import importlib
            hdhive_mod = importlib.import_module('app.plugins.p115strgmsub.lib.hdhive')
            HDHiveMediaType = getattr(hdhive_mod, 'MediaType')
            h_type = HDHiveMediaType.MOVIE if (media_type or "movie").lower() == "movie" else HDHiveMediaType.TV

            # API 模式
            if (self._hdhive_query_mode or "api").lower() == "api":
                cookie = self._hdhive_cookie or ""
                # 自动刷新 Cookie（若开启）
                try:
                    if self._hdhive_auto_refresh:
                        utils_mod = importlib.import_module('app.plugins.p115strgmsub.utils')
                        check_valid = getattr(utils_mod, 'check_hdhive_cookie_valid', None)
                        do_refresh = getattr(utils_mod, 'refresh_hdhive_cookie_with_playwright', None)
                        if check_valid:
                            is_valid, reason = check_valid(cookie, self._hdhive_refresh_before)
                        else:
                            is_valid, reason = (bool(cookie), 'no-check-func')
                        if not cookie or not is_valid:
                            logger.info(f"HDHive: Cookie 需要刷新 - {reason}")
                            if self._hdhive_username and self._hdhive_password and do_refresh:
                                new_cookie = do_refresh(self._hdhive_username, self._hdhive_password)
                                if new_cookie:
                                    cookie = new_cookie
                                    self._hdhive_cookie = new_cookie
                                    # 持久化更新
                                    cfg = self.get_config()
                                    if isinstance(cfg, dict):
                                        cfg["hdhive_cookie"] = new_cookie
                                        self.update_config(cfg)
                                    logger.info("HDHive: Cookie 刷新成功并已保存到配置")
                except Exception:
                    logger.warning("HDHive: 自动刷新 Cookie 失败", exc_info=True)

                if not cookie:
                    logger.warning("HDHive API 模式需要有效的 Cookie")
                    return results
                try:
                    proxy = getattr(settings, "PROXY", None)
                    create_client = getattr(hdhive_mod, 'create_client')
                    with create_client(cookie=cookie, proxy=proxy) as client:
                        media = client.get_media_by_tmdb_id(tmdb_id, h_type)
                        if not media:
                            return results
                        res = client.get_resources(media.slug, h_type, media_id=media.id)
                        if not res or not res.success:
                            return results
                        for item in res.resources:
                            if hasattr(item, 'website') and getattr(item.website, 'value', '') == '115' and getattr(item, 'is_free', False):
                                share = client.get_share_url(item.slug)
                                if share and share.url:
                                    results.append(share.url)
                except Exception:
                    logger.error("HDHive (API) 查询失败", exc_info=True)
                return results

            # Playwright 模式
            if not self._hdhive_username or not self._hdhive_password:
                logger.warning("HDHive Playwright 模式需要配置用户名和密码")
                return results
            try:
                import asyncio
                proxy = getattr(settings, "PROXY", None)
                async def async_search():
                    create_async = getattr(hdhive_mod, 'create_async_client')
                    async with create_async(
                        username=self._hdhive_username,
                        password=self._hdhive_password,
                        cookie=self._hdhive_cookie,
                        browser_type="chromium",
                        headless=True,
                        proxy=proxy
                    ) as client:
                        media = await client.get_media_by_tmdb_id(tmdb_id, h_type)
                        if not media:
                            return []
                        res = await client.get_resources(media.slug, h_type, media_id=media.id)
                        if not res or not res.success:
                            return []
                        links: List[str] = []
                        for item in res.resources:
                            if hasattr(item, 'website') and getattr(item.website, 'value', '') == '115' and getattr(item, 'is_free', False):
                                share_result = await client.get_share_url_by_click(item.slug)
                                if share_result and share_result.url:
                                    links.append(share_result.url)
                        return links
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    results = loop.run_until_complete(async_search())
                finally:
                    loop.close()
            except Exception:
                logger.error("HDHive (Playwright) 查询失败", exc_info=True)
            return results
        except Exception:
            logger.error("mhnotify: __fetch_hdhive_links 异常", exc_info=True)
            return []
