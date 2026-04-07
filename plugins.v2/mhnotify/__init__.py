import time
import re
import threading
from datetime import datetime
from urllib.parse import quote

from typing import List, Tuple, Dict, Any, Optional, Union
from apscheduler.triggers.cron import CronTrigger

from app.core.config import settings
from app.core.event import eventmanager, Event
from app.schemas.types import EventType, NotificationType, MediaType
from app.core.context import MediaInfo
from app.modules.themoviedb.tmdbapi import TmdbApi
from app.chain.download import DownloadChain
from app.utils.http import RequestUtils
from app.log import logger
from app.plugins import _PluginBase
from app.db import SessionFactory
from app.db.subscribe_oper import SubscribeOper

from .mh_api import MHApiMixin
from .mh_assist import MHAssistMixin
from .cloud_download import CloudDownloadMixin
from .ali_to_115 import AliTo115Mixin


class MHNotify(MHApiMixin, MHAssistMixin, CloudDownloadMixin, AliTo115Mixin, _PluginBase):
    # 插件名称
    plugin_name = "MediaHelper增强"
    # 插件描述
    plugin_desc = "配合MediaHelper使用的一些小功能"
    # 插件图标
    plugin_icon = "https://raw.githubusercontent.com/ListeningLTG/MoviePilot-Plugins/refs/heads/main/icons/mh2.jpg"
    # 插件版本
    plugin_version = "1.7.5"
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

    # 线程锁
    _tmdb_locks: Dict[int, threading.Lock] = {}
    _locks_lock = threading.Lock()
    # 待检查订阅的定时器映射 (sid -> threading.Timer)
    _pending_timers: Dict[str, threading.Timer] = {}
    _timers_lock = threading.Lock()

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
    # mh订阅辅助：MH订阅检查频率（Cron表达式）
    _mh_assist_cron: str = "0 */6 * * *"
    # 转发监听关键词同步（白名单）
    _mh_forwarder_whitelist_enabled: bool = False
    _mh_forwarder_whitelist_listeners: List[str] = []
    # 转发监听关键词同步（黑名单）
    _mh_forwarder_blacklist_enabled: bool = False
    _mh_forwarder_blacklist_listeners: List[str] = []
    # 转发监听配置列表缓存（供配置界面下拉使用）
    _mh_listeners_cache: List[Dict[str, Any]] = []
    # 助手：待检查的mh订阅映射（mp_sub_id -> {mh_uuid, created_at, type}）
    _ASSIST_PENDING_KEY = "mhnotify_assist_pending"
    # 助手：等待MP完成后删除mh订阅的监听映射（mp_sub_id -> {mh_uuid}）
    _ASSIST_WATCH_KEY = "mhnotify_assist_watch"
    # 助手：云下载辅助映射（info_hash -> {sid, mh_uuid}）
    _ASSIST_CLOUD_MAP_KEY = "mhnotify_assist_cloud_map"
    # MH登录缓存
    _mh_token: Optional[str] = None
    _mh_token_expire_ts: int = 0
    _mh_token_ttl_seconds: int = 600  # 默认缓存10分钟
    # 助手调度延迟/重试常量（首次查询2分钟，之后每1分钟重试）
    _assist_initial_delay_seconds: int = 120
    _assist_retry_interval_seconds: int = 60
    # 115 Cookie
    _p115_cookie: str = ""
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
    # 云下载辅助订阅开关（仅新电影订阅）
    _cloud_download_assist: bool = False
    # 阿里云盘秒传开关
    _ali2115_enabled: bool = False
    # 阿里云盘 Refresh Token
    _ali2115_token: str = ""
    # 阿里云盘秒传临时文件夹路径
    _ali2115_ali_folder: str = "/秒传转存"
    # 115云盘秒传接收文件夹路径
    _ali2115_115_folder: str = "/秒传接收"
    # 阿里云盘秒传后移动整理开关
    _ali2115_organize: bool = False
    # BTL鉴权参数
    _btl_app_id: str = "83768d9ad4"
    _btl_identity: str = "23734adac0301bccdcb107c4aa21f96c"

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
            # mh订阅辅助：检查频率（Cron表达式）
            self._mh_assist_cron = config.get("mh_assist_cron", "0 */6 * * *") or "0 */6 * * *"
            # 转发监听关键词同步配置
            self._mh_forwarder_whitelist_enabled = bool(config.get("mh_forwarder_whitelist_enabled", False))
            self._mh_forwarder_whitelist_listeners = config.get("mh_forwarder_whitelist_listeners", []) or []
            self._mh_forwarder_blacklist_enabled = bool(config.get("mh_forwarder_blacklist_enabled", False))
            self._mh_forwarder_blacklist_listeners = config.get("mh_forwarder_blacklist_listeners", []) or []

            self._p115_cookie = config.get("p115_cookie", "") or ""
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
            self._available_storages = self._get_available_storages()
            # 初始化时获取转发监听配置列表
            self._mh_listeners_cache = self._get_mh_listeners_for_select()
            
            # 云下载配置
            self._cloud_download_enabled = bool(config.get("cloud_download_enabled", False))
            self._cloud_download_path = config.get("cloud_download_path", "/云下载") or "/云下载"
            self._cloud_download_remove_small_files = bool(config.get("cloud_download_remove_small_files", False))
            self._cloud_download_organize = bool(config.get("cloud_download_organize", False))
            self._cloud_download_assist = bool(config.get("cloud_download_assist", False))
            # 忽略MP洗版订阅开关
            self._ignore_mp_wash = bool(config.get("ignore_mp_wash", False))
            
            if self._cloud_download_assist:
                logger.info("mhnotify: 云下载辅助订阅功能已开启（仅新电影订阅生效）")
            if self._ignore_mp_wash:
                logger.info("mhnotify: 忽略MP洗版订阅功能已开启")
            
            # 阿里云盘秒传配置
            self._ali2115_enabled = bool(config.get("ali2115_enabled", False))
            self._ali2115_token = config.get("ali2115_token", "") or ""
            self._ali2115_ali_folder = config.get("ali2115_ali_folder", "/秒传转存") or "/秒传转存"
            self._ali2115_115_folder = config.get("ali2115_115_folder", "/秒传接收") or "/秒传接收"
            self._ali2115_organize = bool(config.get("ali2115_organize", False))

            # 打印当前记录的sid信息
            try:
                pending = self.get_data(self._ASSIST_PENDING_KEY) or {}
                watch = self.get_data(self._ASSIST_WATCH_KEY) or {}
                
                if pending:
                    pending_details = []
                    with SessionFactory() as db:
                        for sid in pending.keys():
                            try:
                                sub = SubscribeOper(db=db).get(int(sid))
                                if sub:
                                    s_name = getattr(sub, 'name', '')
                                    s_tmdb = getattr(sub, 'tmdbid', '')
                                    s_season = getattr(sub, 'season', '')
                                    pending_details.append(f"[{sid}]{s_name}({s_tmdb}) S{s_season}")
                                else:
                                    pending_details.append(f"[{sid}]未知(已删?)")
                            except:
                                pending_details.append(f"[{sid}]查询失败")
                    logger.info(f"mhnotify: 当前待检查进度订阅数={len(pending)} 详情: {', '.join(pending_details)}")

                if watch:
                    watch_details = []
                    with SessionFactory() as db:
                        for sid in watch.keys():
                            try:
                                sub = SubscribeOper(db=db).get(int(sid))
                                if sub:
                                    s_name = getattr(sub, 'name', '')
                                    s_tmdb = getattr(sub, 'tmdbid', '')
                                    s_season = getattr(sub, 'season', '')
                                    watch_details.append(f"[{sid}]{s_name}({s_tmdb}) S{s_season}")
                                else:
                                    watch_details.append(f"[{sid}]未知(已删?)")
                            except:
                                watch_details.append(f"[{sid}]查询失败")
                    logger.info(f"mhnotify: 当前待完成/清理MH订阅数={len(watch)} 详情: {', '.join(watch_details)}")
            except Exception as e:
                logger.warning(f"mhnotify: 打印SID信息失败: {e}")

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
                "func": self._notify_mh,
                "kwargs": {}
            })
        # mh订阅辅助调度
        if self._mh_assist_enabled:
            services.append({
                "id": "MHAssist",
                "name": "mh订阅辅助",
                "trigger": CronTrigger.from_crontab("*/5 * * * *"),
                "func": self._assist_scheduler,
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
            },
            {
                "cmd": "/mhaly2115",
                "event": EventType.PluginAction,
                "desc": "阿里云盘分享秒传到115",
                "category": "下载",
                "data": {
                    "action": "mh_ali_to_115"
                }
            }
        ]

    def get_api(self) -> List[Dict[str, Any]]:
        return []

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """
        拼装插件配置页面，需要返回两块数据：1、页面配置；2、数据结构
        """
        # 如果存储列表为空，尝试获取一次
        if not self._available_storages:
            self._available_storages = self._get_available_storages()
        if not self._mh_listeners_cache:
            self._mh_listeners_cache = self._get_mh_listeners_for_select()
        
        # 构建默认值字典，包含现有规则
        defaults = {
            "_tabs": "tab_basic",
            "enabled": False,
            "mh_username": "",
            "mh_password": "",
            "mh_job_names": "",
            "mh_domain": "",
            "wait_minutes": 5,
            "mh_assist": False,
            "mh_assist_auto_delete": False,
            "mh_assist_cron": "0 */6 * * *",
            "mh_forwarder_whitelist_enabled": False,
            "mh_forwarder_whitelist_listeners": [],
            "mh_forwarder_blacklist_enabled": False,
            "mh_forwarder_blacklist_listeners": [],
            "ignore_mp_wash": False,
            "mp_event_enabled": False,
            "mp_event_wait_minutes": 5,
            "mp_event_storages": [],
            "cloud_download_enabled": False,
            "cloud_download_path": "/云下载",
            "cloud_download_assist": False,
            "cloud_download_remove_small_files": False,
            "cloud_download_organize": False,
            "ali2115_enabled": False,
            "ali2115_token": "",
            "ali2115_ali_folder": "/秒传转存",
            "ali2115_115_folder": "/秒传接收",
            "ali2115_organize": False
        }
        

        return [
            {
                'component': 'VForm',
                'content': [
                    {
                        'component': 'VTabs',
                        'props': {
                            'model': '_tabs',
                            'fixed-tabs': True,
                            'show-arrows': True,
                            'slider-color': 'primary'
                        },
                        'content': [
                            {'component': 'VTab', 'props': {'value': 'tab_basic'}, 'text': '基础配置'},
                            {'component': 'VTab', 'props': {'value': 'tab_monitor'}, 'text': '订阅辅助'},
                            {'component': 'VTab', 'props': {'value': 'tab_cloud'}, 'text': '云下载'},
                            {'component': 'VTab', 'props': {'value': 'tab_ali'}, 'text': '阿里云盘秒传'},
                        ]
                    },
                    {
                        'component': 'VWindow',
                        'props': {
                            'model': '_tabs'
                        },
                        'content': [
                            # Tab 0: 基础配置
                            {
                                'component': 'VWindowItem',
                                'props': {'value': 'tab_basic'},
                                'content': [
                                    {'component': 'VRow', 'content': [{'component': 'VCol', 'props': {'cols': 12, 'md': 6}, 'content': [{'component': 'VSwitch', 'props': {'model': 'enabled', 'label': '启用插件'}}]}]},
                                    {'component': 'VRow', 'content': [{'component': 'VCol', 'props': {'cols': 12, 'md': 6}, 'content': [{'component': 'VTextField', 'props': {'model': 'mh_domain', 'label': 'MediaHelper地址'}}]}, {'component': 'VCol', 'props': {'cols': 12, 'md': 6}, 'content': [{'component': 'VTextField', 'props': {'model': 'mh_username', 'label': 'MediaHelper_用户名'}}]}]},
                                    {'component': 'VRow', 'content': [{'component': 'VCol', 'props': {'cols': 12, 'md': 6}, 'content': [{'component': 'VTextField', 'props': {'model': 'mh_password', 'label': 'MediaHelper_密码', 'type': 'password'}}]}, {'component': 'VCol', 'props': {'cols': 12, 'md': 6}, 'content': [{'component': 'VTextField', 'props': {'model': 'mh_job_names', 'label': 'strm任务名称（英文逗号分隔）', 'placeholder': '例如：115网盘1,115网盘2', 'hint': '填写strm生成任务名称；留空则默认匹配名称含“115网盘”'}}]}]},
                                    {'component': 'VRow', 'content': [{'component': 'VCol', 'props': {'cols': 12}, 'content': [{'component': 'VTextField', 'props': {'model': 'p115_cookie', 'label': '115 Cookie', 'type': 'password', 'placeholder': 'UID=...; CID=...; SEID=...（粘贴完整 Cookie）', 'hint': '从 115 网页版复制完整 Cookie；仅本地使用，不会对外发送'}}]}]}
                                ]
                            },
                            # Tab 1: 订阅辅助
                            {
                                'component': 'VWindowItem',
                                'props': {'value': 'tab_monitor'},
                                'content': [
                                    {'component': 'VRow', 'content': [{'component': 'VCol', 'props': {'cols': 12, 'md': 6}, 'content': [{'component': 'VSwitch', 'props': {'model': 'mh_assist', 'label': 'mh订阅辅助（仅新订阅生效）', 'hint': '开启后，新添加的订阅将默认在MP中暂停，并由插件在MH创建订阅、延时查询进度、按规则删除或恢复MP订阅；不影响已有订阅'}}]}, {'component': 'VCol', 'props': {'cols': 12, 'md': 6}, 'content': [{'component': 'VSwitch', 'props': {'model': 'mh_assist_auto_delete', 'label': '取消或完成订阅后自动删除MH订阅', 'hint': '开启后，当MP订阅完成或取消时，自动删除或更新对应的MH115订阅。关闭则保留MH订阅'}}]}]},
                                    {'component': 'VRow', 'content': [{'component': 'VCol', 'props': {'cols': 12, 'md': 6}, 'content': [{'component': 'VSwitch', 'props': {'model': 'ignore_mp_wash', 'label': '忽略MP洗版订阅', 'hint': '开启后，当检测到MH订阅完成时，不进入洗版流程，直接完成MP订阅'}}]}]},
                                    {'component': 'VRow', 'content': [{'component': 'VCol', 'props': {'cols': 12, 'md': 6}, 'content': [{'component': 'VTextField', 'props': {'model': 'mh_assist_cron', 'label': 'MH订阅检查频率（Cron）', 'placeholder': '0 */6 * * *', 'hint': 'MH内订阅的查询触发频率，默认每6小时（0 */6 * * *）'}}]}]},
                                    {'component': 'VRow', 'content': [{'component': 'VCol', 'props': {'cols': 12, 'md': 6}, 'content': [{'component': 'VSwitch', 'props': {'model': 'mh_forwarder_whitelist_enabled', 'label': '同步订阅名到转发白名单', 'hint': '新增订阅时将媒体名称加入所选监听配置的白名单关键词，完成或取消订阅时自动移除'}}]}, {'component': 'VCol', 'props': {'cols': 12, 'md': 6}, 'content': [{'component': 'VSwitch', 'props': {'model': 'mh_forwarder_blacklist_enabled', 'label': '同步订阅名到转发黑名单', 'hint': '新增订阅时将媒体名称加入所选监听配置的黑名单关键词，完成或取消订阅时自动移除'}}]}]},
                                    {'component': 'VRow', 'content': [{'component': 'VCol', 'props': {'cols': 12, 'md': 6}, 'content': [{'component': 'VSelect', 'props': {'model': 'mh_forwarder_whitelist_listeners', 'label': '白名单监听配置（多选）', 'items': self._mh_listeners_cache or [], 'multiple': True, 'chips': True, 'closable-chips': True, 'clearable': True, 'density': 'compact', 'hint': '选择需要同步白名单关键词的TG转发监听配置'}}]}, {'component': 'VCol', 'props': {'cols': 12, 'md': 6}, 'content': [{'component': 'VSelect', 'props': {'model': 'mh_forwarder_blacklist_listeners', 'label': '黑名单监听配置（多选）', 'items': self._mh_listeners_cache or [], 'multiple': True, 'chips': True, 'closable-chips': True, 'clearable': True, 'density': 'compact', 'hint': '选择需要同步黑名单关键词的TG转发监听配置'}}]}]},
                                    {'component': 'VRow', 'content': [{'component': 'VCol', 'props': {'cols': 12, 'md': 6}, 'content': [{'component': 'VSwitch', 'props': {'model': 'mp_event_enabled', 'label': 'MP事件触发（整理/刮削完成）', 'hint': '开启后，当MP整理或刮削媒体完成时，自动通知MH执行strm生成任务（无运行任务则立即触发）'}}]}, {'component': 'VCol', 'props': {'cols': 12, 'md': 6}, 'content': [{'component': 'VTextField', 'props': {'model': 'mp_event_wait_minutes', 'label': 'MP事件等待分钟数', 'type': 'number', 'placeholder': '默认 5', 'hint': 'MP整理完成后，等待该分钟数以确保所有整理任务完成后再触发MH任务'}}]}]},
                                    {'component': 'VRow', 'content': [{'component': 'VCol', 'props': {'cols': 12}, 'content': [{'component': 'VSelect', 'props': {'model': 'mp_event_storages', 'label': '监听的存储类型', 'items': self._available_storages or [{'title': '本地', 'value': 'local'}, {'title': '115网盘', 'value': 'u115'}, {'title': '阿里云盘', 'value': 'alipan'}, {'title': 'RClone', 'value': 'rclone'}, {'title': 'OpenList', 'value': 'alist'}], 'multiple': True, 'chips': True, 'closable-chips': True, 'clearable': True, 'density': 'compact', 'hint': '留空则监听所有存储类型的整理/刮削事件'}}]}]}
                                ]
                            },
                            {
                                'component': 'VWindowItem',
                                'props': {'value': 'tab_cloud'},
                                'content': [
                                    {'component': 'VRow', 'content': [{'component': 'VCol', 'props': {'cols': 12, 'md': 6}, 'content': [{'component': 'VSwitch', 'props': {'model': 'cloud_download_enabled', 'label': '启用115云下载功能', 'hint': '开启后，可使用 /mhol 命令添加115离线下载任务'}}]}, {'component': 'VCol', 'props': {'cols': 12, 'md': 6}, 'content': [{'component': 'VTextField', 'props': {'model': 'cloud_download_path', 'label': '115云下载保存路径', 'placeholder': '/云下载', 'hint': '115网盘中保存离线下载文件的目录路径'}}]}]},
                                    {'component': 'VRow', 'content': [{'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [{'component': 'VSwitch', 'props': {'model': 'cloud_download_remove_small_files', 'label': '剔除小文件', 'hint': '云下载完成后自动删除小于10MB的文件', 'persistent-hint': True}}]}, {'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [{'component': 'VSwitch', 'props': {'model': 'cloud_download_organize', 'label': '移动整理', 'hint': '云下载完成后自动移动到MH默认目录并整理', 'persistent-hint': True}}]}, {'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [{'component': 'VSwitch', 'props': {'model': 'cloud_download_assist', 'label': '云下载辅助订阅（仅新电影订阅）', 'hint': '新订阅电影未完成时按质量优先级自动匹配资源并触发云下载；失败则恢复订阅启用'}}]}]},
                                    {'component': 'VRow', 'content': [{'component': 'VCol', 'props': {'cols': 12}, 'content': [{'component': 'VAlert', 'props': {'type': 'info', 'variant': 'tonal', 'density': 'comfortable', 'text': '/mhol — 添加115云下载任务；传入磁力链接，保存到配置的云下载路径。支持多个链接，用英文逗号、空格或换行分隔。'}}]}]}
                                ]
                            },
                            # Tab 4: 阿里云盘秒传
                            {
                                'component': 'VWindowItem',
                                'props': {'value': 'tab_ali'},
                                'content': [
                                    {'component': 'VRow', 'content': [{'component': 'VCol', 'props': {'cols': 12, 'md': 6}, 'content': [{'component': 'VSwitch', 'props': {'model': 'ali2115_enabled', 'label': '启用阿里云盘秒传', 'hint': '开启后，可使用 /mhaly2115 命令将阿里云盘分享秒传到115'}}]}, {'component': 'VCol', 'props': {'cols': 12, 'md': 6}, 'content': [{'component': 'VSwitch', 'props': {'model': 'ali2115_organize', 'label': '秒传后移动整理', 'hint': '秒传完成后自动移动到MH默认目录并整理', 'persistent-hint': True}}]}]},
                                    {'component': 'VRow', 'content': [{'component': 'VCol', 'props': {'cols': 12, 'md': 6}, 'content': [{'component': 'VTextField', 'props': {'model': 'ali2115_ali_folder', 'label': '阿里云盘临时文件夹', 'placeholder': '/秒传转存', 'hint': '阿里云盘中用于临时转存分享文件的目录'}}]}, {'component': 'VCol', 'props': {'cols': 12, 'md': 6}, 'content': [{'component': 'VTextField', 'props': {'model': 'ali2115_115_folder', 'label': '115云盘秒传接收文件夹', 'placeholder': '/秒传接收', 'hint': '115网盘中接收秒传文件的目录'}}]}]},
                                    {'component': 'VRow', 'content': [{'component': 'VCol', 'props': {'cols': 12}, 'content': [{'component': 'VTextField', 'props': {'model': 'ali2115_token', 'label': '阿里云盘 Refresh Token', 'type': 'password', 'placeholder': '输入阿里云盘的 refresh_token', 'hint': '从阿里云盘客户端或浏览器获取的 refresh_token，用于认证阿里云盘API'}}]}]},
                                    {'component': 'VRow', 'content': [{'component': 'VCol', 'props': {'cols': 12}, 'content': [{'component': 'VAlert', 'props': {'type': 'info', 'variant': 'tonal', 'density': 'comfortable', 'text': '/mhaly2115 — 阿里云盘分享秒传到115；需已配置阿里云盘Refresh Token。'}}]}]}
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
        self._last_event_time = self._get_time()
        
        # 检查是否有正在运行的整理任务
        if self._has_running_transfers():
            logger.info("mhnotify: 检测到正在运行的整理任务，延迟触发")
            # 设置等待窗口
            now_ts = self._get_time()
            wait_seconds = self._mp_event_wait_minutes * 60
            self._next_notify_time = now_ts + wait_seconds
        else:
            logger.info("mhnotify: 无运行中的整理任务，将在下次调度时立即触发")
            # 清零等待时间，下次调度立即触发
            self._next_notify_time = 0

    def stop_service(self):
        """
        退出插件
        """
        pass

    def _get_time(self):
        return int(time.time())

    def _get_available_storages(self) -> List[Dict[str, str]]:
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

    def _get_mh_listeners_for_select(self) -> List[Dict[str, Any]]:
        """获取TG转发监听配置列表，格式化为下拉选项"""
        try:
            if not self._mh_domain or not self._mh_username:
                return []
            token = self._mh_login()
            if not token:
                return []
            listeners = self._mh_get_listeners(token)
            return [
                {"title": l.get("source_name") or l.get("id"), "value": l.get("id")}
                for l in listeners if l.get("id")
            ]
        except Exception:
            logger.warning("mhnotify: 获取转发监听配置列表失败", exc_info=True)
            return []

    def _has_running_transfers(self) -> bool:
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

    def _notify_mh(self):
        try:
            # 当有待通知时，根据是否存在运行中整理任务决定立即触发或进入等待窗口
            now_ts = self._get_time()
            if self._wait_notify_count > 0:
                # 若启用 MP 事件触发，检查 MP 事件等待窗口
                if self._mp_event_enabled and self._next_notify_time:
                    if now_ts < self._next_notify_time:
                        # 如果仍有运行中的整理任务，延长等待时间
                        if self._has_running_transfers():
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

