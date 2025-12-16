import re
import threading
import time
import json
from typing import Any, List, Dict, Tuple, Optional
from jinja2 import Template

from app.core.cache import cached
from app.core.config import settings
from app.core.event import eventmanager, Event
from app.helper.mediaserver import MediaServerHelper
from app.log import logger
from app.modules.themoviedb import CategoryHelper
from app.plugins import _PluginBase
from app.schemas import WebhookEventInfo, ServiceInfo
from app.schemas.types import EventType, MediaType, MediaImageType, NotificationType
from app.utils.web import WebUtils


class MediaServerMsgModify(_PluginBase):
    """
    媒体服务器通知插件

    功能：
    1. 监听Emby/Jellyfin/Plex等媒体服务器的Webhook事件
    2. 根据配置发送播放、入库等通知消息
    3. 对TV剧集入库事件进行智能聚合，避免消息轰炸
    4. 支持多种媒体服务器和丰富的消息类型配置
    5. 可选自定义入库消息模板
    """

    # 常量定义
    DEFAULT_EXPIRATION_TIME = 600                  # 默认过期时间（秒）
    DEFAULT_AGGREGATE_TIME = 15                   # 默认聚合时间（秒）

    # 插件基本信息
    plugin_name = "媒体库服务器通知-修改版"
    # 插件描述
    plugin_desc = "发送Emby/Jellyfin/Plex服务器的播放、入库等通知消息。"
    # 插件图标
    plugin_icon = "mediaplay.png"
    # 插件版本
    plugin_version = "0.4"
    # 插件作者
    plugin_author = "ListeningLTG"
    # 作者主页
    author_url = "https://github.com/ListeningLTG"
    # 插件配置项ID前缀
    plugin_config_prefix = "mediaservermsgmodify_"
    # 加载顺序
    plugin_order = 14
    # 可使用的用户级别
    auth_level = 1

    # 插件运行时状态配置
    _enabled = False                           # 插件是否启用
    _add_play_link = False                     # 是否添加播放链接
    _mediaservers = None                       # 媒体服务器列表
    _types = []                                # 启用的消息类型
    _webhook_msg_keys = {}                     # Webhook消息去重缓存
    _aggregate_enabled = True                   # 是否启用TV剧集聚合功能

    # TV剧集消息聚合配置
    _aggregate_time = DEFAULT_AGGREGATE_TIME   # 聚合时间窗口（秒）
    _pending_messages = {}                     # 待聚合的消息 {series_key: [event_info, ...]}
    _aggregate_timers = {}                     # 聚合定时器 {series_key: timer}

    # Webhook事件映射配置
    _webhook_actions = {
        "library.new": "新入库",
        "system.notificationtest": "测试",
        "playback.start": "开始播放",
        "playback.stop": "停止播放",
        "user.authenticated": "登录成功",
        "user.authenticationfailed": "登录失败",
        "media.play": "开始播放",
        "media.stop": "停止播放",
        "PlaybackStart": "开始播放",
        "PlaybackStop": "停止播放",
        "item.rate": "标记了"
    }

    # 媒体服务器默认图标
    _webhook_images = {
        "emby": "https://emby.media/notificationicon.png",
        "plex": "https://www.plex.tv/wp-content/uploads/2022/04/new-logo-process-lines-gray.png",
        "jellyfin": "https://play-lh.googleusercontent.com/SCsUK3hCCRqkJbmLDctNYCfehLxsS4ggD1ZPHIFrrAN1Tn9yhjmGMPep2D9lMaaa9eQi"
    }

    def __init__(self):
        super().__init__()
        self.category = CategoryHelper()
        logger.debug("媒体服务器消息插件初始化完成")

    def init_plugin(self, config: dict = None):
        """
        初始化插件配置

        Args:
            config (dict, optional): 插件配置参数
        """
        if config:
            self._enabled = config.get("enabled")
            self._types = config.get("types") or []
            self._mediaservers = config.get("mediaservers") or []
            self._add_play_link = config.get("add_play_link", False)
            self._aggregate_enabled = config.get("aggregate_enabled", False)
            self._aggregate_time = int(config.get("aggregate_time", self.DEFAULT_AGGREGATE_TIME))
            # 可选：自定义新入库消息的 Jinja2 模板
            self._library_new_template = config.get("library_new_template") or ""
            # 模板调试日志开关（默认开启，便于调试，可在UI中关闭）
            self._template_debug = bool(config.get("template_debug", True))


    def service_infos(self, type_filter: Optional[str] = None) -> Optional[Dict[str, ServiceInfo]]:
        """
        获取媒体服务器信息服务信息

        Args:
            type_filter (str, optional): 媒体服务器类型过滤器

        Returns:
            Dict[str, ServiceInfo]: 活跃的媒体服务器服务信息字典
        """
        if not self._mediaservers:
            logger.warning("尚未配置媒体服务器，请检查配置")
            return None

        services = MediaServerHelper().get_services(type_filter=type_filter, name_filters=self._mediaservers)
        if not services:
            logger.warning("获取媒体服务器实例失败，请检查配置")
            return None

        active_services = {}
        for service_name, service_info in services.items():
            if service_info.instance.is_inactive():
                logger.warning(f"媒体服务器 {service_name} 未连接，请检查配置")
            else:
                active_services[service_name] = service_info

        if not active_services:
            logger.warning("没有已连接的媒体服务器，请检查配置")
            return None

        return active_services

    def service_info(self, name: str) -> Optional[ServiceInfo]:
        """
        根据名称获取特定媒体服务器服务信息

        Args:
            name (str): 媒体服务器名称

        Returns:
            ServiceInfo: 媒体服务器服务信息
        """
        service_infos = self.service_infos() or {}
        return service_infos.get(name)

    def get_state(self) -> bool:
        """
        获取插件状态

        Returns:
            bool: 插件是否启用
        """
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """
        获取插件命令
        （当前未实现）

        Returns:
            List[Dict[str, Any]]: 空列表
        """
        pass

    def get_api(self) -> List[Dict[str, Any]]:
        """
        获取插件API
        （当前未实现）

        Returns:
            List[Dict[str, Any]]: 空列表
        """
        pass

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """
        拼装插件配置页面，需要返回两块数据：1、页面配置；2、数据结构
        """
        types_options = [
            {"title": "新入库", "value": "library.new"},
            {"title": "开始播放", "value": "playback.start|media.play|PlaybackStart"},
            {"title": "停止播放", "value": "playback.stop|media.stop|PlaybackStop"},
            {"title": "用户标记", "value": "item.rate"},
            {"title": "测试", "value": "system.webhooktest"},
            {"title": "登录成功", "value": "user.authenticated"},
            {"title": "登录失败", "value": "user.authenticationfailed"},
        ]
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
                                            'model': 'add_play_link',
                                            'label': '添加播放链接',
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
                                        'component': 'VSelect',
                                        'props': {
                                            'multiple': True,
                                            'chips': True,
                                            'clearable': True,
                                            'model': 'mediaservers',
                                            'label': '媒体服务器',
                                            'items': [{"title": config.name, "value": config.name}
                                                      for config in MediaServerHelper().get_configs().values()]
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
                                        'component': 'VSelect',
                                        'props': {
                                            'chips': True,
                                            'multiple': True,
                                            'model': 'types',
                                            'label': '消息类型',
                                            'items': types_options
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
                                        'component': 'VTextarea',
                                        'props': {
                                            'model': 'library_new_template',
                                            'label': '自定义新入库消息模板（Jinja2）',
                                            'rows': 6,
                                            'placeholder': '例如：\n标题：{{ item_name }}\n季集：{{ episodes_detail }}\n评分：{{ tmdb.vote_average | round(1) if tmdb.vote_average }}\n剧情：{{ overview }}',
                                                'hint': '可选。如果填写，将用于渲染 library.new 的消息内容。支持两种格式：\n1) 纯文本模板（渲染为消息正文）\n2) JSON 模板：{"title": "...", "text": ["...", "..."]}，分别渲染标题与正文行。可用变量：event、item_name、item_type、tmdb、overview、episodes_detail、count、is_multiple、category、user_name、device_name、ip、location、time。'
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
                                            'model': 'template_debug',
                                            'label': '模板调试日志',
                                            'hint': '开启后将在渲染前输出模板上下文（去敏）到日志，便于排查变量。'
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
                                            'model': 'aggregate_enabled',
                                            'label': '启用TV剧集结入库聚合',
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'props': {'show': '{{aggregate_enabled}}'},
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
                                            'model': 'aggregate_time',
                                            'label': 'TV剧集结入库聚合时间（秒）',
                                            'placeholder': '15'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'props': {'show': '{{aggregate_enabled}}'},
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
                                            'type': 'warning',
                                            'variant': 'tonal',
                                            'text': '请在整理刮削设置中添加tmdbid,以保证准确性。仅保证在Emby和整理刮削添加tmdbid后功能正常。'
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
                                            'text': '需要设置媒体服务器Webhook，回调相对路径为 /api/v1/webhook?token=API_TOKEN&source=媒体服务器名（3001端口），其中 API_TOKEN 为设置的 API_TOKEN。'
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
            "types": [],
            "aggregate_enabled": False,
            "aggregate_time": 15,
            "library_new_template": "",
            "template_debug": True
        }

    def get_page(self) -> List[dict]:
        """
        获取插件页面
        （当前未实现）

        Returns:
            List[dict]: 空列表
        """
        pass

    @eventmanager.register(EventType.WebhookMessage)
    def send(self, event: Event):
        """
        发送通知消息主入口函数
        处理来自媒体服务器的Webhook事件，并根据配置决定是否发送通知消息

        处理流程：
        1. 检查插件是否启用
        2. 验证事件数据有效性
        3. 检查事件类型是否在支持范围内
        4. 检查事件类型是否在用户配置的允许范围内
        5. 验证媒体服务器配置
        6. 特殊处理TV剧集入库事件（聚合处理）
        7. 处理常规消息事件
        8. 构造并发送通知消息

        Args:
            event (Event): Webhook事件对象
        """
        # 检查插件是否启用
        if not self._enabled:
            logger.debug("插件未启用")
            return

        # 获取事件数据
        event_info: WebhookEventInfo = event.event_data
        if not event_info:
            logger.debug("事件数据为空")
            return

        # 打印event_info用于调试（对象repr + 属性JSON）
        logger.debug(f"收到Webhook事件: {event_info}")
        try:
            info_dict = self._event_info_to_dict(event_info)
            import json as _json
            logger.info(f"WebhookEventInfo 属性: {_json.dumps(info_dict, ensure_ascii=False, indent=2)}")
        except Exception as e:
            logger.warning(f"打印事件属性失败: {e}")

        # 检查事件类型是否在支持范围内
        if not self._webhook_actions.get(event_info.event):
            logger.debug(f"事件类型 {event_info.event} 不在支持范围内")
            return

        # 检查事件类型是否在用户配置的允许范围内
        # 将配置的类型预处理为一个扁平集合，提高查找效率
        allowed_types = set()
        for _type in self._types:
            allowed_types.update(_type.split("|"))

        if event_info.event not in allowed_types:
            logger.info(f"未开启 {event_info.event} 类型的消息通知")
            return

        # 验证媒体服务器配置
        if not self.service_infos():
            logger.info(f"未开启任一媒体服务器的消息通知")
            return

        if event_info.server_name and not self.service_info(name=event_info.server_name):
            logger.info(f"未开启媒体服务器 {event_info.server_name} 的消息通知")
            return

        if event_info.channel and not self.service_infos(type_filter=event_info.channel):
            logger.info(f"未开启媒体服务器类型 {event_info.channel} 的消息通知")
            return

        # TV剧集结入库聚合处理
        logger.debug("检查是否需要进行TV剧集聚合处理")
        logger.debug(f"event_info.event={event_info.event}, item_type={event_info.item_type}")
        logger.debug(f"json_object存在: {bool(event_info.json_object)}, 类型: {type(event_info.json_object)}")

        # 判断是否需要进行TV剧集入库聚合处理
        if (self._aggregate_enabled and
                event_info.event == "library.new" and
                event_info.item_type in ["TV", "SHOW"] and
                event_info.json_object and
                isinstance(event_info.json_object, dict)):

            logger.debug("满足TV剧集聚合条件，尝试获取series_id")
            series_id = self._get_series_id(event_info)
            logger.debug(f"获取到的series_id: {series_id}")
            if series_id:
                logger.debug(f"开始聚合处理，series_id={series_id}")
                self._aggregate_tv_episodes(series_id, event_info)
                logger.debug("TV剧集消息已处理并返回")
                return  # TV剧集消息已处理，直接返回
            else:
                logger.debug("未能获取到有效的series_id")

        logger.debug("未进行聚合处理，继续普通消息处理流程")
        expiring_key = f"{event_info.item_id}-{event_info.client}-{event_info.user_name}"
        # 过滤停止播放重复消息
        if str(event_info.event) == "playback.stop" and expiring_key in self._webhook_msg_keys.keys():
            # 刷新过期时间
            self.__add_element(expiring_key)
            return

        # 构造消息标题
        if event_info.item_type in ["TV", "SHOW"]:
            message_title = f"{self._webhook_actions.get(event_info.event)}剧集 {event_info.item_name}"
        elif event_info.item_type == "MOV":
            message_title = f"{self._webhook_actions.get(event_info.event)}电影 {event_info.item_name}"
        elif event_info.item_type == "AUD":
            message_title = f"{self._webhook_actions.get(event_info.event)}有声书 {event_info.item_name}"
        else:
            message_title = f"{self._webhook_actions.get(event_info.event)}"

        # 构造消息内容
        message_texts = []
        if event_info.user_name:
            message_texts.append(f"用户：{event_info.user_name}")
        if event_info.device_name:
            message_texts.append(f"设备：{event_info.client} {event_info.device_name}")
        if event_info.ip:
            message_texts.append(f"IP地址：{event_info.ip} {WebUtils.get_location(event_info.ip)}")
        if event_info.percentage:
            percentage = round(float(event_info.percentage), 2)
            message_texts.append(f"进度：{percentage}%")
        if event_info.overview:
            message_texts.append(f"剧情：{event_info.overview}")
        message_texts.append(f"时间：{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))}")

        # 消息内容
        message_content = "\n".join(message_texts)

        # 处理消息图片
        image_url = event_info.image_url
        if not image_url and event_info.tmdb_id: 
            # 查询电影图片
            if event_info.item_type == "MOV" :
                image_url = self.chain.obtain_specific_image(
                    mediaid=event_info.tmdb_id,
                    mtype=MediaType.MOVIE,
                    image_type=MediaImageType.Poster
                )

            # 查询剧集图片
            elif event_info.item_type in ["TV", "SHOW"]:
                season_id = event_info.season_id if event_info.season_id else None
                episode_id = event_info.episode_id if event_info.episode_id else None

                specific_image = self.chain.obtain_specific_image(
                    mediaid=event_info.tmdb_id,
                    mtype=MediaType.TV,
                    image_type=MediaImageType.Backdrop,
                    season=season_id,
                    episode=episode_id
                )
                if specific_image:
                    image_url = specific_image
        # 使用默认图片
        if not image_url:
            image_url = self._webhook_images.get(event_info.channel)

        # 处理播放链接
        play_link = None
        if self._add_play_link:
            play_link = self._get_play_link(event_info)

        # 更新播放状态缓存
        if str(event_info.event) == "playback.stop":
            # 停止播放消息，添加到过期字典
            self.__add_element(expiring_key)
        if str(event_info.event) == "playback.start":
            # 开始播放消息，删除过期字典
            self.__remove_element(expiring_key)

        # 发送消息
        # 如果是新入库且配置了模板，尝试用模板渲染内容
        if event_info.event == "library.new" and getattr(self, "_library_new_template", None):
            try:
                # 非聚合路径，基于 event_info 构建上下文
                tmdb_info = None
                cat = None
                overview = getattr(event_info, 'overview', None)
                if getattr(event_info, 'tmdb_id', None):
                    try:
                        if event_info.item_type in ["TV", "SHOW"]:
                            tmdb_info = self._get_tmdb_info(tmdb_id=event_info.tmdb_id, mtype=MediaType.TV,
                                                            season=getattr(event_info, 'season_id', None))
                        elif event_info.item_type == "MOV":
                            tmdb_info = self._get_tmdb_info(tmdb_id=event_info.tmdb_id, mtype=MediaType.MOVIE)
                    except Exception:
                        tmdb_info = None
                if tmdb_info:
                    try:
                        if tmdb_info.get('media_type') == MediaType.TV:
                            cat = self.category.get_tv_category(tmdb_info)
                        else:
                            cat = self.category.get_movie_category(tmdb_info)
                    except Exception:
                        cat = None

                # 解析标题中的年份
                title_year = None
                if getattr(event_info, 'item_name', None):
                    m = re.search(r"\((\d{4})\)", event_info.item_name)
                    if m:
                        title_year = m.group(1)
                if not title_year and tmdb_info:
                    date_field = tmdb_info.get('release_date') or tmdb_info.get('first_air_date')
                    if date_field:
                        m2 = re.match(r"(\d{4})", str(date_field))
                        if m2:
                            title_year = m2.group(1)

                # 季集展示（非聚合场景）
                season_episode = None
                s = getattr(event_info, 'season_id', None)
                e = getattr(event_info, 'episode_id', None)
                if s is not None and e is not None:
                    try:
                        season_episode = f"S{int(s):02d}E{int(e):02d}"
                    except Exception:
                        season_episode = None

                # 类型映射
                type_map = {"MOV": "电影", "TV": "剧集", "SHOW": "剧集", "AUD": "有声书"}
                media_type = type_map.get(getattr(event_info, 'item_type', ''), getattr(event_info, 'item_type', ''))

                # 扩展上下文（缺失字段留空）
                # 文件与质量信息
                file_count, total_size_bytes = self._extract_file_metrics(event_info)
                file_name = None
                jo = getattr(event_info, 'json_object', None)
                if isinstance(jo, dict):
                    item = jo.get('Item') or jo
                    file_name = item.get('FileName') or item.get('Name')
                if not file_name:
                    file_name = getattr(event_info, 'item_name', None)
                resource_quality = self._parse_quality(file_name or "")
                release_group = self._parse_release_group(file_name or "")
                time_usage = self._calc_time_usage_movie(event_info, tmdb_info or {})

                extras = {
                    "episodes_detail": season_episode or "",
                    "season_episode": season_episode or "",
                    "count": 1,
                    "is_multiple": False,
                    "category": cat,
                    "overview": overview,
                    "title_year": title_year,
                    "tmdbid": getattr(event_info, 'tmdb_id', None),
                    "vote_average": (tmdb_info or {}).get('vote_average') if tmdb_info else None,
                    "media_type": media_type,
                    "resource_quality": resource_quality,
                    "file_count": file_count,
                    "total_size": self._human_size(total_size_bytes),
                    "release_group": release_group,
                    "time_usage": time_usage,
                    "err_msg": None,
                }

                context = self._build_template_context(event=event_info, tmdb=tmdb_info or {}, extras=extras)
                if getattr(self, "_template_debug", False):
                    self._log_template_context(context, where="单条 library.new")
                tpl = self._library_new_template.strip()
                rendered = None
                if (tpl.startswith('{') and tpl.endswith('}')):
                    try:
                        parsed = json.loads(tpl)
                        if isinstance(parsed, dict):
                            if 'title' in parsed and isinstance(parsed['title'], str):
                                try:
                                    message_title = Template(parsed['title']).render(context)
                                except Exception as e:
                                    logger.warning(f"渲染模板标题失败，使用默认标题: {e}")
                            text_lines = []
                            if 'text' in parsed:
                                if isinstance(parsed['text'], list):
                                    for line in parsed['text']:
                                        if isinstance(line, str):
                                            try:
                                                text_lines.append(Template(line).render(context))
                                            except Exception as e:
                                                logger.warning(f"渲染模板文本行失败，跳过: {e}")
                                elif isinstance(parsed['text'], str):
                                    try:
                                        text_lines.append(Template(parsed['text']).render(context))
                                    except Exception as e:
                                        logger.warning(f"渲染模板文本失败，跳过: {e}")
                            rendered = "\n".join([l for l in text_lines if l])
                    except Exception as e:
                        logger.warning(f"JSON 模板解析失败（原始模板），将先渲染后再解析：{e}")
                        # 先进行 Jinja 渲染，再尝试按 JSON 解析一次
                        try:
                            rendered_tpl = Template(tpl).render(context)
                            parsed2 = json.loads(rendered_tpl)
                            if isinstance(parsed2, dict):
                                if 'title' in parsed2 and isinstance(parsed2['title'], str):
                                    try:
                                        message_title = Template(parsed2['title']).render(context)
                                    except Exception as e:
                                        logger.warning(f"渲染模板标题失败，使用默认标题: {e}")
                                text_lines = []
                                if 'text' in parsed2:
                                    if isinstance(parsed2['text'], list):
                                        for line in parsed2['text']:
                                            if isinstance(line, str):
                                                try:
                                                    text_lines.append(Template(line).render(context))
                                                except Exception as e:
                                                    logger.warning(f"渲染模板文本行失败，跳过: {e}")
                                    elif isinstance(parsed2['text'], str):
                                        try:
                                            text_lines.append(Template(parsed2['text']).render(context))
                                        except Exception as e:
                                            logger.warning(f"渲染模板文本失败，跳过: {e}")
                                rendered = "\n".join([l for l in text_lines if l])
                            else:
                                rendered = rendered_tpl
                        except Exception as e2:
                            logger.warning(f"渲染后 JSON 解析失败，将按纯文本模板处理：{e2}")
                            rendered = Template(tpl).render(context)
                else:
                    rendered = Template(tpl).render(context)

                if rendered:
                    message_content = rendered
            except Exception as e:
                logger.warning(f"渲染自定义模板失败，回退默认内容: {e}")

        self.post_message(mtype=NotificationType.MediaServer,
                          title=message_title, text=message_content, image=image_url, link=play_link)

    def _get_series_id(self, event_info: WebhookEventInfo) -> Optional[str]:
        """
        获取剧集ID，用于TV剧集消息聚合

        优先级顺序：
        1. 从JSON对象的Item中获取SeriesId
        2. 从JSON对象的Item中获取SeriesName（作为备选）
        3. 从event_info中直接获取series_id（fallback方案）

        Args:
            event_info (WebhookEventInfo): Webhook事件信息

        Returns:
            Optional[str]: 剧集ID或None（如果无法获取）
        """
        # 从json_object中提取series_id
        if event_info.json_object and isinstance(event_info.json_object, dict):
            item = event_info.json_object.get("Item", {})
            series_id = item.get("SeriesId") or item.get("SeriesName")
            if series_id:
                return series_id

        # fallback到event_info中的series_id
        return getattr(event_info, "series_id", None)

    def _aggregate_tv_episodes(self, series_id: str, event_info: WebhookEventInfo):
        """
        聚合TV剧集结入库消息

        当同一剧集的多集在短时间内入库时，将它们聚合为一条消息发送，
        避免消息轰炸。通过设置定时器实现延迟发送，定时器时间内到达的
        同剧集消息会被聚合在一起。

        Args:
            series_id (str): 剧集ID
            event_info (WebhookEventInfo): Webhook事件信息
        """
        try:
            logger.debug(f"开始执行聚合处理: series_id={series_id}")
            # 初始化该series_id的消息列表
            if series_id not in self._pending_messages:
                logger.debug(f"为series_id={series_id}初始化消息列表")
                self._pending_messages[series_id] = []

            # 添加消息到待处理列表
            logger.debug(f"添加消息到待处理列表: series_id={series_id}")
            self._pending_messages[series_id].append(event_info)

            # 如果已经有定时器，取消它并重新设置
            if series_id in self._aggregate_timers:
                logger.debug(f"取消已存在的定时器: {series_id}")
                self._aggregate_timers[series_id].cancel()

            # 设置新的定时器
            logger.debug(f"设置新的定时器，将在 {self._aggregate_time} 秒后触发")
            timer = threading.Timer(self._aggregate_time, self._send_aggregated_message, [series_id])
            self._aggregate_timers[series_id] = timer
            timer.start()

            logger.debug(f"已添加剧集 {series_id} 的消息到聚合队列，当前队列长度: {len(self._pending_messages[series_id])}，定时器将在 {self._aggregate_time} 秒后触发")
            logger.debug(f"完成聚合处理: series_id={series_id}")
        except Exception as e:
            logger.error(f"聚合处理过程中出现异常: {str(e)}", exc_info=True)

    def _send_aggregated_message(self, series_id: str):
        """
        发送聚合后的TV剧集消息

        当聚合定时器到期或插件退出时调用此方法，将累积的同剧集消息
        合并为一条消息发送给用户。

        Args:
            series_id (str): 剧集ID
        """
        logger.debug(f"定时器触发，准备发送聚合消息: {series_id}")

        # 获取该series_id的所有待处理消息
        if series_id not in self._pending_messages or not self._pending_messages[series_id]:
            logger.debug(f"消息队列为空或不存在: {series_id}")
            # 清除定时器引用
            if series_id in self._aggregate_timers:
                del self._aggregate_timers[series_id]
            return

        events = self._pending_messages.pop(series_id)
        logger.debug(f"从队列中获取 {len(events)} 条消息: {series_id}")
        # 清除定时器引用
        if series_id in self._aggregate_timers:
            del self._aggregate_timers[series_id]

        # 构造聚合消息
        if not events:
            logger.debug(f"事件列表为空: {series_id}")
            return

        # 使用第一个事件的信息作为基础
        first_event = events[0]

        # 预计算事件数量，避免重复调用len(events)
        events_count = len(events)
        is_multiple_episodes = events_count > 1

        # 尝试从item_path中提取tmdb_id
        tmdb_pattern = r'[\[{](?:tmdbid|tmdb)[=-](\d+)[\]}]'
        if match := re.search(tmdb_pattern, first_event.item_path):
            first_event.tmdb_id = match.group(1)
            logger.info(f"从路径提取到tmdb_id: {first_event.tmdb_id}")
        else:
            logger.info(f"未从路径中提取到tmdb_id: {first_event.item_path}")
        # 通过TMDB ID获取详细信息
        tmdb_info = None
        overview = None
        try:
            if not first_event.tmdb_id:
                logger.debug("tmdb_id为空，使用原有逻辑发送消息")
                # 使用原有逻辑构造消息
                message_title = f"📺 {self._webhook_actions.get(first_event.event)}剧集：{first_event.item_name}"
                message_texts = []
                message_texts.append(f"⏰ 时间：{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))}")

                # 收集集数信息
                episode_details = []
                for event in events:
                    if event.season_id is not None and event.episode_id is not None:
                        episode_details.append(f"S{int(event.season_id):02d}E{int(event.episode_id):02d}")

                if episode_details:
                    message_texts.append(f"📺 季集：{', '.join(episode_details)}")

                message_content = "\n".join(message_texts)

                # 使用默认图片
                image_url = first_event.image_url or self._webhook_images.get(first_event.channel)

                # 处理播放链接
                play_link = None
                if self._add_play_link:
                    play_link = self._get_play_link(first_event)

                # 发送消息
                self.post_message(mtype=NotificationType.MediaServer,
                                    title=message_title,
                                    text=message_content,
                                    image=image_url,
                                    link=play_link)
                return
            if first_event.item_type in ["TV", "SHOW"]:
                logger.debug("查询TV类型的TMDB信息")
                tmdb_info = self._get_tmdb_info(
                    tmdb_id=first_event.tmdb_id,
                    mtype=MediaType.TV,
                    season=first_event.season_id
                )
            logger.debug(f"从TMDB获取到的信息: {tmdb_info}")
        except Exception as e:
            logger.debug(f"获取TMDB信息时出错: {str(e)}")

        if first_event.overview:
            overview = first_event.overview
        elif tmdb_info:
            if is_multiple_episodes:
                if tmdb_info.get('overview'):
                    overview = tmdb_info.get('overview')
                    logger.debug(f"从TMDB获取到overview: {overview}")
                else:
                    logger.debug("未能从TMDB获取到有效的overview信息")
            else:
                if (tmdb_info.get('episodes') and tmdb_info.get('episodes')[int(first_event.episode_id)-1]
                        and tmdb_info.get('episodes')[int(first_event.episode_id)-1].get('overview')):
                    overview = tmdb_info.get('episodes')[int(first_event.episode_id)-1].get('overview')
                elif tmdb_info.get('overview'):
                    overview = tmdb_info.get('overview')
                else:
                    logger.debug("未能从TMDB获取到有效的overview信息")
        else:
            logger.debug("未能从TMDB获取到有效的overview信息")

        events[0] = first_event
        # 消息标题
        message_title = f"📺 {self._webhook_actions.get(first_event.event)}剧集：{first_event.item_name.split(' ', 1)[0]}"

        if is_multiple_episodes:
            message_title += f" 等{events_count}个文件"

        logger.debug(f"构建消息标题: {message_title}")

        # 消息内容（默认构建 + 可选模板渲染）
        episodes_detail = self._merge_continuous_episodes(events)
        # 确定二级分类
        cat = None
        if tmdb_info and tmdb_info.get('media_type') == MediaType.TV:
            cat = self.category.get_tv_category(tmdb_info)
        elif tmdb_info:
            cat = self.category.get_movie_category(tmdb_info)

        default_texts = []
        default_texts.append(f"⏰ 时间：{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))}")
        default_texts.append(f"📺 季集：{episodes_detail}")
        if cat:
            default_texts.append(f"📚 分类：{cat}")
        if tmdb_info and tmdb_info.get('vote_average'):
            rating = round(float(tmdb_info.get('vote_average')), 1)
            default_texts.append(f"⭐ 评分：{rating}/10")
            if tmdb_info.get('genres'):
                genres_list = []
                for genre in tmdb_info.get('genres')[:3]:
                    if isinstance(genre, dict):
                        genres_list.append(genre.get('name', ''))
                    else:
                        genres_list.append(str(genre))
                if genres_list:
                    default_texts.append(f"🎭 类型：{'、'.join(genres_list)}")
        if overview:
            if len(overview) > 100:
                overview = overview[:100] + "..."
            default_texts.append(f"📖 剧情：{overview}")

        message_content = "\n".join(default_texts)

        # 如果配置了模板，使用模板渲染内容
        if getattr(self, "_library_new_template", None):
            # 资源与文件信息（聚合）
            # 使用第一个事件的文件名推断质量/发布组
            file_name = None
            jo0 = getattr(first_event, 'json_object', None)
            if isinstance(jo0, dict):
                item0 = jo0.get('Item') or jo0
                file_name = item0.get('FileName') or item0.get('Name')
            if not file_name:
                file_name = getattr(first_event, 'item_name', None)
            resource_quality = self._parse_quality(file_name or "")
            release_group = self._parse_release_group(file_name or "")

            # 汇总大小
            total_size_bytes = 0
            any_size = False
            for ev in events:
                cnt_i, size_i = self._extract_file_metrics(ev)
                if isinstance(size_i, int):
                    total_size_bytes += size_i
                    any_size = True
            total_size_str = self._human_size(total_size_bytes) if any_size else None

            # 预计总时长
            time_usage = self._calc_time_usage_tv(events, tmdb_info or {})

            context = self._build_template_context(
                event=first_event,
                tmdb=tmdb_info or {},
                extras={
                    "episodes_detail": episodes_detail,
                    "count": events_count,
                    "is_multiple": is_multiple_episodes,
                    "category": cat,
                    "overview": overview,
                    "resource_quality": resource_quality,
                    "file_count": events_count,
                    "total_size": total_size_str,
                    "release_group": release_group,
                    "time_usage": time_usage,
                }
            )
            if getattr(self, "_template_debug", False):
                self._log_template_context(context, where="聚合 library.new")
            try:
                tpl = self._library_new_template.strip()
                if tpl.startswith('{') and tpl.endswith('}'):
                    try:
                        parsed = json.loads(tpl)
                        if isinstance(parsed, dict):
                            if 'title' in parsed and isinstance(parsed['title'], str):
                                try:
                                    message_title = Template(parsed['title']).render(context)
                                except Exception as e:
                                    logger.warning(f"渲染模板标题失败，使用默认标题: {e}")
                            text_lines = []
                            if 'text' in parsed:
                                if isinstance(parsed['text'], list):
                                    for line in parsed['text']:
                                        if isinstance(line, str):
                                            try:
                                                text_lines.append(Template(line).render(context))
                                            except Exception as e:
                                                logger.warning(f"渲染模板文本行失败，跳过: {e}")
                                elif isinstance(parsed['text'], str):
                                    try:
                                        text_lines.append(Template(parsed['text']).render(context))
                                    except Exception as e:
                                        logger.warning(f"渲染模板文本失败，跳过: {e}")
                            message_content = "\n".join([l for l in text_lines if l])
                    except Exception as e:
                        logger.warning(f"JSON 模板解析失败（原始模板），将先渲染后再解析：{e}")
                        try:
                            rendered_tpl = Template(tpl).render(context)
                            parsed2 = json.loads(rendered_tpl)
                            if isinstance(parsed2, dict):
                                if 'title' in parsed2 and isinstance(parsed2['title'], str):
                                    try:
                                        message_title = Template(parsed2['title']).render(context)
                                    except Exception as e:
                                        logger.warning(f"渲染模板标题失败，使用默认标题: {e}")
                                text_lines = []
                                if 'text' in parsed2:
                                    if isinstance(parsed2['text'], list):
                                        for line in parsed2['text']:
                                            if isinstance(line, str):
                                                try:
                                                    text_lines.append(Template(line).render(context))
                                                except Exception as e:
                                                    logger.warning(f"渲染模板文本行失败，跳过: {e}")
                                    elif isinstance(parsed2['text'], str):
                                        try:
                                            text_lines.append(Template(parsed2['text']).render(context))
                                        except Exception as e:
                                            logger.warning(f"渲染模板文本失败，跳过: {e}")
                                message_content = "\n".join([l for l in text_lines if l])
                            else:
                                message_content = rendered_tpl
                        except Exception as e2:
                            logger.warning(f"渲染后 JSON 解析失败，将按纯文本模板处理：{e2}")
                            message_content = Template(tpl).render(context)
                else:
                    message_content = Template(tpl).render(context)
            except Exception as e:
                logger.warning(f"渲染自定义模板失败，回退默认内容: {e}")
        logger.debug(f"构建消息内容: {message_content}")

        # 消息图片
        image_url = first_event.image_url
        logger.debug(f"初始图片URL: {image_url}")

        if not image_url and tmdb_info and tmdb_info.get('poster_path') and not is_multiple_episodes:
            # 剧集图片
            image_url = self.backdrop_path = f"https://{settings.TMDB_IMAGE_DOMAIN}/t/p/original{tmdb_info.get('poster_path')}"
            logger.debug(f"使用剧集图片URL: {image_url}")
        elif not image_url and tmdb_info and tmdb_info.get('backdrop_path') and is_multiple_episodes:
            # 使用TMDB背景
            image_url = self.backdrop_path = f"https://{settings.TMDB_IMAGE_DOMAIN}/t/p/original{tmdb_info.get('backdrop_path')}"
            logger.debug(f"使用TMDB背景URL: {image_url}")
        # 使用默认图片
        if not image_url:
            image_url = self._webhook_images.get(first_event.channel)
            logger.debug(f"使用默认图片URL: {image_url}")

        # 处理播放链接
        play_link = None
        if self._add_play_link:
            play_link = self._get_play_link(first_event)

        # 发送聚合消息
        logger.debug(f"准备发送消息 - 标题: {message_title}, 内容: {message_content}, 图片: {image_url}")
        self.post_message(mtype=NotificationType.MediaServer,
                          title=message_title, text=message_content, image=image_url, link=play_link)

        logger.info(f"已发送聚合消息：{message_title}")

    def _merge_continuous_episodes(self, events: List[WebhookEventInfo]) -> str:
        """
        合并连续的集数信息，使消息展示更美观

        将同一季中连续的集数合并为一个区间显示，例如：
        S01E01-E03 而不是 S01E01, S01E02, S01E03

        Args:
            events (List[WebhookEventInfo]): Webhook事件信息列表

        Returns:
            str: 合并后的集数信息字符串
        """
        # 按季分组集数信息
        season_episodes = {}
        tmdb_info = self._get_tmdb_info(
            tmdb_id=events[0].tmdb_id,
            mtype=MediaType.TV,
            season=events[0].season_id
        )
        for event in events:
            # 提取季号和集号
            season, episode = None, None
            episode_name = ""

            if event.json_object and isinstance(event.json_object, dict):
                item = event.json_object.get("Item", {})
                season = item.get("ParentIndexNumber")
                episode = item.get("IndexNumber")
                if episode is not None and int(episode) <= len(tmdb_info.get('episodes')):
                    episode_name = tmdb_info.get("episodes")[int(episode)-1].get('name')
                else:
                    episode_name = item.get("Name", "")

            # 如果无法从json_object获取信息，则尝试从event_info直接获取
            if season is None:
                season = getattr(event, "season_id", None)
            if episode is None:
                episode = getattr(event, "episode_id", None)
            if not episode_name:
                episode_name = getattr(event, "item_name", "")

            # 确保季号和集号都存在
            if season is not None and episode is not None:
                if season not in season_episodes:
                    season_episodes[season] = []
                season_episodes[season].append({
                    "episode": episode,
                    "name": episode_name
                })


        # 对每季的集数进行排序并合并连续区间
        merged_details = []
        for season in sorted(season_episodes.keys()):
            episodes = season_episodes[season]
            # 按集号排序
            episodes.sort(key=lambda x: x["episode"])

            # 合并连续集数
            if not episodes:
                continue

            # 初始化第一个区间
            start = episodes[0]["episode"]
            end = episodes[0]["episode"]
            episode_names = [episodes[0]["name"]]

            for i in range(1, len(episodes)):
                current = episodes[i]["episode"]
                # 如果当前集号与上一集连续
                if current == end + 1:
                    end = current
                    episode_names.append(episodes[i]["name"])
                else:
                    # 保存当前区间
                    if start == end:
                        merged_details.append(f"S{season:02d}E{start:02d} {episode_names[0]}")
                    else:
                        # 合并区间
                        merged_details.append(f"S{season:02d}E{start:02d}-E{end:02d}")
                    # 开始新区间
                    start = end = current
                    episode_names = [episodes[i]["name"]]

            # 添加最后一个区间
            if start == end:
                merged_details.append(f"S{season:02d}E{start:02d} {episode_names[-1]}")
            else:
                merged_details.append(f"S{season:02d}E{start:02d}-E{end:02d}")

        return ", ".join(merged_details)

    def __add_element(self, key, duration=DEFAULT_EXPIRATION_TIME):
        """
        添加元素到过期字典中，用于过滤短时间内的重复消息

        Args:
            key (str): 元素键值
            duration (int, optional): 过期时间（秒），默认DEFAULT_EXPIRATION_TIME秒
        """
        expiration_time = time.time() + duration
        # 如果元素已经存在，更新其过期时间
        self._webhook_msg_keys[key] = expiration_time

    def __remove_element(self, key):
        """
        从过期字典中移除指定元素

        Args:
            key (str): 要移除的元素键值
        """
        self._webhook_msg_keys = {k: v for k, v in self._webhook_msg_keys.items() if k != key}

    def __get_elements(self):
        """
        获取所有未过期的元素键值列表，并清理过期元素

        Returns:
            List[str]: 未过期的元素键值列表
        """
        current_time = time.time()
        # 创建新的字典，只保留未过期的元素
        valid_keys = []
        expired_keys = []

        for key, expiration_time in self._webhook_msg_keys.items():
            if expiration_time > current_time:
                valid_keys.append(key)
            else:
                expired_keys.append(key)

        # 从字典中移除过期元素
        for key in expired_keys:
            del self._webhook_msg_keys[key]

        return valid_keys

    def _event_info_to_dict(self, event_info: WebhookEventInfo) -> Dict[str, Any]:
        """
        调试辅助：尽可能把 WebhookEventInfo 转为字典，便于日志查看。

        兼容几种常见结构：
        - pydantic BaseModel: 使用 .dict()
        - dataclass: 使用 vars() 或 __dict__
        - 一般对象: 过滤可序列化的属性
        """
        # pydantic
        if hasattr(event_info, 'dict') and callable(getattr(event_info, 'dict')):
            try:
                return event_info.dict()
            except Exception:
                pass
        # dataclass/普通对象
        try:
            raw = dict(vars(event_info))
        except Exception:
            try:
                raw = dict(getattr(event_info, '__dict__', {}))
            except Exception:
                raw = {}

        # 简单清洗不可序列化内容
        def _safe(v):
            try:
                import json as _json
                _json.dumps(v, ensure_ascii=False)
                return v
            except Exception:
                return str(v)

        return {k: _safe(v) for k, v in raw.items()}

    def _build_template_context(self, event: WebhookEventInfo, tmdb: Dict[str, Any], extras: Dict[str, Any]) -> Dict[str, Any]:
        """
        构建用于 Jinja2 模板渲染的上下文。

        可用字段尽量覆盖默认消息中的信息，同时提供扩展字段。
        """
        location = WebUtils.get_location(event.ip) if getattr(event, 'ip', None) else None
        return {
            "event": event,
            "event_name": self._webhook_actions.get(event.event),
            "item_name": event.item_name,
            "item_type": event.item_type,
            "tmdb": tmdb or {},
            "overview": getattr(event, 'overview', None),
            "user_name": getattr(event, 'user_name', None),
            "device_name": getattr(event, 'device_name', None),
            "client": getattr(event, 'client', None),
            "ip": getattr(event, 'ip', None),
            "location": location,
            "time": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())),
            **(extras or {})
        }

    def _human_size(self, size_bytes: Optional[int]) -> Optional[str]:
        if not isinstance(size_bytes, (int, float)) or size_bytes <= 0:
            return None
        units = ["B", "KB", "MB", "GB", "TB"]
        s = float(size_bytes)
        idx = 0
        while s >= 1024 and idx < len(units) - 1:
            s /= 1024.0
            idx += 1
        if idx == 0:
            return f"{int(s)} {units[idx]}"
        return f"{s:.2f} {units[idx]}"

    def _parse_quality(self, name: str) -> Optional[str]:
        if not name:
            return None
        n = name.lower()
        parts = []
        # 分辨率
        if m := re.search(r"(2160p|1080p|720p|480p)", n):
            parts.append(m.group(1).upper())
        # 来源
        if m := re.search(r"(webrip|web[-. ]?dl|bluray|remux|hdtv)", n):
            src = m.group(1).upper().replace(" ", "").replace("-", "")
            src = src.replace("WEBDL", "WEB-DL")
            parts.append(src)
        # HDR/DV
        if re.search(r"dolby[\s_-]*vision|\bDV\b", n):
            parts.append("DV")
        elif re.search(r"hdr10\+?", n):
            parts.append("HDR10+")
        elif re.search(r"\bhdr\b", n):
            parts.append("HDR")
        # 视频编码
        if re.search(r"(hevc|h\.265|h265|x265)", n):
            parts.append("HEVC")
        elif re.search(r"(avc|h\.264|h264|x264)", n):
            parts.append("AVC")
        # 音频
        if re.search(r"(dolby[\s_-]*atmos|\batmos\b)", n):
            parts.append("Atmos")
        elif re.search(r"dts[- ]?hd(?:[- ]?ma)?", n):
            parts.append("DTS-HD MA")
        elif re.search(r"truehd", n):
            parts.append("TrueHD")
        elif re.search(r"e-?ac-?3|ddp|dd\+", n):
            parts.append("EAC3")
        elif re.search(r"\bac3\b", n):
            parts.append("AC3")
        elif re.search(r"\bdts\b", n):
            parts.append("DTS")
        elif re.search(r"\baac\b", n):
            parts.append("AAC")
        return " ".join(dict.fromkeys(parts)) if parts else None

    def _parse_release_group(self, name: str) -> Optional[str]:
        if not name:
            return None
        base = name.rsplit('.', 1)[0]
        if m := re.search(r"[-_\.](?P<grp>[A-Za-z0-9._-]{2,})$", base):
            return m.group("grp")
        if m := re.search(r"[\[{(]([A-Za-z0-9._-]{2,})[\]})]", base):
            return m.group(1)
        return None

    def _extract_file_metrics(self, event_info: WebhookEventInfo) -> Tuple[int, Optional[int]]:
        count = 1
        total_size = None
        jo = getattr(event_info, 'json_object', None)
        if isinstance(jo, dict):
            item = jo.get('Item') or jo
            sources = item.get('MediaSources') or jo.get('MediaSources')
            if isinstance(sources, list) and sources:
                count = len(sources)
                total = 0
                for s in sources:
                    try:
                        v = s.get('Size') if isinstance(s, dict) else None
                        if isinstance(v, (int, float)):
                            total += int(v)
                    except Exception:
                        continue
                total_size = total or None
            else:
                sz = item.get('Size') or jo.get('Size')
                if isinstance(sz, (int, float)) and sz > 0:
                    total_size = int(sz)
        return count, total_size

    def _calc_time_usage_movie(self, event_info: WebhookEventInfo, tmdb: Dict[str, Any]) -> Optional[str]:
        minutes = None
        if tmdb and isinstance(tmdb, dict):
            v = tmdb.get('runtime')
            if isinstance(v, (int, float)) and v > 0:
                minutes = int(v)
        if minutes is None:
            jo = getattr(event_info, 'json_object', None)
            if isinstance(jo, dict):
                item = jo.get('Item') or jo
                ticks = item.get('RunTimeTicks') or jo.get('RunTimeTicks')
                try:
                    if ticks:
                        seconds = float(ticks) / 10_000_000.0
                        minutes = int(round(seconds / 60.0))
                except Exception:
                    pass
        if minutes is None:
            return None
        h = minutes // 60
        m = minutes % 60
        return f"{h}小时{m}分钟" if h else f"{m}分钟"

    def _calc_time_usage_tv(self, events: List[WebhookEventInfo], tmdb: Dict[str, Any]) -> Optional[str]:
        minutes = 0
        got = False
        if tmdb and isinstance(tmdb, dict):
            episodes = tmdb.get('episodes')
            if isinstance(episodes, list) and episodes:
                for ev in events:
                    try:
                        idx = int(getattr(ev, 'episode_id', 0)) - 1
                        if 0 <= idx < len(episodes):
                            rt = episodes[idx].get('runtime')
                            if isinstance(rt, (int, float)) and rt > 0:
                                minutes += int(rt)
                                got = True
                    except Exception:
                        continue
        if not got and tmdb:
            ert = tmdb.get('episode_run_time')
            if isinstance(ert, list) and ert:
                try:
                    avg = int(sum(ert) / len(ert)) if ert else 0
                    if avg > 0:
                        minutes = avg * len(events)
                        got = True
                except Exception:
                    pass
        if not got:
            return None
        h = minutes // 60
        m = minutes % 60
        return f"{h}小时{m}分钟" if h else f"{m}分钟"

    def _log_template_context(self, context: Dict[str, Any], where: str = ""):
        """
        输出用于模板渲染的上下文的精简版本至日志（去敏）。

        仅保留关键字段，避免输出完整 tmdb 或对象过大内容。
        """
        try:
            safe = {}
            # 直取一级字段
            keys = [
                "event_name", "item_name", "item_type", "overview", "user_name", "device_name",
                "client", "ip", "location", "time", "episodes_detail", "season_episode",
                "count", "is_multiple", "category", "title_year", "tmdbid", "vote_average",
                "media_type"
            ]
            for k in keys:
                if k in context:
                    safe[k] = context.get(k)

            # tmdb 精简
            tmdb = context.get("tmdb") or {}
            if isinstance(tmdb, dict):
                safe["tmdb_summary"] = {
                    "media_type": tmdb.get("media_type"),
                    "vote_average": tmdb.get("vote_average"),
                    "release_date": tmdb.get("release_date") or tmdb.get("first_air_date"),
                    "genres": [g.get("name") if isinstance(g, dict) else g for g in (tmdb.get("genres") or [])][:5]
                }

            # 使用 default=str 以处理 Enum、datetime 等不可直接序列化的类型
            logger.info(f"模板上下文{(' - ' + where) if where else ''}: {json.dumps(safe, ensure_ascii=False, indent=2, default=str)}")
        except Exception as e:
            logger.warning(f"打印模板上下文失败: {e}")

    def _get_play_link(self, event_info: WebhookEventInfo) -> Optional[str]:
        """
        获取媒体项目的播放链接

        Args:
            event_info (WebhookEventInfo): 事件信息

        Returns:
            Optional[str]: 播放链接，如果无法获取则返回None
        """
        play_link = None
        if event_info.server_name:
            service = self.service_infos().get(event_info.server_name)
            if service:
                play_link = service.instance.get_play_url(event_info.item_id)
        elif event_info.channel:
            services = MediaServerHelper().get_services(type_filter=event_info.channel)
            for service in services.values():
                play_link = service.instance.get_play_url(event_info.item_id)
                if play_link:
                    break

        return play_link

    @cached(
        region="MediaServerMsgModify",           # 缓存区域，用于隔离不同插件的缓存
        maxsize=128,                  # 最大缓存条目数（仅内存缓存有效）
        ttl=600,                     # 缓存存活时间（秒）
        skip_none=True,               # 是否跳过None值缓存
        skip_empty=False              # 是否跳过空值缓存（空列表、空字典等）
    )
    def _get_tmdb_info(self, tmdb_id: str, mtype: MediaType, season: Optional[int] = None):
        """
        获取TMDB信息

        Args:
            tmdb_id: TMDB ID
            mtype: 媒体类型
            season: 季数（仅电视剧需要）

        Returns:
            dict: TMDB信息
        """
        if mtype == MediaType.MOVIE:
            return self.chain.tmdb_info(tmdbid=tmdb_id, mtype=mtype)
        else:  # TV类型
            tmdb_info = self.chain.tmdb_info(tmdbid=tmdb_id, mtype=mtype, season=season)
            tmdb_info2 = self.chain.tmdb_info(tmdbid=tmdb_id, mtype=mtype)
            return tmdb_info | tmdb_info2


    def stop_service(self):
        """
        退出插件时的清理工作

        在插件被停用或系统关闭时调用，确保：
        1. 所有待处理的聚合消息被立即发送出去
        2. 所有正在进行的定时器被取消
        3. 清空所有内部缓存数据
        """
        # 发送所有待处理的聚合消息
        for series_id in list(self._pending_messages.keys()):
            # 直接发送消息而不依赖定时器
            self._send_aggregated_message(series_id)

        # 取消所有定时器
        for timer in self._aggregate_timers.values():
            timer.cancel()
        self._aggregate_timers.clear()
        self._pending_messages.clear()
        self._get_tmdb_info.cache_clear()
