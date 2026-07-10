from typing import Any, List, Dict, Tuple, Optional

from app.plugins import _PluginBase
from app.core.event import eventmanager, Event
from app.schemas import Notification, NotificationType
from app.schemas.types import EventType
from app.log import logger

from .config import configer
from .utils import extract_115_links
from .logic import task_queue, _resolve_mtype_by_tmdb_names


class p115sharestrm(_PluginBase):
    """
    115分享内容生成STRM并异步队列整理插件
    """

    # 插件名称
    plugin_name = "分享STRM生成助手"
    # 插件描述
    plugin_desc = "发送/sharestrm 115分享链接 给TG机器人，自动生成分享STRM"
    # 插件图标
    plugin_icon = "https://raw.githubusercontent.com/ListeningLTG/MoviePilot-Plugins/refs/heads/main/icons/u115.png"
    # 插件版本
    plugin_version = "1.0.55"
    # 插件作者
    plugin_author = "ListeningLTG"
    # 作者主页
    author_url = "https://github.com/ListeningLTG"
    # 插件配置项ID前缀
    plugin_config_prefix = "p115sharestrm_"
    # 加载顺序
    plugin_order = 10
    # 可使用的用户级别
    auth_level = 1

    def __init__(self):
        """
        初始化
        """
        super().__init__()

    def init_plugin(self, config: dict = None):
        """
        加载/更新配置并启动服务
        """
        if config:
            configer.load_from_dict(config)
            configer.update_plugin_config()

        # 注入通知回调，避免 logic.py 直接依赖 app 内部模块
        task_queue.set_notify_callback(self._send_notify)

        if configer.enabled:
            task_queue.start()
            logger.info("【P115ShareStrm】插件已启动，等待 TG 指令...")
        else:
            task_queue.stop()
            logger.info("【P115ShareStrm】插件已停止")

    def get_state(self) -> bool:
        """
        获取状态
        """
        return configer.enabled

    def get_service(self) -> List[Dict[str, Any]]:
        """
        注册插件公共服务
        """
        return []

    def _send_notify(self, user_id: Optional[str], title: str, text: str, buttons: Optional[List[List[dict]]] = None):
        """
        通知发送包装，通过 MoviePilot 标准 post_message 接口发出
        """
        try:
            self.post_message(
                mtype=NotificationType.Plugin,
                title=title,
                text=text,
                userid=user_id,
                buttons=buttons,
            )
        except Exception as e:
            logger.warning(f"【P115ShareStrm】通知发送失败: {e}")

    def get_state(self) -> bool:
        """
        获取插件启用状态
        """
        return configer.enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """
        定义远程控制命令

        :return: 命令关键字、事件、描述、附带数据
        """
        return [
            {
                "cmd": "/sharestrm",
                "event": EventType.PluginAction,
                "desc": "提取消息中的115分享链接并加入STRM生成队列",
                "category": "115",
                "data": {"plugin_id": "p115sharestrm", "action": "sharestrm"},
            },
            {
                "cmd": "/sharesubdown",
                "event": EventType.PluginAction,
                "desc": "对 115 分享链接仅重新下载并放置字幕（根据整理历史）",
                "category": "115",
                "data": {"plugin_id": "p115sharestrm", "action": "sharesubdown"},
            },
        ]

    def get_api(self) -> List[Dict[str, Any]]:
        """
        获取插件 API 端点
        """
        return []

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """
        拼装插件配置页面，返回两块数据：1、页面配置；2、数据结构
        """
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VTabs",
                        "props": {
                            "model": "_tabs",
                            "fixed-tabs": True,
                            "show-arrows": True,
                            "slider-color": "primary",
                        },
                        "content": [
                            {"component": "VTab", "props": {"value": "tab_basic"}, "text": "基础配置"},
                            {"component": "VTab", "props": {"value": "tab_custom"}, "text": "自定义STRM"},
                        ],
                    },
                    {
                        "component": "VWindow",
                        "props": {"model": "_tabs"},
                        "content": [
                            # ── Tab 0: 基础配置 ──
                            {
                                "component": "VWindowItem",
                                "props": {"value": "tab_basic"},
                                "content": [
                                    # ── 第一行：开关区 ──
                                    {
                                        "component": "VRow",
                                        "content": [
                                            {
                                                "component": "VCol",
                                                "props": {"cols": 12, "md": 4},
                                                "content": [
                                                    {
                                                        "component": "VSwitch",
                                                        "props": {
                                                            "model": "enabled",
                                                            "label": "启用插件",
                                                        },
                                                    }
                                                ],
                                            },
                                            {
                                                "component": "VCol",
                                                "props": {"cols": 12, "md": 4},
                                                "content": [
                                                    {
                                                        "component": "VSwitch",
                                                        "props": {
                                                            "model": "moviepilot_transfer",
                                                            "label": "STRM 交由 MoviePilot 整理",
                                                        },
                                                    }
                                                ],
                                            },
                                            {
                                                "component": "VCol",
                                                "props": {"cols": 12, "md": 4},
                                                "content": [
                                                    {
                                                        "component": "VSwitch",
                                                        "props": {
                                                            "model": "tmdb_extract",
                                                            "label": "自动提取 TMDB ID",
                                                            "hint": "从指令文本中提取 TMDB ID，格式如 TMDB:123 或 TMDB ID: 123，传给 MP 整理时直接匹配媒体信息",
                                                            "persistent-hint": True,
                                                        },
                                                    }
                                                ],
                                            },
                                            {
                                                "component": "VCol",
                                                "props": {"cols": 12, "md": 4},
                                                "content": [
                                                    {
                                                        "component": "VSwitch",
                                                        "props": {
                                                            "model": "imdb_extract",
                                                            "label": "自动提取 IMDB ID",
                                                            "hint": "从指令文本中提取 IMDB ID，格式如 tt1234567 或 IMDB: tt1234567，两个开关同时开启时优先使用 TMDB ID",
                                                            "persistent-hint": True,
                                                        },
                                                    }
                                                ],
                                            },
                                        ],
                                    },
                                    # ── 提取识别黑名单行 ──
                                    {
                                        "component": "VRow",
                                        "content": [
                                            {
                                                "component": "VCol",
                                                "props": {"cols": 12},
                                                "content": [
                                                    {
                                                        "component": "VTextField",
                                                        "props": {
                                                            "model": "extract_blacklist",
                                                            "label": "提取识别黑名单",
                                                            "hint": "英文逗号隔开输入多个。若消息中包含任意关键词，则不提取 TMDB/IMDB ID，如：定时分享,最近接收",
                                                            "persistent-hint": True,
                                                        },
                                                    }
                                                ],
                                            },
                                        ],
                                    },
                                    # ── 字幕下载行 ──
                                    {
                                        "component": "VRow",
                                        "content": [
                                            {
                                                "component": "VCol",
                                                "props": {"cols": 12, "md": 3},
                                                "content": [
                                                    {
                                                        "component": "VSwitch",
                                                        "props": {
                                                            "model": "download_subtitle",
                                                            "label": "同步下载字幕文件",
                                                        },
                                                    }
                                                ],
                                            },
                                            {
                                                "component": "VCol",
                                                "props": {"cols": 12, "md": 9},
                                                "content": [
                                                    {
                                                        "component": "VTextField",
                                                        "props": {
                                                            "model": "user_subtitle_ext",
                                                            "label": "字幕文件后缀",
                                                            "hint": "逗号分隔，开启后将此类后缀文件从分享转存下载到本地，例如 srt,ass,ssa",
                                                            "persistent-hint": True,
                                                        },
                                                    }
                                                ],
                                            },
                                        ],
                                    },
                                    # ── 第二行：Cookie 与 地址 ──
                                    {
                                        "component": "VRow",
                                        "content": [
                                            {
                                                "component": "VCol",
                                                "props": {"cols": 12, "md": 6},
                                                "content": [
                                                    {
                                                        "component": "VTextField",
                                                        "props": {
                                                            "model": "cookies",
                                                            "label": "115 Cookie",
                                                            "hint": "115网盘的 Cookie 配置",
                                                            "persistent-hint": True,
                                                        },
                                                    }
                                                ],
                                            },
                                            {
                                                "component": "VCol",
                                                "props": {"cols": 12, "md": 6},
                                                "content": [
                                                    {
                                                        "component": "VTextField",
                                                        "props": {
                                                            "model": "moviepilot_address_custom",
                                                            "label": "MoviePilot 地址",
                                                            "hint": "手动指定访问地址 (如 http://192.168.1.10:3000)，留空则优先使用系统设置的外部域地址来进行生成strm",
                                                            "persistent-hint": True,
                                                        },
                                                    }
                                                ],
                                            },
                                        ],
                                    },
                                    # ── 第三行：STRM 保存路径 ──
                                    {
                                        "component": "VRow",
                                        "content": [
                                            {
                                                "component": "VCol",
                                                "props": {"cols": 12},
                                                "content": [
                                                    {
                                                        "component": "VTextField",
                                                        "props": {
                                                            "model": "strm_save_path",
                                                            "label": "STRM 保存路径",
                                                            "hint": "生成的 .strm 文件保存到本地的目录，例如 /media/share_strm",
                                                            "persistent-hint": True,
                                                        },
                                                    }
                                                ],
                                            },
                                        ],
                                    },
                                    # ── 第四行：媒体后缀 ──
                                    {
                                        "component": "VRow",
                                        "content": [
                                            {
                                                "component": "VCol",
                                                "props": {"cols": 12},
                                                "content": [
                                                    {
                                                        "component": "VTextField",
                                                        "props": {
                                                            "model": "user_rmt_mediaext",
                                                            "label": "可识别媒体后缀",
                                                            "hint": "逗号分隔，仅对此类后缀文件生成 STRM，例如 mp4,mkv,ts",
                                                            "persistent-hint": True,
                                                        },
                                                    }
                                                ],
                                            },
                                        ],
                                    },
                                    # ── 说明区 ──
                                    {
                                        "component": "VRow",
                                        "content": [
                                            {
                                                "component": "VCol",
                                                "props": {"cols": 12},
                                                "content": [
                                                    {
                                                        "component": "VAlert",
                                                        "props": {
                                                            "type": "info",
                                                            "variant": "tonal",
                                                            "density": "compact",
                                                            "class": "mt-2",
                                                        },
                                                        "content": [
                                                            {
                                                                "component": "div",
                                                                "html": "<b>使用说明</b>",
                                                            },
                                                            {
                                                                "component": "div",
                                                                "text": "• 向 TG 机器人发送 /sharestrm <链接> 即可触发，链接中的 password=xxx 参数自动识别为提取码",
                                                            },
                                                            {
                                                                "component": "div",
                                                                "text": "• 多个链接会依次加入队列，避免并发风控，实时通过 TG 推送处理进度",
                                                            },
                                                            {
                                                                "component": "div",
                                                                "text": "• 默认生成的 STRM 为 http://<MoviePilot地址>/api/v1/plugin/P115StrmHelper/redirect_url?share_code=...&id=... 结构，需要安装并开启【115网盘STRM助手 (P115StrmHelper)】插件配合使用。",
                                                            },
                                                        ],
                                                    },
                                                ],
                                            },
                                        ],
                                    },
                                ],
                            },
                            # ── Tab 1: 自定义STRM ──
                            {
                                "component": "VWindowItem",
                                "props": {"value": "tab_custom"},
                                "content": [
                                    # ── 启用模板开关 ──
                                    {
                                        "component": "VRow",
                                        "content": [
                                            {
                                                "component": "VCol",
                                                "props": {"cols": 12, "md": 4},
                                                "content": [
                                                    {
                                                        "component": "VSwitch",
                                                        "props": {
                                                            "model": "strm_url_template_enabled",
                                                            "label": "启用 STRM URL 自定义模板",
                                                        },
                                                    }
                                                ],
                                            },
                                        ],
                                    },
                                    # ── Jinja2 URL 模板 ──
                                    {
                                        "component": "VRow",
                                        "content": [
                                            {
                                                "component": "VCol",
                                                "props": {"cols": 12},
                                                "content": [
                                                    {
                                                        "component": "VTextarea",
                                                        "props": {
                                                            "model": "strm_url_template",
                                                            "label": "STRM URL 自定义模板 (Jinja2)",
                                                            "hint": (
                                                                "需开启上方【启用 STRM URL 自定义模板】开关后生效。"
                                                                "可用变量：{{ share_code }}、{{ receive_code }}、{{ file_id }}、{{ file_name }}、{{ file_path }}、{{ base_url }}、{{ sha }}、{{ file_size }}"
                                                            ),
                                                            "persistent-hint": True,
                                                            "rows": 4,
                                                            "auto-grow": True,
                                                        },
                                                    }
                                                ],
                                            },
                                        ],
                                    },
                                    # ── 扩展名特定模板 ──
                                    {
                                        "component": "VRow",
                                        "content": [
                                            {
                                                "component": "VCol",
                                                "props": {"cols": 12},
                                                "content": [
                                                    {
                                                        "component": "VTextarea",
                                                        "props": {
                                                            "model": "strm_url_template_custom",
                                                            "label": "STRM URL 扩展名特定模板 (Jinja2)",
                                                            "hint": (
                                                                "每行一条规则，格式：扩展名1,扩展名2 => URL模板 [=> /自定义保存路径]，"
                                                                "保存路径可选。示例：iso => {{ base_url }}{{ file_path | urlencode }}?id={{ file_id }} => /data/strm/iso"
                                                            ),
                                                            "persistent-hint": True,
                                                            "rows": 6,
                                                            "auto-grow": True,
                                                        },
                                                    }
                                                ],
                                            },
                                        ],
                                    },
                                ],
                            },
                        ],
                    },
                ],
            }
        ], {
            "_tabs": "tab_basic",
            "enabled": False,
            "cookies": "",
            "moviepilot_address_custom": "",
            "strm_save_path": "",
            "moviepilot_transfer": True,
            "strm_url_template_enabled": False,
            "tmdb_extract": False,
            "imdb_extract": False,
            "extract_blacklist": "",
            "strm_url_template": "",
            "strm_url_template_custom": "",
            "user_rmt_mediaext": "mp4,mkv,ts,iso,rmvb,avi,mov,mpeg,mpg,wmv,3gp,asf,m4v,flv,m2ts,tp,f4v",
            "download_subtitle": False,
            "user_subtitle_ext": "srt,ass,ssa",
        }

    def get_page(self) -> List[dict]:
        """
        获取插件数据页面（队列状态展示）
        """
        queue_size = task_queue._queue.qsize() if task_queue._queue else 0
        processing_count = task_queue._processing_count
        is_running = task_queue._running

        return [
            {
                "component": "VCard",
                "props": {"variant": "outlined"},
                "content": [
                    {
                        "component": "VCardTitle",
                        "props": {"class": "pa-4 pb-0"},
                        "text": "任务队列状态",
                    },
                    {
                        "component": "VCardText",
                        "props": {"class": "pa-4"},
                        "content": [
                            {
                                "component": "VRow",
                                "content": [
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 6, "md": 3},
                                        "content": [
                                            {
                                                "component": "VChip",
                                                "props": {
                                                    "color": "success" if is_running else "default",
                                                    "variant": "tonal",
                                                    "prepend-icon": "mdi-check-circle" if is_running else "mdi-stop-circle",
                                                },
                                                "text": "运行中" if is_running else "已停止",
                                            }
                                        ],
                                    },
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 6, "md": 3},
                                        "content": [
                                            {
                                                "component": "VChip",
                                                "props": {
                                                    "color": "primary" if processing_count > 0 else "default",
                                                    "variant": "tonal",
                                                    "prepend-icon": "mdi-cog-sync",
                                                },
                                                "text": f"处理中: {processing_count} 个任务",
                                            }
                                        ],
                                    },
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 6, "md": 3},
                                        "content": [
                                            {
                                                "component": "VChip",
                                                "props": {
                                                    "color": "warning" if queue_size > 0 else "default",
                                                    "variant": "tonal",
                                                    "prepend-icon": "mdi-format-list-numbered",
                                                },
                                                "text": f"等待中: {queue_size} 个任务",
                                            }
                                        ],
                                    },
                                ],
                            },
                        ],
                    },
                ],
            },
        ]

    @eventmanager.register(EventType.PluginAction)
    def handle_plugin_action(self, event: Event):
        """
        处理 /sharestrm 指令触发的 PluginAction 事件

        MoviePilot 命令系统触发机制:
          - get_command() 中注册的 event 类型为 PluginAction
          - event.event_data["action"] 存放 data 字典中的 action 值
          - event.event_data["arg_str"] 存放用户命令后的全部文本参数
          - event.event_data["channel"] / "source" / "userid" 用于回复消息
        """
        if not configer.enabled:
            return
        if not event or not event.event_data:
            return

        event_data = event.event_data
        if event_data.get("plugin_id") and event_data.get("plugin_id") != "p115sharestrm":
            return
        action = event_data.get("action")
        if action not in ["sharestrm", "sharesubdown"]:
            return
        sub_only = (action == "sharesubdown")

        user_id = str(
            event_data.get("userid") or event_data.get("user_id") or ""
        )
        channel = event_data.get("channel")
        source = event_data.get("source")

        # arg_str 是指令后的全部文本，例如 "/sharestrm <url>" 中的 "<url>"
        arg_str = (event_data.get("arg_str") or "").strip()

        logger.info(f"【P115ShareStrm】收到指令，参数: {arg_str!r}")

        # 仅在开启开关时提取消息中的 TMDB ID 和 IMDB ID 信息以供匹配
        import re
        tmdbid = None
        imdbid = None
        mtype = None

        # 检查是否命中提取黑名单
        hit_blacklist = False
        if configer.extract_blacklist:
            keywords = [k.strip() for k in configer.extract_blacklist.split(",") if k.strip()]
            for word in keywords:
                if word.lower() in arg_str.lower():
                    logger.info(f"【P115ShareStrm】指令参数中包含黑名单关键词 '{word}'，跳过 TMDB/IMDB ID 提取识别")
                    hit_blacklist = True
                    break

        # 提取 TMDB ID（如果开关打开且没有命中黑名单）
        if configer.tmdb_extract and not hit_blacklist:
            # 优先从 TMDB 链接中提取 ID 和媒体类型，格式如:
            #   https://www.themoviedb.org/tv/289690
            #   https://www.themoviedb.org/movie/1474050
            #   https://www.themoviedb.org/collection/17149
            tmdb_urls = re.findall(r'themoviedb\.org/(tv|movie|collection)/(\d+)', arg_str, re.IGNORECASE)
            # 回退：支持格式: TMDB ID: 123, TMDBID: 123, tmdb-123, tmdb=123, tmdb 123 等
            tmdb_texts = re.findall(r'TMDB(?:[\s\-_]*ID)?\s*[：:= \-]+(\d+)', arg_str, re.IGNORECASE)

            unique_tmdb_ids = set()
            for _, tid in tmdb_urls:
                unique_tmdb_ids.add(int(tid))
            for tid in tmdb_texts:
                unique_tmdb_ids.add(int(tid))

            if len(unique_tmdb_ids) > 1:
                logger.info(f"【P115ShareStrm】检测到消息中存在多个不同的 TMDB ID: {list(unique_tmdb_ids)}，跳过提取")
            elif len(unique_tmdb_ids) == 1:
                tmdbid = list(unique_tmdb_ids)[0]
                mtypes_for_id = {m.lower() for m, tid in tmdb_urls if int(tid) == tmdbid}
                if len(mtypes_for_id) == 1:
                    mtype = list(mtypes_for_id)[0]
                    logger.info(f"【P115ShareStrm】从 TMDB 链接中提取到唯一的 TMDB ID: {tmdbid}，类型: {mtype}")
                else:
                    logger.info(f"【P115ShareStrm】从指令文本中提取到唯一的 TMDB ID: {tmdbid}")
                    # 剥掉 URL，只在纯文本上做类型关键词匹配，避免密码/参数干扰
                    clean_text = re.sub(r'https?://\S+', '', arg_str)
                    # 尝试通过关键词判断类型
                    # 1. 优先匹配电视剧强特征（S01E01 或仅 S01 均视为剧集）
                    if re.search(r'电视剧|剧集|番剧|[美日韩台港英泰]剧|动漫|综艺|Season|S[0-9]+E[0-9]+|S[0-9]+|第[0-9]+[季集]', clean_text, re.I):
                        mtype = "tv"
                    # 2. 匹配系列强特征
                    elif re.search(r'系列|合集|Collection', clean_text, re.I):
                        mtype = "collection"
                    # 3. 匹配电影强特征
                    elif re.search(r'电影|Movie', clean_text, re.I):
                        mtype = "movie"
                    # 4. 如果都没有匹配，mtype 固定为 None，交由 recognize_media 自动根据 TMDB ID 获取正确类型
                    logger.info(f"【P115ShareStrm】关键词匹配媒体类型: {mtype or '未识别，将通过TMDBID 进行名称匹配推断'}")

                    # 4. 关键词匹配到类型后，用 TMDB 接口做二次验证，纠正可能的误判
                    if mtype:
                        verified = _resolve_mtype_by_tmdb_names(tmdbid, arg_str, prefer=mtype)
                        if verified is None:
                            logger.info(f"【P115ShareStrm】TMDB验证无法确认，保留关键词结果: {mtype}")
                        elif verified != mtype:
                            logger.info(f"【P115ShareStrm】TMDB验证修正媒体类型: {mtype} → {verified}")
                            mtype = verified
                        else:
                            logger.info(f"【P115ShareStrm】TMDB验证确认媒体类型: {mtype}")

        # 提取 IMDB ID（如果开关打开，且未提取到 TMDB ID，且未命中黑名单时）
        if configer.imdb_extract and not tmdbid and not hit_blacklist:
            # 优先从 IMDB 链接中提取 ID，格式如:
            #   https://www.imdb.com/title/tt1234567/
            #   https://imdb.com/title/tt1234567
            imdb_urls = re.findall(r'imdb\.com/title/(tt\d+)', arg_str, re.IGNORECASE)
            # 回退：支持格式: IMDB: tt1234567, IMDBID: tt1234567, imdb-tt1234567, 或直接 tt1234567
            imdb_texts = re.findall(r'(?:IMDB(?:[\s\-_]*ID)?\s*[：:= \-]+)?(tt\d{7,10})', arg_str, re.IGNORECASE)

            unique_imdb_ids = set()
            for iid in imdb_urls:
                unique_imdb_ids.add(iid.lower())
            for iid in imdb_texts:
                unique_imdb_ids.add(iid.lower())

            if len(unique_imdb_ids) > 1:
                logger.info(f"【P115ShareStrm】检测到消息中存在多个不同的 IMDB ID: {list(unique_imdb_ids)}，跳过提取")
            elif len(unique_imdb_ids) == 1:
                imdbid = list(unique_imdb_ids)[0]
                logger.info(f"【P115ShareStrm】从指令文本或链接中提取到唯一的 IMDB ID: {imdbid}")

                # 如果提取到 IMDB ID，通过 TMDB API 查询媒体信息并推断类型
                from .logic import _resolve_mtype_by_imdb
                imdb_result = _resolve_mtype_by_imdb(imdbid, arg_str)
                if imdb_result:
                    # imdb_result 返回 (tmdbid, mtype)
                    tmdbid, mtype = imdb_result
                    logger.info(f"【P115ShareStrm】通过 IMDB ID 查询到 TMDB ID: {tmdbid}，类型: {mtype}")
                else:
                    logger.warning(f"【P115ShareStrm】无法通过 IMDB ID {imdbid} 查询到对应的 TMDB 信息")
                    imdbid = None  # 查询失败，清空 IMDB ID

        if not arg_str:
            logger.warning("【P115ShareStrm】指令参数为空，未提供链接")
            self._send_notify(user_id, "【115分享STRM】提示", "❌ 请在 /sharestrm 后附上 115 分享链接")
            return

        # 从参数文本中提取所有 115 链接（支持富文本/实）
        links = extract_115_links(event_data)

        logger.info(f"【P115ShareStrm】提取到链接数: {len(links)}，链接: {links}")

        if not links:
            logger.warning(f"【P115ShareStrm】未从参数中提取到有效 115 链接: {arg_str!r}")
            self._send_notify(user_id, "【115分享STRM】提示", f"❌ 未识别到有效的 115 分享链接\n输入内容: {arg_str}")
            return

        for link_info in links:
            sc = link_info["share_code"]
            rc = link_info["receive_code"]
            queued = task_queue.add_task(sc, rc, user_id, tmdbid=tmdbid, mtype=mtype, arg_str=arg_str, sub_only=sub_only)
            if queued:
                logger.info(f"【P115ShareStrm】加入队列 share_code={sc}, receive_code={rc}, sub_only={sub_only}")
                title = "【115分享STRM】字幕重试已入队" if sub_only else "【115分享STRM】已入队"
                text = (
                    f"📥 分享码: {sc}\n🔑 提取码: {rc or '无'}\n"
                    f"正在排队{'下载字幕' if sub_only else '处理'}，请稍候..."
                )
                self._send_notify(
                    user_id,
                    title,
                    text,
                )
            else:
                logger.warning(f"【P115ShareStrm】任务已被去重过滤，跳过: {sc}")

    @eventmanager.register(EventType.MessageAction)
    def handle_message_action(self, event: Event):
        """
        处理 Telegram 按钮回调事件
        """
        if not configer.enabled:
            return
        if not event or not event.event_data:
            return
        event_data = event.event_data
        if event_data.get("plugin_id") != "p115sharestrm":
            return
        
        text = event_data.get("text") or ""
        if not text.startswith("retry_sub:"):
            return
        
        # 格式: retry_sub:share_code:receive_code
        parts = text.split(":", 2)
        if len(parts) < 2:
            return
        
        share_code = parts[1]
        receive_code = parts[2] if len(parts) > 2 else ""
        user_id = str(event_data.get("userid") or event_data.get("user_id") or "")
        
        logger.info(f"【P115ShareStrm】通过 TG 按钮触发字幕重试: share_code={share_code}, receive_code={receive_code}")
        
        # 加入队列（sub_only=True）
        queued = task_queue.add_task(share_code, receive_code, user_id, sub_only=True)
        if queued:
            self._send_notify(
                user_id,
                "【115分享STRM】字幕重试已入队",
                f"📥 分享码: {share_code}\n正在排队重试下载字幕，请稍候...",
            )

    def stop_service(self):
        """
        退出插件
        """
        task_queue.stop()
        logger.info("【P115ShareStrm】插件已退出，队列已停止")
