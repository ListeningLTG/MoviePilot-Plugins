import json
import os
import random
import re
import shutil
import subprocess
import threading
import time
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Any, List, Dict, Tuple, Optional, Union
from urllib.parse import unquote

from app.core.config import settings
from app.core.context import MediaInfo
from app.core.event import eventmanager, Event
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import (
    TransferInfo,
    TransferRenameBuildEventData,
    TransferRenameEventData,
    TransferInterceptEventData,
)
from app.schemas.types import (
    ChainEventType,
    EventType,
    MediaType,
    NotificationType,
)
from app.utils.system import SystemUtils

lock = threading.Lock()


class shortdramacompilation(_PluginBase):
    # 插件名称
    plugin_name = "短剧自动分类"
    # 插件描述
    plugin_desc = "多策略自动分类微短剧到独立目录，支持平台ID匹配、TMDB/豆瓣片长、STRM/文件FFprobe探测及本地JSON结果缓存。"
    # 插件图标
    plugin_icon = "https://raw.githubusercontent.com/ListeningLTG/MoviePilot-Plugins/refs/heads/main/icons/hg.jpeg"
    # 插件版本
    plugin_version = "0.2.3"
    # 插件作者
    plugin_author = "ListeningLTG"
    # 作者主页
    author_url = "https://github.com/ListeningLTG"
    # 插件配置项ID前缀
    plugin_config_prefix = "shortdramacompilation_"
    # 加载顺序
    plugin_order = 29
    # 可使用的用户级别
    auth_level = 1

    @property
    def name(self) -> str:
        """兼容 MoviePilot 获取插件名称属性"""
        return self.plugin_name

    _enabled = False
    _notify = True
    _delay = 0
    _category_dir = ""
    _category_name = "短剧"
    _episode_duration = 8
    _enable_network_check = True
    _short_drama_networks = "8020"
    _enable_tmdb_runtime = True
    _enable_douban_runtime = True
    _enable_ffprobe = True
    _enable_anime_category = False
    _anime_category_name = "动画短剧"
    _anime_category_dir = ""
    _cache_data = {}

    def init_plugin(self, config: dict = None):
        if config:
            self._enabled = config.get("enabled")
            self._delay = config.get("delay") or 0
            self._notify = config.get("notify")
            self._category_dir = config.get("category_dir") or ""
            self._category_name = config.get("category_name") or "短剧"
            self._episode_duration = config.get("episode_duration") or 8
            self._enable_network_check = config.get("enable_network_check") if config.get("enable_network_check") is not None else True
            self._short_drama_networks = config.get("short_drama_networks") or "8020"
            self._enable_tmdb_runtime = config.get("enable_tmdb_runtime") if config.get("enable_tmdb_runtime") is not None else True
            self._enable_douban_runtime = config.get("enable_douban_runtime") if config.get("enable_douban_runtime") is not None else True
            self._enable_ffprobe = config.get("enable_ffprobe") if config.get("enable_ffprobe") is not None else True
            self._enable_cache = config.get("enable_cache") if config.get("enable_cache") is not None else True
            self._enable_anime_category = config.get("enable_anime_category") if config.get("enable_anime_category") is not None else False
            self._anime_category_name = config.get("anime_category_name") or "动画短剧"
            self._anime_category_dir = config.get("anime_category_dir") or ""

        self._load_cache()

    @property
    def _cache_file_path(self) -> Path:
        cache_dir = settings.CONFIG_PATH / "plugins" / "shortdramacompilation"
        cache_dir.mkdir(parents=True, exist_ok=True)
        return cache_dir / "cache.json"

    def _load_cache(self):
        with lock:
            if self._cache_file_path.exists():
                try:
                    self._cache_data = json.loads(self._cache_file_path.read_text(encoding="utf-8"))
                except Exception as e:
                    logger.error(f"【短剧自动分类】加载缓存文件失败: {e}")
                    self._cache_data = {}
            else:
                self._cache_data = {}

    def _update_cache(
        self,
        tmdb_id: Union[int, str],
        is_short: bool,
        strategy_type: str,
        runtime: float = 0.0,
        network_id: Optional[str] = None,
        is_anime: bool = False,
    ):
        if not tmdb_id or not self._enable_cache:
            return
        with lock:
            now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self._cache_data[str(tmdb_id)] = {
                "is_short_drama": is_short,
                "strategy_type": strategy_type,
                "runtime": runtime,
                "network_id": network_id,
                "is_anime": is_anime,
                "anime_checked_at": now_str,
                "updated_at": now_str,
            }
            try:
                self._cache_file_path.write_text(
                    json.dumps(self._cache_data, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
            except Exception as e:
                logger.error(f"【短剧自动分类】写入缓存文件失败: {e}")

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        pass

    def get_api(self) -> List[Dict[str, Any]]:
        pass

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """
        拼装插件配置页面
        """
        return [
            {
                'component': 'VForm',
                'content': [
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {'model': 'enabled', 'label': '启用插件'}
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {'model': 'notify', 'label': '发送消息通知'}
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {'model': 'enable_cache', 'label': '持久化 JSON 缓存'}
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
                                'props': {'cols': 12, 'md': 3},
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {'model': 'category_name', 'label': '二级分类名称', 'placeholder': '短剧'}
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 6},
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {'model': 'category_dir', 'label': '分类目录绝对路径', 'placeholder': '/media/短剧'}
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {'model': 'delay', 'label': '入库延迟时间（秒）', 'placeholder': '0'}
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
                                'props': {'cols': 12, 'md': 3},
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {'model': 'episode_duration', 'label': '单集时长阈值（分钟）', 'placeholder': '8'}
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {'model': 'enable_network_check', 'label': '开启平台ID匹配'}
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 6},
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {'model': 'short_drama_networks', 'label': '短剧平台 ID 列表', 'placeholder': '8020'}
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
                                'props': {'cols': 12, 'md': 4},
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {'model': 'enable_tmdb_runtime', 'label': '开启 TMDB 片长匹配'}
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {'model': 'enable_douban_runtime', 'label': '开启豆瓣片长匹配'}
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {'model': 'enable_ffprobe', 'label': '开启 FFprobe 探测(兜底)'}
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
                                'props': {'cols': 12, 'md': 3},
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {'model': 'enable_anime_category', 'label': '开启动画短剧独立分类'}
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {'model': 'anime_category_name', 'label': '动画短剧分类名称', 'placeholder': '动画短剧'}
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 6},
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {'model': 'anime_category_dir', 'label': '动画短剧目录绝对路径', 'placeholder': '/media/动画短剧'}
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
                                'props': {'cols': 12},
                                'content': [
                                    {
                                        'component': 'VAlert',
                                        'props': {
                                            'type': 'info',
                                            'variant': 'tonal',
                                            'text': '【多策略管道分类】1.TMDB播出平台ID -> 2.TMDB标注片长 -> 3.豆瓣标注片长 -> 4.FFprobe探测。判定结果自动存入 cache.json（包含 24 小时自动刷新动画类型评估），支持将动画短剧路由至独立目录。'
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
            "notify": True,
            "delay": 0,
            "category_name": '短剧',
            "category_dir": '',
            "episode_duration": 8,
            "enable_network_check": True,
            "short_drama_networks": '8020',
            "enable_tmdb_runtime": True,
            "enable_douban_runtime": True,
            "enable_ffprobe": True,
            "enable_cache": True,
            "enable_anime_category": False,
            "anime_category_name": '动画短剧',
            "anime_category_dir": '',
        }

    def get_page(self) -> List[dict]:
        pass

    def check_is_anime(self, mediainfo: Optional[MediaInfo], tmdb_id: Optional[Union[int, str]] = None) -> bool:
        """
        检查剧集是否属于动画类型（包含 24 小时 TTL 缓存刷新机制）
        """
        now = datetime.now()

        # Step A: 检查缓存中的 is_anime 与 anime_checked_at
        if tmdb_id and str(tmdb_id) in self._cache_data:
            cache_item = self._cache_data[str(tmdb_id)]
            if isinstance(cache_item, dict) and "is_anime" in cache_item:
                checked_at_str = cache_item.get("anime_checked_at")
                if checked_at_str:
                    try:
                        checked_at = datetime.strptime(checked_at_str, "%Y-%m-%d %H:%M:%S")
                        # 如果在 24 小时 (86400 秒) 内，直接返回缓存值
                        if (now - checked_at).total_seconds() < 86400:
                            return bool(cache_item["is_anime"])
                    except Exception:
                        pass

        # Step B: 24 小时超时或首次获取，从 mediainfo 或 TMDB 刷新判断
        is_anime = False
        if mediainfo:
            genre_ids = getattr(mediainfo, "genre_ids", None) or []
            if 16 in genre_ids or "16" in [str(i) for i in genre_ids]:
                is_anime = True
            else:
                genres = getattr(mediainfo, "genres", None) or []
                for g in genres:
                    if isinstance(g, dict):
                        g_id = g.get("id")
                        g_name = str(g.get("name", "")).lower()
                        if g_id == 16 or any(kw in g_name for kw in ["动画", "animation", "anime", "短片"]):
                            is_anime = True
                            break
                    elif isinstance(g, (str, int)):
                        g_str = str(g).lower()
                        if g_str == "16" or any(kw in g_str for kw in ["动画", "animation", "anime", "短片"]):
                            is_anime = True
                            break

        if not is_anime and tmdb_id:
            try:
                from app.modules.themoviedb import TheMovieDbModule
                tmdb_info = (
                    mediainfo.tmdb_info
                    if (mediainfo and mediainfo.tmdb_info)
                    else TheMovieDbModule().tmdb_info(int(tmdb_id), MediaType.TV)
                )
                if tmdb_info and tmdb_info.get("genres"):
                    for g in tmdb_info["genres"]:
                        g_id = g.get("id")
                        g_name = str(g.get("name", "")).lower()
                        if g_id == 16 or "动画" in g_name or "animation" in g_name:
                            is_anime = True
                            break
            except Exception as e:
                logger.debug(f"【短剧自动分类】检测动画类型失败: {e}")

        # Step C: 更新 cache 中的 is_anime 和 anime_checked_at
        if tmdb_id and str(tmdb_id) in self._cache_data:
            with lock:
                if str(tmdb_id) in self._cache_data:
                    self._cache_data[str(tmdb_id)]["is_anime"] = is_anime
                    self._cache_data[str(tmdb_id)]["anime_checked_at"] = now.strftime("%Y-%m-%d %H:%M:%S")
                    try:
                        self._cache_file_path.write_text(
                            json.dumps(self._cache_data, ensure_ascii=False, indent=2),
                            encoding="utf-8"
                        )
                    except Exception:
                        pass

        return is_anime

    def get_target_category(self, mediainfo: Optional[MediaInfo]) -> Tuple[str, str]:
        """
        根据动画类型与配置，返回目标 (category_name, category_dir)
        """
        tmdb_id = mediainfo.tmdb_id if mediainfo else None

        if self._enable_anime_category and self.check_is_anime(mediainfo=mediainfo, tmdb_id=tmdb_id):
            return self._anime_category_name, self._anime_category_dir

        return self._category_name, self._category_dir

    def check_is_short_drama(self, mediainfo: Optional[MediaInfo], video_path: Optional[str] = None) -> bool:
        """
        多策略判定入口（按优先级：缓存 -> 平台ID -> TMDB片长 -> 豆瓣片长 -> FFprobe探测）
        """
        if not self.get_state():
            return False

        if mediainfo and mediainfo.type and mediainfo.type != MediaType.TV:
            return False

        tmdb_id = mediainfo.tmdb_id if mediainfo else None
        title = mediainfo.title if mediainfo else ""

        # Step 0: 查询本地 JSON 缓存
        if self._enable_cache and tmdb_id and str(tmdb_id) in self._cache_data:
            cache_item = self._cache_data[str(tmdb_id)]
            if isinstance(cache_item, dict) and "is_short_drama" in cache_item:
                logger.info(f"【短剧自动分类】TMDB ID {tmdb_id} ({title}) 命中本地缓存: {'[短剧]' if cache_item['is_short_drama'] else '[普通长剧]'}")
                return bool(cache_item["is_short_drama"])

        # 获取 tmdb_info
        tmdb_info = None
        if mediainfo and mediainfo.tmdb_info:
            tmdb_info = mediainfo.tmdb_info
        elif tmdb_id:
            try:
                from app.modules.themoviedb import TheMovieDbModule
                tmdb_info = TheMovieDbModule().tmdb_info(int(tmdb_id), MediaType.TV)
            except Exception as e:
                logger.debug(f"【短剧自动分类】获取 TMDB 信息失败: {e}")

        # 检查是否为动画剧集（如果开启了动画独立分类，也同步维护 is_anime 记录）
        is_anime = self.check_is_anime(mediainfo=mediainfo, tmdb_id=tmdb_id)

        # Step 1: 平台 ID 策略
        if self._enable_network_check and tmdb_info:
            networks = tmdb_info.get("networks") or []
            target_networks = [n.strip() for n in str(self._short_drama_networks).split(",") if n.strip()]
            for net in networks:
                net_id = str(net.get("id"))
                if net_id in target_networks:
                    logger.info(f"【短剧自动分类】TMDB ID {tmdb_id} ({title}) 命中短剧平台 ID: {net_id}")
                    self._update_cache(tmdb_id, True, "network", network_id=net_id, is_anime=is_anime)
                    return True

        # Step 2: TMDB 单集片长策略
        if self._enable_tmdb_runtime and tmdb_info:
            episode_run_time = tmdb_info.get("episode_run_time") or []
            if episode_run_time and isinstance(episode_run_time, list) and len(episode_run_time) > 0:
                avg_runtime = sum(episode_run_time) / len(episode_run_time)
                threshold = float(self._episode_duration)
                is_short = (avg_runtime <= threshold)
                logger.info(
                    f"【短剧自动分类】TMDB ID {tmdb_id} ({title}) TMDB 片长: {avg_runtime:.1f}分钟 (阈值: {threshold}m) -> {'[短剧]' if is_short else '[普通长剧]'}"
                )
                self._update_cache(tmdb_id, is_short, "tmdb_runtime", runtime=avg_runtime, is_anime=is_anime)
                return is_short

        # Step 3: 豆瓣单集片长策略
        douban_id = mediainfo.douban_id if mediainfo else None
        if self._enable_douban_runtime and douban_id:
            douban_runtime = self.__get_douban_runtime(douban_id)
            if douban_runtime > 0:
                threshold = float(self._episode_duration)
                is_short = (douban_runtime <= threshold)
                logger.info(
                    f"【短剧自动分类】TMDB ID {tmdb_id} ({title}) 豆瓣片长: {douban_runtime:.1f}分钟 (阈值: {threshold}m) -> {'[短剧]' if is_short else '[普通长剧]'}"
                )
                self._update_cache(tmdb_id, is_short, "douban_runtime", runtime=douban_runtime, is_anime=is_anime)
                return is_short

        # Step 4: FFprobe 探测策略 (STRM URL 或 本地媒体文件)
        if self._enable_ffprobe and video_path:
            probe_target = self._resolve_probe_target(video_path)
            if probe_target:
                duration_sec = self.__get_duration(probe_target)
                if duration_sec > 0:
                    duration_min = duration_sec / 60.0
                    threshold = float(self._episode_duration)
                    is_short = (duration_min <= threshold)
                    logger.info(
                        f"【短剧自动分类】TMDB ID {tmdb_id} ({title}) FFprobe 探测片长: {duration_min:.1f}分钟 (阈值: {threshold}m) -> {'[短剧]' if is_short else '[普通长剧]'}"
                    )
                    self._update_cache(tmdb_id, is_short, "ffprobe", runtime=duration_min, is_anime=is_anime)
                    return is_short

        return False

    def __get_douban_runtime(self, douban_id: Union[int, str]) -> float:
        """
        抓取豆瓣页面获取单集片长
        """
        url = f"https://movie.douban.com/subject/{douban_id}/"
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=8) as resp:
                html = resp.read().decode("utf-8")
                match = re.search(r"单集片长:</span>\s*(\d+)分钟", html)
                if match:
                    return float(match.group(1))
        except Exception as e:
            logger.debug(f"【短剧自动分类】获取豆瓣 {douban_id} 单集片长失败: {e}")
        return 0.0

    @eventmanager.register(ChainEventType.TransferRenameBuild)
    def on_transfer_rename_build(self, event: Event):
        """
        1. 重命名构建事件 Hook
        """
        if not self.get_state():
            return
        data = event.event_data
        if not isinstance(data, TransferRenameBuildEventData):
            return
        source_path = data.source_path or (data.source_item.path if data.source_item else None)
        if not source_path:
            return
        rename_dict = data.rename_dict
        if not isinstance(rename_dict, dict):
            return

        if Path(source_path).suffix.lower() not in settings.RMT_MEDIAEXT:
            return

        mediainfo = rename_dict.get("__mediainfo__")
        if mediainfo and mediainfo.type and mediainfo.type != MediaType.TV:
            return
        if self.check_is_short_drama(mediainfo=mediainfo, video_path=str(source_path)):
            cat_name, _ = self.get_target_category(mediainfo)
            rename_dict["category"] = cat_name
            if rename_dict.get("__mediainfo__"):
                rename_dict["__mediainfo__"].category = cat_name

    @eventmanager.register(ChainEventType.TransferRename)
    def on_transfer_rename(self, event: Event):
        """
        1.2 重命名渲染改写 Hook：在整理预览及实际整理计算渲染路径后，改写为短剧绝对路径，
        确保【整理预览】界面直接展示短剧/动画短剧分类目录绝对路径。
        """
        if not self.get_state():
            return
        data = event.event_data
        if not isinstance(data, TransferRenameEventData):
            return
        if not data.path or not data.render_str:
            return
        source_path = data.source_path or (data.source_item.path if data.source_item else None)
        if not source_path:
            return

        if Path(source_path).suffix.lower() not in settings.RMT_MEDIAEXT:
            return

        mediainfo = data.rename_dict.get("__mediainfo__") if data.rename_dict else None
        if mediainfo and mediainfo.type and mediainfo.type != MediaType.TV:
            return
        if self.check_is_short_drama(mediainfo=mediainfo, video_path=str(source_path)):
            cat_name, cat_dir = self.get_target_category(mediainfo)
            if data.rename_dict and data.rename_dict.get("__mediainfo__"):
                data.rename_dict["__mediainfo__"].category = cat_name

            current_base_path = Path(data.path)

            if cat_dir:
                category_dir_path = Path(cat_dir)
                if not str(current_base_path).startswith(str(category_dir_path)):
                    try:
                        clean_abs_target = (category_dir_path / data.render_str).as_posix()
                        logger.info(
                            f"【短剧自动分类】整理预览/重命名改写：源文件 {source_path} 识别为短剧，直显目标绝对路径 -> {clean_abs_target}"
                        )
                        data.updated = True
                        data.updated_str = clean_abs_target
                        data.source = self.plugin_name
                    except Exception as e:
                        logger.error(f"【短剧自动分类】改写重命名路径失败: {e}")
            else:
                # 动态改写：未配置分类目录绝对路径，但 base path 包含原二级分类（如 国产剧），动态修正为短剧分类
                if current_base_path.name != cat_name:
                    try:
                        new_base_path = current_base_path.parent / cat_name
                        clean_abs_target = (new_base_path / data.render_str).as_posix()
                        logger.info(
                            f"【短剧自动分类】整理预览/重命名改写：源文件 {source_path} 识别为短剧，动态修正二级分类路径 -> {clean_abs_target}"
                        )
                        data.updated = True
                        data.updated_str = clean_abs_target
                        data.source = self.plugin_name
                    except Exception as e:
                        logger.error(f"【短剧自动分类】动态修正二级分类路径失败: {e}")

    @eventmanager.register(ChainEventType.TransferIntercept)
    def on_transfer_intercept(self, event: Event):
        """
        2. 整理拦截 Hook：在实际整理及目录确定时，将目标路径直接重定向改写为短剧/动画短剧分类目录
        """
        if not self.get_state():
            return
        data = event.event_data
        if not isinstance(data, TransferInterceptEventData):
            return
        if not data.fileitem or not data.target_path:
            return
        if data.mediainfo and data.mediainfo.type != MediaType.TV:
            return

        source_path = data.fileitem.path
        if self.check_is_short_drama(mediainfo=data.mediainfo, video_path=str(source_path)):
            cat_name, cat_dir = self.get_target_category(data.mediainfo)
            if data.mediainfo:
                data.mediainfo.category = cat_name

            category_dir_path = Path(cat_dir)
            target_path = data.target_path

            if not str(target_path).startswith(str(category_dir_path)):
                tv_name = data.mediainfo.title if (data.mediainfo and data.mediainfo.title) else None
                parts = target_path.parts
                idx = -1
                if tv_name:
                    for i, part in enumerate(parts):
                        if part == tv_name:
                            idx = i
                            break
                if idx != -1:
                    rel_subpath = Path(*parts[idx:])
                else:
                    rel_subpath = Path(target_path.name)

                new_target = category_dir_path / rel_subpath
                logger.info(f"【短剧自动分类】整理拦截重定向：目标路径由 {target_path} 修正为 {new_target}")
                data.target_path = new_target
                data.source = self.plugin_name

    @eventmanager.register(EventType.TransferComplete)
    def category_handler(self, event: Event):
        """
        3. 整理完成事件兜底：若前两重未覆盖到（如事后扫描），兜底将非短剧目录文件移动至对应短剧目录
        """
        if not event:
            return
        if not self.get_state() or not self._category_dir:
            return
        event_data = event.event_data
        mediainfo: MediaInfo = event_data.get("mediainfo")
        transferinfo: TransferInfo = event_data.get("transferinfo")
        if not mediainfo or not transferinfo:
            return
        if mediainfo.type != MediaType.TV:
            return

        file_list_new = transferinfo.file_list_new or []
        file_list = [file for file in file_list_new if Path(file).exists()]
        if not file_list:
            return

        sample_file = Path(file_list[0])
        target_path = sample_file.parent
        cat_name, cat_dir = self.get_target_category(mediainfo)
        category_dir_path = Path(cat_dir)

        if str(target_path).startswith(str(category_dir_path)):
            if self._notify:
                self.post_message(
                    mtype=NotificationType.Organize,
                    title="【短剧自动分类】",
                    text=f"已将短剧《{target_path.parent.name}》直接分类入库至 {cat_dir} 目录",
                )
            return

        with lock:
            if len(file_list) > 3:
                check_files = random.choices(file_list, k=3)
            else:
                check_files = file_list

            need_category = False
            for file in check_files:
                if self.check_is_short_drama(mediainfo=mediainfo, video_path=file):
                    need_category = True
                    break

            if need_category:
                if self._delay and float(self._delay) > 0:
                    time.sleep(float(self._delay))

                logger.info(f"【短剧自动分类】确认属于短剧，兜底机制触发：{target_path} 开始二次移动...")
                self.__move_files(target_path=target_path, dest_dir=cat_dir)

    @classmethod
    def _resolve_probe_target(cls, video_path: str) -> Optional[str]:
        """
        解析 ffprobe 探测目标：若是 .strm 文件，读取内部网络流 URL；否则返回本地文件路径
        """
        p = Path(video_path)
        if p.suffix.lower() != ".strm":
            return str(p)

        try:
            content = p.read_text(encoding="utf-8-sig", errors="replace")
            for line in content.splitlines():
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if len(line) >= 2 and line[0] == line[-1] and line[0] in "\"'":
                    line = line[1:-1].strip()
                if "%" in line:
                    try:
                        line = unquote(line)
                    except Exception:
                        pass
                if line:
                    return line
        except Exception as e:
            logger.error(f"【短剧自动分类】读取 STRM 文件失败 {video_path}: {e}")
        return None

    def __get_duration(self, video_path: str) -> float:
        """
        获取视频文件或 STRM 指向网络流的时长（分钟）
        """
        probe_target = self._resolve_probe_target(video_path)
        if not probe_target:
            return 0.0

        cmd = [
            'ffprobe', '-v', 'error',
            '-probesize', '1000000',
            '-analyzeduration', '2000000',
            '-show_entries', 'format=duration',
            '-of', 'default=noprint_wrappers=1:nokey=1',
            probe_target
        ]
        try:
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            output, error = process.communicate(timeout=30)
            duration_str = output.decode('utf-8', errors='ignore').strip()
            if duration_str:
                duration_sec = float(duration_str)
                return round(duration_sec / 60, 1)
        except subprocess.TimeoutExpired:
            logger.error(f"【短剧自动分类】ffprobe 探测超时 (30s): {probe_target}")
        except Exception as e:
            logger.error(f"【短剧自动分类】ffprobe 执行出错: {e}")

        return 0.0

    def __move_files(self, target_path: Path, dest_dir: str = None):
        """
        移动文件到分类目录
        """
        if not target_path.exists():
            return
        if target_path.is_file():
            target_path = target_path.parent
        tv_path = target_path.parent
        category_dir = dest_dir or self._category_dir
        new_path = Path(category_dir) / tv_path.name

        if not new_path.exists():
            try:
                shutil.move(tv_path, new_path)
            except Exception as e:
                logger.error(f"【短剧自动分类】移动目录失败：{e}")
                return
        else:
            for file in tv_path.iterdir():
                if file.is_file():
                    try:
                        relative_path = file.relative_to(tv_path)
                        dest_file = new_path / relative_path
                        dest_file.parent.mkdir(parents=True, exist_ok=True)
                        shutil.move(file, dest_file)
                    except Exception as e:
                        logger.error(f"【短剧自动分类】移动文件失败：{e}")
                        return
            if not SystemUtils.list_files(tv_path, extensions=settings.RMT_MEDIAEXT + settings.DOWNLOAD_TMPEXT):
                try:
                    shutil.rmtree(tv_path, ignore_errors=True)
                except Exception as e:
                    logger.error(f"【短剧自动分类】删除空目录失败：{e}")

        if self._notify:
            self.post_message(
                mtype=NotificationType.Organize,
                title="【短剧自动分类】",
                text=f"已将短剧《{tv_path.name}》移动分类至 {category_dir} 目录",
            )

    def stop_service(self):
        """
        停止服务
        """
        pass
