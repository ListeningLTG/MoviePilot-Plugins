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
    plugin_version = "0.1.1"
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
    _enable_cache = True

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

        self._load_cache()

    @property
    def _cache_file_path(self) -> Path:
        cache_dir = settings.CONFIG_PATH / "plugins" / "shortdramacompilation"
        cache_dir.mkdir(parents=True, exist_ok=True)
        return cache_dir / "cache.json"

    def _load_cache(self):
        with lock:
            path = self._cache_file_path
            if path.exists():
                try:
                    self._cache_data = json.loads(path.read_text(encoding="utf-8"))
                    logger.info(f"【短剧自动分类】成功加载本地 JSON 缓存，共记录 {len(self._cache_data)} 条剧集结果")
                except Exception as e:
                    logger.error(f"【短剧自动分类】读取缓存文件失败: {e}")
                    self._cache_data = {}
            else:
                self._cache_data = {}

    def _update_cache(self, tmdb_id: Union[int, str], title: str, is_short_drama: bool):
        if not self._enable_cache or not tmdb_id:
            return
        key = str(tmdb_id)
        with lock:
            self._cache_data[key] = {
                "title": title or "",
                "is_short_drama": bool(is_short_drama),
                "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            try:
                self._cache_file_path.write_text(
                    json.dumps(self._cache_data, ensure_ascii=False, indent=2),
                    encoding="utf-8"
                )
            except Exception as e:
                logger.error(f"【短剧自动分类】保存缓存文件失败: {e}")

    def get_state(self) -> bool:
        return True if self._enabled and (self._category_dir or self._category_name) and self._episode_duration else False

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
                                'props': {'cols': 12},
                                'content': [
                                    {
                                        'component': 'VAlert',
                                        'props': {
                                            'type': 'info',
                                            'variant': 'tonal',
                                            'text': '【多策略管道分类】1.TMDB播出平台ID -> 2.TMDB标注片长 -> 3.豆瓣标注片长 -> 4.FFprobe探测。判定结果自动存入 cache.json，下次整理直接 0ms 响应，支持手动在 cache.json 文件中修改判定结果。'
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
            "delay": '0',
            "category_name": '短剧',
            "category_dir": '',
            "episode_duration": '8',
            "enable_network_check": True,
            "short_drama_networks": '8020',
            "enable_tmdb_runtime": True,
            "enable_douban_runtime": True,
            "enable_ffprobe": True,
            "enable_cache": True,
        }

    def get_page(self) -> List[dict]:
        pass

    def check_is_short_drama(self, mediainfo: Optional[MediaInfo], video_path: Optional[str] = None) -> bool:
        """
        多策略判定入口（按优先级：缓存 -> 平台ID -> TMDB片长 -> 豆瓣片长 -> FFprobe探测）
        """
        if not self.get_state():
            return False

        tmdb_id = mediainfo.tmdb_id if mediainfo else None
        title = mediainfo.title if mediainfo else ""

        # Step 0: 查询本地 JSON 缓存 (0ms 响应)
        if self._enable_cache and tmdb_id:
            key = str(tmdb_id)
            if key in self._cache_data:
                cache_item = self._cache_data[key]
                if isinstance(cache_item, dict) and "is_short_drama" in cache_item:
                    res = bool(cache_item["is_short_drama"])
                    logger.info(
                        f"【短剧自动分类】命中 TMDB ID {tmdb_id} ({title}) 本地缓存判定结果 -> {'[短剧]' if res else '[普通长剧]'}"
                    )
                    return res
                elif isinstance(cache_item, bool):
                    logger.info(
                        f"【短剧自动分类】命中 TMDB ID {tmdb_id} ({title}) 本地缓存判定结果 -> {'[短剧]' if cache_item else '[普通长剧]'}"
                    )
                    return cache_item

        # 获取 tmdb_info
        tmdb_info = None
        if mediainfo and mediainfo.tmdb_info:
            tmdb_info = mediainfo.tmdb_info
        elif tmdb_id:
            try:
                from app.modules.themoviedb import TheMovieDbModule
                tmdb_info = TheMovieDbModule().tmdb_info(tmdb_id, MediaType.TV)
            except Exception as e:
                logger.debug(f"【短剧自动分类】获取 TMDB {tmdb_id} 详情失败: {e}")

        # Step 1: TMDB 播出平台 Network ID 匹配
        if self._enable_network_check and tmdb_info:
            networks = tmdb_info.get("networks") or []
            if networks and isinstance(networks, list):
                configured_nets = [s.strip() for s in str(self._short_drama_networks).split(",") if s.strip()]
                for net in networks:
                    net_id = str(net.get("id"))
                    if net_id in configured_nets:
                        net_name = net.get("name") or net_id
                        logger.info(
                            f"【短剧自动分类】策略 1 命中：TMDB 播出平台 '{net_name}' (ID: {net_id}) 属于短剧平台 -> 判定为 [短剧]"
                        )
                        self._update_cache(tmdb_id, title, True)
                        return True

        # Step 2: TMDB 单集片长 (S1E1 / episode_run_time) 匹配
        if self._enable_tmdb_runtime and tmdb_info:
            runtimes = tmdb_info.get("episode_run_time") or []
            if isinstance(runtimes, list) and runtimes:
                valid_runtimes = [float(r) for r in runtimes if float(r) > 0]
                if valid_runtimes:
                    if all(r <= float(self._episode_duration) for r in valid_runtimes):
                        logger.info(
                            f"【短剧自动分类】策略 2 命中：TMDB 标注单集片长 {valid_runtimes} 分钟 ≤ 阈值 {self._episode_duration} -> 判定为 [短剧]"
                        )
                        self._update_cache(tmdb_id, title, True)
                        return True
                    elif any(r > float(self._episode_duration) for r in valid_runtimes):
                        logger.info(
                            f"【短剧自动分类】策略 2 确定：TMDB 标注单集片长 {valid_runtimes} 分钟 > 阈值 {self._episode_duration} -> 确定为 [普通长剧]，终结后续探测"
                        )
                        self._update_cache(tmdb_id, title, False)
                        return False

        # Step 3: 豆瓣单集片长解析匹配
        if self._enable_douban_runtime and mediainfo:
            douban_id = mediainfo.douban_id
            if douban_id:
                douban_runtime = self.__get_douban_runtime(douban_id)
                if douban_runtime > float(self._episode_duration):
                    logger.info(
                        f"【短剧自动分类】策略 3 确定：豆瓣标注单集片长 {douban_runtime} 分钟 > 阈值 {self._episode_duration} -> 确定为 [普通长剧]，终结后续探测"
                    )
                    self._update_cache(tmdb_id, title, False)
                    return False
                elif 0 < douban_runtime <= float(self._episode_duration):
                    logger.info(
                        f"【短剧自动分类】策略 3 命中：豆瓣标注单集片长 {douban_runtime} 分钟 ≤ 阈值 {self._episode_duration} -> 判定为 [短剧]"
                    )
                    self._update_cache(tmdb_id, title, True)
                    return True

        # Step 4: FFprobe 媒体文件真实时长探测 (兜底)
        if self._enable_ffprobe and video_path:
            duration = self.__get_duration(str(video_path))
            if duration > float(self._episode_duration):
                logger.info(
                    f"【短剧自动分类】策略 4 探测：FFprobe 文件片长 {duration} 分钟 > 阈值 {self._episode_duration} -> 判定为 [普通长剧]"
                )
                self._update_cache(tmdb_id, title, False)
                return False
            elif 0 < duration <= float(self._episode_duration):
                logger.info(
                    f"【短剧自动分类】策略 4 命中：FFprobe 文件片长 {duration} 分钟 ≤ 阈值 {self._episode_duration} -> 判定为 [短剧]"
                )
                self._update_cache(tmdb_id, title, True)
                return True

        # 终极处理：若确定了 tmdb_id，但未被判为短剧，则保存 False 结果到缓存
        if tmdb_id:
            self._update_cache(tmdb_id, title, False)
        return False

    def __get_douban_runtime(self, douban_id: Union[int, str]) -> float:
        """
        抓取豆瓣页面并解析 '单集片长'
        """
        url = f"https://movie.douban.com/subject/{douban_id}/"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=5) as response:
                html = response.read().decode('utf-8', errors='ignore')
                match = re.search(r'单集片长:?\s*</span>\s*([^<]+)', html)
                if match:
                    text = match.group(1).strip()
                    min_match = re.search(r'(\d+)\s*分(?:\s*(\d+)\s*秒)?', text)
                    if min_match:
                        mins = float(min_match.group(1))
                        secs = float(min_match.group(2)) if min_match.group(2) else 0.0
                        return mins + (secs / 60.0)
                    digit_match = re.search(r'(\d+(?:\.\d+)?)', text)
                    if digit_match:
                        return float(digit_match.group(1))
        except Exception as e:
            logger.debug(f"【短剧自动分类】获取豆瓣 {douban_id} 单集片长失败: {e}")
        return 0.0

    @eventmanager.register(ChainEventType.TransferRenameBuild)
    def on_transfer_rename_build(self, event: Event):
        """
        1. 重命名构建事件 Hook：在整理预览及实际整理渲染模板前，识别短剧。
        若属于短剧，注入 rename_dict["category"] = "短剧"。
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
        if self.check_is_short_drama(mediainfo=mediainfo, video_path=str(source_path)):
            rename_dict["category"] = self._category_name
            if rename_dict.get("__mediainfo__"):
                rename_dict["__mediainfo__"].category = self._category_name

    @eventmanager.register(ChainEventType.TransferRename)
    def on_transfer_rename(self, event: Event):
        """
        1.2 重命名渲染改写 Hook：在整理预览及实际整理计算渲染路径后，改写为短剧绝对路径，
        确保【整理预览】界面直接展示短剧分类目录绝对路径。
        """
        if not self.get_state() or not self._category_dir:
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
        if self.check_is_short_drama(mediainfo=mediainfo, video_path=str(source_path)):
            if data.rename_dict and data.rename_dict.get("__mediainfo__"):
                data.rename_dict["__mediainfo__"].category = self._category_name

            category_dir_path = Path(self._category_dir)
            current_base_path = Path(data.path)

            if not str(current_base_path).startswith(str(category_dir_path)):
                try:
                    clean_abs_target = (category_dir_path / data.render_str).as_posix()
                    logger.info(
                        f"【短剧自动分类】整理预览/重命名改写：源文件 {source_path} 识别为短剧，直显短剧绝对路径 -> {clean_abs_target}"
                    )
                    data.updated = True
                    data.updated_str = clean_abs_target
                except Exception as e:
                    logger.error(f"【短剧自动分类】改写重命名路径失败: {e}")

    @eventmanager.register(ChainEventType.TransferIntercept)
    def on_transfer_intercept(self, event: Event):
        """
        2. 整理拦截 Hook：在实际整理及目录确定时，将目标路径直接重定向改写为短剧分类目录
        """
        if not self.get_state() or not self._category_dir:
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
            if data.mediainfo:
                data.mediainfo.category = self._category_name

            category_dir_path = Path(self._category_dir)
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

    @eventmanager.register(EventType.TransferComplete)
    def category_handler(self, event: Event):
        """
        3. 整理完成事件兜底：若前两重未覆盖到（如事后扫描），兜底将非短剧目录文件移动至短剧目录
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
        category_dir_path = Path(self._category_dir)

        if str(target_path).startswith(str(category_dir_path)):
            if self._notify:
                self.post_message(
                    mtype=NotificationType.Organize,
                    title="【短剧自动分类】",
                    text=f"已将短剧《{target_path.parent.name}》直接分类入库至 {self._category_dir} 目录",
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
                self.__move_files(target_path=target_path)

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

    def __move_files(self, target_path: Path):
        """
        移动文件到分类目录
        """
        if not target_path.exists():
            return
        if target_path.is_file():
            target_path = target_path.parent
        tv_path = target_path.parent
        new_path = Path(self._category_dir) / tv_path.name

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
                text=f"已将短剧《{tv_path.name}》移动分类至 {self._category_dir} 目录",
            )

    def stop_service(self):
        """
        停止服务
        """
        pass
