import os
import random
import shutil
import subprocess
import threading
import time
from pathlib import Path
from typing import Any, List, Dict, Tuple, Optional
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
    plugin_desc = "网络短剧自动分类到独立目录，支持STRM格式、整理预览直显及一次性直存。"
    # 插件图标
    plugin_icon = "https://raw.githubusercontent.com/ListeningLTG/MoviePilot-Plugins/refs/heads/main/icons/hg.jpeg"
    # 插件版本
    plugin_version = "0.0.3"
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

    def init_plugin(self, config: dict = None):
        if config:
            self._enabled = config.get("enabled")
            self._delay = config.get("delay") or 0
            self._notify = config.get("notify")
            self._category_dir = config.get("category_dir") or ""
            self._category_name = config.get("category_name") or "短剧"
            self._episode_duration = config.get("episode_duration") or 8

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
                                            'model': 'notify',
                                            'label': '发送消息通知',
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
                                    'md': 3,
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'category_name',
                                            'label': '二级分类名称',
                                            'placeholder': '短剧'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4,
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'category_dir',
                                            'label': '分类目录绝对路径',
                                            'placeholder': '/media/短剧'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 3,
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'episode_duration',
                                            'label': '单集时长阈值（分钟）',
                                            'placeholder': '8'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 2,
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'delay',
                                            'label': '入库延迟时间（秒）',
                                            'placeholder': '0'
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
                                            'text': '【全流程分类】小于单集时长的视频/STRM文件自动注入“短剧”分类。支持在MP【整理预览】中直接预览短剧路径，并一次性直存至短剧目录。需要系统安装FFmpeg。'
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
            "category_dir": '短剧',
            "episode_duration": '8'
        }

    def get_page(self) -> List[dict]:
        pass

    @eventmanager.register(ChainEventType.TransferRenameBuild)
    def on_transfer_rename_build(self, event: Event):
        """
        1. 重命名构建事件 Hook：在整理预览及实际整理渲染模板前，检测视频/STRM时长。
        若属于短剧，注入 rename_dict["category"] = "短剧"，使预览与模板渲染直接生效。
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

        # 验证后缀
        if Path(source_path).suffix.lower() not in settings.RMT_MEDIAEXT:
            return

        # 测算时长
        duration = self.__get_duration(str(source_path))
        if 0 < duration <= float(self._episode_duration):
            logger.info(
                f"【短剧自动分类】预览/整理上下文构建：源文件 {source_path} 识别时长 {duration} 分钟 ≤ 阈值 {self._episode_duration}，注入分类：{self._category_name}"
            )
            rename_dict["category"] = self._category_name

    @eventmanager.register(ChainEventType.TransferRename)
    def on_transfer_rename(self, event: Event):
        """
        1.2 重命名渲染改写 Hook：在整理预览及实际整理计算渲染路径后，改写相对路径，
        确保【整理预览】界面直接展示短剧分类目录路径。
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

        # 验证后缀
        if Path(source_path).suffix.lower() not in settings.RMT_MEDIAEXT:
            return

        # 测算时长
        duration = self.__get_duration(str(source_path))
        if 0 < duration <= float(self._episode_duration):
            category_dir_path = Path(self._category_dir)
            current_base_path = Path(data.path)

            # 如果当前基础路径未包含短剧目录
            if not str(current_base_path).startswith(str(category_dir_path)):
                try:
                    rel_dir = os.path.relpath(category_dir_path, current_base_path)
                    new_render_str = (Path(rel_dir) / data.render_str).as_posix()
                    logger.info(
                        f"【短剧自动分类】整理预览重命名改写：源文件 {source_path} 识别时长 {duration} 分钟 ≤ 阈值 {self._episode_duration}，修正预览路径相对前缀 -> {new_render_str}"
                    )
                    data.updated = True
                    data.updated_str = new_render_str
                except Exception as e:
                    logger.error(f"【短剧自动分类】改写重命名相对路径失败: {e}")

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
        duration = self.__get_duration(str(source_path))
        if 0 < duration <= float(self._episode_duration):
            category_dir_path = Path(self._category_dir)
            target_path = data.target_path

            # 如果当前计算出的目标路径尚未包含短剧目录，进行路径改写
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

        # 已在短剧目录，无需再次移动
        if str(target_path).startswith(str(category_dir_path)):
            if self._notify:
                self.post_message(
                    mtype=NotificationType.Organize,
                    title="【短剧自动分类】",
                    text=f"已将短剧《{target_path.parent.name}》直接分类入库至 {self._category_dir} 目录",
                )
            return

        # 若不在短剧目录，进行后置兜底移动
        with lock:
            if len(file_list) > 3:
                check_files = random.choices(file_list, k=3)
            else:
                check_files = file_list

            need_category = False
            valid_durations = []
            for file in check_files:
                duration = self.__get_duration(file)
                if duration <= 0:
                    logger.warning(f"【短剧自动分类】{file} 无法获取有效时长（可能超时或链接不可达），跳过分类移动")
                    valid_durations.clear()
                    break
                if duration > float(self._episode_duration):
                    logger.info(f"【短剧自动分类】{file} 时长 {duration} 分钟 > 阈值 {self._episode_duration} 分钟，判定为普通长剧，不移动")
                    valid_durations.clear()
                    break
                valid_durations.append(duration)

            if valid_durations:
                need_category = True

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
