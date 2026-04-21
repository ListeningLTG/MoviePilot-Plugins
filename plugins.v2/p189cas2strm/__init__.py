from typing import Any, List, Dict, Tuple, Optional

import importlib
import time
from apscheduler.triggers.cron import CronTrigger
from app.plugins import _PluginBase
from app.core.event import eventmanager, Event
from app.schemas import NotificationType
from app.schemas.types import EventType
from app.log import logger


_configer = None
_extract_189_links = None
_task_queue = None
_cas_record_manager = None
_cas_redirect = None
_P189ClientWrapper = None


def _resolve_deps():
    global _configer, _extract_189_links, _task_queue, _cas_record_manager, _cas_redirect, _P189ClientWrapper

    if all([
        _configer,
        _extract_189_links,
        _task_queue,
        _cas_record_manager,
        _cas_redirect,
        _P189ClientWrapper,
    ]):
        return _configer, _extract_189_links, _task_queue, _cas_record_manager, _cas_redirect, _P189ClientWrapper

    last_err = None
    module_base = __package__ or "app.plugins.p189cas2strm"
    for _ in range(6):
        try:
            _configer = importlib.import_module(f"{module_base}.config").configer
            _extract_189_links = importlib.import_module(f"{module_base}.utils").extract_189_links
            logic_mod = importlib.import_module(f"{module_base}.logic")
            _task_queue = logic_mod.task_queue
            _cas_record_manager = logic_mod.cas_record_manager
            _cas_redirect = logic_mod.cas_redirect
            _P189ClientWrapper = importlib.import_module(f"{module_base}.p189_client").P189ClientWrapper
            return _configer, _extract_189_links, _task_queue, _cas_record_manager, _cas_redirect, _P189ClientWrapper
        except Exception as err:
            last_err = err
            time.sleep(0.2)

    raise last_err

class p189cas2strm(_PluginBase):
    """
    cas文件生成strm
    """

    # 插件名称
    plugin_name = "cas生成strm"
    # 插件描述
    plugin_desc = "将含有cas文件的天翼云盘分享链接生成STRM，支持播放时自动秒传"
    # 插件图标
    plugin_icon = "https://raw.githubusercontent.com/ListeningLTG/MoviePilot-Plugins/refs/heads/main/icons/p189.png"
    # 插件版本
    plugin_version = "1.0.2"
    # 插件作者
    plugin_author = "ListeningLTG"
    # 作者主页
    author_url = "https://github.com/ListeningLTG"
    # 插件配置项ID前缀
    plugin_config_prefix = "p189cas2strm_"
    # 加载顺序
    plugin_order = 11
    # 可使用的用户级别
    auth_level = 1

    def __init__(self):
        super().__init__()

    def init_plugin(self, config: dict = None):
        """
        加载配置并启动
        """
        configer, _, task_queue, _, _, _ = _resolve_deps()

        if config:
            configer.load_from_dict(config)
            configer.update_plugin_config()

        task_queue.set_notify_callback(self._send_notify)

        if configer.enabled:
            task_queue.start()
            logger.info("【P189Cas2Strm】插件已启动，异步任务工作线程就绪。")
        else:
            task_queue.stop()
            logger.info("【P189Cas2Strm】插件服务已停止。")

    def get_state(self) -> bool:
        configer, _, _, _, _, _ = _resolve_deps()
        return configer.enabled

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """
        插件配置页面
        """
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "enabled", "label": "启用插件"},
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "moviepilot_transfer", "label": "STRM 交由 MP 整理"},
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
                                            "hint": "从指令文本中提取 TMDB ID，传给 MP 整理时直接匹配媒体信息",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {"model": "username", "label": "天翼云盘账号"},
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {"model": "password", "label": "天翼云盘密码/Cookie", "type": "password"},
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {"model": "strm_save_path", "label": "STRM 本地保存路径"},
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {"model": "p189_target_path", "label": "网盘秒传缓存目录"},
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {"model": "cleanup_cron", "label": "定时清理 Cron"},
                                    }
                                ],
                            },
                        ],
                    },
                ],
            }
        ], {
            "enabled": False,
            "username": "",
            "password": "",
            "strm_save_path": "",
            "p189_target_path": "/p189cas2strm_cache",
            "cleanup_cron": "0 2 * * *",
            "moviepilot_transfer": True,
            "tmdb_extract": False,
        }

    def get_page(self) -> List[dict]:
        """
        插件数据展示页
        """
        _, _, task_queue, _, _, _ = _resolve_deps()
        queue_size = task_queue._queue.qsize()
        processing = task_queue.processing_count
        return [
            {
                "component": "VCard",
                "content": [
                    {
                        "component": "VCardTitle",
                        "text": "任务监控看板",
                    },
                    {
                        "component": "VCardText",
                        "content": [
                            {"component": "div", "text": f"当前排队任务数: {queue_size}"},
                            {"component": "div", "text": f"当前正在处理: {processing}"},
                        ],
                    },
                ],
            }
        ]

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return [
            {
                "cmd": "/cas2strm",
                "event": EventType.PluginAction,
                "desc": "提取 189 分享中的 CAS 内容并加入 STRM 队列",
                "category": "189",
                "data": {"plugin_id": "p189cas2strm", "action": "cas2strm"},
            },
        ]

    def get_service(self) -> List[Dict[str, Any]]:
        """
        注册定时清理服务
        """
        configer, _, _, _, _, _ = _resolve_deps()
        cron_expr = (configer.cleanup_cron or "0 2 * * *").strip()
        try:
            trigger = CronTrigger.from_crontab(cron_expr)
        except Exception as err:
            logger.warning(f"【P189Cas2Strm】cleanup_cron 配置无效: {cron_expr}，使用默认 0 2 * * *，错误: {err}")
            trigger = CronTrigger.from_crontab("0 2 * * *")

        return [
            {
                "id": "p189_cleanup",
                "name": "189秒传清理",
                "trigger": trigger,
                "func": self._do_cleanup,
                "kwargs": {}
            }
        ]

    def get_api(self) -> List[Dict[str, Any]]:
        """
        获取插件 API 端点
        """
        _, _, _, _, cas_redirect, _ = _resolve_deps()
        return [
            {
                "path": "/redirect",
                "endpoint": cas_redirect,
                "methods": ["GET"],
                "summary": "189 CAS 重定向"
            }
        ]

    @eventmanager.register(EventType.PluginAction)
    def handle_action(self, event: Event):
        configer, extract_189_links, task_queue, _, _, _ = _resolve_deps()
        if not configer.enabled:
            return
        data = event.event_data or {}
        if data.get("plugin_id") != "p189cas2strm" or data.get("action") != "cas2strm":
            return
            
        arg_str = data.get("arg_str", "")
        logger.info(f"【P189Cas2Strm】接收指令: {arg_str}")
        
        links = extract_189_links(data)
        if not links:
            logger.warning("【P189Cas2Strm】未识别到任何 189 分享链接")
            self._send_notify(data.get("userid"), "提示", "❌ 未识别到有效的 189 分享链接")
            return
            
        count = 0
        for link in links:
            share_code = link["share_code"]
            logger.info(f"【P189Cas2Strm】提取到分享码: {share_code}，正在压入队列...")
            queued = task_queue.add_task(
                share_code=share_code,
                access_code=link["access_code"],
                user_id=data.get("userid"),
                arg_str=arg_str
            )
            if queued:
                logger.info(f"【P189Cas2Strm】任务入队成功: {share_code}")
                count += 1
            else:
                logger.warning(f"【P189Cas2Strm】任务重复，已自动跳过: {share_code}")
        
        if count > 0:
            self._send_notify(data.get("userid"), "成功", f"✅ 已将 {count} 个新任务加入处理队列")
        elif len(links) > 0:
            self._send_notify(data.get("userid"), "提示", "ℹ️ 链接已在处理队列中，请勿重复提交")

    async def _do_cleanup(self, **kwargs):
        """
        定时清理网盘目录及回收站
        """
        configer, _, _, cas_record_manager, _, P189ClientWrapper = _resolve_deps()
        if not configer.enabled:
            return
            
        logger.info("【P189Cas2Strm】正在执行定时清理...")
        client = P189ClientWrapper(configer.username, configer.password)
        await client.ensure_logged_in()
        
        target_id = await client.fs_get_path_id(configer.p189_target_path)
        if target_id and target_id != "-11":
            await client.fs_delete(target_id, is_folder=True)
            await client.fs_empty_recycle()
            cas_record_manager.clear()
            logger.info("【P189Cas2Strm】定时清理完成")

    def stop_service(self):
        """
        停止插件服务
        """
        _, _, task_queue, _, _, _ = _resolve_deps()
        task_queue.stop()
        logger.info("【P189Cas2Strm】插件服务已停止")

    def _send_notify(self, user_id, title, text):
        self.post_message(
            mtype=NotificationType.Plugin,
            title=title,
            text=text,
            userid=user_id
        )
