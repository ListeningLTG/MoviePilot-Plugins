import time
from typing import Any, List, Dict, Tuple, Optional

from app.core.config import settings
from app.db.subscribe_oper import SubscribeOper
from app.helper.mediaserver import MediaServerHelper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import NotificationType


class AutoPauseSub(_PluginBase):
    """
    自动暂停订阅插件
    当媒体服务器离线时自动暂停所有订阅，在线时恢复。
    """

    # 插件基本信息
    plugin_name = "自动暂停订阅"
    plugin_desc = "当媒体服务器离线时自动暂停所有订阅，在线时恢复"
    plugin_icon = "https://raw.githubusercontent.com/ListeningLTG/MoviePilot-Plugins/refs/heads/main/icons/autopause.png"
    plugin_version = "1.0.4"
    # 插件作者
    plugin_author = "ListeningLTG"
    # 作者主页
    author_url = "https://github.com/ListeningLTG"
    plugin_config_prefix = "autopausesub_"
    plugin_order = 20

    # 插件运行时状态配置
    _enabled = False
    _mediaservers = []
    _interval = 10
    _pause_condition = "any"
    _restore_condition = "all"

    def __init__(self):
        super().__init__()
        self.subscribe_oper = SubscribeOper()
        self.mediaserver_helper = MediaServerHelper()

    def init_plugin(self, config: dict = None):
        """
        初始化插件配置
        """
        if config:
            self._enabled = config.get("enabled", False)
            self._mediaservers = config.get("mediaservers") or []
            self._interval = int(config.get("interval") or 10)
            self._pause_condition = config.get("pause_condition") or "any"
            self._restore_condition = config.get("restore_condition") or "all"

    def get_state(self) -> bool:
        return self._enabled

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
                                'props': {'cols': 12, 'md': 6},
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
                                'props': {'cols': 12, 'md': 6},
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'interval',
                                            'label': '检测间隔时间（分钟）',
                                            'placeholder': '10',
                                            'type': 'number'
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
                                'props': {'cols': 12},
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
                                                      for config in self.mediaserver_helper.get_configs().values()]
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
                                'props': {'cols': 12, 'md': 6},
                                'content': [
                                    {
                                        'component': 'VSelect',
                                        'props': {
                                            'model': 'pause_condition',
                                            'label': '暂停触发条件',
                                            'items': [
                                                {'title': '任一服务器离线', 'value': 'any'},
                                                {'title': '所有服务器离线', 'value': 'all'}
                                            ]
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
                                            'model': 'restore_condition',
                                            'label': '恢复触发条件',
                                            'items': [
                                                {'title': '任一服务器在线', 'value': 'any'},
                                                {'title': '所有服务器在线', 'value': 'all'}
                                            ]
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
                                'props': {'cols': 12},
                                'content': [
                                    {
                                        'component': 'VAlert',
                                        'props': {
                                            'type': 'info',
                                            'variant': 'tonal',
                                            'text': '启用后，插件将根据所选条件检测媒体服务器状态。若触发暂停条件，将保存当前订阅状态并全部暂停；若触发恢复条件，将自动还原这些订阅。'
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ], {
            "enabled": self._enabled,
            "interval": self._interval,
            "mediaservers": self._mediaservers,
            "pause_condition": self._pause_condition or "any",
            "restore_condition": self._restore_condition or "all"
        }

    def get_service(self) -> List[Dict[str, Any]]:
        """
        注册插件公共服务
        """
        if self._enabled:
            return [{
                "id": "check_mediaserver_status",
                "name": "检测媒体服务器状态",
                "trigger": "interval",
                "func": self.check_mediaserver_status,
                "kwargs": {"minutes": self._interval}
            }]
        return []

    def check_mediaserver_status(self):
        """
        定期检测媒体服务器在线状态
        """
        if not self._enabled or not self._mediaservers:
            return

        logger.info("开始检测媒体服务器状态...")

        # 获取所有配置的服务
        try:
            services = self.mediaserver_helper.get_services(name_filters=self._mediaservers)
        except Exception:
            # 兼容性保护：避免因为配置字段变更或模块初始化异常导致插件逻辑中断
            logger.exception("获取媒体服务器服务实例时发生异常")
            services = {}
        
        # 统计在线状态
        online_servers = []
        offline_servers = []
        
        for name in self._mediaservers:
            logger.info(f"正在检测媒体服务器: {name} ...")
            try:
                service = services.get(name)
                if service and service.instance and self._is_server_online(service.instance):
                    online_servers.append(name)
                    logger.info(f"媒体服务器 {name} 检测完成，结果: 在线")
                else:
                    offline_servers.append(name)
                    logger.info(f"媒体服务器 {name} 检测完成，结果: 离线")
            except Exception as e:
                # 你遇到的报错通常来自旧代码访问 MediaServerConf.host（该字段在当前模型中不存在）
                # 这里做兜底，避免异常导致“无论在线离线都判定为离线且一直报错”
                offline_servers.append(name)
                logger.error(f"检测媒体服务器 {name} 在线状态时发生异常: {e}")

        logger.info(f"媒体服务器检测结果：在线 {online_servers}，离线 {offline_servers}")

        logger.info(f"媒体服务器检测结果：在线 {online_servers}，离线 {offline_servers}")

        # 判断是否应暂停
        # all: 所有选定的服务器都离线 (即没有在线库)
        # any: 任一选定的服务器离线 (即离线库列表不为空)
        should_pause = False
        if self._pause_condition == "any":
            should_pause = len(offline_servers) > 0
        else:
            should_pause = len(online_servers) == 0

        # 判断是否应恢复
        # any: 只要有一个在线
        # all: 必须全部在线 (即离线库列表为空)
        should_restore = False
        if self._restore_condition == "all":
            should_restore = len(offline_servers) == 0
        else:
            should_restore = len(online_servers) > 0

        # 获取当前保存的暂停状态
        saved_states = self.get_data("saved_subscription_states")

        if should_pause and not saved_states:
            # 符合暂停条件且当前未暂停
            self._pause_all_subscriptions()
        elif should_restore and saved_states:
            # 符合恢复条件且当前处于暂停状态
            self._restore_subscriptions()

    @staticmethod
    def _is_server_online(instance: Any) -> bool:
        """判断媒体服务器实例是否在线。

        - 优先兼容项目内置媒体服务器实现：通过 is_inactive() 判断。
        - 兼容部分实现/单测桩：通过 get_server_id() 是否有值判断。
        """
        if not instance:
            return False
        if hasattr(instance, "get_server_id") and callable(getattr(instance, "get_server_id")):
            try:
                return bool(instance.get_server_id())
            except Exception:
                # 兼容不规范实现
                pass

        if hasattr(instance, "is_inactive") and callable(getattr(instance, "is_inactive")):
            try:
                inactive = instance.is_inactive()
                if isinstance(inactive, bool):
                    return not inactive
            except Exception:
                pass
        # 最保守的兜底：无法判断时按离线处理
        return False

    def _pause_all_subscriptions(self):
        """
        暂停所有订阅
        """
        # 检查是否已经处于“已保存并暂停”状态，避免重复操作
        saved_states = self.get_data("saved_subscription_states")
        if saved_states:
            logger.debug("订阅已处于暂停状态，跳过。")
            return

        logger.warning("检测到所选媒体服务器全部离线，准备暂停所有订阅...")
        
        # 获取所有非暂停状态的订阅
        subscriptions = self.subscribe_oper.list()
        states_to_save = {}
        
        paused_count = 0
        for sub in subscriptions:
            if sub.state != 'S':
                states_to_save[str(sub.id)] = sub.state
                # 更新为暂停
                self.subscribe_oper.update(sub.id, {"state": 'S'})
                paused_count += 1
        
        if states_to_save:
            self.save_data("saved_subscription_states", states_to_save)
            msg = f"检测到媒体服务器 {self._mediaservers} 全部离线，已自动暂停 {paused_count} 个订阅并保存其状态。"
            logger.warning(msg)
            self.post_message(mtype=NotificationType.MediaServer, title="订阅自动暂停", text=msg)

    def _restore_subscriptions(self):
        """
        还原订阅状态
        """
        saved_states = self.get_data("saved_subscription_states")
        if not saved_states:
            return

        logger.info("检测到媒体服务器恢复在线，准备还原订阅状态...")
        
        restored_count = 0
        for sid, state in saved_states.items():
            self.subscribe_oper.update(int(sid), {"state": state})
            restored_count += 1
        
        # 清除保存的数据
        self.del_data("saved_subscription_states")
        
        msg = f"检测到媒体服务器已恢复在线，已还原 {restored_count} 个订阅的原始状态。"
        logger.info(msg)
        self.post_message(mtype=NotificationType.MediaServer, title="订阅已还原", text=msg)

    def stop_service(self):
        """
        停止插件
        """
        pass

    def get_api(self) -> List[Dict[str, Any]]:
        return []

    def get_page(self) -> Optional[List[dict]]:
        pass
