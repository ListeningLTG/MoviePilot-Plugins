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
    plugin_desc = "当媒体服务器离线时自动暂停所有订阅，在线时恢复。"
    plugin_icon = "pause.png"
    plugin_version = "1.0"
    plugin_author = "ListeningLTG"
    plugin_config_prefix = "autopausesub_"
    plugin_order = 20

    # 插件运行时状态配置
    _enabled = False
    _mediaservers = []
    _interval = 10

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
                                'props': {'cols': 12},
                                'content': [
                                    {
                                        'component': 'VAlert',
                                        'props': {
                                            'type': 'info',
                                            'variant': 'tonal',
                                            'text': '启用后，插件将每隔指定时间检测所选媒体服务器的状态。如果所有服务器都不在线，将保存当前订阅状态并全部暂停；当任一服务器恢复在线时，将自动还原这些订阅的状态。'
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
            "interval": 10,
            "mediaservers": []
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
        services = self.mediaserver_helper.get_services(name_filters=self._mediaservers)
        
        # 统计在线状态
        online_servers = []
        offline_servers = []
        
        for name in self._mediaservers:
            service = services.get(name)
            if service and not service.instance.is_inactive():
                online_servers.append(name)
            else:
                offline_servers.append(name)

        logger.info(f"媒体服务器检测结果：在线 {online_servers}，离线 {offline_servers}")

        # 如果所有选定的服务器都离线
        if not online_servers:
            self._pause_all_subscriptions()
        else:
            # 只要有一个在线，就尝试恢复
            self._restore_subscriptions()

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

    def get_page(self) -> List[dict]:
        return []
