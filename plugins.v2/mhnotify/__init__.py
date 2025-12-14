import time

from typing import List, Tuple, Dict, Any
from apscheduler.triggers.cron import CronTrigger

from app.core.config import settings
from app.core.event import eventmanager
from app.schemas.types import EventType
from app.utils.http import RequestUtils
from app.log import logger
from app.plugins import _PluginBase


class MHNotify(_PluginBase):
    # 插件名称
    plugin_name = "MediaHelper通知"
    # 插件描述
    plugin_desc = "整理完成115里的媒体后，通知MediaHelper进行增量同步（strm生成）"
    # 插件图标
    plugin_icon = "https://raw.githubusercontent.com/JieWSOFT/MediaHelp/main/frontend/apps/web-antd/public/icon.png"
    # 插件版本
    plugin_version = "0.1"
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
    _mh_notify_type = None
    _mh_domain = None
    _mh_username = None
    _mh_password = None
    _mh_job_names = None
    _enabled = False
    _last_event_time = 0
    # 等待通知数量
    _wait_notify_count = 0

    def init_plugin(self, config: dict = None):
        if config:
            self._enabled = config.get("enabled")
            self._mh_notify_type = config.get("mh_notify_type")
            self._mh_domain = config.get("mh_domain")
            self._mh_username = config.get('mh_username')
            self._mh_password = config.get('mh_password')
            self._mh_job_names = config.get('mh_job_names') or ""

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
        if self._enabled:
            return [{
                "id": "MHNotify",
                "name": "MediaHelper通知",
                "trigger": CronTrigger.from_crontab("* * * * *"),
                "func": self.__notify_mh,
                "kwargs": {}
            }]
        return []

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        pass

    def get_api(self) -> List[Dict[str, Any]]:
        # 不需要前端即时API
        return []

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """
        拼装插件配置页面，需要返回两块数据：1、页面配置；2、数据结构
        """
        # 不在渲染阶段请求后端，改为名称输入方案

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
                                        'component': 'VSelect',
                                        'props': {
                                            'model': 'mh_notify_type',
                                            'label': '通知类型',
                                            'items': [
                                                {'title': '增量同步',
                                                    'value': 'lift_sync'},
                                                # {'title': '增量同步+自动整理',
                                                #     'value': 'auto_organize'},
                                            ]
                                        }
                                    }
                                ]
                            },
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
                                            'hint': '填写名称后将匹配 /api/v1/scheduled/tasks 中启用的 cloud_strm_sync 任务，按名称获取对应UUID批量执行；留空则默认匹配名称含“115网盘”'
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
                                            'text': '当MP整理或刮削好115里的媒体后，会通知MediaHelper进行增量同步（strm生成）'
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
                                            'text': '为了防止通知次数过于频繁，会有1-2分钟的等待，只有在此期间再无其它整理或刮削时，才会进行通知'
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
            "mh_notify_type": "lift_sync",
            "mh_username": "admin",
            "mh_password": "admin",
            "mh_job_names": "115网盘1,天翼云盘1",
            "mh_domain": "http://192.168.100.139:3303"
        }

    def get_page(self) -> List[dict]:
        pass

    @eventmanager.register(EventType)
    def send(self, event):
        """
        向第三方Webhook发送请求
        """
        if not self._enabled or not self._mh_domain or not self._mh_username or not self._mh_password:
            return

        if not event or not event.event_type:
            return

        def __to_dict(_event):
            """
            递归将对象转换为字典
            """
            if isinstance(_event, dict):
                for k, v in _event.items():
                    _event[k] = __to_dict(v)
                return _event
            elif isinstance(_event, list):
                for i in range(len(_event)):
                    _event[i] = __to_dict(_event[i])
                return _event
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

        version = getattr(settings, "VERSION_FLAG", "v1")
        event_type = event.event_type if version == "v1" else event.event_type.value
        if event_type not in ["transfer.complete", "metadata.scrape"]:
            return
        event_data = __to_dict(event.event_data)

        # logger.info(f"event_data: {event_data}")
        if event_type == "transfer.complete":
            transferinfo = event_data["transferinfo"]
            success = transferinfo["success"]
            if success:
                storage = transferinfo["target_diritem"]["storage"]
                name = transferinfo["target_item"]["name"]
                if storage == "u115":
                    logger.info(f"115整理完成：{name}")
                    self._wait_notify_count += 1
                    self._last_event_time = self.__get_time()
        elif event_type == "metadata.scrape":
            storage = event_data["fileitem"]
            name = event_data["name"]
            if storage == "u115":
                self._wait_notify_count += 1
                self._last_event_time = self.__get_time()
                logger.info(f"115刮削完成：{name}")

    def __get_time(self):
        return int(time.time())

    def __notify_mh(self):
        try:
            # 当等待通知数量超过1000或者有等待通知且最后事件时间超过60秒时触发通知
            if self._wait_notify_count > 0 and (self._wait_notify_count > 1000 or self.__get_time() - self._last_event_time > 60):
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
                    return
                try:
                    list_data = list_res.json() or {}
                    tasks = list_data.get("data", [])
                except Exception:
                    tasks = []
                # 过滤 cloud_strm_sync 任务
                strm_tasks = [t for t in tasks if t.get('task') == 'cloud_strm_sync' and t.get('enabled')]
                # 根据名称匹配（英文逗号分隔），否则默认名称包含“115网盘”
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
                success_any = False
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
