import random
from datetime import datetime, timedelta

import pytz
from app.chain.media import MediaChain
from app.chain.tmdb import TmdbChain
from app.core.config import settings
from app.db.subscribe_oper import SubscribeOper
from app.plugins import _PluginBase
from typing import Any, List, Dict, Tuple, Optional
from app.log import logger
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.schemas import NotificationType, MediaType


class subscribeairstime(_PluginBase):
    # 插件名称
    plugin_name = "订阅播出提醒"
    # 插件描述
    plugin_desc = "推送当天订阅更新内容及播出时间。"
    # 插件图标
    plugin_icon = "https://raw.githubusercontent.com/ListeningLTG/MoviePilot-Plugins/main/icons/subscribe_reminder.png"
    # 插件版本
    plugin_version = "1.0.2"
    # 插件作者
    plugin_author = "ListeningLTG"
    # 作者主页
    author_url = "https://github.com/ListeningLTG"
    # 插件配置项ID前缀
    plugin_config_prefix = "subscribeairstime_"
    # 加载顺序
    plugin_order = 33
    # 可使用的用户级别
    auth_level = 1

    # 私有属性
    _enabled: bool = False
    _onlyonce: bool = False
    _time = None
    tmdb = None
    media = None
    _subtype = None
    _msgtype = None
    subscribe_oper = None
    _scheduler: Optional[BackgroundScheduler] = None

    def init_plugin(self, config: dict = None):
        self.subscribe_oper = SubscribeOper()
        self.tmdb = TmdbChain()
        self.media = MediaChain()

        # 停止现有任务
        self.stop_service()

        if config:
            self._enabled = config.get("enabled")
            self._onlyonce = config.get("onlyonce")
            self._time = config.get("time")
            self._subtype = config.get("subtype")
            self._msgtype = config.get("msgtype")

            if self._enabled or self._onlyonce:
                # 周期运行
                self._scheduler = BackgroundScheduler(timezone=settings.TZ)

                if self._time and str(self._time).isdigit():
                    cron = f"0 {int(self._time)} * * *"
                    try:
                        self._scheduler.add_job(func=self.__send_notify,
                                                trigger=CronTrigger.from_crontab(cron),
                                                name="订阅提醒")
                    except Exception as err:
                        logger.error(f"定时任务配置错误：{err}")
                        # 推送实时消息
                        self.systemmessage.put(f"执行周期配置错误：{err}")

                # 立即运行一次
                if self._onlyonce:
                    logger.info(f"订阅提醒服务启动，立即运行一次")
                    self._scheduler.add_job(self.__send_notify, 'date',
                                            run_date=datetime.now(
                                                tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3),
                                            name="订阅提醒")
                    # 关闭一次性开关
                    self._onlyonce = False

                    # 保存配置
                    self.__update_config()

                # 启动任务
                if self._scheduler.get_jobs():
                    self._scheduler.print_jobs()
                    self._scheduler.start()

    def __update_config(self):
        self.update_config({
            "enabled": self._enabled,
            "onlyonce": self._onlyonce,
            "time": self._time,
            "subtype": self._subtype,
            "msgtype": self._msgtype
        })

    def __send_notify(self):
        # 查询所有订阅
        subscribes = self.subscribe_oper.list()
        if not subscribes:
            logger.error("当前没有订阅，跳过处理")
            return

        if not self._subtype:
            logger.error("订阅类型不能为空")
            return

            # 当前日期
        current_date = datetime.now().date().strftime("%Y-%m-%d")

        mtype = NotificationType.Plugin
        if self._msgtype:
            mtype = NotificationType.__getitem__(str(self._msgtype)) or NotificationType.Manual

        current_tv_subscribe = []
        current_movie_subscribe = []
        # 遍历订阅，查询tmdb
        for subscribe in subscribes:
            # 电视剧
            if "tv" in self._subtype and subscribe.type == "电视剧":
                if not subscribe.tmdbid or not subscribe.season:
                    continue

                # 电视剧某季所有集
                episodes_info = self.tmdb.tmdb_episodes(tmdbid=subscribe.tmdbid, season=subscribe.season)
                if not episodes_info:
                    continue

                episodes = []
                # 遍历集，筛选当前日期发布的剧集
                for episode in episodes_info:
                    if episode and episode.air_date and str(episode.air_date) == current_date:
                        episodes.append(episode.episode_number)

                if episodes:
                    # 尝试从 TVDB 获取系列级播出时间
                    airs_time = ""
                    if subscribe.tvdbid:
                        try:
                            tvdb_data = self.media.tvdb_info(tvdbid=subscribe.tvdbid)
                            if tvdb_data:
                                airs_time = tvdb_data.get("airsTime") or ""
                        except Exception as e:
                            logger.warning(f"获取TVDB播出时间失败 ({subscribe.name}): {e}")
                    current_tv_subscribe.append({
                        'name': f"{subscribe.name} ({subscribe.year})",
                        'season': f"S{str(subscribe.season).rjust(2, '0')}",
                        'episode': f"E{str(episodes[0]).rjust(2, '0')}-E{str(episodes[-1]).rjust(2, '0')}" if len(
                            episodes) > 1 else f"E{str(episodes[0]).rjust(2, '0')}",
                        "image": subscribe.backdrop or subscribe.poster,
                        "airs_time": airs_time
                    })

            # 电影
            if "movie" in self._subtype and subscribe.type == "电影":
                if not subscribe.tmdbid:
                    continue
                mediainfo = self.media.recognize_media(tmdbid=subscribe.tmdbid, mtype=MediaType.MOVIE)
                if not mediainfo:
                    continue
                if str(mediainfo.release_date) == current_date:
                    current_movie_subscribe.append({
                        'name': f"{subscribe.name} ({subscribe.year})",
                        "image": subscribe.backdrop or subscribe.poster
                    })

        # 处理电视剧订阅
        if "tv" in self._subtype and current_tv_subscribe:
            text = ""
            image = []
            count = 0
            for sub in current_tv_subscribe:
                airs_time = sub.get('airs_time')
                time_label = f" {airs_time}" if airs_time else ""
                text += f"📺︎{sub.get('name')} {sub.get('season')}{sub.get('episode')}{time_label}\n"
                count += 1
                image.append(sub.get('image'))
                if count % 8 == 0:  # 每8条发送一次
                    self.post_message(mtype=mtype,
                                      title="电视剧更新",
                                      text=text,
                                      image=random.choice(image))
                    logger.info(f"推送电视剧更新：{text}")
                    text = ""  # 重置text变量以开始新的消息
                    image = []

            # 如果还有剩余未发送的内容
            if text:
                self.post_message(mtype=mtype,
                                  title="电视剧更新",
                                  text=text,
                                  image=random.choice(image))
                logger.info(f"推送电视剧更新：{text}")

        # 处理电影订阅
        if "movie" in self._subtype and current_movie_subscribe:
            text = ""
            image = []
            count = 0
            for sub in current_movie_subscribe:
                text += f"📽︎{sub.get('name')}\n"
                count += 1
                image.append(sub.get('image'))
                if count % 8 == 0:  # 每8条发送一次
                    self.post_message(mtype=mtype,
                                      title="电影更新",
                                      text=text,
                                      image=random.choice(image))
                    text = ""  # 重置text变量以开始新的消息
                    image = []
                    logger.info(f"推送电影更新：{text}")

            # 如果还有剩余未发送的内容
            if text:
                self.post_message(mtype=mtype,
                                  title="电影更新",
                                  text=text,
                                  image=random.choice(image))
                logger.info(f"推送电影更新：{text}")

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        pass

    def get_api(self) -> List[Dict[str, Any]]:
        pass

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """
        拼装插件配置页面，需要返回两块数据：1、页面配置；2、数据结构
        """
        # 编历 NotificationType 枚举，生成消息类型选项
        MsgTypeOptions = []
        for item in NotificationType:
            MsgTypeOptions.append({
                "title": item.value,
                "value": item.name
            })
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
                                    'md': 4
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
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'onlyonce',
                                            'label': '立即运行一次',
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
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'time',
                                            'label': '时间',
                                            'placeholder': '默认9点'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSelect',
                                        'props': {
                                            'multiple': True,
                                            'chips': True,
                                            'model': 'subtype',
                                            'label': '订阅类型',
                                            'items': [
                                                {"title": "电影", "value": "movie"},
                                                {"title": "电视剧", "value": "tv"}
                                            ]
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSelect',
                                        'props': {
                                            'multiple': False,
                                            'chips': True,
                                            'model': 'msgtype',
                                            'label': '消息类型',
                                            'items': MsgTypeOptions
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
                                            'text': '默认每天9点推送，需开启（订阅）通知类型。'
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
            "onlyonce": False,
            "subtype": ["movie", "tv"],
            "msgtype": "Plugin",
            "time": 9,
        }

    def get_page(self) -> List[dict]:
        pass

    def stop_service(self):
        """
        退出插件
        """
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._scheduler.shutdown()
                self._scheduler = None
        except Exception as e:
            logger.error("退出插件失败：%s" % str(e))
