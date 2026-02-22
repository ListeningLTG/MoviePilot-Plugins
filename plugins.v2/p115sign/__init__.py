import time
import subprocess
import random
import json
from datetime import datetime, timedelta
from typing import Any, List, Dict, Tuple, Optional

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.core.config import settings
from app.core.event import eventmanager, Event
from app.plugins import _PluginBase
from app.log import logger
from app.schemas import NotificationType
from app.schemas.types import EventType

class p115sign(_PluginBase):
    # 插件名称
    plugin_name = "115网盘签到"
    # 插件描述
    plugin_desc = "115网盘签到，支持失败重试和历史记录统计"
    # 插件图标
    plugin_icon = "https://raw.githubusercontent.com/jxxghp/MoviePilot-Frontend/refs/heads/v2/src/assets/images/misc/u115.png"
    # 插件版本
    plugin_version = "1.0.3"
     # 插件作者
    plugin_author = "ListeningLTG"
    # 作者主页
    author_url = "https://github.com/ListeningLTG"
    # 插件配置项ID前缀
    plugin_config_prefix = "p115sign_"
    # 加载顺序
    plugin_order = 1
    # 可使用的用户级别
    auth_level = 2

    # 私有属性
    _enabled = False
    _cookie = None
    _notify = False
    _onlyonce = False
    _cron = None
    _history_days = 30
    _random_delay_sec = 600
    _max_retries = 3
    _retry_interval = 30
    _scheduler: Optional[BackgroundScheduler] = None
    
    def init_plugin(self, config: dict = None):
        self.stop_service()
        
        if config:
            self._enabled = config.get("enabled")
            self._cookie = config.get("cookie")
            self._notify = config.get("notify")
            self._cron = config.get("cron")
            self._onlyonce = config.get("onlyonce")
            self._history_days = int(config.get("history_days", 30))
            self._random_delay_sec = int(config.get("random_delay_sec", 600))
            self._max_retries = int(config.get("max_retries", 3))
            self._retry_interval = int(config.get("retry_interval", 30))
        
        if self._onlyonce:
            logger.info("执行一次性115签到")
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            self._scheduler.add_job(func=self.sign, trigger='date',
                                    run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3),
                                    name="115网盘签到")
            self._onlyonce = False
            self.update_config({
                "onlyonce": False,
                "enabled": self._enabled,
                "cookie": self._cookie,
                "notify": self._notify,
                "cron": self._cron,
                "history_days": self._history_days
            })
            if self._scheduler.get_jobs():
                self._scheduler.start()

    def get_state(self) -> bool:
        return self._enabled

    def get_service(self) -> List[Dict[str, Any]]:
        if self._enabled and self._cron:
            return [{
                "id": "p115sign",
                "name": "115网盘签到",
                "trigger": CronTrigger.from_crontab(self._cron),
                "func": self._trigger_sign_with_random_delay,
                "kwargs": {}
            }]
        return []

    def _trigger_sign_with_random_delay(self):
        try:
            delay = 0
            if isinstance(self._random_delay_sec, int) and self._random_delay_sec > 0:
                delay = random.randint(1, self._random_delay_sec)
            if delay > 0:
                logger.info(f"随机延迟 {delay} 秒后执行签到")
                time.sleep(delay)
        except Exception as e:
            logger.warning(f"计算或等待随机延迟时发生异常: {e}")
        finally:
            self.sign()

    def stop_service(self):
        if self._scheduler:
            self._scheduler.remove_all_jobs()
            if self._scheduler.running:
                self._scheduler.shutdown()
            self._scheduler = None

    def sign(self):
        """
        执行签到
        """
        logger.info("============= 开始115签到 =============")

        if not self._cookie:
            logger.error("未配置115 Cookie")
            return

        cookie_str = self._cookie.strip()
        cookies = [cookie_str] if cookie_str else []

        results = []

        use_client = False
        P115Client = None
        try:
            from p115client import P115Client as _P115Client
            P115Client = _P115Client
            use_client = True
        except Exception:
            use_client = False

        for idx, cookie in enumerate(cookies, start=1):

            # 随机延迟1-10秒，避免并发过快
            time.sleep(random.randint(1, 10))

            try:
                attempt = 0
                final_result = None
                while True:
                    if use_client and P115Client:
                        client = P115Client(cookie, check_for_relogin=True)
                        app_kwargs = {
                            "headers": {"user-agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 115wangpan_ios/36.2.20"},
                            "app": "ios"
                        }
                        resp = client.user_points_sign_post(**app_kwargs)
                        if isinstance(resp, dict):
                            code = resp.get("code")
                            state = resp.get("state")
                            data = resp.get("data") or {}
                            continuous_day = data.get("continuous_day")
                            points_num = data.get("points_num")
                            first_require_sign = data.get("first_require_sign")
                            if state and code == 0:
                                if first_require_sign == 1:
                                    status = "签到成功"
                                    msg = f"已连续签到{continuous_day}天，获得枫叶值为{points_num}"
                                else:
                                    status = "已经签到过了"
                                    msg = f"无需再签到，已连续签到{continuous_day}天，获得枫叶值为{points_num}"
                            else:
                                status = "签到失败"
                                msg = str(resp)
                        else:
                            status = "签到成功" if resp else "签到失败"
                            msg = str(resp)
                        final_result = {
                            "date": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            "status": status,
                            "message": msg,
                            "continuous_day": continuous_day if 'continuous_day' in locals() else '-',
                            "points_num": points_num if 'points_num' in locals() else '-'
                        }
                    else:
                        command = f'p115 check -c "{cookie}"'
                        process = subprocess.run(command, shell=True, capture_output=True, text=True, encoding='utf-8')
                        output = (process.stdout or "").strip()
                        error = (process.stderr or "").strip()
                        status = "签到成功" if process.returncode == 0 else "签到失败"
                        message = output if process.returncode == 0 else f"{output}\n{error}"
                        final_result = {
                            "date": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            "status": status,
                            "message": message,
                            "continuous_day": "-",
                            "points_num": "-"
                        }
                    logger.info(f"执行结果: {final_result['status']}")
                    if final_result["status"] in ["签到成功", "已经签到过了"] or attempt >= self._max_retries:
                        results.append(final_result)
                        break
                    attempt += 1
                    logger.info(f"将在{self._retry_interval}秒后进行第{attempt}次重试")
                    time.sleep(self._retry_interval)
            except Exception as e:
                logger.error(f"执行异常: {str(e)}")
                results.append({
                    "date": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    "status": "执行异常",
                    "message": str(e),
                    "continuous_day": "-",
                    "points_num": "-"
                })

        # 保存记录
        self._save_sign_history(results)

        # 发送通知
        if self._notify:
            self._send_notification(results)

        return results

    def _save_sign_history(self, new_records: List[dict]):
        """
        保存签到历史记录
        """
        try:
            history = self.get_data('sign_history') or []
            
            # 添加新记录
            history.extend(new_records)
            
            # 清理旧记录
            retention_days = int(self._history_days)
            now = datetime.now()
            valid_history = []
            
            for record in history:
                try:
                    record_date = datetime.strptime(record["date"], '%Y-%m-%d %H:%M:%S')
                    if (now - record_date).days < retention_days:
                        valid_history.append(record)
                except:
                    pass
            
            # 按时间倒序排序
            valid_history.sort(key=lambda x: x.get("date", ""), reverse=True)
            
            self.save_data(key="sign_history", value=valid_history)
            
        except Exception as e:
            logger.error(f"保存历史记录失败: {str(e)}")

    def _send_notification(self, results: List[dict]):
        """
        发送通知
        """
        title = "【115网盘签到报告】"
        text = ""
        
        for res in results:
            status_icon = "✅" if res["status"] in ["签到成功", "已经签到过了"] else "❌"
            text += f"{status_icon} {res['status']}\n"
            if res['message']:
                # 截取部分消息避免过长
                msg_preview = res['message'][:100] + "..." if len(res['message']) > 100 else res['message']
                text += f"   ℹ️ {msg_preview}\n"
            text += "━━━━━━━━━━\n"
            
        text += f"⏱️ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        
        self.post_message(
            mtype=NotificationType.SiteMessage,
            title=title,
            text=text
        )

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
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
                                    {'component': 'VSwitch', 'props': {'model': 'enabled', 'label': '启用插件'}}
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [
                                    {'component': 'VSwitch', 'props': {'model': 'notify', 'label': '开启通知'}}
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [
                                    {'component': 'VSwitch', 'props': {'model': 'onlyonce', 'label': '立即运行一次'}}
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
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'cookie',
                                            'label': '115 Cookie',
                                            'placeholder': '请输入115 Cookie',
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
                                'props': {'cols': 12, 'md': 3},
                                'content': [
                                    {'component': 'VCronField', 'props': {'model': 'cron', 'label': '签到周期'}}
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [
                                    {'component': 'VTextField', 'props': {'model': 'random_delay_sec', 'label': '最大随机延迟(秒)', 'type': 'number', 'placeholder': '600'}}
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [
                                    {'component': 'VTextField', 'props': {'model': 'max_retries', 'label': '最大重试次数', 'type': 'number', 'placeholder': '3'}}
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [
                                    {'component': 'VTextField', 'props': {'model': 'retry_interval', 'label': '重试间隔(秒)', 'type': 'number', 'placeholder': '30'}}
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 12},
                                'content': [
                                    {'component': 'VTextField', 'props': {'model': 'history_days', 'label': '历史保留天数', 'type': 'number'}}
                                ]
                            }
                        ]
                    }
                ]
            }
        ], {
            "enabled": False,
            "notify": True,
            "onlyonce": False,
            "cookie": "",
            "cron": "0 8 * * *",
            "history_days": 30,
            "random_delay_sec": 600,
            "max_retries": 3,
            "retry_interval": 30
        }

    def get_page(self) -> List[dict]:
        historys = self.get_data('sign_history') or []
        
        if not historys:
            return [{'component': 'VAlert', 'props': {'type': 'info', 'text': '暂无签到记录'}}]
            
        history_rows = []
        for history in historys:
            status_color = "success" if history.get("status") in ["签到成功", "已经签到过了"] else "error"
            history_rows.append({
                'component': 'tr',
                'content': [
                    {'component': 'td', 'text': history.get("date", "")},
                    {'component': 'td', 'content': [{'component': 'VChip', 'props': {'color': status_color, 'size': 'small'}, 'text': history.get("status", "")}]},
                    {'component': 'td', 'text': str(history.get("points_num", "-"))},
                    {'component': 'td', 'text': str(history.get("continuous_day", "-"))}
                ]
            })
            
        return [
            {
                'component': 'VCard',
                'props': {'class': 'mb-4'},
                'content': [
                    {'component': 'VCardTitle', 'text': '📊 115签到历史'},
                    {'component': 'VCardText', 'content': [
                        {
                            'component': 'VTable',
                            'props': {'density': 'compact'},
                            'content': [
                                {'component': 'thead', 'content': [{'component': 'tr', 'content': [
                                    {'component': 'th', 'text': '时间'},
                                    {'component': 'th', 'text': '状态'},
                                    {'component': 'th', 'text': '获得枫叶'},
                                    {'component': 'th', 'text': '连续签到'}
                                ]}]},
                                {'component': 'tbody', 'content': history_rows}
                            ]
                        }
                    ]}
                ]
            }
        ]

    def get_command(self) -> List[Dict[str, Any]]:
        return [{
            "cmd": "/p115_sign",
            "event": EventType.PluginAction,
            "desc": "115网盘签到",
            "category": "插件",
            "data": {"plugin_id": "p115sign", "action": "sign"}
        }]

    @eventmanager.register(EventType.PluginAction)
    def plugin_action(self, event: Event):
        data = event.event_data or {}
        if data.get("plugin_id") != "p115sign":
            return
        if data.get("action") != "sign":
            return
        channel = data.get("channel")
        source = data.get("source")
        userid = data.get("user")
        results = self.sign() or []
        if not self._notify:
            if results:
                first = results[0]
                title = "115网盘签到"
                text = f"{first.get('status')}\n{first.get('message')}"
            else:
                title = "115网盘签到"
                text = "已触发签到"
            self.post_message(channel=channel, mtype=NotificationType.Plugin,
                              title=title, text=text, userid=userid, source=source)

    def get_api(self) -> List[Dict[str, Any]]:
        return []
