from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path
from apscheduler.triggers.cron import CronTrigger

from app.plugins import _PluginBase
from app.log import logger
from app.db import db_query
from app.db.models.transferhistory import TransferHistory
from sqlalchemy.orm import Session

from .storage import AnalyzerStorage
from .analyzer import OrganizeAnalyzerCore


class OrganizeAnalyzer(_PluginBase):
    # 插件元数据
    plugin_name = "媒体整理异常分析"
    plugin_desc = "分析 MP 媒体整理历史记录，识别多文件归并/覆盖冲突、英文未识别标题、整理失败及重集等异常。"
    plugin_icon = "mdi-file-find-outline"
    plugin_version = "1.0.0"
    plugin_author = "ListeningLTG"
    plugin_config_prefix = "organizeanalyzer_"
    plugin_order = 15
    auth_level = 1

    # 私有字段
    _enabled: bool = False
    _cron: str = ""
    _notify: bool = False
    _config: dict = {}
    _storage: Optional[AnalyzerStorage] = None

    def init_plugin(self, config: dict = None):
        """初始化插件配置"""
        config = config or {}
        self._config = config
        self._enabled = bool(config.get("enabled", False))
        self._cron = config.get("cron", "0 3 * * *")
        self._notify = bool(config.get("notify", False))
        self._storage = AnalyzerStorage(self.get_data_path())

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return []

    def get_service(self) -> List[Dict[str, Any]]:
        """注册后台定时周期服务"""
        if not self.get_state() or not self._cron:
            return []
        try:
            return [
                {
                    "id": "OrganizeAnalyzer.CronService",
                    "name": "媒体整理异常定时分析",
                    "trigger": CronTrigger.from_crontab(self._cron),
                    "func": self.run_cron_analysis,
                    "kwargs": {},
                }
            ]
        except Exception as e:
            logger.error(f"【{self.plugin_name}】解析 Cron 表达式失败 [{self._cron}]: {e}")
            return []

    @db_query
    def _query_transfer_histories(self, db: Session, date_after: Optional[str] = None):
        """查询整理历史"""
        if date_after:
            return db.query(TransferHistory).filter(TransferHistory.date > date_after).order_by(TransferHistory.id.asc()).all()
        else:
            return db.query(TransferHistory).order_by(TransferHistory.id.asc()).all()

    def run_analysis(self, mode: str = "incremental") -> Dict[str, Any]:
        """
        执行整理异常分析
        :param mode: 'full' 全量分析, 'incremental' 增量分析
        """
        if not self._storage:
            self._storage = AnalyzerStorage(self.get_data_path())

        current_data = self._storage.load_data()
        date_after = None
        if mode == "incremental":
            date_after = current_data.get("last_run_time") or None

        logger.info(f"【{self.plugin_name}】开始执行 [{mode}] 分析... (上次时间: {date_after or '全量'})")
        histories = self._query_transfer_histories(date_after=date_after)
        
        exceptions, max_id = OrganizeAnalyzerCore.analyze(histories, self._config)
        result_data = self._storage.update_analysis_results(exceptions, mode=mode, max_history_id=max_id)
        
        summary = result_data.get("summary", {})
        logger.info(f"【{self.plugin_name}】分析完成！未处理异常总数: {summary.get('total', 0)}")

        # 消息推送
        if self._notify and summary.get("total", 0) > 0:
            self._send_notification(summary, result_data.get("exceptions", []))

        return result_data

    def run_cron_analysis(self):
        """定时任务回调"""
        logger.info(f"【{self.plugin_name}】触发定时增量分析...")
        self.run_analysis(mode="incremental")

    def _send_notification(self, summary: dict, exceptions: list):
        """发送异常报告系统通知"""
        msg_lines = [
            f"🔍 **{self.plugin_name} 结果通知**",
            f"━━━━━━━━━━━━━━━━━━",
            f"📊 未处理异常总数: **{summary.get('total', 0)}**",
            f"• 多文件合并冲突: {summary.get('merged_files', 0)}",
            f"• 英文标题未中文化: {summary.get('english_title', 0)}",
            f"• 未识别/TMDB缺失: {summary.get('unidentified', 0)}",
            f"• 整理运行失败: {summary.get('failed_status', 0)}",
            f"• 重复季集冲突: {summary.get('duplicate_episode', 0)}",
            f"• 目标文件缺失/损坏: {summary.get('missing_dest', 0)}",
        ]
        
        # 附带前 5 条未处理异常简明摘要
        active_items = [x for x in exceptions if x.get("status") != "ignored"][:5]
        if active_items:
            msg_lines.append("\n⚠️ **最新未处理条目示例:**")
            for item in active_items:
                msg_lines.append(f"• [{item.get('type_name')}] {item.get('title')} -> {item.get('detail')}")

        msg_body = "\n".join(msg_lines)
        self.post_message(title="媒体整理异常汇总报告", text=msg_body)

    def get_api(self) -> List[Dict[str, Any]]:
        """暴露插件 API"""
        return [
            {
                "path": "/stats",
                "endpoint": self.api_get_stats,
                "methods": ["GET"],
                "auth": "bear",
                "summary": "获取分析概览统计",
            },
            {
                "path": "/exceptions",
                "endpoint": self.api_get_exceptions,
                "methods": ["GET"],
                "auth": "bear",
                "summary": "获取异常列表",
            },
            {
                "path": "/analyze",
                "endpoint": self.api_run_analyze,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "手动触发分析",
            },
            {
                "path": "/ignore",
                "endpoint": self.api_ignore_exception,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "标记忽略某条异常",
            },
            {
                "path": "/clear_ignored",
                "endpoint": self.api_clear_ignored,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "清空忽略白名单",
            },
        ]

    # --- API 实现 ---
    async def api_get_stats(self) -> dict:
        if not self._storage:
            self._storage = AnalyzerStorage(self.get_data_path())
        data = self._storage.load_data()
        return {
            "code": 0,
            "msg": "success",
            "data": {
                "summary": data.get("summary", {}),
                "last_run_time": data.get("last_run_time", ""),
            }
        }

    async def api_get_exceptions(self, status: str = "active", type_filter: str = "") -> dict:
        if not self._storage:
            self._storage = AnalyzerStorage(self.get_data_path())
        data = self._storage.load_data()
        all_items = data.get("exceptions", [])
        
        filtered = []
        for item in all_items:
            if status != "all" and item.get("status") != status:
                continue
            if type_filter and item.get("type") != type_filter:
                continue
            filtered.append(item)

        return {
            "code": 0,
            "msg": "success",
            "data": filtered
        }

    async def api_run_analyze(self, mode: str = "incremental") -> dict:
        result = self.run_analysis(mode=mode)
        return {
            "code": 0,
            "msg": "分析完成",
            "data": result.get("summary", {})
        }

    async def api_ignore_exception(self, key: str = "") -> dict:
        if not key:
            return {"code": 400, "msg": "key 参数不能为空"}
        if not self._storage:
            self._storage = AnalyzerStorage(self.get_data_path())
        ok = self._storage.ignore_exception(key)
        return {"code": 0 if ok else 500, "msg": "已标记忽略" if ok else "保存失败"}

    async def api_clear_ignored(self) -> dict:
        if not self._storage:
            self._storage = AnalyzerStorage(self.get_data_path())
        ok = self._storage.clear_ignored()
        return {"code": 0 if ok else 500, "msg": "已清空忽略标记" if ok else "保存失败"}

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """配置页面 Vuetify 表单"""
        form_schema = [
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
                                        "props": {"model": "notify", "label": "分析完成后发送系统通知"},
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "cron",
                                            "label": "定时分析 Cron 表达式",
                                            "placeholder": "0 3 * * *",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VDivider",
                        "props": {"class": "my-3"},
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {"component": "VSubheader", "content": "【核心规则开关及参数】"}
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "detect_merged_files", "label": "检测多文件归并/覆盖同一目标"},
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
                                            "model": "min_merged_files",
                                            "label": "归并文件最小数量阈值",
                                            "type": "number",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "detect_english_title", "label": "检测英文未中文化标题"},
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "detect_unidentified", "label": "检测未识别 / TMDB 缺失"},
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "detect_failed_status", "label": "检测整理状态失败记录"},
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "detect_duplicate_episode", "label": "检测重复季集冲突"},
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "detect_missing_dest", "label": "检测目标物理文件缺失/0字节 (本地路径)"},
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "detect_invalid_episode", "label": "检测离群/格式异常集数 (>500)"},
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
                                        "component": "VTextarea",
                                        "props": {
                                            "model": "ignore_paths",
                                            "label": "忽略路径关键词白名单 (英文逗号分隔)",
                                            "placeholder": "/downloads/, /temp/",
                                            "rows": 2,
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                ],
            }
        ]
        
        default_model = {
            "enabled": False,
            "notify": False,
            "cron": "0 3 * * *",
            "min_merged_files": 2,
            "detect_merged_files": True,
            "detect_english_title": True,
            "detect_unidentified": True,
            "detect_failed_status": True,
            "detect_duplicate_episode": True,
            "detect_missing_dest": False,
            "detect_invalid_episode": False,
            "ignore_paths": "",
        }
        return form_schema, default_model

    def get_dashboard(self, key: str = None, **kwargs) -> Optional[Tuple[Dict[str, Any], Dict[str, Any], List[dict]]]:
        """首页仪表盘卡片组"""
        if not self._storage:
            self._storage = AnalyzerStorage(self.get_data_path())
        data = self._storage.load_data()
        summary = data.get("summary", {})
        last_time = data.get("last_run_time") or "尚未运行"

        col_config = {"cols": 12, "md": 12}
        global_config = {
            "title": "媒体整理异常分析概览",
            "refresh": 30,
            "border": True,
        }

        page = [
            {
                "component": "VRow",
                "content": [
                    {
                        "component": "VCol",
                        "props": {"cols": 12, "sm": 6, "md": 3},
                        "content": [
                            {
                                "component": "VCard",
                                "props": {"color": "error", "variant": "tonal", "class": "pa-2"},
                                "content": [
                                    {"component": "VCardTitle", "content": f"未处理异常总数: {summary.get('total', 0)}"},
                                    {"component": "VCardSubtitle", "content": f"上次检测时间: {last_time}"},
                                ],
                            }
                        ],
                    },
                    {
                        "component": "VCol",
                        "props": {"cols": 12, "sm": 6, "md": 3},
                        "content": [
                            {
                                "component": "VCard",
                                "props": {"color": "warning", "variant": "tonal", "class": "pa-2"},
                                "content": [
                                    {"component": "VCardTitle", "content": f"多文件覆盖冲突: {summary.get('merged_files', 0)}"},
                                    {"component": "VCardSubtitle", "content": f"英文未中文化: {summary.get('english_title', 0)}"},
                                ],
                            }
                        ],
                    },
                    {
                        "component": "VCol",
                        "props": {"cols": 12, "sm": 6, "md": 3},
                        "content": [
                            {
                                "component": "VCard",
                                "props": {"color": "info", "variant": "tonal", "class": "pa-2"},
                                "content": [
                                    {"component": "VCardTitle", "content": f"未识别/TMDB缺失: {summary.get('unidentified', 0)}"},
                                    {"component": "VCardSubtitle", "content": f"整理状态失败: {summary.get('failed_status', 0)}"},
                                ],
                            }
                        ],
                    },
                    {
                        "component": "VCol",
                        "props": {"cols": 12, "sm": 6, "md": 3},
                        "content": [
                            {
                                "component": "VCard",
                                "props": {"color": "secondary", "variant": "tonal", "class": "pa-2"},
                                "content": [
                                    {"component": "VCardTitle", "content": f"重复季集: {summary.get('duplicate_episode', 0)}"},
                                    {"component": "VCardSubtitle", "content": f"缺失/0字节: {summary.get('missing_dest', 0)}"},
                                ],
                            }
                        ],
                    },
                ],
            },
        ]
        return col_config, global_config, page

    def get_page(self) -> List[dict]:
        """插件详情页，显示说明与状态"""
        if not self._storage:
            self._storage = AnalyzerStorage(self.get_data_path())
        data = self._storage.load_data()
        summary = data.get("summary", {})
        last_time = data.get("last_run_time") or "尚未运行"

        return [
            {
                "component": "VAlert",
                "props": {
                    "type": "info",
                    "variant": "tonal",
                    "text": f"状态: {'已启用' if self._enabled else '未启用'} | 上次扫描时间: {last_time} | 当前未处理异常数: {summary.get('total', 0)}",
                },
            }
        ]

    def stop_service(self):
        """停止插件"""
        pass
