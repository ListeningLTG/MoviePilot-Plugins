"""
影视洗板插件 (MediaBoardWash)
==============================
扫描 .strm 文件 / 视频文件，从文件名解析质量信息，
自动识别重复和低质量版本，保留最佳版本。
核心用途：清理云盘挂载后的重复 .strm 文件，只保留最佳质量版本。

支持手动执行和定时扫描，可视化展示对比结果。

Author: Senior Developer
Version: 2.5.0
"""

import json
import threading
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytz

from app.core.config import settings
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import NotificationType, EventType
from app.core.event import eventmanager, Event
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger


def _now() -> datetime:
    """返回带 MoviePilot 时区的当前时间"""
    return datetime.now(tz=pytz.timezone(settings.TZ))

from .quality import (
    parse_quality,
    parse_season_episode,
    parse_custom_rules,
    read_tier_scores,
    format_file_size,
    get_patterns,
    SCORE_PROFILES,
)
from .scanner import (
    resolve_scan_directories,
    collect_target_files,
    guess_media_title,
    guess_media_year,
    parse_tmdb_id,
    group_by_media,
    compare_and_rank,
    make_version_entry,
)
from .cleaner import execute_cleanup
from .ui import build_form, build_page
from .api import send_scan_notification, build_status_text


def _safe_int(value, default: int = 0) -> int:
    """安全转换整数，非数字输入返回默认值"""
    try:
        return int(float(str(value)))
    except (ValueError, TypeError, OverflowError):
        return default


def _safe_float(value, default: float = 0.0) -> float:
    """安全转换浮点数，非数字输入返回默认值"""
    try:
        return float(str(value))
    except (ValueError, TypeError, OverflowError):
        return default


class MediaBoardWash(_PluginBase):
    """
    影视洗板插件 - 专为 .strm 文件和视频文件设计
    """

    # ============================================================
    # 插件元数据
    # ============================================================
    plugin_name = "影视洗板"
    plugin_desc = "扫描 .strm/视频文件，从文件名解析质量信息，去重留优。适用于云盘挂载后清理重复.strm文件。"
    plugin_icon = "mdi-filmstrip-box-multiple"
    plugin_version = "2.5.0"
    plugin_author = "Dave ."
    author_url = "https://github.com/"
    plugin_config_prefix = "mediaboardwash_"
    plugin_order = 20
    auth_level = 1

    # ============================================================
    # 私有属性
    # ============================================================
    _enabled: bool = False
    _cron: str = ""
    _onlyonce: bool = False
    _trigger_cleanup: bool = False
    _media_dirs: str = ""
    _min_size: int = 100
    _notify: bool = True
    _keep_count: int = 1
    _keep_mode: str = "top_n"  # v2.4.0: 多版本保留策略
    _min_score: int = 0  # 最低保留分数，低于此值强制删除
    _scan_mode: str = "manual"

    # 自定义评分权重
    _res_weight: float = 40.0
    _src_weight: float = 35.0
    _aud_weight: float = 15.0
    _hdr_weight: float = 15.0
    _vid_weight: float = 12.0
    _bonus_weight: float = 6.0

    # 自定义评分规则
    _custom_rules: Optional[dict] = None
    _custom_rules_raw: str = ""  # 用户输入的原始 JSON 字符串（用于回存）
    _tier_values: Optional[dict] = None  # 各档位原始值 {tier_key: score}，用于回存配置

    # 缺失的配置属性（修复: 被 __update_config 引用但从未定义）
    _auto_cleanup: bool = False
    _score_profile: str = "custom"

    # 扫描并发锁（类级别，所有实例共享，防止 APScheduler + 手动同时触发）
    _scan_lock = threading.Lock()

    # ============================================================
    # 生命周期方法
    # ============================================================

    def init_plugin(self, config: dict = None):
        """初始化插件"""
        self.stop_service()

        if config:
            self._enabled = config.get("enabled", False)
            self._cron = config.get("cron", "")
            self._onlyonce = config.get("onlyonce", False)
            self._media_dirs = config.get("media_dirs", "")
            self._min_size = _safe_int(config.get("min_size"), 100)
            self._notify = config.get("notify", True)
            self._keep_count = _safe_int(config.get("keep_count"), 1)
            self._keep_mode = config.get("keep_mode", "top_n")
            self._min_score = _safe_int(config.get("min_score"), 0)
            self._trigger_cleanup = config.get("trigger_cleanup", False)
            self._auto_cleanup = config.get("auto_cleanup", False)
            self._score_profile = config.get("score_profile", "custom")

            # 读取自定义评分权重
            self._res_weight = _safe_float(config.get("res_weight"), 40.0)
            self._src_weight = _safe_float(config.get("src_weight"), 35.0)
            self._aud_weight = _safe_float(config.get("aud_weight"), 15.0)
            self._hdr_weight = _safe_float(config.get("hdr_weight"), 15.0)
            self._vid_weight = _safe_float(config.get("vid_weight"), 12.0)
            self._bonus_weight = _safe_float(config.get("bonus_weight"), 6.0)

            # 读取自定义评分规则
            # 优先级: custom_rules(JSON) > 单个tier字段 > 默认值
            rules_json = config.get("custom_rules", "")
            self._custom_rules_raw = rules_json  # 保存原始JSON用于回存
            self._custom_rules = {}  # 防御性初始化
            if rules_json and rules_json.strip():
                self._custom_rules = parse_custom_rules(rules_json)
            else:
                tier_config = read_tier_scores(config)
                self._custom_rules = tier_config if tier_config else {}

            # 保存各档位微调原始值，用于 __update_config 回存（修复: 不依赖索引提取）
            from .quality import TIER_FIELDS
            self._tier_values = {}  # 防御性初始化
            for tier_key, dim, default in TIER_FIELDS:
                val = config.get(tier_key)
                if val is not None:
                    try:
                        self._tier_values[tier_key] = int(float(str(val)))
                    except (ValueError, TypeError):
                        self._tier_values[tier_key] = default

        # 启动服务 — 修复: trigger_cleanup 独立于 enabled/onlyonce 执行
        if self._enabled or self._onlyonce:
            if self._onlyonce:
                logger.info("影视洗板插件触发立即执行")
                self._set_action_message("🔄 扫描任务已提交，约5秒后开始...")
                self._scheduler = BackgroundScheduler(timezone=settings.TZ)
                self._scheduler.add_job(
                    func=lambda: self._scan_and_wash(auto_delete=False),
                    trigger="date",
                    run_date=_now() + timedelta(seconds=5),
                    name="影视洗板_立即执行"
                )
                if self._scheduler.get_jobs():
                    self._scheduler.start()
                self._onlyonce = False
                self.__update_config()

            if self._enabled and self._cron:
                logger.info(f"影视洗板定时任务已设置: {self._cron}")

        # 确认清理触发器 — 修复 Bug: 独立于插件状态执行
        if self._trigger_cleanup:
            logger.info("影视洗板插件触发确认清理")
            self._trigger_cleanup = False
            self._set_action_message("🔄 清理任务已提交，正在执行...")
            if not hasattr(self, '_scheduler') or not self._scheduler:
                self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            self._scheduler.add_job(
                func=lambda: self._execute_cleanup_and_notify(),
                trigger="date",
                run_date=_now() + timedelta(seconds=3),
                name="影视洗板_确认清理"
            )
            if self._scheduler.get_jobs():
                self._scheduler.start()
            self.__update_config()

    def stop_service(self):
        """停止插件服务"""
        try:
            if hasattr(self, '_scheduler') and self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._scheduler.shutdown()
                self._scheduler = None
        except Exception as e:
            logger.error(f"影视洗板停止服务出错: {str(e)}")

    def get_state(self) -> bool:
        """获取插件状态"""
        return self._enabled

    # ============================================================
    # 配置管理
    # ============================================================

    def __update_config(self):
        """保存配置

        v2.1.2 修复: 分别保存 custom_rules(原始JSON) 和各 tier_* 字段，
        避免「自定义评分机制」和「各档位微调」相互覆盖导致表单冲突。
        使用 _tier_values 字典直接回存（不依赖索引提取，避免索引偏移 bug）。
        """
        config_data = {
            "enabled": self._enabled,
            "cron": self._cron,
            "onlyonce": self._onlyonce,
            "media_dirs": self._media_dirs,
            "min_size": self._min_size,
            "notify": self._notify,
            "keep_count": self._keep_count,
            "keep_mode": self._keep_mode,
            "min_score": self._min_score,
            "trigger_cleanup": self._trigger_cleanup,
            "auto_cleanup": self._auto_cleanup,
            "res_weight": self._res_weight,
            "src_weight": self._src_weight,
            "aud_weight": self._aud_weight,
            "hdr_weight": self._hdr_weight,
            "vid_weight": self._vid_weight,
            "bonus_weight": self._bonus_weight,
            "score_profile": self._score_profile,
            "custom_rules": self._custom_rules_raw,
        }

        # 回存各档位微调原始值（修复: 不依赖 _custom_rules 索引提取）
        if self._tier_values:
            config_data.update(self._tier_values)

        self.update_config(config_data)

    # ============================================================
    # 质量解析（转发到 quality.py）
    # ============================================================

    def _parse_quality(self, filename: str) -> Dict[str, Any]:
        """从文件名中解析质量信息（委托至 quality.parse_quality）"""
        return parse_quality(
            filename,
            custom_rules=self._custom_rules,
            res_weight=self._res_weight,
            src_weight=self._src_weight,
            aud_weight=self._aud_weight,
            hdr_weight=self._hdr_weight,
            vid_weight=self._vid_weight,
            bonus_weight=self._bonus_weight,
        )

    def _parse_season_episode(self, filename: str) -> Dict[str, Any]:
        """从文件名解析季/集信息（委托至 quality.parse_season_episode）"""
        return parse_season_episode(filename)

    @staticmethod
    def _format_file_size(size_bytes: int) -> str:
        """格式化文件大小"""
        return format_file_size(size_bytes)

    # ============================================================
    # 扫描工具（转发到 scanner.py）
    # ============================================================

    def _resolve_scan_directories(self) -> List[Path]:
        return resolve_scan_directories(self._media_dirs)

    def _collect_target_files(self, directories: List[Path]) -> List[Path]:
        return collect_target_files(directories, self._min_size)

    @staticmethod
    def _guess_media_title(file_path: Path) -> str:
        return guess_media_title(file_path)

    @staticmethod
    def _guess_media_year(file_path: Path) -> Optional[str]:
        return guess_media_year(file_path)

    @staticmethod
    def _parse_tmdb_id(file_path: Path) -> Optional[str]:
        return parse_tmdb_id(file_path)

    @staticmethod
    def _group_by_media(items: List[Dict]) -> Dict[str, List[Dict]]:
        return group_by_media(items)

    def _compare_and_rank(self, groups: Dict[str, List[Dict]]) -> Dict[str, Any]:
        return compare_and_rank(groups, self._keep_count, self._min_score, self._keep_mode)

    @staticmethod
    def _make_version_entry(v: Dict, is_best: bool, rank: int, deleted: bool) -> Dict:
        return make_version_entry(v, is_best, rank, deleted)

    # ============================================================
    # 核心扫描逻辑
    # ============================================================

    def _set_progress(self, current: int, total: int, stage: str):
        """P1: 保存扫描进度供 UI 展示"""
        self.save_data("_scan_progress", {
            "current": current,
            "total": total,
            "stage": stage,
            "pct": round(current / total * 100, 1) if total > 0 else 0,
            "time": _now().strftime("%H:%M:%S"),
            "scanning": True
        })

    def _clear_progress(self):
        """P1: 清除扫描进度"""
        self.save_data("_scan_progress", None)

    def _clear_results(self):
        """v2.1.1: 清除旧扫描结果，确保每次扫描只显示本次记录"""
        self.save_data("wash_results", None)
        logger.info("影视洗板: 已清除上次扫描结果")

    def _scan_and_wash(self, auto_delete: bool = False):
        """执行影视洗板扫描"""
        # v2.3.0: 并发锁，防止 APScheduler 定时任务和手动触发同时执行
        acquired = MediaBoardWash._scan_lock.acquire(blocking=False)
        if not acquired:
            logger.info("影视洗板: 扫描正在进行中，跳过本次触发")
            self.post_message(
                title="影视洗板",
                text="扫描正在进行中，请稍后再试",
                mtype=NotificationType.Plugin
            )
            return

        try:
            logger.info("=== 影视洗板: 开始扫描 === 模式=%s", "自动清理" if auto_delete else "仅扫描展示")
            self._scan_mode = "auto" if auto_delete else "manual"

            # v2.1.1: 清除旧结果，确保每次扫描只显示本次记录
            self._clear_results()

            try:
                scan_dirs = self._resolve_scan_directories()
                if not scan_dirs:
                    logger.warning("影视洗板: 没有可扫描的目录")
                    self._clear_progress()
                    self.post_message(
                        title="影视洗板扫描失败",
                        text="未配置媒体目录，请在插件设置中添加媒体目录路径",
                        mtype=NotificationType.Manual
                    )
                    return

                logger.info(f"影视洗板: 扫描目录共 {len(scan_dirs)} 个: {scan_dirs}")
                self._set_progress(0, 1, "扫描目录中...")

                all_files = self._collect_target_files(scan_dirs)
                if not all_files:
                    logger.warning("影视洗板: 未找到任何 .strm 或视频文件")
                    self._clear_progress()
                    self.post_message(
                        title="影视洗板扫描完成",
                        text="未找到任何 .strm 或视频文件，请检查媒体目录路径是否正确",
                        mtype=NotificationType.Plugin
                    )
                    return

                logger.info(f"影视洗板: 共发现 {len(all_files)} 个目标文件（.strm / 视频）")
                self._set_progress(0, len(all_files), "解析文件质量中...")

                # 多线程解析文件质量
                scanned_items = []
                def _parse_single_file(file_path):
                    try:
                        quality = self._parse_quality(file_path.name)
                        se = self._parse_season_episode(file_path.name)
                        return {
                            "filepath": str(file_path),
                            "filename": file_path.name,
                            "parent_dir": str(file_path.parent),
                            "size_bytes": file_path.stat().st_size,
                            "size_display": self._format_file_size(file_path.stat().st_size),
                            "quality_score": quality["score"],
                            "quality_details": quality,
                            "media_title": self._guess_media_title(file_path),
                            "media_year": self._guess_media_year(file_path),
                            "tmdbid": self._parse_tmdb_id(file_path),
                            "season_episode": se,
                            "season_num": se.get("season_num", 0),
                            "episode_num": se.get("episode_num", 0),
                            "is_movie": se.get("is_movie", True),
                        }
                    except Exception as e:
                        logger.debug(f"解析文件质量出错 {file_path.name}: {str(e)}")
                        return None

                with ThreadPoolExecutor(max_workers=10) as executor:
                    futures = {executor.submit(_parse_single_file, fp): fp for fp in all_files}
                    done_count = 0
                    total_files = len(all_files)
                    for future in as_completed(futures):
                        result = future.result()
                        if result:
                            scanned_items.append(result)
                        done_count += 1
                        if done_count % max(1, total_files // 10) == 0 or done_count == total_files:
                            self._set_progress(done_count, total_files, "解析文件质量中...")

                grouped = self._group_by_media(scanned_items)
                results = self._compare_and_rank(grouped)
                # v2.5.0: 移除 Dry-Run 模式，直接执行真实操作
                results["scan_mode"] = self._scan_mode
                self._save_scan_results(results, len(scanned_items))

                # 自动清理（仅定时任务模式）
                deleted_count = 0
                if auto_delete:
                    deleted_count = self._execute_cleanup(results)
                    if deleted_count > 0:
                        logger.info(f"影视洗板: 自动清理完成，删除了 {deleted_count} 个低质量文件")
                        results["last_cleanup"] = _now().strftime("%Y-%m-%d %H:%M:%S")
                        results["last_cleanup_count"] = deleted_count
                        self.save_data("wash_results", results)
                else:
                    logger.info(f"影视洗板: 手动模式，待用户确认删除（共 {results['groups_with_duplicates']} 组重复）")

                if self._notify:
                    send_scan_notification(
                        self.post_message,
                        results, len(scanned_items),
                        auto_delete, deleted_count,
                    )

                logger.info("=== 影视洗板: 扫描完成 ===")
                self._clear_progress()

            except Exception as e:
                logger.error(f"影视洗板扫描出错: {str(e)}")
                import traceback
                logger.error(traceback.format_exc())
                self._clear_progress()
                if self._notify:
                    self.post_message(
                        title="影视洗板扫描出错",
                        text=f"扫描过程发生错误: {str(e)}",
                        mtype=NotificationType.Manual
                    )
        finally:
            MediaBoardWash._scan_lock.release()

    # ============================================================
    # 结果管理
    # ============================================================

    def _save_scan_results(self, results: Dict[str, Any], total_scanned: int):
        """保存扫描结果"""
        self.save_data("wash_results", results)

        scan_tag = _now().strftime("scan_%Y%m%d_%H%M%S")
        self.save_data(scan_tag, results)
        self._cleanup_old_scans(scan_tag)

        today = _now().strftime("%Y-%m-%d")
        history = self.get_data(f"wash_history_{today}") or {}
        history.update({
            "scan_time": results["last_scan"],
            "total_scanned": total_scanned,
            "total_groups": results["total_groups"],
            "duplicates_found": results["groups_with_duplicates"],
            "potential_savings_gb": results["potential_savings_gb"],
        })
        self.save_data(f"wash_history_{today}", history)

        logger.info(
            f"影视洗板扫描结果: 共扫描 {total_scanned} 个文件, "
            f"{results['total_groups']} 组媒体, "
            f"{results['groups_with_duplicates']} 组有重复, "
            f"可节省 {results['potential_savings_gb']} GB"
        )

    def _cleanup_old_scans(self, current_tag: str):
        """清理超过10条的旧扫描记录"""
        try:
            tags = self.get_data("_scan_tags") or []
            tags.append(current_tag)
            tags = sorted(set(tags))[-10:]
            self.save_data("_scan_tags", tags)
            for old_tag in tags[:-10]:
                try:
                    self.save_data(old_tag, None)
                except Exception:
                    pass
        except Exception:
            pass

    def _set_action_message(self, msg: str):
        """设置操作反馈消息"""
        self.save_data("_action_msg", {
            "text": msg,
            "time": _now().strftime("%H:%M:%S")
        })

    def _get_action_message(self) -> str:
        """获取并清除操作反馈消息"""
        msg_data = self.get_data("_action_msg")
        if msg_data:
            self.save_data("_action_msg", None)
            return msg_data.get("text", "")
        return ""

    # ============================================================
    # 文件清理逻辑
    # ============================================================

    def _execute_cleanup(self, results: Dict[str, Any] = None) -> int:
        """执行文件清理（真实删除模式）"""
        if results is None:
            results = self.get_data("wash_results")
        if not results or "items" not in results:
            logger.warning("影视洗板: 没有扫描结果，无法执行清理")
            return 0

        deleted = execute_cleanup(results["items"])
        if deleted > 0:
            results["last_cleanup"] = _now().strftime("%Y-%m-%d %H:%M:%S")
            results["last_cleanup_count"] = deleted
            self.save_data("wash_results", results)

        return deleted

    def _execute_cleanup_and_notify(self):
        """执行清理并发送通知"""
        results = self.get_data("wash_results")
        if not results:
            logger.warning("影视洗板: 触发清理但无扫描结果")
            self.post_message(
                title="影视洗板清理",
                text="没有扫描结果，请先执行扫描",
                mtype=NotificationType.Manual
            )
            return

        deleted = self._execute_cleanup(results)
        if deleted > 0:
            self.post_message(
                title="影视洗板清理完成",
                text=f"已确认删除 {deleted} 个低质量文件",
                mtype=NotificationType.Manual
            )
        else:
            self.post_message(
                title="影视洗板清理",
                text="没有需要清理的文件",
                mtype=NotificationType.Manual
            )

    # ============================================================
    # 配置表单 UI (get_form)
    # ============================================================

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """插件配置表单"""
        return build_form()

    # ============================================================
    # 结果展示页面 UI (get_page)
    # ============================================================

    def get_page(self) -> List[dict]:
        """插件详情页面 - 展示影视洗板对比结果"""
        results = self.get_data("wash_results")
        action_msg = self._get_action_message()
        # P1: 获取扫描进度
        progress = self.get_data("_scan_progress")
        return build_page(results, action_msg, progress)

    # ============================================================
    # 定时服务注册 (get_service)
    # ============================================================

    def get_service(self) -> List[Dict[str, Any]]:
        """注册定时服务"""
        if self._enabled and self._cron:
            try:
                return [{
                    "id": "MediaBoardWash",
                    "name": "影视洗板定时扫描",
                    "trigger": CronTrigger.from_crontab(self._cron),
                    "func": lambda: self._scan_and_wash(auto_delete=True),
                    "kwargs": {}
                }]
            except Exception as e:
                logger.error(f"影视洗板定时任务配置错误: {str(e)}")
        return []

    # ============================================================
    # 远程命令注册 (get_command)
    # ============================================================

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """注册远程控制命令"""
        return [
            {
                "cmd": "/media_wash",
                "event": EventType.PluginAction,
                "desc": "影视洗板扫描",
                "category": "影视洗板",
                "data": {"action": "media_wash_scan"}
            },
            {
                "cmd": "/media_wash_cleanup",
                "event": EventType.PluginAction,
                "desc": "影视洗板清理",
                "category": "影视洗板",
                "data": {"action": "media_wash_cleanup"}
            },
            {
                "cmd": "/media_wash_status",
                "event": EventType.PluginAction,
                "desc": "影视洗板状态",
                "category": "影视洗板",
                "data": {"action": "media_wash_status"}
            }
        ]

    # ============================================================
    # 前端按钮动作
    # ============================================================

    def action_scan(self) -> dict:
        """前端按钮「🔄 扫描」"""
        logger.info("影视洗板: 前端触发立即扫描")
        if not self._enabled:
            return {"msg": "⚠️ 插件未启用，请先在配置中启用", "code": 400}
        thread = threading.Thread(target=lambda: self._scan_and_wash(auto_delete=False), daemon=True)
        thread.start()
        return {"msg": "✅ 扫描已启动，请稍后刷新页面查看结果", "code": 200}

    def action_cleanup(self) -> dict:
        """前端按钮「🗑️ 删除」"""
        logger.info("影视洗板: 前端触发确认清理")
        if not self._enabled:
            return {"msg": "⚠️ 插件未启用，请先在配置中启用", "code": 400}
        results = self.get_data("wash_results")
        if not results:
            return {"msg": "⚠️ 暂无扫描结果，请先执行扫描", "code": 400}
        deleted = self._execute_cleanup(results)
        if deleted > 0:
            msg = f"✅ 已删除 {deleted} 个文件，刷新页面查看"
            self.post_message(title="影视洗板清理完成", text=msg, mtype=NotificationType.Manual)
            return {"msg": msg, "code": 200}
        return {"msg": "ℹ️ 没有需要清理的文件", "code": 200}

    # ============================================================
    # 自定义 API (get_api)
    # ============================================================

    def get_api(self) -> List[Dict[str, Any]]:
        """注册自定义API"""
        return [
            {
                "path": "/scan",
                "endpoint": self.api_scan,
                "methods": ["GET", "POST"],
                "auth": "bear",
                "summary": "触发影视洗板扫描（手动模式，仅展示结果）",
                "description": "手动触发一次影视质量对比扫描，不删除文件"
            },
            {
                "path": "/confirm_cleanup",
                "endpoint": self.api_confirm_cleanup,
                "methods": ["GET", "POST"],
                "auth": "bear",
                "summary": "确认删除低质量文件",
                "description": "手动确认删除扫描结果中标记为待清理的文件"
            },
            {
                "path": "/results",
                "endpoint": self.api_results,
                "methods": ["GET"],
                "auth": "bear",
                "summary": "获取扫描结果",
                "description": "获取最近一次的影视洗板扫描结果"
            },
            {
                "path": "/history",
                "endpoint": self.api_history,
                "methods": ["GET"],
                "auth": "bear",
                "summary": "获取扫描历史",
                "description": "获取历史扫描记录"
            }
        ]

    def api_scan(self) -> Dict[str, Any]:
        """API: 触发扫描"""
        if not self._enabled:
            return {"success": False, "message": "插件未启用"}
        thread = threading.Thread(target=lambda: self._scan_and_wash(auto_delete=False), daemon=True)
        thread.start()
        return {
            "success": True,
            "message": "影视洗板扫描已启动（手动模式），请在插件详情页查看结果，确认后点击删除",
            "redirect": "/plugins/mediaboardwash"
        }

    def api_confirm_cleanup(self) -> Any:
        """API: 确认删除"""
        if not self._enabled:
            return {"success": False, "message": "插件未启用"}
        results = self.get_data("wash_results")
        if not results:
            return {"success": False, "message": "暂无扫描结果，请先执行扫描"}
        deleted = self._execute_cleanup(results)
        msg = f"确认删除完成，共清理 {deleted} 个文件" if deleted else "没有需要清理的文件"
        return {"success": True, "message": msg, "redirect": "/plugins/mediaboardwash"}

    def api_results(self) -> Dict[str, Any]:
        """API: 获取扫描结果"""
        results = self.get_data("wash_results")
        if not results:
            return {"success": False, "message": "暂无扫描结果"}
        return {"success": True, "data": results}

    def api_history(self) -> Dict[str, Any]:
        """API: 获取扫描历史"""
        history = {}
        for i in range(30):
            day = (_now() - timedelta(days=i)).strftime("%Y-%m-%d")
            day_data = self.get_data(f"wash_history_{day}")
            if day_data:
                history[day] = day_data
        return {"success": True, "data": history}

    # ============================================================
    # 事件监听
    # ============================================================

    @eventmanager.register(EventType.PluginAction)
    def handle_action(self, event: Event = None):
        """处理远程命令事件"""
        if not event or not event.event_data:
            return

        action = event.event_data.get("action")

        if action == "media_wash_scan":
            logger.info("收到命令: 影视洗板扫描（手动模式）")
            self.post_message(
                channel=event.event_data.get("channel"),
                title="影视洗板扫描",
                text="开始扫描媒体库质量对比（仅展示结果，不会删除文件）...",
                userid=event.event_data.get("user")
            )
            thread = threading.Thread(target=lambda: self._scan_and_wash(auto_delete=False), daemon=True)
            thread.start()

        elif action == "media_wash_cleanup":
            logger.info("收到命令: 影视洗板确认删除")
            results = self.get_data("wash_results")
            if not results:
                self.post_message(
                    channel=event.event_data.get("channel"),
                    title="影视洗板清理",
                    text="没有扫描结果，请先执行 /media_wash 扫描",
                    userid=event.event_data.get("user")
                )
                return
            self.post_message(
                channel=event.event_data.get("channel"),
                title="影视洗板清理",
                text="开始执行低质量文件清理...",
                userid=event.event_data.get("user")
            )
            deleted = self._execute_cleanup(results)
            self.post_message(
                channel=event.event_data.get("channel"),
                title="影视洗板清理完成",
                text=f"成功处理 {deleted} 个文件",
                userid=event.event_data.get("user")
            )

        elif action == "media_wash_status":
            results = self.get_data("wash_results")
            text = build_status_text(results)
            self.post_message(
                channel=event.event_data.get("channel"),
                title="影视洗板状态",
                text=text,
                userid=event.event_data.get("user")
            )
