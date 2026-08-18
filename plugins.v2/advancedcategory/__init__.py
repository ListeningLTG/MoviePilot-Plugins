import json
import os
import shutil
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

try:
    import ruamel.yaml as ruamel_yaml
except ImportError:
    import yaml as ruamel_yaml

from app.core.config import settings
from app.core.event import eventmanager, Event
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import (
    TransferRenameBuildEventData,
    TransferRenameEventData,
)
from app.schemas.types import (
    ChainEventType,
    MediaType,
    NotificationType,
)

from .helper import RuleEngine, TmdbExtraHelper, CacheManager

lock = threading.Lock()


class advancedcategory(_PluginBase):
    # 插件名称
    plugin_name = "高级二级分类"
    # 插件描述
    plugin_desc = "扩展 MoviePilot 二级分类识别能力，支持根据关键词库、系列名称、演职员名单及自定义 YAML 规则分类，并在整理转移时自动重组二级目录。"
    # 插件图标
    plugin_icon = "https://raw.githubusercontent.com/ListeningLTG/MoviePilot-Plugins/refs/heads/main/icons/category.png"
    # 插件版本
    plugin_version = "1.0.8"
    # 插件作者
    plugin_author = "ListeningLTG"
    # 作者主页
    author_url = "https://github.com/ListeningLTG"
    # 插件配置项ID前缀
    plugin_config_prefix = "advancedcategory_"
    # 加载顺序
    plugin_order = 28
    # 可使用的用户级别
    auth_level = 1

    @property
    def name(self) -> str:
        """兼容 MoviePilot 获取插件名称属性"""
        return self.plugin_name

    _enabled: bool = False
    _notify: bool = False
    _rules: Dict[str, Any] = {}
    _cache_mgr: Optional[CacheManager] = None
    _tmdb_extra: Optional[TmdbExtraHelper] = None

    def init_plugin(self, config: dict = None):
        rules_yaml = None
        need_reset_switch = False

        if config:
            self._enabled = config.get("enabled", False)
            self._notify = config.get("notify", False)
            rules_yaml = config.get("rules_yaml")

        # 先初始化路径、规则与缓存管理器
        self._init_rules_and_cache()

        # 如果勾选了清空缓存开关，执行清空操作并打印详细日志
        if config and config.get("clear_cache"):
            count = len(self._cache_mgr._cache_data) if self._cache_mgr and hasattr(self._cache_mgr, "_cache_data") else 0
            if self._cache_mgr:
                self._cache_mgr.clear()
            logger.info(f"【高级二级分类】检测到开启【立即清空分类缓存】开关，已成功清空全部识别分类历史缓存（共清空 {count} 条记录）")
            need_reset_switch = True

        # 如果保存配置提交了新的 YAML 规则文本，写入磁盘并同步刷新缓存与规则
        if rules_yaml and isinstance(rules_yaml, str):
            self._save_rules_yaml(rules_yaml)
            # 重新载入写盘后的规则
            self._init_rules_and_cache()

        # 将 clear_cache 开关回写重置为 False，确保下次打开设置界面时恢复关闭状态
        if need_reset_switch:
            try:
                if hasattr(self, "update_config"):
                    self.update_config({
                        "enabled": self._enabled,
                        "notify": self._notify,
                        "clear_cache": False,
                        "rules_yaml": rules_yaml if rules_yaml else self._get_current_rules_yaml_text(),
                    })
                    logger.info("【高级二级分类】已自动将【立即清空分类缓存】开关恢复为关闭状态")
            except Exception as e:
                logger.error(f"【高级二级分类】重置清空缓存开关状态失败: {e}")


    def _save_rules_yaml(self, rules_yaml: str):
        """校验并将用户在表单编辑的 YAML 文本保存回磁盘配置文件"""
        try:
            # 校验 YAML 语法合法性
            if hasattr(ruamel_yaml, "YAML"):
                yaml_loader = ruamel_yaml.YAML()
                parsed = yaml_loader.load(rules_yaml)
            else:
                parsed = ruamel_yaml.safe_load(rules_yaml)

            if not isinstance(parsed, dict):
                logger.error("【高级二级分类】表单提交的 YAML 规则格式不符合 Dict 结构，放弃写盘更新")
                return

            # 写入磁盘文件
            with open(self._rules_file_path, mode="w", encoding="utf-8") as f:
                f.write(rules_yaml)
            logger.info(f"【高级二级分类】通过插件配置界面成功保存更新规则文件: {self._rules_file_path}")

            # 清理已有缓存以使新规则立刻在后续整理中生效
            if self._cache_mgr:
                self._cache_mgr.clear()
        except Exception as e:
            logger.error(f"【高级二级分类】保存提交的 YAML 规则文本失败（语法错误或写盘失败）: {e}")


    @property
    def _plugin_data_dir(self) -> Path:
        data_dir = settings.CONFIG_PATH / "plugins" / "advancedcategory"
        data_dir.mkdir(parents=True, exist_ok=True)
        return data_dir

    @property
    def _rules_file_path(self) -> Path:
        return self._plugin_data_dir / "category_rules.yaml"

    @property
    def _cache_file_path(self) -> Path:
        return self._plugin_data_dir / "cache.json"

    def _init_rules_and_cache(self):
        """初始化规则配置与缓存"""
        # 1. 初始化规则文件
        if not self._rules_file_path.exists():
            example_file = Path(__file__).parent / "category_rules.yaml.example"
            if example_file.exists():
                try:
                    shutil.copy(example_file, self._rules_file_path)
                    logger.info(f"【高级二级分类】初始化生成默认分类规则文件: {self._rules_file_path}")
                except Exception as e:
                    logger.error(f"【高级二级分类】复制规则模版文件失败: {e}")

        # 加载规则
        self._rules = {}
        if self._rules_file_path.exists():
            try:
                with open(self._rules_file_path, mode="r", encoding="utf-8", errors="replace") as f:
                    if hasattr(ruamel_yaml, "YAML"):
                        yaml_loader = ruamel_yaml.YAML()
                        self._rules = yaml_loader.load(f) or {}
                    else:
                        self._rules = ruamel_yaml.safe_load(f) or {}
                logger.info(f"【高级二级分类】成功加载规则文件 {self._rules_file_path}")
            except Exception as e:
                logger.error(f"【高级二级分类】解析规则文件 {self._rules_file_path} 失败: {e}")

        # 2. 初始化缓存与辅助类
        self._cache_mgr = CacheManager(self._cache_file_path)
        self._tmdb_extra = TmdbExtraHelper()

    def get_state(self) -> bool:
        return self._enabled

    def get_matched_category(self, mediainfo: Any) -> Optional[str]:
        """
        判断 mediainfo 匹配到的高级二级分类
        :param mediainfo: MediaInfo 对象
        :return: 规则匹配到的分类名称，若未匹配到则返回 None
        """
        if not mediainfo or not self._rules:
            return None

        title = getattr(mediainfo, "title", "") or ""
        mtype_str = "movie" if getattr(mediainfo, "type", None) == MediaType.MOVIE else "tv"
        category_rules_dict = self._rules.get(mtype_str) or {}
        if not category_rules_dict:
            return None

        tmdb_info = getattr(mediainfo, "tmdb_info", {}) or {}
        tmdb_id = getattr(mediainfo, "tmdb_id", None) or tmdb_info.get("id")

        # 检查缓存
        cache_key = f"{mtype_str}_{tmdb_id}" if tmdb_id else f"{mtype_str}_{title}"
        cached_cat = self._cache_mgr.get(cache_key)
        if cached_cat is not None:
            if cached_cat != "":
                logger.info(f"【高级二级分类】命中缓存 [{cache_key}] -> 分类 [{cached_cat}]")
                return cached_cat
            return None

        logger.info(f"【高级二级分类】开始规则比对: 作品 [{title}] (TMDB: {tmdb_id}, 类型: {mtype_str})")

        # 收集扩展信息 (演职员、关键词、别名池)
        extra_data = self._tmdb_extra.build_extra_data(tmdb_info, mtype=mtype_str)

        matched_cat_name = None
        for cat_name, rule_dict in category_rules_dict.items():
            if not rule_dict:
                continue

            matched, hit_reason = RuleEngine.match_rule(rule_dict, tmdb_info, extra_data)
            if matched:
                matched_cat_name = cat_name
                logger.info(f"【高级二级分类】匹配成功: 作品 [{title}] (TMDB: {tmdb_id}) 命中分类规则 -> [{cat_name}] (原因: {hit_reason})")
                break


        if not matched_cat_name:
            logger.info(f"【高级二级分类】作品 [{title}] 未命中任何高级分类规则")

        # 写入缓存
        self._cache_mgr.set(cache_key, matched_cat_name or "")
        return matched_cat_name

    @eventmanager.register(ChainEventType.TransferRenameBuild)
    def on_transfer_rename_build(self, event: Event):
        """
        重命名构建事件 Hook：匹配高级分类并载入 rename_dict
        """
        if not self.get_state():
            return

        data = getattr(event, "event_data", None)
        if not data or not hasattr(data, "rename_dict"):
            return

        rename_dict = getattr(data, "rename_dict", None)
        if not isinstance(rename_dict, dict):
            return

        mediainfo = rename_dict.get("__mediainfo__")
        if not mediainfo:
            return

        source_path = getattr(data, "source_path", None)
        logger.info(f"【高级二级分类】收到 TransferRenameBuild 事件: 文件 [{source_path}]")

        matched_cat = self.get_matched_category(mediainfo)
        if matched_cat:
            rename_dict["category"] = matched_cat
            if rename_dict.get("__mediainfo__"):
                rename_dict["__mediainfo__"].category = matched_cat

    @eventmanager.register(ChainEventType.TransferRename)
    def on_transfer_rename(self, event: Event):
        """
        重命名改写 Hook：在整理预览及实际整理计算渲染路径后，改写为原媒体库下对应二级分类目录路径
        """
        if not self.get_state():
            return

        data = getattr(event, "event_data", None)
        if not data or not hasattr(data, "render_str") or not hasattr(data, "path"):
            return

        if not data.path or not data.render_str:
            return

        source_path = getattr(data, "source_path", None) or (data.source_item.path if getattr(data, "source_item", None) else None)
        rename_dict = getattr(data, "rename_dict", {}) or {}
        mediainfo = rename_dict.get("__mediainfo__")

        if not mediainfo:
            return

        logger.info(f"【高级二级分类】收到 TransferRename 事件: 预渲染目标 [{data.render_str}], 基础路径 [{data.path}]")

        matched_cat = self.get_matched_category(mediainfo)
        if not matched_cat:
            return

        # 更新 mediainfo 属性
        if rename_dict and rename_dict.get("__mediainfo__"):
            data.rename_dict["__mediainfo__"].category = matched_cat

        try:
            old_render_path = Path(data.render_str)
            base_dir = Path(data.path)
            old_cat = getattr(mediainfo, "category", None) or ""

            # 判断基础路径（如 /mnt/data/strm/shareStrm/电影/港台电影），取其父级目录重新拼接高级分类 R级电影
            if base_dir.parent and base_dir.name:
                new_base_dir = base_dir.parent / matched_cat
            else:
                new_base_dir = base_dir / matched_cat

            # 拼接最终改写目标路径
            clean_abs_target = (new_base_dir / old_render_path).as_posix()


            logger.info(
                f"【高级二级分类】整理路径重构成功: 源文件 [{source_path}] -> 目标路径改写为 [{clean_abs_target}]"
            )
            data.updated = True
            data.updated_str = clean_abs_target
            data.source = self.plugin_name

            if self._notify:
                self.post_message(
                    mtype=NotificationType.Plugin,
                    title="高级二级分类路径改写",
                    text=f"作品：{getattr(mediainfo, 'title', '')}\n匹配分类：{matched_cat}\n目标路径：{clean_abs_target}"
                )
        except Exception as e:
            logger.error(f"【高级二级分类】改写整理路径失败: {e}")


    def get_api(self) -> List[Dict[str, Any]]:
        """获取插件API列表"""
        return []

    def _get_current_rules_yaml_text(self) -> str:
        """读取当前分类规则 YAML 文件文本，供表单编辑器展示"""
        if self._rules_file_path.exists():
            try:
                with open(self._rules_file_path, mode="r", encoding="utf-8", errors="replace") as f:
                    return f.read()
            except Exception as e:
                logger.error(f"【高级二级分类】读取规则文件失败: {e}")

        example_file = Path(__file__).parent / "category_rules.yaml.example"
        if example_file.exists():
            try:
                with open(example_file, mode="r", encoding="utf-8", errors="replace") as f:
                    return f.read()
            except Exception as e:
                logger.error(f"【高级二级分类】读取规则模版文件失败: {e}")
        return ""

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """
        拼装插件配置页面
        """
        rules_path_str = str(self._rules_file_path.as_posix() if hasattr(self._rules_file_path, "as_posix") else self._rules_file_path)
        current_yaml_text = self._get_current_rules_yaml_text()

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
                                        "props": {
                                            "model": "enabled",
                                            "label": "启用插件",
                                        },
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
                                            "model": "notify",
                                            "label": "匹配成功发送通知",
                                        },
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
                                            "model": "clear_cache",
                                            "label": "立即清空分类缓存",
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
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "info",
                                            "variant": "tonal",
                                            "text": f"规则配置文件路径：{rules_path_str}。在下方编辑 YAML 规则或勾选【立即清空分类缓存】，点击【保存】即可生效。",
                                        },
                                    }
                                ],
                            }
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
                                            "model": "rules_yaml",
                                            "label": "高级二级分类 YAML 规则配置",
                                            "hint": "可以在此处直接修改/添加分类规则（支持 keywords、series_keywords、series_actors 等）。点击【保存】后自动校验并生效。",
                                            "persistent-hint": True,
                                            "rows": 16,
                                            "auto-grow": True,
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                ],
            }
        ], {
            "enabled": False,
            "notify": False,
            "clear_cache": False,
            "rules_yaml": current_yaml_text,
        }


    def get_page(self) -> List[dict]:
        """
        拼装插件详情页面
        """
        rules_path_str = str(self._rules_file_path.as_posix() if hasattr(self._rules_file_path, "as_posix") else self._rules_file_path)

        rule_rows = []
        if self._rules:
            for mtype, cat_dict in self._rules.items():
                mtype_label = "电影" if mtype == "movie" else "电视剧/动漫"
                if isinstance(cat_dict, dict):
                    for cat_name, rdict in cat_dict.items():
                        if not isinstance(rdict, dict):
                            continue
                        genre_ids = str(rdict.get("genre_ids") or "-")
                        languages = str(rdict.get("original_language") or "-")
                        countries = str(rdict.get("production_countries") or rdict.get("origin_country") or "-")
                        kw = str(rdict.get("keywords") or rdict.get("include_keywords") or "-")
                        actors = str(rdict.get("series_actors") or rdict.get("actors") or "-")

                        if len(kw) > 35:
                            kw = kw[:35] + "..."
                        if len(actors) > 35:
                            actors = actors[:35] + "..."

                        rule_rows.append({
                            "component": "tr",
                            "content": [
                                {"component": "td", "content": [{"component": "VChip", "props": {"color": "primary", "size": "small"}, "text": mtype_label}]},
                                {"component": "td", "content": [{"component": "strong", "text": str(cat_name)}]},
                                {"component": "td", "text": genre_ids},
                                {"component": "td", "text": languages},
                                {"component": "td", "text": countries},
                                {"component": "td", "text": kw},
                                {"component": "td", "text": actors},
                            ]
                        })

        cache_data = getattr(self._cache_mgr, "_cache_data", {}) if self._cache_mgr else {}
        cache_rows = []
        if isinstance(cache_data, dict):
            for cache_key, cat_val in list(cache_data.items())[-20:]:
                if not cat_val:
                    continue
                cache_rows.append({
                    "component": "tr",
                    "content": [
                        {"component": "td", "text": str(cache_key)},
                        {"component": "td", "content": [{"component": "VChip", "props": {"color": "success", "size": "small"}, "text": str(cat_val)}]},
                    ]
                })

        page_content = []

        page_content.append({
            "component": "VAlert",
            "props": {
                "type": "info",
                "variant": "tonal",
                "class": "mb-4",
                "text": f"【高级二级分类】使用配置文件：{rules_path_str}，当前已解析 {len(rule_rows)} 条自定义分类规则。"
            }
        })

        if rule_rows:
            page_content.append({
                "component": "VCard",
                "props": {"class": "mb-4"},
                "content": [
                    {"component": "VCardTitle", "text": "🏷️ 已生效的高级分类规则概览"},
                    {"component": "VCardText", "content": [
                        {
                            "component": "VTable",
                            "props": {"density": "compact"},
                            "content": [
                                {"component": "thead", "content": [{"component": "tr", "content": [
                                    {"component": "th", "text": "媒体类型"},
                                    {"component": "th", "text": "二级分类名"},
                                    {"component": "th", "text": "Genre IDs"},
                                    {"component": "th", "text": "语种"},
                                    {"component": "th", "text": "国家/地区"},
                                    {"component": "th", "text": "关键词示例"},
                                    {"component": "th", "text": "演职员示例"},
                                ]}]},
                                {"component": "tbody", "content": rule_rows}
                            ]
                        }
                    ]}
                ]
            })

        if cache_rows:
            page_content.append({
                "component": "VCard",
                "props": {"class": "mb-4"},
                "content": [
                    {"component": "VCardTitle", "text": "⚡ 识别分类命中缓存"},
                    {"component": "VCardText", "content": [
                        {
                            "component": "VTable",
                            "props": {"density": "compact"},
                            "content": [
                                {"component": "thead", "content": [{"component": "tr", "content": [
                                    {"component": "th", "text": "识别键名 (Media / TMDB)"},
                                    {"component": "th", "text": "判定二级分类"},
                                ]}]},
                                {"component": "tbody", "content": cache_rows}
                            ]
                        }
                    ]}
                ]
            })

        if not rule_rows and not cache_rows:
            page_content.append({
                "component": "VAlert",
                "props": {"type": "warning", "text": f"未在 {rules_path_str} 中读取到有效规则，请确保配置文件正确存在。"}
            })

        return page_content

    def stop_service(self):
        """
        停止插件服务（无后台任务，直接返回）
        """
        pass
