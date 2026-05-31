"""
影视洗板插件 — 扫描与分组引擎
================================
文件扫描、媒体信息提取、分组对比、质量排名。
"""

import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pytz

from app.core.config import settings
from app.log import logger

from .quality import (
    parse_season_episode, format_file_size, TARGET_EXTENSIONS,
    normalize_hdr_label, normalize_res_label,
)


def _now() -> datetime:
    """返回带 MoviePilot 时区的当前时间（与 __init__.py 保持一致）"""
    return datetime.now(tz=pytz.timezone(settings.TZ))


# ============================================================
# 文件工具
# ============================================================

def resolve_scan_directories(media_dirs: str = "") -> List[Path]:
    """
    解析要扫描的目录列表。
    优先使用用户配置的目录，未配置时回退到系统媒体库目录。

    Args:
        media_dirs: 用户配置的媒体目录，多个用逗号分隔

    Returns:
        要扫描的目录列表
    """
    dirs: List[Path] = []

    # 优先使用用户配置的目录
    if media_dirs:
        for d in media_dirs.split(","):
            d = d.strip()
            if d:
                path = Path(d)
                if path.exists() and path.is_dir():
                    dirs.append(path)
                else:
                    logger.warning(f"影视洗板: 配置的目录不存在: {d}")

    # 如果未配置，使用系统媒体库目录
    if not dirs:
        try:
            media_dirs_cfg = settings.MEDIA_DIR
            if media_dirs_cfg:
                if isinstance(media_dirs_cfg, list):
                    for d in media_dirs_cfg:
                        path = Path(d)
                        if path.exists() and path.is_dir():
                            dirs.append(path)
                elif isinstance(media_dirs_cfg, str):
                    for d in media_dirs_cfg.split(","):
                        path = Path(d.strip())
                        if path.exists() and path.is_dir():
                            dirs.append(path)

            # 也检查 LIBRARY_PATH
            if not dirs:
                lib_path = getattr(settings, 'LIBRARY_PATH', None)
                if lib_path:
                    path = Path(lib_path)
                    if path.exists() and path.is_dir():
                        dirs.append(path)
        except Exception as e:
            logger.debug(f"获取系统媒体库目录失败: {str(e)}")

    return dirs


def collect_target_files(directories: List[Path], min_size: int = 100) -> List[Path]:
    """
    收集目录下的所有 .strm 和视频文件。

    .strm 文件没有文件大小下限，
    视频文件默认需 >= min_size MB。

    Args:
        directories: 要扫描的目录列表
        min_size: 视频文件最小大小(MB)

    Returns:
        目标文件路径列表
    """
    target_files = []
    min_bytes = min_size * 1024 * 1024

    for directory in directories:
        try:
            for root, _, files in os.walk(directory):
                root_path = Path(root)
                for f in files:
                    file_path = root_path / f
                    ext = file_path.suffix.lower()

                    if ext not in TARGET_EXTENSIONS:
                        continue

                    # .strm 文件始终收录
                    if ext == ".strm":
                        target_files.append(file_path)
                        continue

                    # 视频文件按最小大小过滤（v2.3.0: 保护网络挂载超时）
                    try:
                        if file_path.stat().st_size >= min_bytes:
                            target_files.append(file_path)
                    except OSError:
                        logger.debug(f"影视洗板: 无法读取文件信息（可能网络挂载断开）: {file_path}")

        except PermissionError:
            logger.warning(f"影视洗板: 无权限访问目录 {directory}")
        except Exception as e:
            logger.warning(f"影视洗板: 扫描目录 {directory} 出错: {str(e)}")

    return target_files


# ============================================================
# 媒体信息提取
# ============================================================

def clean_title(dir_name: str) -> str:
    """
    从目录名中提取纯净标题，移除 [tmdb=xxxx] 和 (year)。

    移除 [tmdb=xxx] 和所有括号内年份标记，保留年份前后的其他内容。
    不同于旧版 .*$ 会吞掉年份后的内容，这里替换为精确匹配的整段括号年份。

    MoviePilot 格式示例:
        "Avatar (2009) [tmdb=123]"        -> "Avatar"
        "The Movie (2024) Directors Cut"  -> "The Movie Directors Cut"
    """
    title = re.sub(r'\s*\[tmdb=\d+\]', '', dir_name).strip()
    # 移除所有括号内的年份标记（任何位置），不吞后续内容
    title = re.sub(r'\s*[\(\[（【]\d{4}[\)\]）】]', '', title).strip()
    # 也匹配无括号的年份
    title = re.sub(r'\s+\d{4}(?:\s|$)', ' ', title).strip()
    title = re.sub(r'[\s\-_]+$', '', title).strip()
    return title


def parse_tmdb_id(file_path: Path) -> Optional[str]:
    """
    从路径中提取 TMDB ID。
    MoviePilot 格式: .../Title (Year) [tmdb=1396]/...
    """
    for part in file_path.parts:
        tmdb_match = re.search(r'\[tmdb=(\d+)\]', part)
        if tmdb_match:
            return tmdb_match.group(1)
    return None


def guess_media_year(file_path: Path) -> Optional[str]:
    """从文件名或路径猜测年份。"""
    name = str(file_path)
    year_match = re.search(r'[\(\[（【]?(\d{4})[\)\]）】]?', name)
    if year_match:
        year = int(year_match.group(1))
        current_year = _now().year
        if 1900 <= year <= current_year + 5:
            return str(year)
    return None


def guess_media_title(file_path: Path) -> str:
    """
    从文件路径猜测媒体标题。
    MoviePilot 命名规范:
        Movie: /媒体库/电影/阿凡达 (2009) [tmdb=123]/阿凡达.2009.2160p.mkv
        TV:    /媒体库/电视剧/Breaking Bad (2008) [tmdb=1396]/Season 01/...
    """
    parent = file_path.parent
    grandparent = parent.parent

    # 检查是否在 Season XX 子目录中（电视剧）
    if re.match(r'^Season\s+\d+', parent.name, re.IGNORECASE):
        show_dir_name = grandparent.name
        title = clean_title(show_dir_name)
        if title and len(title) < 100:
            return title

    # 电影或目录结构：直接父目录就是剧集名
    parent_name = parent.name
    if parent_name and parent_name != grandparent.name:
        title = clean_title(parent_name)
        if title and len(title) < 100:
            return title

    # 从文件名中提取标题（去掉年份、分辨率等信息）
    stem = file_path.stem
    clean = re.sub(
        r'(?:\.(?:2160p|4K|1080p|720p|480p|UHD|HD|FHD|BluRay|WEB-DL|WEBRip|HDRip|DVDRip|Remux|REMUX|HDR|SDR|x264|x265|HEVC|AVC|DTS|AC3|AAC|FLAC|TrueHD|Atmos).*)$',
        '', stem, flags=re.IGNORECASE
    )
    parts = re.split(r'[. _-]', clean)
    title_parts = []
    for p in parts:
        if re.match(r'^\d{4}$', p):
            break
        if p and not p.startswith(('第', 'E', 'S', 'EP')) and not re.match(r'^\d{3,4}p$', p, re.IGNORECASE):
            title_parts.append(p)

    if title_parts:
        return ' '.join(title_parts).strip()
    return stem


# ============================================================
# 分组与对比算法
# ============================================================

def group_by_media(items: List[Dict]) -> Dict[str, List[Dict]]:
    """
    按媒体唯一标识分组：title + year + tmdbid + season + episode。
    MoviePilot 命名规范中，只有这些字段全部相同才是真正的"重复"。
    单版本每集始终保留，仅同集多版本才去重。
    """
    groups: Dict[str, List[Dict]] = {}

    for item in items:
        if "sample" in item.get("filename", "").lower():
            continue  # 跳过样本文件

        title = item.get("media_title") or "未知标题"
        year = item.get("media_year") or "0000"
        tmdbid = item.get("tmdbid") or ""
        s_num = item.get("season_num") or 0
        e_num = item.get("episode_num") or 0

        # 分组键: title + year + tmdb + season + episode
        key_parts = [title, year]
        if tmdbid:
            key_parts.append(tmdbid)
        key_parts.append(str(s_num))
        key_parts.append(str(e_num))
        key = "|".join(key_parts)

        if key not in groups:
            groups[key] = []
        groups[key].append(item)

    return groups


def make_version_entry(v: Dict, is_best: bool, rank: int, deleted: bool,
                       keep_reason: str = "") -> Dict:
    """构建单一版本条目。"""
    se = v.get("season_episode", {})
    return {
        "filepath": v.get("filepath"),
        "filename": v.get("filename"),
        "size_bytes": v.get("size_bytes", 0),
        "size_display": v.get("size_display", ""),
        "quality_score": v.get("quality_score", 0),
        "quality_details": v.get("quality_details", {}),
        "season_episode": se,
        "season_num": se.get("season_num", 0),
        "episode_num": se.get("episode_num", 0),
        "action": "keep" if is_best else "delete",
        "is_best": is_best,
        "rank": rank,
        "deleted": deleted,
        "keep_reason": keep_reason,
    }


def _select_keepers(
    versions: List[Dict],
    keep_count: int,
    keep_mode: str = "top_n",
) -> List[Tuple[Dict, str]]:
    """
    根据保留策略选择要保留的版本。

    Args:
        versions: 同一集的多个版本（已按 score 降序排列）
        keep_count: 最大保留数
        keep_mode:
            - "top_n": 保留前 N 个最高分（现有逻辑，向后兼容）
            - "per_hdr": 每种 HDR 类型各保留 1 个最高分
            - "per_hdr_res": 每 HDR+分辨率 组合各保留 1 个最高分

    Returns:
        [(version, keep_reason), ...] — 仅返回被保留的版本，非保留版本不在此列表中
    """
    if keep_mode == "top_n":
        # 向后兼容：返回所有版本，前 keep_count 个有 reason
        return [(v, "总分最高" if i < keep_count else "")
                for i, v in enumerate(versions)]

    # per_hdr / per_hdr_res: 仅返回被选中保留的版本
    selected: List[Tuple[Dict, str]] = []

    if keep_mode == "per_hdr":
        groups: Dict[str, List[Dict]] = {}
        for v in versions:
            hdr_raw = v.get("quality_details", {}).get("hdr", {}).get("label", "SDR")
            hdr = normalize_hdr_label(hdr_raw)
            groups.setdefault(hdr, []).append(v)
        for hdr, group in groups.items():
            best = max(group, key=lambda x: x["quality_score"])
            selected.append((best, f"HDR {hdr} 最高分"))

    elif keep_mode == "per_hdr_res":
        groups2: Dict[str, List[Dict]] = {}
        for v in versions:
            hdr_raw = v.get("quality_details", {}).get("hdr", {}).get("label", "SDR")
            res_raw = v.get("quality_details", {}).get("resolution", {}).get("label", "Unknown")
            hdr = normalize_hdr_label(hdr_raw)
            res = normalize_res_label(res_raw)
            key = f"{hdr}|{res}"
            groups2.setdefault(key, []).append(v)
        for key, group in groups2.items():
            best = max(group, key=lambda x: x["quality_score"])
            selected.append((best, f"HDR {key.replace('|', ' ')} 最高分"))

    # 按总分降序排序，截断到 keep_count
    selected.sort(key=lambda x: x[0]["quality_score"], reverse=True)
    selected = selected[:keep_count]

    return selected


def compare_and_rank(groups: Dict[str, List[Dict]], keep_count: int = 1,
                     min_score: int = 0, keep_mode: str = "top_n") -> Dict[str, Any]:
    """
    按集分组对比版本质量，生成清洗建议。

    分组键已精确到 `title|year|tmdbid|season_num|episode_num`，
    每组内只有真正同集的多个版本才会被对比。

    v2.1.1 增强: 支持 min_score 最低分数阈值。
    评分低于 min_score 的文件强制标记删除，不占用 keep_count 名额。

    v2.4.0 增强: 支持 keep_mode 多版本保留策略。
    """
    results = {
        "last_scan": _now().strftime("%Y-%m-%d %H:%M:%S"),
        "total_items": sum(len(items) for items in groups.values()),
        "total_groups": len(groups),
        "groups_with_duplicates": 0,
        "potential_savings_gb": 0.0,
        "items": {},
        "shows": {}
    }

    for group_key, versions in groups.items():
        first = versions[0]
        title = first.get("media_title", "未知标题")
        year = first.get("media_year", "0000")
        tmdbid = first.get("tmdbid", "")
        s_num = first.get("season_num", 0)
        e_num = first.get("episode_num", 0)
        se_display = first.get("season_episode", {}).get("display", "")
        is_movie = first.get("season_episode", {}).get("is_movie", True)

        if len(versions) <= 1:
            # 单版本 -> 保留（即使低于 min_score 也保留，避免丢失唯一副本）
            v = versions[0]
            item_data = {
                "title": title, "year": year, "tmdbid": tmdbid,
                "season_num": s_num, "episode_num": e_num,
                "episode_display": se_display, "is_movie": is_movie,
                "media_group_key": group_key, "versions": [],
                "has_duplicates": False
            }
            item_data["versions"].append(make_version_entry(v, is_best=True, rank=1, deleted=False))
            results["items"][group_key] = item_data
        else:
            # 多版本 -> min_score 阈值过滤 + 保留最佳
            results["groups_with_duplicates"] += 1

            if min_score > 0:
                # 按 min_score 分割：达标 vs 不达标
                qualified = [v for v in versions if v["quality_score"] >= min_score]
                unqualified = [v for v in versions if v["quality_score"] < min_score]
            else:
                qualified = list(versions)
                unqualified = []

            # 达标文件按评分降序排列
            # v2.1.2: 同分时按各维度得分依次优选：视频编码→分辨率→片源→音频→HDR
            def _sort_key(v):
                qd = v.get("quality_details", {})
                return (
                    v["quality_score"],
                    qd.get("video", {}).get("value", 0),
                    qd.get("resolution", {}).get("value", 0),
                    qd.get("source", {}).get("value", 0),
                    qd.get("audio", {}).get("value", 0),
                    qd.get("hdr", {}).get("value", 0),
                )
            sorted_qualified = sorted(qualified, key=_sort_key, reverse=True)

            item_data = {
                "title": title, "year": year, "tmdbid": tmdbid,
                "season_num": s_num, "episode_num": e_num,
                "episode_display": se_display, "is_movie": is_movie,
                "media_group_key": group_key, "versions": [],
                "has_duplicates": True
            }

            rank = 0
            # v2.4.0: 使用 _select_keepers 根据保留策略选择要保留的版本
            keepers = _select_keepers(sorted_qualified, keep_count, keep_mode)
            keeper_reasons = {id(k[0]): k[1] for k in keepers}

            for v in sorted_qualified:
                rank += 1
                is_best = bool(keeper_reasons.get(id(v), ""))
                keep_reason = keeper_reasons.get(id(v), "")
                if not is_best:
                    results["potential_savings_gb"] += v["size_bytes"] / (1024 ** 3)
                item_data["versions"].append(
                    make_version_entry(v, is_best=is_best, rank=rank, deleted=False,
                                       keep_reason=keep_reason)
                )

            # 不达标文件：全部标记删除（不占用 keep_count 名额）
            for v in unqualified:
                rank += 1
                results["potential_savings_gb"] += v["size_bytes"] / (1024 ** 3)
                item_data["versions"].append(
                    make_version_entry(v, is_best=False, rank=rank, deleted=False)
                )

            results["items"][group_key] = item_data

    # 构建 show 索引（按剧集分组）
    for gk, gd in results["items"].items():
        show_key = f"{gd['title']}|{gd['year']}|{gd['tmdbid'] or ''}"
        if show_key not in results["shows"]:
            results["shows"][show_key] = {
                "title": gd["title"], "year": gd["year"], "tmdbid": gd["tmdbid"],
                "episode_keys": [], "season_count": 0, "seasons": {}
            }
        results["shows"][show_key]["episode_keys"].append(gk)
        sn = gd.get("season_num", 0)
        if sn not in results["shows"][show_key]["seasons"]:
            results["shows"][show_key]["seasons"][sn] = []
        results["shows"][show_key]["seasons"][sn].append(gk)

    # 每部剧的集数按季+集升序排列
    for show_data in results["shows"].values():
        show_data["episode_keys"].sort(key=lambda k: (
            results["items"][k].get("season_num", 0),
            results["items"][k].get("episode_num", 0)
        ))
        show_data["season_count"] = len(show_data["seasons"])

    # 按 quality_score 降序排列 items（有重复的靠前）
    items_sorted = sorted(
        results["items"].values(),
        key=lambda x: (
            0 if x["has_duplicates"] else 1,
            -(max(v["quality_score"] for v in x["versions"]) if x["versions"] else 0)
        )
    )
    results["items_ordered"] = [it["media_group_key"] for it in items_sorted]
    results["potential_savings_gb"] = round(results["potential_savings_gb"], 1)

    return results
