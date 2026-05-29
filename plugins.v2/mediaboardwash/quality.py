"""
影视洗板插件 — 质量解析引擎
================================
从文件名中解析分辨率、片源、音频、HDR、视频编码等质量信息并评分。
"""

import json
import re
from typing import Any, Dict, List, Optional, Tuple

from app.log import logger


# ============================================================
# 公共工具函数
# ============================================================

def format_file_size(size_bytes: int) -> str:
    """格式化文件大小为可读字符串。"""
    if size_bytes < 1024:
        return f"{size_bytes}B"
    elif size_bytes < 1024 ** 2:
        return f"{size_bytes / 1024:.1f}KB"
    elif size_bytes < 1024 ** 3:
        return f"{size_bytes / 1024 ** 2:.1f}MB"
    else:
        return f"{size_bytes / 1024 ** 3:.2f}GB"


# ============================================================
# 常量定义
# ============================================================

# 目标文件扩展名
TARGET_EXTENSIONS: set = {
    ".strm",
    ".mkv", ".mp4", ".avi", ".ts", ".iso", ".m2ts",
    ".mov", ".m4v", ".wmv", ".flv", ".webm", ".rmvb",
    ".divx", ".vob", ".mpeg", ".mpg", ".3gp"
}

# 分辨率档位
RESOLUTION_PATTERNS = [
    (r'(8K|4320p)', 50),
    (r'(2160p|4K|UHD|4k)', 40),
    (r'(1080p|FHD|FullHD|1080i)', 30),
    (r'(720p|HD|1280x720)', 20),
    (r'(540p|qHD)', 15),
    (r'(480p|SD|852x480|640x480)', 10),
    (r'(360p|nHD)', 5),
]

# 片源类型
SOURCE_PATTERNS = [
    (r'(Remux|REMUX)', 35),
    (r'(BluRay|BDRip|BDMV|BD[0-9]{2}|蓝光|原盘)', 30),
    (r'(WEB.?DL|WEB.?HD|WEB-DL|WEBDL|WEBHD)', 25),
    (r'(WEBRip|WEB.?Rip)', 20),
    (r'(HDTV|PDTV|DVB)', 15),
    (r'(HDRip|HD.?CAM|HDCAM)', 10),
    (r'(DVDRip|DVD[0-9]|DVD)', 5),
    (r'(CAM|TS|HDTS|TC|HDTC|SCR|SCREENER|R5|DVDSCR)', 0),
]

# 音频编码
AUDIO_PATTERNS = [
    (r'(TrueHD.?Atmos|Atmos|DTS.?HD.?MA|DTS.?HD.?HR|DTS-HD)', 15),
    (r'(DTS|DTS.?ES|DTS.?X)', 12),
    (r'(TrueHD|Dolby.?TrueHD)', 11),
    (r'(FLAC|LPCM|PCM)', 10),
    (r'(E.?AC3|DD\+|DDP|AC3|DD|Dolby.?Digital)', 8),
    (r'(AAC|AACLC|HE.?AAC)', 5),
    (r'(MP3|MP2)', 2),
]

# HDR类型
HDR_PATTERNS = [
    (r'(DV|Dolby.?Vision|DoVi|DolbyVision)', 15),
    (r'(HDR10\+|HDR10P)', 15),
    (r'(HDR10|HDR|HDR10)', 15),
    (r'(HLG)', 5),
    (r'(SDR)', 0),
]

# 视频编码
VIDEO_PATTERNS = [
    (r'(AV1)', 15),
    (r'(HEVC|h265|H265|x265)', 12),
    (r'(h264|H264|x264|AVC)', 8),
    (r'(VC1|MPEG2|MPEG4|VP9|VP8)', 5),
]

# 加分特征
BONUS_PATTERNS = [
    (r'(Complete|COMPLETE)', 2),
    (r'(3D|ThreeD)', 2),
    (r'(IMAX|IMAXEnhanced)', 2),
    (r'(Director.?Cut|导演剪辑版)', 2),
    (r'(Extended|加长版|Extension)', 2),
    (r'(Criterion|CC版)', 3),
]

# 维度中文名映射
DIMENSION_NAMES = {
    "resolution": "分辨率", "source": "片源", "audio": "音频",
    "hdr": "HDR类型", "bonus": "加分特征"
}

# 预设评分方案
SCORE_PROFILES = {
    "custom": {
        "name": "自定义",
        "res": 40, "src": 35, "aud": 15, "hdr": 15, "vid": 12, "bonus": 6
    },
    "movie_source": {
        "name": "电影(偏重片源)",
        "res": 30, "src": 50, "aud": 15, "hdr": 10, "vid": 10, "bonus": 5
    },
    "movie_resolution": {
        "name": "电影(偏重画质)",
        "res": 55, "src": 25, "aud": 10, "hdr": 15, "vid": 12, "bonus": 5
    },
    "tv_balanced": {
        "name": "电视剧(均衡)",
        "res": 35, "src": 30, "aud": 20, "hdr": 10, "vid": 12, "bonus": 5
    },
    "anime_resolution": {
        "name": "动漫(偏重画质)",
        "res": 50, "src": 25, "aud": 10, "hdr": 20, "vid": 12, "bonus": 5
    }
}

def _get_default_labels_dict() -> dict:
    """
    获取默认评分规则标签字典（用于 UI 展示）。

    注意：为避免与模块级常量重复维护，这里用 Python 字典替代了之前
    的 DEFAULT_RULES_JSON 硬编码字符串，但 label 字段需要单独维护，
    因为从正则模式中无法精确推导出人类可读的标签名。
    """
    return {
        "resolution": [
            {"pattern": "8K|4320p", "score": 50, "label": "8K"},
            {"pattern": "2160p|4K|UHD|4k", "score": 40, "label": "4K"},
            {"pattern": "1080p|FHD|FullHD|1080i", "score": 30, "label": "1080p"},
            {"pattern": "720p|HD|1280x720", "score": 20, "label": "720p"},
            {"pattern": "540p|qHD", "score": 15, "label": "540p"},
            {"pattern": "480p|SD|852x480|640x480", "score": 10, "label": "480p"},
            {"pattern": "360p|nHD", "score": 5, "label": "360p"},
        ],
        "source": [
            {"pattern": "Remux|REMUX", "score": 35, "label": "Remux"},
            {"pattern": "BluRay|BDRip|BDMV|BD[0-9]{2}|蓝光|原盘", "score": 30, "label": "BluRay"},
            {"pattern": "WEB.?DL|WEB.?HD|WEB-DL|WEBDL|WEBHD", "score": 25, "label": "WEB-DL"},
            {"pattern": "WEBRip|WEB.?Rip", "score": 20, "label": "WEBRip"},
            {"pattern": "HDTV|PDTV|DVB", "score": 15, "label": "HDTV"},
            {"pattern": "HDRip|HD.?CAM|HDCAM", "score": 10, "label": "HDRip"},
            {"pattern": "DVDRip|DVD[0-9]|DVD", "score": 5, "label": "DVDRip"},
            {"pattern": "CAM|TS|HDTS|TC|HDTC|SCR|SCREENER|R5|DVDSCR", "score": 0, "label": "CAM"},
        ],
        "audio": [
            {"pattern": "TrueHD.?Atmos|Atmos|DTS.?HD.?MA|DTS.?HD.?HR|DTS-HD", "score": 15, "label": "无损环绕"},
            {"pattern": "DTS|DTS.?ES|DTS.?X", "score": 12, "label": "DTS"},
            {"pattern": "TrueHD|Dolby.?TrueHD", "score": 11, "label": "TrueHD"},
            {"pattern": "FLAC|LPCM|PCM", "score": 10, "label": "无损"},
            {"pattern": "E.?AC3|DD\\+|DDP|AC3|DD|Dolby.?Digital", "score": 8, "label": "AC3/DD+"},
            {"pattern": "AAC|AACLC|HE.?AAC", "score": 5, "label": "AAC"},
            {"pattern": "MP3|MP2", "score": 2, "label": "MP3"},
        ],
        "hdr": [
            {"pattern": "DV|Dolby.?Vision|DoVi|DolbyVision", "score": 15, "label": "Dolby Vision"},
            {"pattern": "HDR10\\+|HDR10P", "score": 15, "label": "HDR10+"},
            {"pattern": "HDR10|HDR|HDR10", "score": 15, "label": "HDR10"},
            {"pattern": "HLG", "score": 5, "label": "HLG"},
        ],
        "bonus": [
            {"pattern": "Complete|COMPLETE", "score": 2, "label": "Complete版"},
            {"pattern": "3D|ThreeD", "score": 2, "label": "3D"},
            {"pattern": "IMAX|IMAXEnhanced", "score": 2, "label": "IMAX"},
            {"pattern": "Director.?Cut|导演剪辑版", "score": 2, "label": "导演剪辑版"},
            {"pattern": "Extended|加长版|Extension", "score": 2, "label": "加长版"},
            {"pattern": "Criterion|CC版", "score": 3, "label": "CC标准版"},
        ],
    }


def get_patterns(dimension: str, custom_rules: dict = None) -> list:
    """
    获取指定维度的评分模式列表。
    优先使用自定义规则，无自定义则使用默认值。

    Args:
        dimension: 维度名称 (resolution/source/audio/hdr/video/bonus)
        custom_rules: 自定义评分规则字典

    Returns:
        [(pattern_regex, score), ...]
    """
    # 尝试从自定义规则获取
    if custom_rules and dimension in custom_rules:
        custom_items = custom_rules[dimension]
        if isinstance(custom_items, list) and len(custom_items) > 0:
            patterns = []
            for item in custom_items:
                p = item.get("pattern", "")
                s = item.get("score", 0)
                if p and isinstance(s, (int, float)):
                    patterns.append((p, int(s)))
            if patterns:
                return patterns

    # 回退到默认常量
    default_map = {
        "resolution": RESOLUTION_PATTERNS,
        "source": SOURCE_PATTERNS,
        "audio": AUDIO_PATTERNS,
        "hdr": HDR_PATTERNS,
        "video": VIDEO_PATTERNS,
        "bonus": BONUS_PATTERNS,
    }
    return default_map.get(dimension, [])


def get_pattern_labels(dimension: str, custom_rules: dict = None) -> list:
    """
    获取指定维度的标签列表，用于UI展示评分规则。

    Args:
        dimension: 维度名称 (resolution/source/audio/hdr/video/bonus)
        custom_rules: 自定义评分规则字典

    Returns:
        [(label, score), ...]
    """
    if custom_rules and dimension in custom_rules:
        items = custom_rules[dimension]
        return [(item.get("label", item.get("pattern", "?")), item.get("score", 0)) for item in items]

    defaults = _get_default_labels_dict()
    if dimension in defaults:
        return [(item.get("label", "?"), item.get("score", 0)) for item in defaults[dimension]]
    return []


def parse_custom_rules(rules_json: str) -> dict:
    """
    解析自定义评分规则JSON字符串。

    Args:
        rules_json: JSON格式的规则字符串

    Returns:
        解析后的规则字典，格式错误返回空字典
    """
    if not rules_json or not rules_json.strip():
        return {}
    try:
        rules = json.loads(rules_json)
        if not isinstance(rules, dict):
            return {}
        valid = {}
        for dim in ["resolution", "source", "audio", "hdr", "bonus"]:
            if dim in rules and isinstance(rules[dim], list):
                valid[dim] = rules[dim]
        return valid
    except (json.JSONDecodeError, Exception) as e:
        logger.warning(f"自定义评分规则解析失败: {str(e)}")
        return {}


def read_tier_scores(config: dict) -> dict:
    """
    从配置中的单个tier字段构建评分规则JSON。
    小白用户通过UI界面调整各档位分值，无需手写JSON。

    Args:
        config: 插件配置字典，包含 tier_xxx 字段

    Returns:
        评分规则字典，若无任何 tier 配置则返回空字典
    """
    tier_map = {
        "resolution": [
            ("tier_res_8k", "8K|4320p", "8K", 50),
            ("tier_res_4k", "2160p|4K|UHD|4k", "4K", 40),
            ("tier_res_fhd", "1080p|FHD|FullHD|1080i", "1080p", 30),
            ("tier_res_hd", "720p|HD|1280x720", "720p", 20),
            ("tier_res_540p", "540p|qHD", "540p", 15),  # 修复: 补全 540p 档位，防止 TIER_FIELDS 与 tier_map 索引偏移
            ("tier_res_480p", "480p|SD|852x480|640x480", "480p", 10),
            ("tier_res_360p", "360p|nHD", "360p", 5),
        ],
        "source": [
            ("tier_src_remux", "Remux|REMUX", "Remux", 35),
            ("tier_src_bluray", "BluRay|BDRip|BDMV|BD[0-9]{2}|蓝光|原盘", "BluRay", 30),
            ("tier_src_webdl", "WEB.?DL|WEB.?HD|WEB-DL|WEBDL|WEBHD", "WEB-DL", 25),
            ("tier_src_webrip", "WEBRip|WEB.?Rip", "WEBRip", 20),
            ("tier_src_hdtv", "HDTV|PDTV|DVB", "HDTV", 15),
            ("tier_src_hdrip", "HDRip|HD.?CAM|HDCAM", "HDRip", 10),
            ("tier_src_dvdrip", "DVDRip|DVD[0-9]|DVD", "DVDRip", 5),
            ("tier_src_cam", "CAM|TS|HDTS|TC|HDTC|SCR|SCREENER|R5|DVDSCR", "CAM", 0),
        ],
        "audio": [
            ("tier_aud_atmos", "TrueHD.?Atmos|Atmos|DTS.?HD.?MA|DTS.?HD.?HR|DTS-HD", "无损环绕", 15),
            ("tier_aud_dts", "DTS|DTS.?ES|DTS.?X", "DTS", 12),
            ("tier_aud_truehd", "TrueHD|Dolby.?TrueHD", "TrueHD", 11),
            ("tier_aud_flac", "FLAC|LPCM|PCM", "无损", 10),
            ("tier_aud_ac3", "E.?AC3|DD\\+|DDP|AC3|DD|Dolby.?Digital", "AC3/DD+", 8),
            ("tier_aud_aac", "AAC|AACLC|HE.?AAC", "AAC", 5),
            ("tier_aud_mp3", "MP3|MP2", "MP3", 2),
        ],
        "hdr": [
            ("tier_hdr_dv", "DV|Dolby.?Vision|DoVi|DolbyVision", "Dolby Vision", 15),
            ("tier_hdr_10p", "HDR10\\+|HDR10P", "HDR10+", 15),
            ("tier_hdr_10", "HDR10|HDR|HDR10", "HDR10", 15),
            ("tier_hdr_hlg", "HLG", "HLG", 5),
            ("tier_hdr_sdr", "SDR", "SDR", 0),
        ],
        "video": [
            ("tier_vid_av1", "AV1", "AV1", 15),
            ("tier_vid_hevc", "HEVC|h265|H265|x265", "HEVC/H265", 12),
            ("tier_vid_h264", "h264|H264|x264|AVC", "H264/AVC", 8),
            ("tier_vid_other", "VC1|MPEG2|MPEG4|VP9|VP8", "其他", 5),
        ],
        "bonus": [
            ("tier_bonus_cc", "Criterion|CC版", "CC标准版", 3),
            ("tier_bonus_complete", "Complete|COMPLETE", "Complete", 2),
            ("tier_bonus_3d", "3D|ThreeD", "3D", 2),
            ("tier_bonus_imax", "IMAX|IMAXEnhanced", "IMAX", 2),
            ("tier_bonus_director", "Director.?Cut|导演剪辑版", "导演剪辑版", 2),
            ("tier_bonus_extended", "Extended|加长版|Extension", "加长版", 2),
        ],
    }

    result = {}
    has_any = False
    for dim, tiers in tier_map.items():
        items = []
        for tier_key, pattern, label, default in tiers:
            val = config.get(tier_key)
            if val is not None:
                try:
                    score = int(float(str(val)))
                    has_any = True
                except (ValueError, TypeError):
                    score = default
                # 修复 Bug 2: 不需要额外包裹()，get_patterns 中的 re.search 直接使用即可
                # 原始 pattern 中已不含 capture group，无需二次转义
                items.append({"pattern": pattern, "score": score, "label": label})
        if items:
            result[dim] = items

    return result if has_any else {}


# 配置字段名称列表（供 __init__.py __update_config 回存时使用）
# 每个元素: (config_key, dimension, default_score)
TIER_FIELDS = [
    # 分辨率
    ("tier_res_8k", "resolution", 50), ("tier_res_4k", "resolution", 40),
    ("tier_res_fhd", "resolution", 30), ("tier_res_hd", "resolution", 20),
    ("tier_res_540p", "resolution", 15), ("tier_res_480p", "resolution", 10),
    ("tier_res_360p", "resolution", 5),
    # 片源
    ("tier_src_remux", "source", 35), ("tier_src_bluray", "source", 30),
    ("tier_src_webdl", "source", 25), ("tier_src_webrip", "source", 20),
    ("tier_src_hdtv", "source", 15), ("tier_src_hdrip", "source", 10),
    ("tier_src_dvdrip", "source", 5), ("tier_src_cam", "source", 0),
    # 音频
    ("tier_aud_atmos", "audio", 15), ("tier_aud_dts", "audio", 12),
    ("tier_aud_truehd", "audio", 11), ("tier_aud_flac", "audio", 10),
    ("tier_aud_ac3", "audio", 8), ("tier_aud_aac", "audio", 5),
    ("tier_aud_mp3", "audio", 2),
    # HDR
    ("tier_hdr_dv", "hdr", 15), ("tier_hdr_10p", "hdr", 15),
    ("tier_hdr_10", "hdr", 15), ("tier_hdr_hlg", "hdr", 5),
    ("tier_hdr_sdr", "hdr", 0),
    # 视频编码
    ("tier_vid_av1", "video", 15), ("tier_vid_hevc", "video", 12),
    ("tier_vid_h264", "video", 8), ("tier_vid_other", "video", 5),
    # 加分
    ("tier_bonus_cc", "bonus", 3), ("tier_bonus_complete", "bonus", 2),
    ("tier_bonus_3d", "bonus", 2), ("tier_bonus_imax", "bonus", 2),
    ("tier_bonus_director", "bonus", 2), ("tier_bonus_extended", "bonus", 2),
]


def parse_season_episode(filename: str) -> Dict[str, Any]:
    """
    从文件名解析季/集信息。

    MoviePilot 命名规范:
        TV:   {Title}.{Year}.S{ss}E{ee}.{Quality}.{Codec}.{Audio}{ext}
        Movie: {Title}.{Year}.{Quality}.{Codec}.{Audio}{ext}

    Returns:
        {"season": "S01", "episode": "E01", "display": "S01E01",
         "season_num": 1, "episode_num": 1, "is_movie": False}
    """
    result = {
        "season": "", "episode": "", "display": "",
        "season_num": 0, "episode_num": 0, "is_movie": False
    }

    # 匹配 S01E175-E177 或 S01E175 或 S01E01 格式
    # v2.1.2: 集号支持3位数字（\d{1,3}），修复 S01E175 误读为 S01E17 的问题
    sxe_match = re.search(r'S(\d{1,2})E(\d{1,3})(?:-E?(\d{1,3}))?', filename, re.IGNORECASE)
    if sxe_match:
        s_num = int(sxe_match.group(1))
        ep_start = int(sxe_match.group(2))
        ep_end = sxe_match.group(3)
        season = f"S{s_num:02d}"
        if ep_end:
            episode = f"E{ep_start:02d}-E{int(ep_end):02d}"
            display = f"{season}{episode}"
        else:
            episode = f"E{ep_start:02d}"
            display = f"{season}{episode}"
        result.update({
            "season": season, "episode": episode, "display": display,
            "season_num": s_num, "episode_num": ep_start, "is_movie": False
        })
        return result

    # 匹配 "S01" 或 "Season 01" 格式（整季）
    # v2.1.2: 季号支持3位数字（\d{1,3}），兼容更多命名方式
    season_match = re.search(r'(?:S(\d{1,3})|Season[. _](\d{1,3}))', filename, re.IGNORECASE)
    if season_match:
        s = int(season_match.group(1) or season_match.group(2))
        season = f"S{s:02d}"
        result.update({
            "season": season, "episode": "整季", "display": season,
            "season_num": s, "episode_num": 0, "is_movie": False
        })

    return result


def _extract_match_label(match) -> str:
    """
    从正则匹配结果中提取标签文本。
    优先使用捕获组 group(1)，无捕获组时使用整个匹配 group(0)。
    修复 Bug 5: 对 group(1) 为 None/空字符串时回退到 group(0)。
    """
    try:
        label = match.group(1)
        if label and label.strip():
            return label.upper()
    except (IndexError, AttributeError):
        pass
    return match.group(0).upper()


def parse_quality(
    filename: str,
    custom_rules: dict = None,
    res_weight: float = 40.0,
    src_weight: float = 35.0,
    aud_weight: float = 15.0,
    hdr_weight: float = 15.0,
    vid_weight: float = 12.0,
    bonus_weight: float = 6.0,
) -> Dict[str, Any]:
    """
    从文件名中解析质量信息。

    Args:
        filename: 文件名（不含路径）
        custom_rules: 自定义评分规则
        res_weight: 分辨率权重
        src_weight: 片源权重
        aud_weight: 音频权重
        hdr_weight: HDR权重
        vid_weight: 视频编码权重
        bonus_weight: 加分项权重

    Returns:
        quality_info: {
            "score": int,
            "resolution": {"value": int, "label": str},
            "source": {"value": int, "label": str},
            "audio": {"value": int, "label": str},
            "hdr": {"value": int, "label": str},
            "bonus": int,
            "details": str
        }
    """
    name = str(filename).replace("_", ".")
    result = {
        "score": 0,
        "resolution": {"value": 0, "label": "未知"},
        "source": {"value": 0, "label": "未知"},
        "audio": {"value": 0, "label": "未知"},
        "hdr": {"value": 0, "label": "SDR"},
        "bonus": 0,
        "details": ""
    }

    detected_tags = []

    # 解析分辨率
    res_patterns = get_patterns("resolution", custom_rules)
    for pattern, score in res_patterns:
        match = re.search(pattern, name, re.IGNORECASE)
        if match:
            result["resolution"] = {"value": score, "label": _extract_match_label(match)}
            detected_tags.append(f"分辨率:{_extract_match_label(match)}")
            break

    # 解析片源类型
    src_patterns = get_patterns("source", custom_rules)
    for pattern, score in src_patterns:
        match = re.search(pattern, name, re.IGNORECASE)
        if match:
            result["source"] = {"value": score, "label": _extract_match_label(match)}
            detected_tags.append(f"片源:{_extract_match_label(match)}")
            break

    # 解析音频编码
    aud_patterns = get_patterns("audio", custom_rules)
    for pattern, score in aud_patterns:
        match = re.search(pattern, name, re.IGNORECASE)
        if match:
            result["audio"] = {"value": score, "label": _extract_match_label(match)}
            detected_tags.append(f"音频:{_extract_match_label(match)}")
            break

    # 解析HDR类型
    hdr_patterns = get_patterns("hdr", custom_rules)
    for pattern, score in hdr_patterns:
        match = re.search(pattern, name, re.IGNORECASE)
        if match:
            result["hdr"] = {"value": score, "label": _extract_match_label(match)}
            detected_tags.append(f"HDR:{_extract_match_label(match)}")
            break

    # 解析视频编码
    vid_patterns = get_patterns("video", custom_rules)
    for pattern, score in vid_patterns:
        match = re.search(pattern, name, re.IGNORECASE)
        if match:
            result["video"] = {"value": score, "label": _extract_match_label(match)}
            detected_tags.append(f"编码:{_extract_match_label(match)}")
            break

    # 计算加分
    bonus_patterns = get_patterns("bonus", custom_rules)
    bonus_score = 0
    for pattern, score in bonus_patterns:
        match = re.search(pattern, name, re.IGNORECASE)
        if match:
            bonus_score += score
            detected_tags.append(f"加分:{_extract_match_label(match)}")

    result["bonus"] = bonus_score

    # 使用自定义权重计算最终得分
    res_mult = res_weight / 40.0 if res_weight > 0 else 0
    src_mult = src_weight / 35.0 if src_weight > 0 else 0
    aud_mult = aud_weight / 15.0 if aud_weight > 0 else 0
    hdr_mult = hdr_weight / 15.0 if hdr_weight > 0 else 0
    vid_mult = vid_weight / 15.0 if vid_weight > 0 else 0
    bonus_mult = bonus_weight / 6.0 if bonus_weight > 0 else 0

    result["score"] = (
        round(result["resolution"]["value"] * res_mult)
        + round(result["source"]["value"] * src_mult)
        + round(result["audio"]["value"] * aud_mult)
        + round(result["hdr"]["value"] * hdr_mult)
        + round(result.get("video", {}).get("value", 0) * vid_mult)
        + round(bonus_score * bonus_mult)
    )

    # 记录加权后的明细（方便UI展示）
    result["weighted_details"] = {
        "resolution": round(result["resolution"]["value"] * res_mult),
        "source": round(result["source"]["value"] * src_mult),
        "audio": round(result["audio"]["value"] * aud_mult),
        "hdr": round(result["hdr"]["value"] * hdr_mult),
        "video": round(result.get("video", {}).get("value", 0) * vid_mult),
        "bonus": round(bonus_score * bonus_mult),
        "weights": {
            "res": res_weight,
            "src": src_weight,
            "aud": aud_weight,
            "hdr": hdr_weight,
            "vid": vid_weight,
            "bonus": bonus_weight
        }
    }
    result["details"] = " | ".join(detected_tags) if detected_tags else "未识别到质量信息"

    return result
