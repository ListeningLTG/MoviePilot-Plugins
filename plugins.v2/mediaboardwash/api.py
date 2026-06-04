"""
影视洗版插件 — API与事件处理模块
================================
自定义API端点、远程命令事件处理、通知发送、结果数据管理。
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from app.log import logger
from app.schemas.types import NotificationType


def send_scan_notification(
    post_message_func,
    results: Dict[str, Any],
    total_scanned: int,
    was_auto: bool = False,
    deleted_count: int = 0,
    scanned_directories: Optional[List[str]] = None,
    pending_delete_count: int = 0,
):
    """
    发送扫描完成通知。

    v2.6.0: 新增扫描目录列表、待删除文件数量。

    Args:
        post_message_func: post_message 方法引用
        results: 扫描结果字典
        total_scanned: 扫描的文件总数
        was_auto: 是否为自动清理模式
        deleted_count: 自动删除的文件数
        scanned_directories: 扫描的目录名列表
        pending_delete_count: 待删除的文件总数（非自动清理模式）
    """
    duplicates = results.get("groups_with_duplicates", 0)
    savings = results.get("potential_savings_gb", 0)

    # 构建目录行
    if scanned_directories:
        dirs_text = "、".join(scanned_directories)
        dir_line = f"扫描目录: {dirs_text}\n"
    else:
        dir_line = ""

    if duplicates > 0:
        if was_auto:
            text = (
                f"{dir_line}"
                f"扫描文件: {total_scanned} 个(.strm/视频)\n"
                f"影视总数: {results['total_groups']} 部\n"
                f"重复版本: {duplicates} 组\n"
                f"已删除: {deleted_count} 个低质量文件\n"
                f"可节省空间: 约 {savings} GB"
            )
        else:
            delete_info = f"（共 {pending_delete_count} 个低质量文件待清理）" if pending_delete_count > 0 else ""
            text = (
                f"{dir_line}"
                f"扫描文件: {total_scanned} 个(.strm/视频)\n"
                f"影视总数: {results['total_groups']} 部\n"
                f"重复版本: {duplicates} 组{delete_info}\n"
                f"可节省空间: 约 {savings} GB\n"
                f"\n请进入插件详情 → 点击「确认删除」执行清理"
            )
    else:
        text = (
            f"{dir_line}"
            f"扫描文件: {total_scanned} 个(.strm/视频)\n"
            f"影视总数: {results['total_groups']} 部\n"
            f"未发现重复版本"
        )

    post_message_func(
        title="影视洗版扫描完成",
        text=text
    )


def build_status_text(results: Optional[Dict]) -> str:
    """
    构建洗版状态文本（用于 /media_wash_status 命令）。
    v2.6.0: 新增扫描目录和待删数量。
    """
    if results:
        # 目录行
        scanned_dirs = results.get("scanned_directories", [])
        if scanned_dirs:
            dir_line = f"📂 扫描目录: {'、'.join(scanned_dirs)}\n"
        else:
            dir_line = ""

        # 待删数量行
        pending = results.get("pending_delete_count", 0)
        if pending > 0:
            delete_line = f"🗑️ 待删文件: {pending} 个\n"
        else:
            delete_line = ""

        # 上次清理行
        last_cleanup = results.get("last_cleanup", "")
        cleanup_line = ""
        if last_cleanup:
            cleanup_line = f"🧹 上次清理: {last_cleanup}（{results.get('last_cleanup_count', 0)} 个文件）\n"

        return (
            f"📊 影视洗版状态\n"
            f"━━━━━━━━━━━━━━\n"
            f"{dir_line}"
            f"📁 扫描媒体: {results.get('total_groups', 0)} 部\n"
            f"🎬 视频文件: {results.get('total_items', 0)} 个\n"
            f"🔁 重复版本: {results.get('groups_with_duplicates', 0)} 组\n"
            f"{delete_line}"
            f"💾 可节省: {results.get('potential_savings_gb', 0)} GB\n"
            f"{cleanup_line}"
            f"🕐 上次扫描: {results.get('last_scan', '未知')}"
        )
    return "暂无扫描结果，请先执行 /media_wash 扫描"
