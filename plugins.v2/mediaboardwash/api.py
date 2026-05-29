"""
影视洗板插件 — API与事件处理模块
================================
自定义API端点、远程命令事件处理、通知发送、结果数据管理。
"""

from datetime import datetime
from typing import Any, Dict, Optional

from app.log import logger
from app.schemas.types import NotificationType


def send_scan_notification(
    post_message_func,
    results: Dict[str, Any],
    total_scanned: int,
    was_auto: bool = False,
    deleted_count: int = 0,
    dry_run: bool = True,  # P0: Dry-Run 模式标记
):
    """
    发送扫描完成通知。

    Args:
        post_message_func: post_message 方法引用
        results: 扫描结果字典
        total_scanned: 扫描的文件总数
        was_auto: 是否为自动清理模式
        deleted_count: 自动删除的文件数
        dry_run: 是否为 Dry-Run 模式
    """
    duplicates = results.get("groups_with_duplicates", 0)
    savings = results.get("potential_savings_gb", 0)

    dry_run_tag = "🧪 [模拟] " if dry_run else ""

    if duplicates > 0:
        if was_auto:
            text = (
                f"{dry_run_tag}共扫描 {total_scanned} 个文件(.strm/视频)\n"
                f"覆盖 {results['total_groups']} 部影视\n"
                f"发现 {duplicates} 组重复版本\n"
                + (f"已自动删除 {deleted_count} 个低质量版本\n" if not dry_run else "模拟运行，未删除文件\n")
                + f"可节省约 {savings} GB 空间"
                + ("\n\n💡 关闭 Dry-Run 后可执行真实清理" if dry_run else "")
            )
        else:
            text = (
                f"{dry_run_tag}共扫描 {total_scanned} 个文件(.strm/视频)\n"
                f"覆盖 {results['total_groups']} 部影视\n"
                f"发现 {duplicates} 组重复版本\n"
                f"可节省约 {savings} GB 空间\n"
                f"请进入插件详情 → 点击「确认删除」执行清理"
            )
    else:
        text = (
            f"共扫描 {total_scanned} 个文件(.strm/视频)\n"
            f"覆盖 {results['total_groups']} 部影视\n"
            f"未发现重复版本，所有影视均为单一版本"
        )

    post_message_func(
        title="影视洗板扫描完成",
        text=text
    )


def build_status_text(results: Optional[Dict]) -> str:
    """构建洗板状态文本（用于 /media_wash_status 命令）。"""
    if results:
        return (
            f"📊 影视洗板状态\n"
            f"━━━━━━━━━━━━━━\n"
            f"📁 扫描媒体: {results.get('total_groups', 0)} 部\n"
            f"🎬 视频文件: {results.get('total_items', 0)} 个\n"
            f"🔁 重复版本: {results.get('groups_with_duplicates', 0)} 组\n"
            f"💾 可节省: {results.get('potential_savings_gb', 0)} GB\n"
            f"🕐 上次扫描: {results.get('last_scan', '未知')}"
        )
    return "暂无扫描结果，请先执行 /media_wash 扫描"
