"""
影视洗板插件 — 文件清理引擎
================================
低质量文件删除、关联元数据清理、空目录清理。
v2.1.0 增强: 支持更全面的元数据清理（包含 .torrent/.retry/字幕/海报等）。
"""

import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.log import logger


# 关联元数据文件扩展名 — v2.1.0 增强: 增加 .torrent/.retry 等
METADATA_EXTENSIONS = {
    # 元数据
    '.nfo', '.txt', '.json', '.xml',
    # 图片/海报
    '.jpg', '.jpeg', '.png', '.tbn', '.gif', '.webp', '.bmp',
    # 字幕
    '.srt', '.sub', '.ass', '.ssa', '.idx', '.sup', '.vtt', '.smi', '.psb', '.scc',
    # 下载相关
    '.torrent', '.retry',
    # 其他
    '.url', '.website', '.webloc',
}


# 媒体文件扩展名 — 用于判断父目录是否有其他媒体文件
_MEDIA_EXTENSIONS = {
    '.strm', '.mkv', '.mp4', '.avi', '.wmv', '.flv', '.mov', '.ts', '.m2ts',
    '.rmvb', '.rm', '.iso',
}


def _should_delete_shared_artifacts(parent: Path, filepath: str) -> bool:
    """
    判断是否应该删除父目录下的共享海报图。
    只有当父目录下不存在任何其他媒体文件时才安全。

    场景示例:
      parent/
        movie_low.strm    ← 要删除的目标
        movie_high.strm   ← 要保留的邻居
        poster.jpg        ← 不应被删除

    Args:
        parent: 目标文件的父目录
        filepath: 正在处理的文件完整路径

    Returns:
        True = 安全删除共享海报, False = 有其他媒体文件，跳过
    """
    try:
        for f in parent.iterdir():
            if f.is_file() and f.suffix.lower() in _MEDIA_EXTENSIONS and str(f) != filepath:
                return False  # 还有其他媒体文件，不安全
    except OSError:
        return False
    return True


def delete_metadata_files(filepath: str):
    """
    删除与媒体文件关联的元数据文件。

    v2.1.0 增强: 删除范围覆盖 .torrent/.retry/字幕/海报/图片等。
    v2.2.1 修复: 共享海报图仅在父目录无其他媒体文件时才删除。
    v2.5.0: 日志级别从 debug 提升为 info，确保生产环境可见。

    Args:
        filepath: 要删除的媒体文件路径
    """
    path = Path(filepath)
    parent = path.parent
    stem = path.stem
    ext = path.suffix.lower()

    deleted_any = False

    # 1) 删除同目录下同文件名的元数据文件 (Movie.mkv → Movie.srt, Movie.nfo, ...)
    for meta_ext in METADATA_EXTENSIONS:
        meta_file = parent / f"{stem}{meta_ext}"
        try:
            os.remove(str(meta_file))
            logger.info(f"影视洗板: 已删除关联元数据: {meta_file.name}")
            deleted_any = True
        except FileNotFoundError:
            pass  # 文件不存在是预期行为
        except Exception as e:
            logger.info(f"影视洗板: 删除元数据失败 {meta_file.name}: {e}")

    # 2) 删除 .strm 对应的 .torrent 文件（命名不同: Movie.torrent）
    #    以及同目录下的 .retry 文件
    for related_ext in ['.torrent', '.retry']:
        related_file = parent / f"{stem}{related_ext}"
        try:
            os.remove(str(related_file))
            logger.info(f"影视洗板: 已删除关联文件: {related_file.name}")
            deleted_any = True
        except FileNotFoundError:
            pass
        except Exception as e:
            logger.info(f"影视洗板: 删除关联文件失败 {related_file.name}: {e}")

    # 3) 删除常见的海报/背景图 — 仅在父目录无其他媒体文件时执行（防止误删）
    if not _should_delete_shared_artifacts(parent, filepath):
        logger.info(f"影视洗板: 父目录仍有其他媒体文件，跳过共享海报清理: {parent}")
        return

    common_images = [
        'poster.jpg', 'poster.png', 'poster.webp',
        'fanart.jpg', 'fanart.png', 'fanart.webp',
        'thumb.jpg', 'thumb.png', 'thumb.webp',
        'landscape.jpg', 'landscape.png',
        'logo.png', 'logo.jpg', 'clearart.png', 'clearlogo.png',
    ]
    for img_name in common_images:
        img_file = parent / img_name
        try:
            os.remove(str(img_file))
            logger.info(f"影视洗板: 已删除海报/背景图: {img_file.name}")
            deleted_any = True
        except FileNotFoundError:
            pass
        except Exception as e:
            logger.info(f"影视洗板: 删除海报失败 {img_file.name}: {e}")

    # 4) 删除同目录下的 season-specifc 图片 (Season01-poster.jpg, ...)
    try:
        for f in parent.iterdir():
            fname = f.name.lower()
            if f.is_file() and any(fname.endswith(ext) for ext in ['.jpg', '.png', '.webp']):
                # 匹配 seasonXX-poster, seasonXX-fanart 等模式
                if '-poster' in fname or '-fanart' in fname or '-thumb' in fname or '-landscape' in fname:
                    try:
                        os.remove(str(f))
                        logger.info(f"影视洗板: 已删除季海报: {f.name}")
                        deleted_any = True
                    except FileNotFoundError:
                        pass
                    except Exception as e:
                        logger.info(f"影视洗板: 删除季海报失败 {f.name}: {e}")
    except Exception as e:
        logger.info(f"影视洗板: 删除季海报目录遍历失败: {e}")

    # 5) 安全删除空目录（修复 Bug 3: 安全检查避免删除非空目录）
    try:
        if parent.exists() and parent.is_dir():
            all_entries = list(parent.iterdir())
            if not all_entries:
                parent.rmdir()
                logger.info(f"影视洗板: 删除空目录: {parent}")
            elif deleted_any:
                logger.info(f"影视洗板: 目录非空，保留: {parent} ({len(all_entries)} 个文件)")
    except (OSError, PermissionError):
        pass


def execute_cleanup(
    items: Dict[str, Dict],
) -> int:
    """
    执行文件清理（删除标记为"delete"的低质量文件）。

    v2.5.0: 移除 Dry-Run 模式，直接执行真实删除。

    Args:
        items: 扫描结果中的 items 字典

    Returns:
        删除的文件数量
    """
    if not items:
        logger.warning("影视洗板: 没有扫描结果，无法执行清理")
        return 0

    deleted_count = 0
    failed_count = 0

    for group_key, group_data in items.items():
        for version in group_data.get("versions", []):
            if version.get("action") != "delete" or version.get("deleted"):
                continue

            filepath = version.get("filepath")
            if not filepath:
                continue

            file_path_obj = Path(filepath)

            if not file_path_obj.exists():
                version["deleted"] = True
                version["delete_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                logger.info(f"影视洗板: 文件已不存在，标记为已删除: {filepath}")
                deleted_count += 1
                continue

            try:
                os.remove(filepath)
                logger.info(f"影视洗板: 文件已永久删除: {filepath}")

                # 删除关联的元数据文件
                delete_metadata_files(filepath)

                version["deleted"] = True
                version["delete_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                deleted_count += 1

                # 安全删除空目录（修复 Bug 3: 检查子目录）
                parent_dir = file_path_obj.parent
                try:
                    if parent_dir.exists() and parent_dir.is_dir():
                        all_entries = list(parent_dir.iterdir())
                        if not all_entries:
                            parent_dir.rmdir()
                            logger.info(f"影视洗板: 删除空目录: {parent_dir}")
                except (OSError, PermissionError):
                    pass

            except Exception as e:
                logger.error(f"影视洗板: 删除文件失败 {filepath}: {str(e)}")
                failed_count += 1

    logger.info(
        f"影视洗板清理完成: 成功删除 {deleted_count} 个文件"
        + (f"，失败 {failed_count} 个" if failed_count else "")
    )

    return deleted_count
