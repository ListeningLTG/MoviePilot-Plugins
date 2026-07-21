import re
from pathlib import Path
from urllib.parse import quote
from time import sleep, time
from threading import Thread, Lock, Event
from typing import Dict, Any, List, Optional, Iterator, Callable, Tuple, Set
from queue import Queue, Empty
from uuid import uuid4
import json
import shutil

from p115client import P115Client, check_response
from p115client.util import complete_url
from p115client.tool.attr import normalize_attr
from p115client.tool.iterdir import iterdir

from app.log import logger
from app.chain.transfer import TransferChain
from app.schemas import FileItem

from .config import configer
from .utils import StrmUrlTemplateResolver
from .limiter import (
    ApiEndpointCooldown,
    ShareRateLimitedError,
    ShareReceiveLimitedError,
    api_metrics,
    call_protected_api,
    get_speed_cooldowns,
    global_api_guard,
    handle_waf_405,
    is_receive_limited,
    is_waf_405,
    reset_waf_backoff,
)



class ShareLinkExpiredError(Exception):
    """分享链接已失效或被拒绝"""
    pass


_SHARE_SNAP_PROGRESS_INTERVAL = 10


from app.db import db_query


@db_query
def _fuzzy_query_transfer_history(db, suffix: str) -> Optional[Any]:
    from app.db.models.transferhistory import TransferHistory
    return db.query(TransferHistory).filter(
        TransferHistory.src.like(f"%{suffix}"),
        TransferHistory.src_storage == "local"
    ).first()


@db_query
def _batch_query_transfer_histories(db, srcs: List[str]) -> List[Any]:
    from app.db.models.transferhistory import TransferHistory
    if not srcs:
        return []
    return db.query(TransferHistory).filter(
        TransferHistory.src.in_(srcs),
        TransferHistory.src_storage == "local",
    ).all()


def _batch_load_transfer_histories_by_srcs(srcs: List[str]) -> Dict[str, Any]:
    """按 src 批量加载 TransferHistory，返回 src -> history。"""
    result: Dict[str, Any] = {}
    unique = [s for s in dict.fromkeys(srcs) if s]
    chunk_size = 200
    for i in range(0, len(unique), chunk_size):
        chunk = unique[i:i + chunk_size]
        try:
            rows = _batch_query_transfer_histories(None, chunk)
        except Exception as e:
            logger.warning(f"【P115ShareStrm】批量查询 TransferHistory 失败: {e}")
            continue
        for history in rows or []:
            if history and getattr(history, "src", None) and getattr(history, "dest", None):
                result[history.src] = history
    return result


_PATH_PREFIX_MAPPING_CACHE: Dict[str, str] = {}


def _update_prefix_mapping_cache(local_path: str, db_path: str):
    """
    通过本地生成的 STRM 路径和数据库中记录的转移历史路径，推导并缓存路径前缀映射关系。
    仅当共同后缀至少 2 个路径段且长度 >= 16 时写入，避免短路径污染缓存。
    """
    local_parts = local_path.split('/')
    db_parts = db_path.split('/')

    common_parts = []
    i = 1
    while i < len(local_parts) - 1 and i <= len(db_parts):
        if local_parts[-i] == db_parts[-i]:
            common_parts.append(local_parts[-i])
            i += 1
        else:
            break

    if len(common_parts) < 2:
        return

    common_parts.reverse()
    common_suffix = '/' + '/'.join(common_parts)
    if len(common_suffix) < 16:
        return

    if local_path.endswith(common_suffix) and db_path.endswith(common_suffix):
        local_prefix = local_path[:-len(common_suffix)]
        db_prefix = db_path[:-len(common_suffix)]
        if local_prefix and local_prefix not in _PATH_PREFIX_MAPPING_CACHE:
            _PATH_PREFIX_MAPPING_CACHE[local_prefix] = db_prefix
            logger.info(f"【P115ShareStrm】检测到路径映射关系并存入缓存: {local_prefix} -> {db_prefix}")


def _map_local_path_to_db(local_path: str) -> str:
    """
    根据已缓存的路径前缀映射，将本地路径转换为数据库匹配路径。
    """
    for local_prefix in sorted(_PATH_PREFIX_MAPPING_CACHE.keys(), key=len, reverse=True):
        if local_path.startswith(local_prefix):
            db_prefix = _PATH_PREFIX_MAPPING_CACHE[local_prefix]
            return db_prefix + local_path[len(local_prefix):]
    return local_path


_AMBIGUOUS_STRM_STEM_RE = re.compile(r"^(?:\d+|EP\d+|E\d+)$", re.IGNORECASE)
_GENERIC_PARENT_DIR_RE = re.compile(
    r"^(?:S\d+|Season\s*\d+|中字版|字幕|Subs?|字幕组)$",
    re.IGNORECASE,
)


def _is_ambiguous_strm_filename(stem: str) -> bool:
    """短/集数型文件名在 LIKE 查询中易误匹配，跳过仅文件名级模糊匹配。"""
    if not stem or len(stem) <= 3:
        return True
    return bool(_AMBIGUOUS_STRM_STEM_RE.match(stem))


def _is_generic_parent_dir(name: str) -> bool:
    """过短或通用的父目录名，跳过两级路径模糊匹配。"""
    if not name or len(name) <= 3:
        return True
    if name.isdigit():
        return True
    return bool(_GENERIC_PARENT_DIR_RE.match(name.strip()))


def _get_transfer_history_by_strm_path(th_oper, strm_path: Path) -> Optional[Any]:
    """
    根据 STRM 路径从 TransferHistory 中检索整理记录。
    支持精确匹配和模糊匹配（兼容被 115 屏蔽/重命名导致父目录名称不一致的情况，如 "豆瓣..." -> "豆***..."）。
    """
    src_posix = strm_path.as_posix()

    mapped_src = _map_local_path_to_db(src_posix)
    if mapped_src != src_posix:
        history = th_oper.get_by_src(mapped_src, "local") if th_oper else None
        if history and history.dest:
            return history

    if th_oper:
        history = th_oper.get_by_src(src_posix, "local")
        if history and history.dest:
            return history

    parts = strm_path.parts
    if len(parts) >= 2 and not _is_generic_parent_dir(parts[-2]):
        suffix = f"/{parts[-2]}/{parts[-1]}"
        history = _fuzzy_query_transfer_history(None, suffix)
        if history and history.dest:
            logger.warning(f"【P115ShareStrm】通过两级路径后缀模糊匹配成功: {strm_path.name} -> {history.src}")
            _update_prefix_mapping_cache(src_posix, history.src)
            return history

    if len(parts) >= 1 and not _is_ambiguous_strm_filename(strm_path.stem):
        suffix = f"/{parts[-1]}"
        history = _fuzzy_query_transfer_history(None, suffix)
        if history and history.dest:
            logger.warning(f"【P115ShareStrm】通过文件名后缀模糊匹配成功: {strm_path.name} -> {history.src}")
            _update_prefix_mapping_cache(src_posix, history.src)
            return history

    return None


def _interruptible_sleep(seconds: float, cancel_event: Event) -> bool:
    """分段 sleep，cancel 时返回 True。"""
    end = time() + max(0.0, seconds)
    while time() < end:
        if cancel_event.is_set():
            return True
        sleep(min(1.0, end - time()))
    return cancel_event.is_set()


def _refresh_media_stem_targets(
    media_stem_to_strm_path: Dict[str, Path],
    media_stem_to_target: Dict[str, Path],
) -> Tuple[int, int]:
    """
    批量补全 stem → 整理目标路径。返回 (已映射数, 总数)。
    """
    pending_paths: List[Path] = []
    stem_by_src: Dict[str, str] = {}
    for stem, strm_path in media_stem_to_strm_path.items():
        if stem in media_stem_to_target:
            continue
        src = strm_path.as_posix()
        stem_by_src[src] = stem
        pending_paths.append(strm_path)

    if pending_paths:
        query_srcs: List[str] = []
        for p in pending_paths:
            src = p.as_posix()
            query_srcs.append(src)
            mapped = _map_local_path_to_db(src)
            if mapped != src:
                query_srcs.append(mapped)
        histories = _batch_load_transfer_histories_by_srcs(query_srcs)
        still_miss: List[Path] = []
        for p in pending_paths:
            src = p.as_posix()
            mapped = _map_local_path_to_db(src)
            history = histories.get(mapped) or histories.get(src)
            if history and history.dest:
                media_stem_to_target[stem_by_src[src]] = Path(history.dest)
            else:
                still_miss.append(p)

        for p in still_miss:
            history = _get_transfer_history_by_strm_path(None, p)
            if history and history.dest:
                media_stem_to_target[stem_by_src[p.as_posix()]] = Path(history.dest)

    return len(media_stem_to_target), len(media_stem_to_strm_path)


def _build_dir_to_media_stems(media_files: List[dict]) -> Dict[str, List[str]]:
    dir_to_media_stems: Dict[str, List[str]] = {}
    for m_item in media_files:
        m_path = m_item.get("_full_path", m_item.get("name", ""))
        m_parent = str(Path(m_path).parent)
        m_stem = Path(m_item.get("name", "")).stem
        dir_to_media_stems.setdefault(m_parent, []).append(m_stem)
    return dir_to_media_stems


def _match_stem_for_subtitle(sub_name: str, candidate_stems: List[str]) -> Optional[str]:
    if not candidate_stems:
        return None
    if len(candidate_stems) == 1:
        return candidate_stems[0]
    ep_match = re.search(r"[Ss](\d+)[Ee](\d+)", sub_name)
    if ep_match:
        for cstem in candidate_stems:
            m_ep = re.search(r"[Ss](\d+)[Ee](\d+)", cstem)
            if m_ep and m_ep.group(1) == ep_match.group(1) and m_ep.group(2) == ep_match.group(2):
                return cstem
    best = max(candidate_stems, key=lambda s: len(s) if s in sub_name else 0)
    if best in sub_name:
        return best
    return candidate_stems[0]


def _place_subtitles_to_targets(
    subtitle_files: List[dict],
    downloaded_subtitle_paths: List[Path],
    media_files: List[dict],
    media_stem_to_strm_path: Dict[str, Path],
    media_stem_to_target: Dict[str, Path],
) -> Tuple[int, int]:
    """将已下载字幕放置到整理目标目录。返回 (成功数, miss数)。"""
    place_ok = 0
    place_miss = 0
    dir_to_media_stems = _build_dir_to_media_stems(media_files)

    for sub_item, local_path in zip(subtitle_files, downloaded_subtitle_paths):
        sub_full_path = sub_item.get("_full_path", sub_item.get("name", ""))
        sub_name = Path(sub_full_path).name
        sub_parent = str(Path(sub_full_path).parent)
        candidate_stems = dir_to_media_stems.get(sub_parent, [])
        matched_stem = _match_stem_for_subtitle(sub_name, candidate_stems)
        strm_target = media_stem_to_target.get(matched_stem) if matched_stem else None
        if not strm_target and matched_stem:
            strm_path = media_stem_to_strm_path.get(matched_stem)
            if strm_path:
                history = _get_transfer_history_by_strm_path(None, strm_path)
                if history and history.dest:
                    strm_target = Path(history.dest)
                    media_stem_to_target[matched_stem] = strm_target

        if not strm_target:
            place_miss += 1
            logger.warning(f"【P115ShareStrm】未找到对应 STRM 目标路径，跳过字幕: {sub_name}")
            continue

        lang_tag = _extract_subtitle_lang_tag(sub_name)
        new_name = _truncate_filename(strm_target.stem + lang_tag + local_path.suffix)
        dest_path = strm_target.parent / new_name
        try:
            if dest_path.exists() and dest_path.read_bytes() == local_path.read_bytes():
                logger.debug(f"【P115ShareStrm】字幕已存在且内容相同，跳过: {new_name}")
                place_ok += 1
                continue
        except Exception:
            pass
        idx = 1
        while dest_path.exists():
            new_name = _truncate_filename(strm_target.stem + lang_tag + f".{idx}" + local_path.suffix)
            dest_path = strm_target.parent / new_name
            idx += 1
        try:
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(str(local_path), str(dest_path))
            logger.info(f"【P115ShareStrm】字幕放置到目标: {new_name}")
            place_ok += 1
        except Exception as e:
            place_miss += 1
            logger.warning(f"【P115ShareStrm】字幕放置失败 {sub_name}: {e}")

    return place_ok, place_miss


def _serialize_stem_paths(media_stem_to_strm_path: Dict[str, Path]) -> Dict[str, str]:
    return {stem: path.as_posix() for stem, path in media_stem_to_strm_path.items()}


def _deserialize_stem_paths(data: Dict[str, str]) -> Dict[str, Path]:
    return {stem: Path(path) for stem, path in (data or {}).items()}


def _schedule_subtitle_receive_retry(
    job_id: str,
    share_code: str,
    receive_code: str,
    subtitle_files: List[dict],
    save_path_obj: Path,
    user_id: Optional[str],
    media_files: List[dict],
    media_stem_to_strm_path: Dict[str, Path],
    media_stem_to_target: Dict[str, Path],
    retry_hours: int,
) -> None:
    """4200041 限制接收后延迟重试字幕下载。"""

    def _retry_worker():
        wait_sec = max(3600, retry_hours * 3600)
        logger.info(
            f"【P115ShareStrm】字幕转存限制接收，{retry_hours}h 后重试: {share_code} (job={job_id})"
        )
        if _interruptible_sleep(wait_sec, task_queue._cancel_event):
            return
        if task_queue._cancel_event.is_set():
            return
        task_queue.subtitle_job_update_stage(job_id, "placing")
        client = ShareP115Client(configer.cookies)
        retry_btn = [[{
            "text": "🔁 重试下载字幕",
            "callback_data": f"[PLUGIN]p115sharestrm|retry_sub:{share_code}:{receive_code}",
        }]]
        try:
            downloaded, fail_count = _download_subtitles_from_share(
                client, share_code, receive_code, subtitle_files, save_path_obj
            )
        except ShareReceiveLimitedError as e:
            task_queue.subtitle_job_update_stage(job_id, "receive_limited")
            task_queue._notify(
                user_id,
                "【115分享STRM】字幕下载失败",
                f"❌ {share_code}\n限制接收仍未解除: {e}",
                buttons=retry_btn,
            )
            return
        except Exception as e:
            task_queue.subtitle_job_update_stage(job_id, "failed")
            task_queue._notify(
                user_id,
                "【115分享STRM】字幕下载失败",
                f"❌ {share_code}\n重试失败: {e}",
                buttons=retry_btn,
            )
            task_queue.subtitle_job_remove(job_id)
            return

        place_ok, place_miss = 0, 0
        if downloaded and configer.moviepilot_transfer:
            place_ok, place_miss = _place_subtitles_to_targets(
                subtitle_files, downloaded, media_files,
                media_stem_to_strm_path, media_stem_to_target,
            )
        elif downloaded:
            for stem, sp in media_stem_to_strm_path.items():
                media_stem_to_target.setdefault(stem, sp)
            place_ok, place_miss = _place_subtitles_to_targets(
                subtitle_files, downloaded, media_files,
                media_stem_to_strm_path, media_stem_to_target,
            )
        task_queue.metrics_add_place(place_ok, place_miss)
        msg = f"✅ 字幕限制接收重试完成: {share_code}\n下载成功: {len(downloaded)} 个"
        if fail_count:
            msg += f"，失败 {fail_count} 个"
        task_queue._notify(user_id, "【115分享STRM】字幕自动下载成功", msg, buttons=retry_btn if fail_count else None)
        task_queue.subtitle_job_update_stage(job_id, "done")
        task_queue.subtitle_job_remove(job_id)

    Thread(target=_retry_worker, daemon=True, name=f"P115SubReceiveRetry-{job_id[:8]}").start()


def _start_subtitle_finalize_job(
    share_code: str,
    receive_code: str,
    subtitle_files: List[dict],
    save_path_obj: Path,
    user_id: Optional[str],
    media_files: List[dict],
    media_stem_to_strm_path: Dict[str, Path],
    media_stem_to_target: Optional[Dict[str, Path]] = None,
    job_id: Optional[str] = None,
    retry_count: int = 0,
) -> str:
    """持久化并启动字幕后台收尾线程，返回 job_id。"""
    jid = job_id or str(uuid4())
    task_queue.subtitle_job_upsert({
        "job_id": jid,
        "share_code": share_code,
        "receive_code": receive_code,
        "user_id": user_id,
        "save_path": str(save_path_obj),
        "subtitle_files": subtitle_files,
        "media_files": media_files,
        "media_stem_to_strm_path": _serialize_stem_paths(media_stem_to_strm_path),
        "media_stem_to_target": {
            k: v.as_posix() for k, v in (media_stem_to_target or {}).items()
        },
        "created_at": time(),
        "stage": "waiting_transfer",
        "retry_count": retry_count,
    })
    t = Thread(
        target=_finalize_subtitles_async,
        args=(
            jid,
            share_code,
            receive_code,
            subtitle_files,
            save_path_obj,
            user_id,
            media_files,
            media_stem_to_strm_path,
            dict(media_stem_to_target or {}),
        ),
        daemon=True,
    )
    t.start()
    task_queue.metrics_subtitle_bg_started()
    return jid


def _finalize_subtitles_async(
    job_id: str,
    share_code: str,
    receive_code: str,
    subtitle_files: List[dict],
    save_path_obj: Path,
    user_id: Optional[str],
    media_files: List[dict],
    media_stem_to_strm_path: Dict[str, Path],
    media_stem_to_target: Dict[str, Path],
):
    """
    后台收尾：轮询整理映射 +（如需）等审核通过，再下载并放置字幕，不堵塞主队列。
    """
    task_queue.metrics_subtitle_bg_inc()
    started = time()
    media_stem_to_target = dict(media_stem_to_target or {})
    retry_btn = [[{
        "text": "🔁 重试下载字幕",
        "callback_data": f"[PLUGIN]p115sharestrm|retry_sub:{share_code}:{receive_code}",
    }]]

    try:
        try:
            finalize_hours = int(configer.subtitle_finalize_timeout_hours or 6)
        except (TypeError, ValueError):
            finalize_hours = 6
        try:
            audit_hours = int(configer.subtitle_audit_poll_timeout_hours or 6)
        except (TypeError, ValueError):
            audit_hours = 6
        finalize_hours = max(1, finalize_hours)
        audit_hours = max(1, audit_hours)
        deadline = time() + finalize_hours * 3600
        audit_deadline = time() + audit_hours * 3600

        logger.info(
            f"【P115ShareStrm】已启动字幕后台收尾: {share_code} "
            f"(job={job_id}, 整理超时 {finalize_hours}h, 审核超时 {audit_hours}h)"
        )
        task_queue.subtitle_job_update_stage(job_id, "waiting_transfer")

        client = ShareP115Client(configer.cookies)
        try:
            audit_min = max(10, int(configer.audit_poll_min_sec or 60))
        except (TypeError, ValueError):
            audit_min = 60
        try:
            audit_max = max(audit_min, int(configer.audit_poll_max_sec or 300))
        except (TypeError, ValueError):
            audit_max = 300
        interval = audit_min
        audit_ok = False
        attempt = 0

        while time() < deadline and not task_queue._cancel_event.is_set():
            attempt += 1
            mapped, total = _refresh_media_stem_targets(media_stem_to_strm_path, media_stem_to_target)
            if attempt == 1 or attempt % 6 == 0:
                logger.info(
                    f"【P115ShareStrm】字幕收尾映射进度 {mapped}/{total}: {share_code}"
                )

            state = _get_share_state(client, share_code, receive_code)
            if state == 7:
                logger.warning(f"【P115ShareStrm】字幕后台收尾发现链接已过期: {share_code}")
                task_queue.subtitle_job_update_stage(job_id, "failed")
                task_queue._notify(
                    user_id,
                    "【115分享STRM】字幕下载失败",
                    f"❌ {share_code}\n原因: 链接已失效或在审核期间过期",
                    buttons=retry_btn,
                )
                task_queue.subtitle_job_remove(job_id)
                return

            if state is not None and state != 0:
                audit_ok = True
                task_queue.subtitle_job_update_stage(job_id, "placing")
                # 审核已通过：映射齐全则立即下载；否则至少等一会补映射
                if total == 0 or mapped >= total or (mapped > 0 and time() >= started + 30):
                    break
                if time() >= started + 120:
                    # 已审核通过超过 2 分钟仍未齐全，先下载能放的
                    break
            else:
                task_queue.subtitle_job_update_stage(job_id, "waiting_audit")
                if time() >= audit_deadline:
                    break

            if _interruptible_sleep(interval, task_queue._cancel_event):
                logger.info(f"【P115ShareStrm】字幕后台收尾已取消: {share_code}")
                return
            interval = min(audit_max, interval + max(5, audit_min // 6))

        if task_queue._cancel_event.is_set():
            return

        if not audit_ok:
            # 最后再查一次状态
            state = _get_share_state(client, share_code, receive_code)
            if state is not None and state != 0 and state != 7:
                audit_ok = True
            elif state == 0 or state is None:
                logger.warning(f"【P115ShareStrm】字幕后台收尾审核轮询超时: {share_code}")
                task_queue.subtitle_job_update_stage(job_id, "failed")
                task_queue._notify(
                    user_id,
                    "【115分享STRM】字幕下载失败",
                    f"❌ {share_code}\n原因: 链接审核轮询超时 ({audit_hours}小时)",
                    buttons=retry_btn,
                )
                task_queue.subtitle_job_remove(job_id)
                return
            elif state == 7:
                task_queue.subtitle_job_update_stage(job_id, "failed")
                task_queue._notify(
                    user_id,
                    "【115分享STRM】字幕下载失败",
                    f"❌ {share_code}\n原因: 链接已失效",
                    buttons=retry_btn,
                )
                task_queue.subtitle_job_remove(job_id)
                return

        _refresh_media_stem_targets(media_stem_to_strm_path, media_stem_to_target)
        task_queue.subtitle_job_update_stage(job_id, "placing")
        logger.info(f"【P115ShareStrm】开始后台下载字幕，共 {len(subtitle_files)} 个: {share_code}")
        try:
            downloaded_subtitle_paths, subtitle_fail_count = _download_subtitles_from_share(
                client, share_code, receive_code, subtitle_files, save_path_obj
            )
        except ShareReceiveLimitedError as e:
            try:
                retry_hours = max(1, int(configer.share_receive_retry_hours or 3))
            except (TypeError, ValueError):
                retry_hours = 3
            task_queue.subtitle_job_update_stage(job_id, "receive_limited")
            task_queue._notify(
                user_id,
                "【115分享STRM】字幕下载失败",
                f"❌ {share_code}\n原因: {e}\n将在 {retry_hours} 小时后自动重试转存",
                buttons=retry_btn,
            )
            _schedule_subtitle_receive_retry(
                job_id, share_code, receive_code, subtitle_files, save_path_obj,
                user_id, media_files, media_stem_to_strm_path, media_stem_to_target,
                retry_hours,
            )
            return
        subtitle_count = len(downloaded_subtitle_paths)
        place_ok, place_miss = 0, 0
        if downloaded_subtitle_paths and configer.moviepilot_transfer:
            place_ok, place_miss = _place_subtitles_to_targets(
                subtitle_files,
                downloaded_subtitle_paths,
                media_files,
                media_stem_to_strm_path,
                media_stem_to_target,
            )
        elif downloaded_subtitle_paths and not configer.moviepilot_transfer:
            # 未开启 MP 整理：放到本地 STRM 同目录
            for stem, strm_path in media_stem_to_strm_path.items():
                media_stem_to_target.setdefault(stem, strm_path)
            place_ok, place_miss = _place_subtitles_to_targets(
                subtitle_files,
                downloaded_subtitle_paths,
                media_files,
                media_stem_to_strm_path,
                media_stem_to_target,
            )

        task_queue.metrics_add_place(place_ok, place_miss)
        task_queue.metrics_set_last_finalize_seconds(time() - started)

        msg = (
            f"✅ 字幕后台收尾完成: {share_code}\n"
            f"下载成功: {subtitle_count} 个"
        )
        if subtitle_fail_count:
            msg += f"，下载失败 {subtitle_fail_count} 个"
        msg += f"\n放置成功: {place_ok}，未匹配目标: {place_miss}"
        buttons = retry_btn if (subtitle_fail_count or place_miss) else None
        task_queue._notify(user_id, "【115分享STRM】字幕自动下载成功", msg, buttons=buttons)
        task_queue.subtitle_job_update_stage(job_id, "done")
        task_queue.subtitle_job_remove(job_id)
    except Exception as e:
        logger.error(f"【P115ShareStrm】字幕后台收尾异常: {e}", exc_info=True)
        task_queue.subtitle_job_update_stage(job_id, "failed")
        task_queue._notify(
            user_id,
            "【115分享STRM】字幕下载失败",
            f"❌ {share_code}\n原因: {e}",
            buttons=retry_btn,
        )
    finally:
        task_queue.metrics_subtitle_bg_dec()


def _truncate_filename(filename: str, max_bytes: int = 240) -> str:
    """
    截断文件名以符合文件系统限制 (255 字节)，预留一定空间。
    """
    if not filename:
        return filename
    try:
        b_name = filename.encode('utf-8')
        if len(b_name) <= max_bytes:
            return filename

        # 尝试保留后缀
        p = Path(filename)
        extension = p.suffix
        stem = p.stem

        b_ext = extension.encode('utf-8')
        if len(b_ext) >= max_bytes - 10:
            return b_name[:max_bytes].decode('utf-8', 'ignore')

        b_stem = stem.encode('utf-8')
        available = max_bytes - len(b_ext)
        truncated_stem = b_stem[:available].decode('utf-8', 'ignore')
        return truncated_stem + extension
    except Exception:
        return filename[:max_bytes // 2]  # 极其简化的兜底


def _safe_path(path: Path) -> Path:
    """
    确保路径的每一级组件都不超过 255 字节（Linux 文件系统限制）
    """
    if not path:
        return path
    parts = list(path.parts)
    new_parts = []
    for part in parts:
        if part in ("/", "\\"):
            new_parts.append(part)
        else:
            new_parts.append(_truncate_filename(part))
    return Path(*new_parts)


class ShareP115Client(P115Client):

    """
    分享专用 115 客户端，扩展 share/snap 接口
    """

    def share_snap_cookie(
        self,
        payload: dict,
        /,
        base_url: str | Callable[[], str] = "https://webapi.115.com",
        *,
        async_: bool = False,
        **request_kwargs,
    ) -> dict:
        """
        通过 Cookie 接口获取分享目录列表
        """
        api = complete_url("/share/snap", base_url=base_url)
        payload = {"cid": 0, "limit": 32, "offset": 0, **payload}
        return self.request(url=api, params=payload, async_=async_, **request_kwargs)


class _ShareSnapFetcher:
    """
    分享目录列举：端点冷却 + 分流（对齐 p115strmhelper iter_share_files_with_path）。
    子目录首页走 App HTTPS；翻页走 Cookie；App 页不完整时 fallback Cookie。
    """

    def __init__(self, client: "ShareP115Client") -> None:
        self._client = client
        app_http_cd, app_https_cd, cookie_cd = get_speed_cooldowns(
            configer.share_snap_speed_mode
        )
        _timeout = {"connect": 10, "pool": 10, "read": 60, "write": 60}
        self._headers = {
            "User-Agent": (
                "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) "
                "AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/21E219 "
                "115wangpan_ios/36.2.20"
            ),
        }
        self._ext = {"timeout": _timeout}

        self._app_https = ApiEndpointCooldown(
            lambda p: client.share_snap_app(
                p,
                base_url="https://proapi.115.com",
                headers=self._headers,
                extensions={"timeout": _timeout},
            ),
            app_https_cd,
            "share_snap_app_https",
        )
        self._cookie = ApiEndpointCooldown(
            lambda p: client.share_snap_cookie(
                p,
                headers=self._headers,
                extensions={"timeout": _timeout},
            ),
            cookie_cd,
            "share_snap_cookie",
        )
        self._use_app_for_first = True
        self._request_count = 0

    @staticmethod
    def _extract(resp: dict) -> Tuple[int, list]:
        data = resp.get("data", {}) if isinstance(resp, dict) else {}
        return int(data.get("count") or 0), list(data.get("list") or [])

    def _invoke(self, endpoint: ApiEndpointCooldown, payload: dict) -> Tuple[int, list]:
        with global_api_guard():
            try:
                resp = endpoint(payload)
                check_response(resp)
                reset_waf_backoff()
                api_metrics.record_snap_call()
                self._request_count += 1
                if (
                    self._request_count == 1
                    or self._request_count % _SHARE_SNAP_PROGRESS_INTERVAL == 0
                ):
                    logger.info(
                        f"【P115ShareStrm】分享扫描进度: 已请求 {self._request_count} 页/目录 "
                        f"(cid={payload.get('cid')}, offset={payload.get('offset')})"
                    )
                return self._extract(resp)
            except Exception as e:
                if "4100009" in str(e) or "4100010" in str(e):
                    raise ShareLinkExpiredError(f"分享链接已失效或已取消 ({e})") from e
                if is_waf_405(e):
                    handle_waf_405()
                raise

    def fetch_page(self, payload: dict) -> list:
        offset = int(payload.get("offset") or 0)
        primary = self._app_https if (offset == 0 and self._use_app_for_first) else self._cookie
        primary_name = primary.name

        try:
            count, items = self._invoke(primary, payload)
        except ShareRateLimitedError:
            raise
        except Exception as primary_err:
            if primary_name.startswith("share_snap_app"):
                logger.warning(
                    f"【P115ShareStrm】端点 [{primary_name}] 失败，fallback Cookie: {primary_err}"
                )
                try:
                    count, items = self._invoke(self._cookie, payload)
                except ShareRateLimitedError:
                    raise
                except Exception as cookie_err:
                    logger.error(f"【P115ShareStrm】Cookie 端点也失败: {cookie_err}")
                    raise cookie_err from primary_err
            else:
                raise

        if (
            primary_name.startswith("share_snap_app")
            and items
            and offset + len(items) < count
        ):
            logger.debug(
                f"【P115ShareStrm】App 端分页不完整 ({offset}+{len(items)}<{count})，fallback Cookie"
            )
            try:
                count, items = self._invoke(self._cookie, payload)
            except ShareRateLimitedError:
                raise
            except Exception as fb_err:
                logger.warning(f"【P115ShareStrm】Cookie fallback 失败，沿用 App 结果: {fb_err}")

        return items


def _scan_cache_key(share_code: str, receive_code: str) -> str:
    return f"{share_code}:{receive_code}"


def _get_scan_cache_path() -> Path:
    return task_queue._get_data_dir() / "share_scan_cache.json"


def _load_scan_cache_file() -> Dict[str, Any]:
    path = _get_scan_cache_path()
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        logger.warning(f"【P115ShareStrm】读取 share_scan_cache.json 失败: {e}")
    return {}


def _save_scan_cache_file(data: Dict[str, Any]) -> None:
    path = _get_scan_cache_path()
    tmp = path.with_suffix(".tmp")
    try:
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(path)
    except Exception as e:
        logger.warning(f"【P115ShareStrm】写入 share_scan_cache.json 失败: {e}")


def _save_share_scan_cache(
    share_code: str,
    receive_code: str,
    media_files: List[dict],
    subtitle_files: List[dict],
) -> None:
    key = _scan_cache_key(share_code, receive_code)
    data = _load_scan_cache_file()
    data[key] = {
        "scanned_at": time(),
        "media_files": media_files,
        "subtitle_files": subtitle_files,
    }
    _save_scan_cache_file(data)


def _load_share_scan_cache(
    share_code: str,
    receive_code: str,
    *,
    sub_only: bool,
) -> Optional[Tuple[List[dict], List[dict]]]:
    if not sub_only and not configer.reuse_scan_cache_for_sharestrm:
        return None
    key = _scan_cache_key(share_code, receive_code)
    entry = _load_scan_cache_file().get(key)
    if not entry:
        return None
    try:
        ttl_hours = int(configer.scan_cache_ttl_hours or 72)
    except (TypeError, ValueError):
        ttl_hours = 72
    age = time() - float(entry.get("scanned_at") or 0)
    if age > ttl_hours * 3600:
        return None
    media_files = list(entry.get("media_files") or [])
    subtitle_files = list(entry.get("subtitle_files") or [])
    if not media_files and not subtitle_files:
        return None
    api_metrics.record_cache_hit()
    logger.info(
        f"【P115ShareStrm】使用扫描缓存，跳过 115 列举 "
        f"(sub_only={sub_only}, age={int(age)}s, 媒体={len(media_files)}, 字幕={len(subtitle_files)})"
    )
    return media_files, subtitle_files


def _load_scan_from_subtitle_job(share_code: str) -> Optional[Tuple[List[dict], List[dict]]]:
    jobs = task_queue._load_json_list(task_queue._get_subtitle_jobs_path())
    matched = [j for j in jobs if j.get("share_code") == share_code]
    if not matched:
        return None
    job = max(matched, key=lambda j: float(j.get("created_at") or 0))
    media_files = list(job.get("media_files") or [])
    subtitle_files = list(job.get("subtitle_files") or [])
    if not media_files and not subtitle_files:
        return None
    logger.info(
        f"【P115ShareStrm】sub_only 使用 subtitle_jobs 缓存 "
        f"(媒体={len(media_files)}, 字幕={len(subtitle_files)})"
    )
    return media_files, subtitle_files


def _resolve_share_file_lists(
    client: "ShareP115Client",
    share_code: str,
    receive_code: str,
    media_exts: set,
    subtitle_exts: set,
    sub_only: bool,
) -> Tuple[List[dict], List[dict]]:
    cached = _load_share_scan_cache(share_code, receive_code, sub_only=sub_only)
    if cached:
        media_files, subtitle_files = cached
        if subtitle_exts and not subtitle_files and sub_only:
            pass  # 仍可用 media；字幕列表可能为空
        return media_files, subtitle_files

    if sub_only:
        job_cached = _load_scan_from_subtitle_job(share_code)
        if job_cached:
            return job_cached

    logger.info(f"【P115ShareStrm】正在扫描分享内容: {share_code} ...")
    api_metrics.reset_task_counters()
    media_files: List[dict] = []
    subtitle_files: List[dict] = []
    for item in iter_share_files(client, share_code, receive_code):
        filename = item.get("name", "")
        file_ext = Path(filename).suffix.lower()
        if file_ext in media_exts:
            media_files.append(item)
        elif subtitle_exts and file_ext in subtitle_exts:
            subtitle_files.append(item)

    _save_share_scan_cache(share_code, receive_code, media_files, subtitle_files)
    return media_files, subtitle_files


def iter_share_files(
    client: ShareP115Client,
    share_code: str,
    receive_code: str = "",
    cid: int = 0,
    path_prefix: str = "",
    max_retries: int = 1,
    _fetcher: Optional[_ShareSnapFetcher] = None,
) -> Iterator[dict]:
    """
    递归遍历分享链接下的所有文件，端点冷却 + 分流，避免 405 突发。
    """
    if _fetcher is None:
        _fetcher = _ShareSnapFetcher(client)

    offset = 0
    limit = 1000
    while True:
        payload = {
            "share_code": share_code,
            "receive_code": receive_code,
            "cid": cid,
            "limit": limit,
            "offset": offset,
        }

        try:
            items = _fetcher.fetch_page(payload)
        except ShareRateLimitedError:
            raise
        except Exception as e:
            logger.error(f"【P115ShareStrm】请求分享列表失败: {e}")
            raise

        if not items:
            break

        for item in items:
            item = normalize_attr(item)
            name = item.get("name", "")
            current_path = f"{path_prefix}/{name}" if path_prefix else f"/{name}"
            if item.get("is_dir"):
                if name.upper() in ("BDMV", "CERTIFICATE"):
                    logger.info(
                        f"【P115ShareStrm】检测到蓝光原盘标志性目录 '{name}' "
                        f"(路径: {current_path})，跳过该目录的遍历"
                    )
                    continue
                yield from iter_share_files(
                    client, share_code, receive_code, int(item["id"]), current_path,
                    max_retries, _fetcher,
                )
            else:
                item["_full_path"] = current_path
                yield item

        offset += limit
        if len(items) < limit:
            break


# 字幕语言标签正则（与 MoviePilot transhandler.__rename_subtitles 保持一致）
_ZHCN_SUB_RE = (
    r"([.\[(\s](((zh[-_])?(cn|ch[si]|sg|sc|hans))|zho?"
    r"|chinese|(cn|ch[si]|sg|zho?)[-_&]?(cn|ch[si]|sg|zho?|eng|jap|ja|jpn)"
    r"|eng[-_&]?(cn|ch[si]|sg|zho?)|(jap|ja|jpn)[-_&]?(cn|ch[si]|sg|zho?)"
    r"|简[体中]?)[.\])\s])"
    r"|([\u4e00-\u9fa5]{0,3}[中双][\u4e00-\u9fa5]{0,2}[字文语璃][[\u4e00-\u9fa5]{0,3})"
    r"|简体中[文字]|中[文字]简体|简体|JPSC|sc_jp"
    r"|(?<![a-z0-9])gb(?![a-z0-9])"
)
_ZHHK_SUB_RE = (
    r"([.\[(\s](((zh[-_])?(hk))"
    r"|hk[-_&]?(hk|eng|jap|ja|jpn)"
    r"|eng[-_&]?hk|(jap|ja|jpn)[-_&]?hk"
    r"|香港)[.\])\s])"
    r"|香港中[文字]|中[文字]香港|粤语|廣東話|广东话"
)
_ZHTW_SUB_RE = (
    r"([.\[(\s](((zh[-_])?(tw|cht|tc|hant))"
    r"|cht[-_&]?(cht|eng|jap|ja|jpn)"
    r"|eng[-_&]?cht|(jap|ja|jpn)[-_&]?cht"
    r"|繁[体中]?)[.\])\s])"
    r"|繁体中[文字]|中[文字]繁体|繁体|JPTC|tc_jp"
    r"|(?<![a-z0-9])big5(?![a-z0-9])"
)
_JA_SUB_RE = (
    r"([.\[(\s](ja-jp|jap|ja|jpn"
    r"|(jap|ja|jpn)[-_&]?eng|eng[-_&]?(jap|ja|jpn))[.\])\s])"
    r"|日本語|日語"
)
_ENG_SUB_RE = r"[.\[(\s]eng[.\])\s]"


def _extract_subtitle_lang_tag(sub_filename: str) -> str:
    """
    从字幕文件名提取语言标签（用于拼接到媒体文件主名之后）。

    规则与 MoviePilot transhandler 保持一致：
      简体中文 → .chi.zh-cn
      繁体中文 → .zh-tw
      日语     → .ja
      英文     → .eng
      其他     → 空字符串

    当检测到的语言与系统 settings.DEFAULT_SUB 一致时，自动添加 .default 前缀，
    与 MP 原生整理行为保持一致。
    """
    import re
    from app.core.config import settings

    # 拼接一个前导点，以解决如 Hans.srt、hk.srt 等位于文件名开头无前导边界符的情况
    test_name = f".{sub_filename}"

    if re.search(_ZHCN_SUB_RE, test_name, re.I):
        lang_code = "zh-cn"
        lang = ".chi.zh-cn"
    elif re.search(_ZHHK_SUB_RE, test_name, re.I):
        lang_code = "zh-hk"
        lang = ".zh-hk"
    elif re.search(_ZHTW_SUB_RE, test_name, re.I):
        lang_code = "zh-tw"
        lang = ".zh-tw"
    elif re.search(_JA_SUB_RE, test_name, re.I):
        lang_code = "ja"
        lang = ".ja"
    elif re.search(_ENG_SUB_RE, test_name, re.I):
        lang_code = "eng"
        lang = ".eng"
    else:
        return ""

    # 与 MP transhandler 保持一致：当字幕语言 == settings.DEFAULT_SUB 时加 .default
    default_sub = getattr(settings, "DEFAULT_SUB", "") or ""
    if default_sub.lower() == lang_code.lower():
        return ".default" + lang
    return lang


def _match_subtitle_to_media(
    subtitle_items: List[dict],
    media_items: List[dict],
) -> Dict[str, str]:
    """
    将字幕文件与同目录的媒体文件关联，返回 {字幕原始名: 对应媒体文件stem} 的映射。

    匹配策略：同目录下唯一媒体文件直接关联；多文件则按集号或公共前缀匹配。
    """
    import re

    # 按父目录分组
    from collections import defaultdict
    media_by_dir: Dict[str, List[dict]] = defaultdict(list)
    for item in media_items:
        parent = str(Path(item.get("_full_path", item.get("name", ""))).parent)
        media_by_dir[parent].append(item)

    result: Dict[str, str] = {}
    for sub_item in subtitle_items:
        sub_path = sub_item.get("_full_path", sub_item.get("name", ""))
        sub_name = Path(sub_path).name
        parent = str(Path(sub_path).parent)
        candidates = media_by_dir.get(parent, [])

        if not candidates:
            continue

        if len(candidates) == 1:
            result[sub_name] = Path(candidates[0].get("name", "")).stem
            continue

        # 尝试通过集号匹配
        ep_match = re.search(r"[Ss](\d+)[Ee](\d+)", sub_name)
        if ep_match:
            for media_item in candidates:
                m_name = media_item.get("name", "")
                m_ep = re.search(r"[Ss](\d+)[Ee](\d+)", m_name)
                if m_ep and m_ep.group(1) == ep_match.group(1) and m_ep.group(2) == ep_match.group(2):
                    result[sub_name] = Path(m_name).stem
                    break
            continue

        # 公共前缀匹配：找最长公共前缀最多的媒体文件
        best_media = max(
            candidates,
            key=lambda m: len(
                Path(m.get("name", "")).stem
            ) if Path(m.get("name", "")).stem in sub_name else 0,
        )
        if Path(best_media.get("name", "")).stem and Path(best_media.get("name", "")).stem in sub_name:
            result[sub_name] = Path(best_media.get("name", "")).stem

    return result


def _rename_subtitles_to_match_media(
    downloaded_paths: List[Path],
    subtitle_items: List[dict],
    media_items: List[dict],
) -> List[Path]:
    """
    将已下载的字幕文件重命名，使其前缀与对应媒体文件一致。

    逻辑：
    1. 建立 {字幕原始文件名 → 媒体stem} 的映射
    2. 对每个已下载字幕，提取语言标签，生成 {媒体stem}{lang_tag}{ext} 的新名称
    3. 在本地重命名后，更新路径列表返回

    :return: 重命名后的本地路径列表（重命名失败的保留原路径）
    """
    name_map = _match_subtitle_to_media(subtitle_items, media_items)
    if not name_map:
        return downloaded_paths

    # 建立 {原始文件名 → 下载路径} 的查找表
    path_by_original_name: Dict[str, Path] = {}
    for item, local_path in zip(subtitle_items, downloaded_paths):
        item_name = Path(item.get("_full_path", item.get("name", ""))).name
        path_by_original_name[item_name] = local_path

    renamed_paths: List[Path] = list(downloaded_paths)
    for i, (item, local_path) in enumerate(zip(subtitle_items, downloaded_paths)):
        sub_name = Path(item.get("_full_path", item.get("name", ""))).name
        media_stem = name_map.get(sub_name)
        if not media_stem:
            continue

        lang_tag = _extract_subtitle_lang_tag(sub_name)
        file_ext = local_path.suffix
        new_name = _truncate_filename(media_stem + lang_tag + file_ext)
        new_path = local_path.with_name(new_name)

        if new_path == local_path:
            continue

        try:
            if new_path.exists():
                new_path.unlink()
            local_path.rename(new_path)
            renamed_paths[i] = new_path
            logger.info(
                f"【P115ShareStrm】字幕对齐重命名: {local_path.name} → {new_name}"
            )
        except Exception as e:
            logger.warning(
                f"【P115ShareStrm】字幕对齐重命名失败: {local_path.name}: {e}"
            )

    return renamed_paths


def _download_subtitles_from_share(
    client: ShareP115Client,
    share_code: str,
    receive_code: str,
    subtitle_items: List[dict],
    save_path_obj: Path,
) -> Tuple[List[Path], int]:
    """
    通过转存方式下载分享中的字幕文件：
    1. 在网盘中创建临时文件夹
    2. 用 share_receive 将字幕文件转存到临时文件夹
    3. 通过 fs_video_subtitle 获取转存后文件的下载 URL
    4. 通过 httpx 下载到本地
    5. 清理临时文件夹

    参考 p115strmhelper 插件的 batch_share_subtitle_downloader。

    :return: (成功下载的本地路径列表, 失败数量)
    """
    import httpx

    custom_headers = {
        "User-Agent": (
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) "
            "AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/21E219 "
            "115wangpan_ios/36.2.20"
        ),
    }

    _download_headers = {
        "User-Agent": (
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) "
            "AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/21E219 "
            "115wangpan_ios/36.2.20"
        ),
    }

    downloaded_paths: List[Path] = []
    fail_count = 0
    batch_size = 50

    for batch_start in range(0, len(subtitle_items), batch_size):
        batch_items = subtitle_items[batch_start:batch_start + batch_size]
        scid = None
        try:
            # 1. 在网盘中创建临时文件夹
            logger.info(f"【P115ShareStrm】开始转存并下载分享字幕，准备创建临时文件夹...")
            resp = call_protected_api(client.fs_mkdir, f"subtitle-{uuid4()}", headers=custom_headers)
            check_response(resp)
            if "cid" in resp:
                scid = resp["cid"]
            else:
                data = resp.get("data", {})
                scid = data.get("category_id") or data.get("file_id")
            logger.info(f"【P115ShareStrm】网盘临时文件夹创建成功，scid: {scid}")

            # 2. 将分享中的字幕文件转存到临时文件夹
            file_ids_str = ",".join(str(item["id"]) for item in batch_items)
            logger.info(f"【P115ShareStrm】向 115 提交字幕转存，共 {len(batch_items)} 个文件，ID列表: {file_ids_str}")
            payload = {
                "share_code": share_code,
                "receive_code": receive_code,
                "file_id": file_ids_str,
                "cid": scid,
                "is_check": 0,
            }
            resp = call_protected_api(client.share_receive, payload, headers=custom_headers)
            logger.debug(f"【P115ShareStrm】share_receive 接口返回: {resp}")
            check_response(resp)
            logger.info(f"【P115ShareStrm】字幕转存任务提交完成")

            # 3. 轮询等待 115 异步转存完成（在第一次查询前增加 8s 的延时以给 115 充足时间落盘）
            attr = None
            logger.info(f"【P115ShareStrm】开始等待字幕文件转存落盘...")
            sleep(8)
            for attempt in range(10):
                if attempt > 0:
                    sleep(3)
                try:
                    logger.debug(f"【P115ShareStrm】正在进行第 {attempt + 1}/10 次转存检测...")
                    attr = call_protected_api(
                        lambda c, s: next(iterdir(c, cid=s, page_size=1, app="web")),
                        client,
                        scid,
                    )
                    if attr:
                        logger.info(f"【P115ShareStrm】检测到转存文件成功，首个文件 pickcode: {attr.get('pickcode')}")
                        break
                except StopIteration:
                    pass
                except Exception as poll_err:
                    logger.warning(f"【P115ShareStrm】轮询转存状态时出错: {poll_err}")

            if not attr:
                try:
                    logger.warning(f"【P115ShareStrm】轮询超时，尝试列出临时目录 {scid} 下的所有内容...")
                    tmp_files = call_protected_api(
                        lambda c, s: list(iterdir(c, cid=s, page_size=10, app="web")),
                        client,
                        scid,
                    )
                    logger.warning(f"【P115ShareStrm】临时目录下实际存在的文件列表: {tmp_files}")
                except Exception as list_err:
                    logger.warning(f"【P115ShareStrm】尝试列出临时目录内容失败: {list_err}")
                raise RuntimeError("115 转存文件超时，临时文件夹中未发现字幕文件")

            # 4. 调用 fs_video_subtitle 获取下载链接
            logger.info(f"【P115ShareStrm】通过 fs_video_subtitle 批量获取字幕下载链接，使用 pickcode: {attr['pickcode']}")
            resp = call_protected_api(client.fs_video_subtitle, attr["pickcode"], headers=custom_headers)
            check_response(resp)
            logger.debug(f"【P115ShareStrm】fs_video_subtitle 接口返回数据: {resp}")

            # 列出转存到临时文件夹下的所有文件的 sha1 -> pickcode 映射（用于兜底下载）
            tmp_files_map = {}
            try:
                tmp_files = call_protected_api(
                    lambda c, s: list(iterdir(c, cid=s, page_size=100, app="web")),
                    client,
                    scid,
                )
                logger.info(f"【P115ShareStrm】临时文件夹 {scid} 中当前实际存在的文件数量: {len(tmp_files)}")
                for tf in tmp_files:
                    if tf.get("sha1") and tf.get("pickcode"):
                        tmp_files_map[tf["sha1"].lower()] = tf["pickcode"]
                logger.debug(f"【P115ShareStrm】已构建临时文件 sha1 -> pickcode 映射: {tmp_files_map}")
            except Exception as list_err:
                logger.warning(f"【P115ShareStrm】构建临时文件映射失败: {list_err}")

            # 5. 构建 sha1 -> url 映射
            subtitles_url_map = {
                info["sha1"].lower(): info["url"]
                for info in resp.get("data", {}).get("list", [])
                if info.get("file_id")
            }
            logger.info(f"【P115ShareStrm】成功从 fs_video_subtitle 获取字幕直链，数量: {len(subtitles_url_map)}")

            # 6. 逐个下载字幕文件到本地
            for item in batch_items:
                filename = item.get("name", "")
                full_path = item.get("_full_path", f"/{filename}")
                local_path = save_path_obj / _safe_path(Path(full_path.lstrip("/")))
                sha1 = item.get("sha1", "").lower()

                url = subtitles_url_map.get(sha1)
                if not url:
                    # 尝试进行兜底获取下载直链（支持 .sup 或其它非常规后缀字幕）
                    logger.info(f"【P115ShareStrm】字幕文件 {filename} (sha1: {sha1}) 未能匹配到 fs_video_subtitle 链接，尝试使用 client.download_url 兜底获取直链...")
                    tmp_pickcode = tmp_files_map.get(sha1)
                    if tmp_pickcode:
                        try:
                            logger.info(f"【P115ShareStrm】正在为 {filename} (pickcode: {tmp_pickcode}) 获取下载直链...")
                            url = str(call_protected_api(
                                client.download_url,
                                tmp_pickcode,
                                user_agent=custom_headers.get("User-Agent"),
                            ))
                            logger.info(f"【P115ShareStrm】获取直链成功: {url}")
                        except Exception as dl_url_err:
                            logger.error(f"【P115ShareStrm】调用 client.download_url 获取 {filename} 直链失败: {dl_url_err}", exc_info=True)
                    else:
                        logger.warning(f"【P115ShareStrm】未能在临时网盘目录映射中匹配到 sha1={sha1} 的文件，可能该文件转存落盘未成功")

                if not url:
                    logger.warning(
                        f"【P115ShareStrm】字幕文件 {filename} (sha1: {sha1}) 最终未能匹配到有效下载链接，跳过。"
                        f" 可用 fs_video_subtitle sha1 列表: {list(subtitles_url_map.keys())}，"
                        f" 临时目录可用 sha1 列表: {list(tmp_files_map.keys())}"
                    )
                    fail_count += 1
                    continue

                _dl_success = False
                logger.info(f"【P115ShareStrm】开始从 115 CDN 下载字幕 {filename}，保存路径: {local_path}")
                for _attempt in range(3):
                    try:
                        resp_dl = httpx.get(
                            url,
                            headers=_download_headers,
                            timeout=30,
                            follow_redirects=True,
                        )
                        resp_dl.raise_for_status()
                        local_path.parent.mkdir(parents=True, exist_ok=True)
                        local_path.write_bytes(resp_dl.content)
                        logger.info(f"【P115ShareStrm】字幕下载成功: {filename}")
                        downloaded_paths.append(local_path)
                        _dl_success = True
                        break
                    except Exception as e:
                        logger.error(
                            f"【P115ShareStrm】字幕下载失败 ({_attempt + 1}/3): "
                            f"{filename}: {e}",
                            exc_info=True
                        )
                        if _attempt < 2:
                            sleep(2)

                if not _dl_success:
                    fail_count += 1

        except ShareReceiveLimitedError:
            raise
        except Exception as e:
            if is_receive_limited(e):
                raise ShareReceiveLimitedError(str(e)) from e
            logger.error(f"【P115ShareStrm】字幕批处理失败: {e}", exc_info=True)
            # 本批次中尚未成功下载的全部计为失败
            batch_downloaded = len([p for p in downloaded_paths if p not in downloaded_paths[:batch_start]])
            fail_count += len(batch_items) - batch_downloaded
        finally:
            # 7. 清理临时文件夹
            if scid:
                try:
                    logger.info(f"【P115ShareStrm】正在清理网盘临时目录 {scid}...")
                    resp_del = call_protected_api(client.fs_delete, scid, headers=custom_headers)
                    logger.debug(f"【P115ShareStrm】清理返回结果: {resp_del}")
                except Exception as e:
                    logger.warning(f"【P115ShareStrm】清理字幕临时目录失败: {e}", exc_info=True)

    return downloaded_paths, fail_count


def _resolve_mtype_by_tmdb_names(tmdbid: int, arg_str: str,
                                   prefer: Optional[str] = None) -> Optional[str]:
    """
    通过 TMDB 接口查询媒体名称，与消息原文匹配来推断或验证媒体类型。

    - prefer=None  : 同时查询电影和TV，双向匹配（用于 mtype 未知时）
    - prefer="tv"  : 先查TV验证，命中则确认；未命中再查电影，命中则修正（用于关键词结果验证）
    - prefer="movie": 先查电影验证，命中则确认；未命中再查TV，命中则修正

    兜底规则（名称都未命中时）：若只有一个类型在 TMDB 有数据，则直接使用该类型。

    返回 "tv" / "movie" / "collection"，或 None（无法判断时）。
    """
    import re
    try:
        from app.modules.themoviedb.tmdbapi import TmdbApi
        from app.schemas.types import MediaType

        tmdb = TmdbApi()

        def _collect_names(info: Optional[dict]) -> List[str]:
            if not info:
                return []
            names = []
            for key in ("title", "original_title", "name", "original_name"):
                v = info.get(key)
                if v and isinstance(v, str):
                    names.append(v)
            aka = info.get("also_known_as") or []
            if isinstance(aka, list):
                names.extend(n for n in aka if n and isinstance(n, str))
            return names

            return False
        
        def _collect_collection_names(info: Optional[dict]) -> List[str]:
            if not info:
                return []
            names = [info.get("name"), info.get("original_name")]
            return [n for n in names if n and isinstance(n, str)]

        def _matches(names: List[str]) -> bool:
            for name in names:
                if not name:
                    continue
                try:
                    if re.search(re.escape(name), arg_str, re.IGNORECASE):
                        return True
                except re.error:
                    if name in arg_str:
                        return True
            return False

        def _has_valid_data(info: Optional[dict]) -> bool:
            """判断 TMDB 返回的数据是否包含有效标题（排除空响应或占位数据）"""
            if not info or not isinstance(info, dict):
                return False
            # 检查是否有任何有效的标题/名称字段
            for key in ("title", "original_title", "name", "original_name"):
                v = info.get(key)
                if v and isinstance(v, str) and v.strip():
                    return True
            return False

        def _only_one_exists(info_a: Optional[dict], info_b: Optional[dict],
                              type_a: str, type_b: str) -> Optional[str]:
            """名称匹配都失败后，若只有一侧有有效数据则直接返回该类型"""
            has_a = _has_valid_data(info_a)
            has_b = _has_valid_data(info_b)
            logger.debug(
                f"【P115ShareStrm】兜底数据检查: {type_a}_valid={has_a}, {type_b}_valid={has_b}"
            )
            if has_a and not has_b:
                logger.info(
                    f"【P115ShareStrm】TMDB名称均未命中，但仅 {type_a} 有有效数据，兜底使用: {type_a}"
                )
                return type_a
            if has_b and not has_a:
                logger.info(
                    f"【P115ShareStrm】TMDB名称均未命中，但仅 {type_b} 有有效数据，兜底使用: {type_b}"
                )
                return type_b
            
            # 若包含 "系列"、"合集"、"Collection" 等推断合集
            if re.search(r'系列|合集|Collection', arg_str, re.I):
                coll_info = tmdb.get_collection(tmdbid)
                if _has_valid_data(coll_info):
                    logger.info(f"【P115ShareStrm】TMDB名称匹配推断为合集类型: collection")
                    return "collection"

            return None

        if prefer:
            # 有偏好类型时：先验证偏好类型，若不命中再验证另一类型
            prefer_mtype = MediaType.TV if prefer == "tv" else MediaType.MOVIE
            other_mtype = MediaType.MOVIE if prefer == "tv" else MediaType.TV
            other_str = "movie" if prefer == "tv" else "tv"

            prefer_info = tmdb.get_info(mtype=prefer_mtype, tmdbid=tmdbid)
            prefer_hit = _matches(_collect_names(prefer_info))
            logger.info(
                f"【P115ShareStrm】TMDB验证 tmdbid={tmdbid} prefer={prefer}: {prefer}_hit={prefer_hit}"
            )
            if prefer_hit:
                return prefer

            # 偏好类型未命中，尝试合集识别（如果偏好不是合集）
            if prefer != "collection":
                coll_info = tmdb.get_collection(tmdbid)
                if _matches(_collect_collection_names(coll_info)):
                    logger.info(f"【P115ShareStrm】TMDB验证命中合集类型: collection")
                    return "collection"

            # 查另一类型
            other_info = tmdb.get_info(mtype=other_mtype, tmdbid=tmdbid)
            other_hit = _matches(_collect_names(other_info))
            logger.info(
                f"【P115ShareStrm】TMDB验证 tmdbid={tmdbid} fallback={other_str}: {other_str}_hit={other_hit}"
            )
            if other_hit:
                return other_str

            # 名称都未命中，尝试兜底：只有一侧有数据
            return _only_one_exists(prefer_info, other_info, prefer, other_str)
        else:
            # 无偏好时：同时查询双向匹配
            movie_info = tmdb.get_info(mtype=MediaType.MOVIE, tmdbid=tmdbid)
            tv_info = tmdb.get_info(mtype=MediaType.TV, tmdbid=tmdbid)
            movie_hit = _matches(_collect_names(movie_info))
            tv_hit = _matches(_collect_names(tv_info))
            logger.info(
                f"【P115ShareStrm】TMDB名称匹配结果 tmdbid={tmdbid}: movie_hit={movie_hit}, tv_hit={tv_hit}"
            )
            if tv_hit and not movie_hit:
                return "tv"
            if movie_hit and not tv_hit:
                return "movie"
            
            # 检查合集匹配
            coll_info = tmdb.get_collection(tmdbid)
            if _matches(_collect_collection_names(coll_info)):
                return "collection"

            # 名称都未命中（或都命中），尝试兜底：只有一侧有数据
            if not tv_hit and not movie_hit:
                return _only_one_exists(movie_info, tv_info, "movie", "tv")
            # 两者都命中，无法判断
            return None
    except Exception as e:
        logger.warning(f"【P115ShareStrm】TMDB名称匹配异常: {e}")
        return None


def _resolve_mtype_by_imdb(imdbid: str, arg_str: str) -> Optional[Tuple[int, str]]:
    """
    通过 IMDB ID 查询 TMDB 信息，返回对应的 TMDB ID 和媒体类型。

    使用 TMDB Find API 的 find_by_imdb_id 方法查询。

    :param imdbid: IMDB ID，格式如 tt1234567
    :param arg_str: 原始消息文本，用于辅助推断类型
    :return: (tmdbid, mtype) 元组，其中 mtype 为 "tv" / "movie" / "collection"；失败时返回 None
    """
    import re
    try:
        from app.modules.themoviedb.tmdbapi import TmdbApi
        from app.modules.themoviedb.tmdbv3api import Find

        tmdb = TmdbApi()
        find_api = Find()

        # 通过 IMDB ID 查询
        logger.info(f"【P115ShareStrm】正在通过 IMDB ID 查询 TMDB 信息: {imdbid}")
        result = find_api.find_by_imdb_id(imdbid)

        if not result:
            logger.warning(f"【P115ShareStrm】IMDB ID {imdbid} 未查询到结果")
            return None

        # 解析返回结果，优先使用 movie_results，其次 tv_results
        movie_results = result.get("movie_results", [])
        tv_results = result.get("tv_results", [])
        
        # 判断返回的媒体类型
        if movie_results and tv_results:
            # 两种类型都有结果，通过关键词判断
            clean_text = re.sub(r'https?://\S+', '', arg_str)
            if re.search(r'电视剧|剧集|番剧|[美日韩台港英泰]剧|动漫|综艺|Season|S[0-9]+E[0-9]+|S[0-9]+|第[0-9]+[季集]', clean_text, re.I):
                logger.info(f"【P115ShareStrm】IMDB ID 查询到电影和剧集，根据关键词判断为剧集")
                tmdbid = tv_results[0].get("id")
                mtype = "tv"
            else:
                logger.info(f"【P115ShareStrm】IMDB ID 查询到电影和剧集，默认优先使用电影")
                tmdbid = movie_results[0].get("id")
                mtype = "movie"
        elif movie_results:
            tmdbid = movie_results[0].get("id")
            mtype = "movie"
            logger.info(f"【P115ShareStrm】IMDB ID 查询到电影: TMDB ID={tmdbid}")
        elif tv_results:
            tmdbid = tv_results[0].get("id")
            mtype = "tv"
            logger.info(f"【P115ShareStrm】IMDB ID 查询到剧集: TMDB ID={tmdbid}")
        else:
            logger.warning(f"【P115ShareStrm】IMDB ID {imdbid} 未查询到有效的电影或剧集结果")
            return None

        return (tmdbid, mtype)

    except Exception as e:
        logger.error(f"【P115ShareStrm】通过 IMDB ID 查询失败: {e}", exc_info=True)
        return None


def _get_share_state(client: ShareP115Client, share_code: str, receive_code: str) -> Optional[int]:
    """
    获取分享链接的当前状态：
    0: 审核中/快照生成中
    1: 正常
    7: 链接已失效/过期
    其他: 未知
    """
    try:
        payload = {"share_code": share_code, "receive_code": receive_code}
        # 使用 app 端接口查询快照，获取分享状态
        custom_headers = {
            "User-Agent": (
                "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) "
                "AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/21E219 "
                "115wangpan_ios/36.2.20"
            ),
        }
        resp = call_protected_api(
            client.share_snap_app,
            payload,
            app="android",
            headers=custom_headers,
            timeout=15,
        )
        data = resp.get("data", {})
        share_info = data.get("shareinfo", data.get("share_info", {}))
        share_state = data.get("share_state", share_info.get("share_state", share_info.get("status")))
        if share_state is not None:
            return int(share_state)
    except Exception as e:
        logger.warning(f"【P115ShareStrm】获取分享链接状态失败: {e}")
    return None


def process_share_strm(
    share_code: str,
    receive_code: str,
    tmdbid: Optional[int] = None,
    mtype: Optional[str] = None,
    arg_str: Optional[str] = None,
    user_id: Optional[str] = None,
    sub_only: bool = False,
) -> Dict[str, Any]:
    """
    实际执行 STRM 生成逻辑：遍历分享、生成文件、触发 MP 整理
    """
    if not configer.cookies:
        return {"status": False, "msg": "未配置 115 Cookie"}

    if not configer.strm_save_path:
        return {"status": False, "msg": "未配置 STRM 保存路径"}

    try:
        client = ShareP115Client(configer.cookies)
        save_path_obj = Path(configer.strm_save_path)
        save_path_obj.mkdir(parents=True, exist_ok=True)

        media_exts = {
            f".{ext.strip().lower()}"
            for ext in configer.user_rmt_mediaext.split(",")
            if ext.strip()
        }

        resolver = None
        if configer.strm_url_template_enabled and (
            configer.strm_url_template or configer.strm_url_template_custom
        ):
            resolver = StrmUrlTemplateResolver(
                base_template=configer.strm_url_template or None,
                custom_rules=configer.strm_url_template_custom or None,
            )

        redirect_base = (
            f"{configer.moviepilot_address}/api/v1/plugin/P115StrmHelper/redirect_url"
        )

        strm_count = 0
        media_files = []
        # media stem（分享内）→ 生成的 STRM 本地路径，用于后续字幕直接放置
        media_stem_to_strm_path: Dict[str, Path] = {}

        # 构建字幕文件后缀集合（按需；字幕重试任务强制扫描字幕）
        subtitle_exts: set = set()
        if configer.download_subtitle or sub_only:
            subtitle_exts = {
                f".{ext.strip().lower()}"
                for ext in configer.user_subtitle_ext.split(",")
                if ext.strip()
            }

        # 1. 第一步：获取分享中的媒体与字幕（缓存 / 在线扫描）
        subtitle_files: List[dict] = []
        media_files, subtitle_files = _resolve_share_file_lists(
            client,
            share_code,
            receive_code,
            media_exts,
            subtitle_exts,
            sub_only,
        )

        total_media = len(media_files)
        logger.info(f"【P115ShareStrm】扫描完成，匹配到 {total_media} 个媒体文件")

        # 2. 第二步：提前识别媒体信息（如有提供 ID）
        mediainfo = None
        collection_parts = []

        # 当有 tmdbid 但 mtype 未知时，尝试通过 TMDB 名称匹配推断类型
        if tmdbid and not mtype and arg_str and configer.moviepilot_transfer:
            inferred = _resolve_mtype_by_tmdb_names(tmdbid, arg_str)
            if inferred:
                logger.info(f"【P115ShareStrm】通过TMDB名称匹配推断媒体类型: {inferred}")
                mtype = inferred

        # 处理合集：获取合集下的所有电影
        if tmdbid and mtype == "collection" and configer.moviepilot_transfer:
            logger.info(f"【P115ShareStrm】正在获取 TMDB 合集详情 (ID: {tmdbid})...")
            try:
                from app.modules.themoviedb.tmdbapi import TmdbApi
                coll_details = TmdbApi().get_collection(tmdbid)
                # MoviePilot 的 get_collection 失败时可能返回 []
                if coll_details:
                    if isinstance(coll_details, dict):
                        collection_parts = coll_details.get("parts") or []
                        logger.info(f"【P115ShareStrm】成功获取合集信息: {coll_details.get('name')}，包含 {len(collection_parts)} 部电影")
                    elif isinstance(coll_details, list):
                        # 如果直接返回了列表（某些情况），则认为这就是 parts
                        collection_parts = coll_details
                        logger.info(f"【P115ShareStrm】成功获取合集列表信息，包含 {len(collection_parts)} 部电影")
                    
                    if collection_parts:
                        # 核心优化：按标题长度降序排列，确保长标题（如 12回合2）优先于短标题（如 12回合）匹配
                        collection_parts = sorted(
                            collection_parts, 
                            key=lambda x: max(len(x.get("title") or ""), len(x.get("original_title") or "")), 
                            reverse=True
                        )
                else:
                    logger.warning(f"【P115ShareStrm】未获取到 TMDB 合集详情 (ID: {tmdbid})，请检查网络或 TMDB ID 是否正确")
            except Exception as e:
                logger.warning(f"【P115ShareStrm】获取合集详情异常: {e}")

        # 单体媒体提前识别（非合集时）
        if tmdbid and mtype in ["movie", "tv"] and configer.moviepilot_transfer:
            for i in range(3):
                try:
                    from app.chain.media import MediaChain
                    from app.schemas.types import MediaType
                    media_type = MediaType.from_agent(mtype) if mtype else None
                    mediainfo = MediaChain().recognize_media(tmdbid=tmdbid, mtype=media_type)
                    if mediainfo:
                        logger.info(f"【P115ShareStrm】提前识别成功: {mediainfo.title_year}")
                        break
                    else:
                        logger.warning(f"【P115ShareStrm】识别任务未返回信息 (尝试 {i+1}/3)")
                except Exception as e:
                    logger.warning(f"【P115ShareStrm】提前识别异常 (尝试 {i+1}/3): {e}")
                if i < 2:
                    sleep(2)

        # 3. 第三步：生成所有 STRM 文件，收集待整理路径
        generated_strm_paths: List[Path] = []
        # 记录每个文件对应的 mediainfo（用于批量异步入队时区分）
        strm_to_mediainfo: Dict[str, Any] = {}

        for i, item in enumerate(media_files):
            filename = item.get("name", "")
            if (i + 1) % 10 == 0 or (i + 1) == total_media:
                logger.info(f"【P115ShareStrm】处理进度 ({i+1}/{total_media}): {filename}")

            # 合集模式：为每个文件匹配对应的电影信息
            current_mediainfo = mediainfo
            if mtype == "collection" and collection_parts:
                from app.chain.media import MediaChain
                from app.schemas.types import MediaType
                # 尝试根据文件名匹配合集中的子项
                matched_part = None
                for part in collection_parts:
                    p_title = part.get("title")
                    p_orig = part.get("original_title")
                    # 只要文件名包含标题或原标题即视为命中（忽略特殊字符）
                    # 简单清理文件名干扰字符
                    clean_filename = re.sub(r'[\s._\-]', '', filename).lower()
                    if (p_title and re.sub(r'[\s._\-]', '', p_title).lower() in clean_filename) or \
                       (p_orig and re.sub(r'[\s._\-]', '', p_orig).lower() in clean_filename):
                        matched_part = part
                        break
                
                if matched_part:
                    try:
                        p_id = matched_part.get("id")
                        p_title = matched_part.get("title")
                        logger.debug(f"【P115ShareStrm】正在为文件 {filename} 识别子项: {p_title} (ID: {p_id})")
                        current_mediainfo = MediaChain().recognize_media(tmdbid=p_id, mtype=MediaType.MOVIE)
                        if current_mediainfo:
                            logger.info(f"【P115ShareStrm】文件 {filename} 成功匹配到合集子项: {current_mediainfo.title_year}")
                        else:
                            logger.warning(f"【P115ShareStrm】文件 {filename} 匹配到子项但识别失败: {p_title}")
                    except Exception as e:
                        logger.warning(f"【P115ShareStrm】识别合集子项过程异常: {filename}: {e}")
                else:
                    # 如果开启了日志级别较低，这里可能产生较多日志，保持为 info 方便你观察
                    logger.info(f"【P115ShareStrm】文件 {filename} 未匹配到合集子项，将回退到自主识别")
                    current_mediainfo = None

            full_path = item.get("_full_path", f"/{filename}")
            relative_path = _safe_path(Path(full_path.lstrip("/")))
            strm_relative = relative_path.with_suffix(".strm")

            # 扩展名特定规则可能指定不同的保存目录
            if resolver:
                path_override = resolver.get_save_path_override(filename)
                effective_save_path = Path(path_override) if path_override else save_path_obj
            else:
                effective_save_path = save_path_obj

            strm_file_path = effective_save_path / strm_relative
            strm_file_path.parent.mkdir(parents=True, exist_ok=True)

            # 生成 STRM URL
            if resolver:
                strm_url = resolver.render(
                    file_name=filename,
                    share_code=share_code,
                    receive_code=receive_code,
                    file_id=item["id"],
                    file_path=full_path,
                    base_url=redirect_base,
                    sha=item.get("sha1") or "",
                    sha1=item.get("sha1") or "",
                    file_size=item.get("size") or 0,
                )
            else:
                strm_url = (
                    f"{redirect_base}"
                    f"?share_code={share_code}"
                    f"&receive_code={receive_code}"
                    f"&id={item['id']}"
                    f"&file_name={quote(filename)}"
                )

            if not strm_url:
                continue

            if not sub_only:
                strm_file_path.parent.mkdir(parents=True, exist_ok=True)
                strm_file_path.write_text(strm_url, encoding="utf-8")
                strm_count += 1
                if configer.moviepilot_transfer:
                    generated_strm_paths.append(strm_file_path)
                    strm_to_mediainfo[strm_file_path.as_posix()] = current_mediainfo
            media_stem_to_strm_path[Path(filename).stem] = strm_file_path


        # 4. 第四步：交由 MoviePilot 整理 STRM（主路径不等待；已整理过的跳过入队）
        media_stem_to_target: Dict[str, Path] = {}
        if configer.moviepilot_transfer:
            if not sub_only and generated_strm_paths:
                query_srcs: List[str] = []
                for strm_file_path in generated_strm_paths:
                    src = strm_file_path.as_posix()
                    query_srcs.append(src)
                    mapped = _map_local_path_to_db(src)
                    if mapped != src:
                        query_srcs.append(mapped)
                histories = _batch_load_transfer_histories_by_srcs(query_srcs)
                to_enqueue: List[Path] = []
                already = 0
                for strm_file_path in generated_strm_paths:
                    src = strm_file_path.as_posix()
                    mapped = _map_local_path_to_db(src)
                    history = histories.get(mapped) or histories.get(src)
                    if history and history.dest:
                        already += 1
                        for stem, sp in media_stem_to_strm_path.items():
                            if sp.as_posix() == src:
                                media_stem_to_target[stem] = Path(history.dest)
                                break
                    else:
                        to_enqueue.append(strm_file_path)

                logger.info(
                    f"【P115ShareStrm】开始批量整理 STRM，共 {len(generated_strm_paths)} 个文件"
                    f"（已整理跳过 {already}，待入队 {len(to_enqueue)}）"
                )
                if to_enqueue:
                    transfer_chain = TransferChain()
                    for strm_file_path in to_enqueue:
                        try:
                            stat = strm_file_path.stat()
                            transfer_chain.do_transfer(
                                fileitem=FileItem(
                                    storage="local",
                                    type="file",
                                    path=strm_file_path.as_posix(),
                                    name=strm_file_path.name,
                                    basename=strm_file_path.stem,
                                    extension="strm",
                                    size=stat.st_size,
                                    modify_time=stat.st_mtime,
                                ),
                                mediainfo=strm_to_mediainfo.get(strm_file_path.as_posix()),
                                background=True,
                            )
                        except Exception as e:
                            logger.warning(f"【P115ShareStrm】STRM 整理入队失败: {strm_file_path.name}: {e}")
                logger.info("【P115ShareStrm】STRM 处理完成（主路径不等待整理，字幕将后台收尾）")
            elif sub_only:
                logger.info("【P115ShareStrm】仅重试下载字幕模式：批量检索历史整理记录...")
                _refresh_media_stem_targets(media_stem_to_strm_path, media_stem_to_target)
                logger.info(
                    f"【P115ShareStrm】历史整理记录检索完成，成功匹配到 "
                    f"{len(media_stem_to_target)}/{len(media_stem_to_strm_path)} 个目标路径"
                )

        # 5. 第五步：字幕一律后台收尾（不堵塞主队列）
        subtitle_count = 0
        subtitle_fail_count = 0
        subtitle_in_background = False
        if (configer.download_subtitle or sub_only) and subtitle_files:
            missing_strm = [
                stem for stem, sp in media_stem_to_strm_path.items() if not sp.exists()
            ]
            if sub_only and missing_strm:
                logger.warning(
                    f"【P115ShareStrm】字幕重试发现 {len(missing_strm)} 个本地 STRM 缺失，"
                    f"将仅对已有路径做后台收尾；如需完整重建请重新 /sharestrm"
                )
            logger.info(
                f"【P115ShareStrm】启动字幕后台收尾任务，共 {len(subtitle_files)} 个字幕: {share_code}"
            )
            _start_subtitle_finalize_job(
                share_code,
                receive_code,
                subtitle_files,
                save_path_obj,
                user_id,
                media_files,
                media_stem_to_strm_path,
                media_stem_to_target,
            )
            subtitle_in_background = True

        return {
            "status": True,
            "strm_count": strm_count,
            "total_files": total_media,
            "subtitle_count": subtitle_count,
            "subtitle_fail_count": subtitle_fail_count,
            "subtitle_in_background": subtitle_in_background,
        }

    except ShareLinkExpiredError as e:
        logger.warning(f"【P115ShareStrm】分享链接已失效: {share_code}")
        return {"status": False, "msg": str(e), "terminal": True}
    except ShareRateLimitedError as e:
        logger.warning(f"【P115ShareStrm】115 接口风控: {share_code} - {e}")
        return {"status": False, "msg": str(e), "retryable": True}
    except Exception as e:
        logger.error(f"【P115ShareStrm】逻辑执行异常: {e}", exc_info=True)
        return {"status": False, "msg": str(e)}


class ShareTaskQueue:
    """
    异步任务队列管理器：通过单线程串行处理，避免并发风控。
    支持 JSON 文件持久化，MP/插件重启后自动恢复未完成任务。
    """

    # 去重时间窗口（秒）：同一 share_code 在此时间内只入队一次
    _DEDUP_WINDOW: float = 10.0
    # 单任务最大重试次数（超过后从持久化文件移除，不再重试）
    _MAX_RETRY: int = 3

    def __init__(self):
        self._queue: Queue = Queue()
        self._worker_thread: Optional[Thread] = None
        self._lock = Lock()
        self._running = False
        self._cancel_event = Event()
        self._processing_count: int = 0  # 当前正在处理中的任务数
        self._notify_callback: Optional[Callable[[Optional[str], str, str], None]] = None
        # 正在队列中等待或处理的分享码（全量去重）
        self._pending_codes: Set[str] = set()
        # 去重缓存：{share_code: 最后入队时间戳}，用于短期防抖
        self._recent_tasks: Dict[str, float] = {}
        # 持久化文件路径，在 start() 中通过 settings 初始化
        self._persist_path: Optional[Path] = None
        self._subtitle_jobs_path: Optional[Path] = None
        # 字幕后台指标
        self._subtitle_bg_active: int = 0
        self._subtitle_bg_started_total: int = 0
        self._subtitle_place_ok: int = 0
        self._subtitle_place_miss: int = 0
        self._last_finalize_seconds: float = 0.0

    # ── 持久化 ──────────────────────────────────────────────

    def _get_data_dir(self) -> Path:
        from app.core.config import settings
        data_dir = Path(settings.PLUGIN_DATA_PATH) / "P115ShareStrm"
        data_dir.mkdir(parents=True, exist_ok=True)
        return data_dir

    def _get_persist_path(self) -> Path:
        """懒加载持久化文件路径（避免模块加载时 settings 尚未就绪）"""
        if self._persist_path is None:
            self._persist_path = self._get_data_dir() / "pending_tasks.json"
        return self._persist_path

    def _get_subtitle_jobs_path(self) -> Path:
        if self._subtitle_jobs_path is None:
            self._subtitle_jobs_path = self._get_data_dir() / "subtitle_jobs.json"
        return self._subtitle_jobs_path

    def _load_json_list(self, path: Path) -> List[Dict]:
        try:
            if path.exists():
                return json.loads(path.read_text(encoding="utf-8"))
        except Exception as e:
            logger.warning(f"【P115ShareStrm】读取 {path.name} 失败，将重置: {e}")
        return []

    def _save_json_list(self, path: Path, items: List[Dict]) -> None:
        tmp = path.with_suffix(".tmp")
        try:
            tmp.write_text(json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8")
            tmp.replace(path)
        except Exception as e:
            logger.warning(f"【P115ShareStrm】写入 {path.name} 失败: {e}")

    def _load_tasks(self) -> List[Dict]:
        """从 JSON 文件加载持久化任务列表，文件不存在或损坏时返回空列表"""
        return self._load_json_list(self._get_persist_path())

    def _save_tasks(self, tasks: List[Dict]) -> None:
        """原子写入：先写 .tmp 再 rename，防止崩溃导致文件损坏"""
        self._save_json_list(self._get_persist_path(), tasks)

    def subtitle_job_upsert(self, job: Dict) -> None:
        with self._lock:
            jobs = self._load_json_list(self._get_subtitle_jobs_path())
            jobs = [j for j in jobs if j.get("job_id") != job.get("job_id")]
            jobs.append(job)
            self._save_json_list(self._get_subtitle_jobs_path(), jobs)

    def subtitle_job_update_stage(self, job_id: str, stage: str) -> None:
        with self._lock:
            jobs = self._load_json_list(self._get_subtitle_jobs_path())
            for j in jobs:
                if j.get("job_id") == job_id:
                    j["stage"] = stage
                    break
            self._save_json_list(self._get_subtitle_jobs_path(), jobs)

    def subtitle_job_remove(self, job_id: str) -> None:
        with self._lock:
            jobs = self._load_json_list(self._get_subtitle_jobs_path())
            jobs = [j for j in jobs if j.get("job_id") != job_id]
            self._save_json_list(self._get_subtitle_jobs_path(), jobs)

    def get_subtitle_jobs_active_count(self) -> int:
        with self._lock:
            jobs = self._load_json_list(self._get_subtitle_jobs_path())
            return len([
                j for j in jobs
                if j.get("stage") not in ("done", "failed")
            ])

    def metrics_subtitle_bg_inc(self) -> None:
        with self._lock:
            self._subtitle_bg_active += 1

    def metrics_subtitle_bg_dec(self) -> None:
        with self._lock:
            self._subtitle_bg_active = max(0, self._subtitle_bg_active - 1)

    def metrics_subtitle_bg_started(self) -> None:
        with self._lock:
            self._subtitle_bg_started_total += 1

    def metrics_add_place(self, ok: int, miss: int) -> None:
        with self._lock:
            self._subtitle_place_ok += max(0, ok)
            self._subtitle_place_miss += max(0, miss)

    def metrics_set_last_finalize_seconds(self, seconds: float) -> None:
        with self._lock:
            self._last_finalize_seconds = max(0.0, float(seconds))

    def get_metrics(self) -> Dict[str, Any]:
        with self._lock:
            base = api_metrics.snapshot()
            return {
                **base,
                "subtitle_bg_active": self._subtitle_bg_active,
                "subtitle_bg_started_total": self._subtitle_bg_started_total,
                "subtitle_place_ok": self._subtitle_place_ok,
                "subtitle_place_miss": self._subtitle_place_miss,
                "last_finalize_seconds": round(self._last_finalize_seconds, 1),
                "subtitle_jobs_pending": len([
                    j for j in self._load_json_list(self._get_subtitle_jobs_path())
                    if j.get("stage") not in ("done", "failed")
                ]),
            }

    def _restore_subtitle_jobs(self) -> None:
        jobs = self._load_json_list(self._get_subtitle_jobs_path())
        pending = [j for j in jobs if j.get("stage") not in ("done", "failed")]
        if not pending:
            return
        logger.info(f"【P115ShareStrm】从持久化恢复 {len(pending)} 个字幕收尾任务")
        for job in pending:
            try:
                stage = job.get("stage") or ""
                if stage == "receive_limited":
                    try:
                        retry_hours = max(1, int(configer.share_receive_retry_hours or 3))
                    except (TypeError, ValueError):
                        retry_hours = 3
                    logger.info(
                        f"【P115ShareStrm】恢复限制接收字幕任务，{retry_hours}h 后重试: "
                        f"{job.get('share_code')} (job={job.get('job_id')})"
                    )
                    _schedule_subtitle_receive_retry(
                        job["job_id"],
                        job["share_code"],
                        job["receive_code"],
                        job.get("subtitle_files") or [],
                        Path(job.get("save_path") or configer.strm_save_path),
                        job.get("user_id"),
                        job.get("media_files") or [],
                        _deserialize_stem_paths(job.get("media_stem_to_strm_path") or {}),
                        {
                            k: Path(v)
                            for k, v in (job.get("media_stem_to_target") or {}).items()
                        },
                        retry_hours,
                    )
                    continue
                retry = int(job.get("retry_count", 0) or 0)
                if retry >= self._MAX_RETRY:
                    logger.warning(
                        f"【P115ShareStrm】字幕收尾任务已达最大重试，放弃: {job.get('share_code')}"
                    )
                    self.subtitle_job_remove(job["job_id"])
                    continue
                job["retry_count"] = retry + 1
                self.subtitle_job_upsert(job)
                _start_subtitle_finalize_job(
                    job["share_code"],
                    job["receive_code"],
                    job.get("subtitle_files") or [],
                    Path(job.get("save_path") or configer.strm_save_path),
                    job.get("user_id"),
                    job.get("media_files") or [],
                    _deserialize_stem_paths(job.get("media_stem_to_strm_path") or {}),
                    {
                        k: Path(v)
                        for k, v in (job.get("media_stem_to_target") or {}).items()
                    },
                    job_id=job["job_id"],
                    retry_count=job["retry_count"],
                )
            except Exception as e:
                logger.warning(f"【P115ShareStrm】恢复字幕收尾任务失败: {e}")

    def _persist_add(self, task_id: str, share_code: str, receive_code: str,
                     user_id: Optional[str], tmdbid: Optional[int], mtype: Optional[str],
                     arg_str: Optional[str] = None, sub_only: bool = False) -> None:
        """向持久化文件追加一条任务（需在 _lock 内调用）"""
        tasks = self._load_tasks()
        tasks.append({
            "task_id": task_id,
            "share_code": share_code,
            "receive_code": receive_code,
            "user_id": user_id,
            "tmdbid": tmdbid,
            "mtype": mtype,
            "arg_str": arg_str,
            "sub_only": sub_only,
            "added_at": time(),
            "retry_count": 0,
        })
        self._save_tasks(tasks)

    def _persist_remove(self, task_id: str) -> None:
        """从持久化文件移除已完成/放弃的任务"""
        with self._lock:
            tasks = self._load_tasks()
            tasks = [t for t in tasks if t.get("task_id") != task_id]
            self._save_tasks(tasks)

    def _persist_increment_retry(self, task_id: str) -> int:
        """将任务重试次数 +1，返回更新后的重试次数"""
        with self._lock:
            tasks = self._load_tasks()
            count = 0
            for t in tasks:
                if t.get("task_id") == task_id:
                    t["retry_count"] = t.get("retry_count", 0) + 1
                    count = t["retry_count"]
                    break
            self._save_tasks(tasks)
        return count

    # ── 公共接口 ─────────────────────────────────────────────

    def set_notify_callback(
        self, callback: Callable[[Optional[str], str, str], None]
    ):
        """设置通知回调，由 __init__.py 注入，签名: (user_id, title, text) -> None"""
        self._notify_callback = callback

    def start(self):
        """
        启动工作线程（幂等）。
        启动前先从持久化文件恢复未完成任务，重新入队。
        """
        with self._lock:
            if self._running and self._worker_thread and self._worker_thread.is_alive():
                return

        if self._worker_thread and self._worker_thread.is_alive():
            self.stop()

        with self._lock:
            self._cancel_event.clear()
            self._running = True
            self._worker_thread = Thread(target=self._worker, daemon=True)
            self._worker_thread.start()
            logger.info("【P115ShareStrm】任务队列工作线程已启动")

        # 恢复持久化任务（在 _lock 外执行，避免死锁）
        self._restore_persisted_tasks()
        self._restore_subtitle_jobs()

    def stop(self):
        """停止工作线程，并中断整理等待等长耗时操作"""
        self._running = False
        self._cancel_event.set()
        worker = self._worker_thread
        if worker and worker.is_alive() and worker is not Thread.current_thread():
            worker.join(timeout=5)

    def add_task(
        self,
        share_code: str,
        receive_code: str,
        user_id: Optional[str] = None,
        tmdbid: Optional[int] = None,
        mtype: Optional[str] = None,
        arg_str: Optional[str] = None,
        sub_only: bool = False,
        _task_id: Optional[str] = None,
        _skip_dedup: bool = False,
    ) -> bool:
        """
        向队列添加分享处理任务。

        内置去重：同一 share_code 在 _DEDUP_WINDOW 秒内重复入队时直接忽略。
        _task_id / _skip_dedup 供内部恢复任务使用，外部调用无需传入。

        :return: True 表示成功入队，False 表示被去重过滤
        """
        now = time()
        task_id = _task_id or str(uuid4())

        with self._lock:
            # 1. 全量去重：如果已经在队列中，直接忽略
            if share_code in self._pending_codes:
                logger.warning(f"【P115ShareStrm】任务已在队列中，忽略重复入队: {share_code}")
                return False

            # 2. 短期防抖：同一 share_code 在 _DEDUP_WINDOW 秒内重复入队时忽略
            if not _skip_dedup:
                last_time = self._recent_tasks.get(share_code)
                if last_time is not None and now - last_time < self._DEDUP_WINDOW:
                    logger.warning(
                        f"【P115ShareStrm】触发防抖限制（窗口 {self._DEDUP_WINDOW}s），忽略: {share_code}"
                    )
                    return False
                self._recent_tasks[share_code] = now
                # 新任务才写持久化，恢复任务已在文件中
                self._persist_add(task_id, share_code, receive_code, user_id, tmdbid, mtype, arg_str, sub_only)

            self._pending_codes.add(share_code)

        self._queue.put((task_id, share_code, receive_code, user_id, tmdbid, mtype, arg_str, sub_only))
        logger.info(f"【P115ShareStrm】新任务已入队: {share_code} (id={task_id})，当前队列待处理: {self._queue.qsize()}")
        return True

    def _restore_persisted_tasks(self) -> None:
        """从持久化文件恢复未完成任务，重新放入内存队列"""
        tasks = self._load_tasks()
        if not tasks:
            return
        logger.info(f"【P115ShareStrm】从持久化文件恢复 {len(tasks)} 个待处理任务")
        for t in tasks:
            share_code = t.get("share_code")
            if t.get("retry_count", 0) >= self._MAX_RETRY:
                logger.warning(
                    f"【P115ShareStrm】任务已达最大重试次数，跳过: {share_code} (id={t.get('task_id')})"
                )
                self._persist_remove(t["task_id"])
                continue
            
            with self._lock:
                self._pending_codes.add(share_code)
                
            self._queue.put((
                t["task_id"],
                share_code,
                t["receive_code"],
                t.get("user_id"),
                t.get("tmdbid"),
                t.get("mtype"),
                t.get("arg_str"),
                t.get("sub_only", False),
            ))
            logger.info(f"【P115ShareStrm】已恢复任务: {share_code} (重试次数: {t.get('retry_count', 0)})")

    def get_queue_size(self) -> int:
        """返回当前队列待处理任务数"""
        return self._queue.qsize()

    # ── 工作线程 ──────────────────────────────────────────────

    def _worker(self):
        while self._running:
            try:
                task = self._queue.get(timeout=10)
                # 兼容旧版本未带 sub_only 的情况
                if len(task) == 7:
                    task_id, share_code, receive_code, user_id, tmdbid, mtype, arg_str = task
                    sub_only = False
                else:
                    task_id, share_code, receive_code, user_id, tmdbid, mtype, arg_str, sub_only = task

                # 稍作等待，确保"已入队"通知先到达用户（积压时减小延迟）
                if self._queue.qsize() > 10:
                    sleep(0.2)
                else:
                    sleep(0.8)
                self._processing_count += 1
                
                start_msg = f"🚀 开始处理分享: {share_code}"
                if sub_only:
                    start_msg = f"🚀 开始重试下载分享字幕: {share_code}"
                self._notify(user_id, "【115分享STRM】", start_msg)

                result = process_share_strm(share_code, receive_code, tmdbid=tmdbid, mtype=mtype, arg_str=arg_str, user_id=user_id, sub_only=sub_only)

                if result.get("status"):
                    if sub_only:
                        if result.get("subtitle_in_background"):
                            msg = f"⏳ 字幕重新下载已转为后台收尾: {share_code}\n（STRM 已完成 / 字幕后台处理中）"
                        else:
                            msg = (
                                f"✅ 字幕重新下载完成: {share_code}\n"
                                f"遍历文件: {result.get('total_files')} 个\n"
                                f"成功下载: {result.get('subtitle_count', 0)} 个"
                            )
                            if result.get("subtitle_fail_count"):
                                msg += f"，失败 {result.get('subtitle_fail_count')} 个"
                    else:
                        msg = (
                            f"✅ 处理完成: {share_code}\n"
                            f"生成 STRM: {result.get('strm_count')} 个\n"
                            f"遍历文件: {result.get('total_files')} 个"
                        )
                        if result.get("subtitle_count") or result.get("subtitle_fail_count"):
                            msg += f"\n下载字幕: {result.get('subtitle_count', 0)} 个"
                            if result.get("subtitle_fail_count"):
                                msg += f"，失败 {result.get('subtitle_fail_count')} 个"
                        if result.get("subtitle_in_background"):
                            msg += f"\n⏳ STRM 已完成 / 字幕后台处理中（审核通过并整理后自动放置）"

                    buttons = None
                    if not sub_only and result.get("subtitle_fail_count", 0) > 0:
                        buttons = [[{
                            "text": "🔁 重试下载字幕",
                            "callback_data": f"[PLUGIN]p115sharestrm|retry_sub:{share_code}:{receive_code}"
                        }]]

                    self._notify(user_id, "【115分享STRM】完成", msg, buttons=buttons)
                    # 成功：从持久化文件移除
                    self._persist_remove(task_id)
                else:
                    if result.get("retryable"):
                        retry_count = self._persist_increment_retry(task_id)
                        if retry_count >= self._MAX_RETRY:
                            logger.error(
                                f"【P115ShareStrm】115 风控重试已达上限 ({self._MAX_RETRY})，放弃: {share_code}"
                            )
                            self._persist_remove(task_id)
                            self._notify(
                                user_id,
                                "【115分享STRM】放弃",
                                f"❌ {share_code}\n115 接口风控，已重试 {self._MAX_RETRY} 次\n原因: {result.get('msg')}",
                            )
                        else:
                            self._notify(
                                user_id,
                                "【115分享STRM】风控等待",
                                f"⏳ {share_code}\n115 接口限流，将在重启后自动重试 "
                                f"(第 {retry_count}/{self._MAX_RETRY} 次)\n{result.get('msg')}",
                            )
                    elif result.get("terminal"):
                        logger.warning(f"【P115ShareStrm】任务遇到不可恢复错误，放弃: {share_code}")
                        self._persist_remove(task_id)
                        self._notify(
                            user_id,
                            "【115分享STRM】放弃",
                            f"❌ {share_code}\n原因: {result.get('msg')}",
                        )
                    else:
                        retry_count = self._persist_increment_retry(task_id)
                        if retry_count >= self._MAX_RETRY:
                            logger.error(
                                f"【P115ShareStrm】任务失败且已达最大重试次数 ({self._MAX_RETRY})，放弃: {share_code}"
                            )
                            self._persist_remove(task_id)
                            self._notify(
                                user_id,
                                "【115分享STRM】放弃",
                                f"❌ {share_code}\n已重试 {self._MAX_RETRY} 次，放弃处理\n原因: {result.get('msg')}",
                            )
                        else:
                            self._notify(
                                user_id,
                                "【115分享STRM】失败",
                                f"❌ {share_code}\n原因: {result.get('msg')}\n将在重启后自动重试 (第 {retry_count}/{self._MAX_RETRY} 次)",
                            )

                with self._lock:
                    self._pending_codes.discard(share_code)

                self._queue.task_done()
                self._processing_count = max(0, self._processing_count - 1)
                
                # 动态调整任务间隔：积压时加快速度
                qsize = self._queue.qsize()
                if qsize > 10:
                    sleep(0.5)
                else:
                    sleep(2)

            except Empty:
                continue
            except Exception as e:
                logger.error(f"【P115ShareStrm】工作线程异常: {e}", exc_info=True)
                sleep(5)

    def _notify(self, user_id: Optional[str], title: str, text: str, buttons: Optional[List[List[dict]]] = None):
        if self._notify_callback:
            try:
                import inspect
                sig = inspect.signature(self._notify_callback)
                if "buttons" in sig.parameters:
                    self._notify_callback(user_id, title, text, buttons)
                else:
                    self._notify_callback(user_id, title, text)
            except Exception as e:
                logger.warning(f"【P115ShareStrm】通知发送失败: {e}")


# 全局单例
task_queue = ShareTaskQueue()
