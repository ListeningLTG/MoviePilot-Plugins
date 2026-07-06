import re
from pathlib import Path
from urllib.parse import quote
from time import sleep, time
from threading import Thread, Lock, Event
from typing import Dict, Any, List, Optional, Iterator, Callable, Tuple
from queue import Queue, Empty
from uuid import uuid4
import json
import shutil

from p115client import P115Client, check_response
from p115client.util import complete_url
from p115client.tool.attr import normalize_attr
from p115client.tool.iterdir import _iter_fs_files

from app.log import logger
from app.chain.transfer import TransferChain
from app.schemas import FileItem

from .config import configer
from .utils import StrmUrlTemplateResolver



class ShareLinkExpiredError(Exception):
    """分享链接已失效或被拒绝"""
    pass


from app.db import db_query


@db_query
def _fuzzy_query_transfer_history(db, suffix: str) -> Optional[Any]:
    from app.db.models.transferhistory import TransferHistory
    return db.query(TransferHistory).filter(
        TransferHistory.src.like(f"%{suffix}"),
        TransferHistory.src_storage == "local"
    ).first()


def _get_transfer_history_by_strm_path(th_oper, strm_path: Path) -> Optional[Any]:
    """
    根据 STRM 路径从 TransferHistory 中检索整理记录。
    支持精确匹配和模糊匹配（兼容被 115 屏蔽/重命名导致父目录名称不一致的情况，如 "豆瓣..." -> "豆***..."）。
    """
    # 1. 首先尝试精确匹配
    src_posix = strm_path.as_posix()
    history = th_oper.get_by_src(src_posix, "local")
    if history and history.dest:
        return history

    # 2. 如果精确匹配失败，尝试进行后缀模糊匹配
    # 模糊匹配末尾两级路径: /media_folder/filename.strm
    parts = strm_path.parts
    if len(parts) >= 2:
        suffix = f"/{parts[-2]}/{parts[-1]}"
        history = _fuzzy_query_transfer_history(None, suffix)
        if history and history.dest:
            logger.info(f"【P115ShareStrm】通过两级路径后缀模糊匹配成功: {strm_path.name} -> {history.src}")
            return history

    # 3. 如果依然未匹配成功，尝试仅通过文件名匹配
    if len(parts) >= 1:
        suffix = f"/{parts[-1]}"
        history = _fuzzy_query_transfer_history(None, suffix)
        if history and history.dest:
            logger.info(f"【P115ShareStrm】通过文件名后缀模糊匹配成功: {strm_path.name} -> {history.src}")
            return history

    return None


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


class _EndpointPool:
    """
    轮询多个 115 API 端点，单次请求依次尝试所有端点，成功后自动推进轮询位置。
    """

    def __init__(self, endpoints: List[Tuple[str, Callable]]) -> None:
        # endpoints: list of (name, callable(payload) -> resp)
        self._endpoints = endpoints
        self._idx = 0

    def fetch_page(self, payload: dict, max_retries: int = 5) -> list:
        """
        获取一页数据，轮询端点并在全部失败时退避重试。

        :return: 文件/目录列表
        :raises: 最后一次异常（当所有端点在所有重试中均失败时）
        """
        n = len(self._endpoints)
        last_exc: Optional[Exception] = None

        for attempt in range(max_retries):
            for i in range(n):
                ep_idx = (self._idx + i) % n
                ep_name, ep_func = self._endpoints[ep_idx]
                try:
                    resp = ep_func(payload)
                    data = check_response(resp).get("data", {})
                    # 成功：推进到下一个端点，保持轮询效果
                    self._idx = (ep_idx + 1) % n
                    last_exc = None
                    return data.get("list", [])
                except Exception as e:
                    last_exc = e
                    # 4100009: 链接已失效 / 分享已拒绝
                    # 4100010: 分享已取消
                    # 均属于不可恢复错误，直接终止重试
                    if "4100009" in str(e) or "4100010" in str(e):
                        raise ShareLinkExpiredError(f"分享链接已失效或已取消 ({e})") from e
                    logger.warning(
                        f"【P115ShareStrm】端点 [{ep_name}] 失败，切换到下一个: {e}"
                    )

            # 当前轮次所有端点均失败，等待后重试
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 2 + (time() % 2)
                logger.warning(
                    f"【P115ShareStrm】所有端点均失败 (cid={payload.get('cid')}, "
                    f"offset={payload.get('offset')})，"
                    f"第 {attempt + 1}/{max_retries} 次重试 (等待 {wait_time:.1f}s)"
                )
                sleep(wait_time)

        raise last_exc


def _make_endpoint_pool(client: ShareP115Client) -> _EndpointPool:
    """
    构建三端点轮询池：
      1. https://proapi.115.com  (App 接口)
      2. http://pro.api.115.com  (App 接口，HTTP)
      3. https://webapi.115.com  (Cookie 接口)

    注意：p115client 底层使用 httpx，timeout 必须通过 extensions={"timeout": {...}}
    传入，不能使用 requests 风格的 (connect, read) 元组。
    """
    _timeout = {"connect": 10, "pool": 10, "read": 60, "write": 60}
    custom_headers = {
        "User-Agent": (
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) "
            "AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/21E219 "
            "115wangpan_ios/36.2.20"
        ),
    }
    return _EndpointPool([
        (
            "share_snap_app_https",
            lambda p: client.share_snap_app(
                p, base_url="https://proapi.115.com",
                headers=custom_headers,
                extensions={"timeout": _timeout},
            ),
        ),
        (
            "share_snap_app_http",
            lambda p: client.share_snap_app(
                p, base_url="http://pro.api.115.com",
                headers=custom_headers,
                extensions={"timeout": _timeout},
            ),
        ),
        (
            "share_snap_cookie",
            lambda p: client.share_snap_cookie(
                p, headers=custom_headers,
                extensions={"timeout": _timeout},
            ),
        ),
    ])


def iter_share_files(
    client: ShareP115Client,
    share_code: str,
    receive_code: str = "",
    cid: int = 0,
    path_prefix: str = "",
    max_retries: int = 5,
    _pool: Optional[_EndpointPool] = None,
) -> Iterator[dict]:
    """
    递归遍历分享链接下的所有文件，轮询三个 115 API 端点并自动容错切换。

    端点优先级（轮询）：
      proapi.115.com (App/HTTPS) → pro.api.115.com (App/HTTP) → webapi.115.com (Cookie)
    """
    if _pool is None:
        _pool = _make_endpoint_pool(client)

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
            items = _pool.fetch_page(payload, max_retries=max_retries)
        except Exception as e:
            logger.error(f"【P115ShareStrm】请求分享列表失败，已达最大重试次数: {e}")
            raise

        if not items:
            break

        for item in items:
            item = normalize_attr(item)
            name = item.get("name", "")
            current_path = f"{path_prefix}/{name}" if path_prefix else f"/{name}"
            if item.get("is_dir"):
                if name.upper() in ("BDMV", "CERTIFICATE"):
                    logger.info(f"【P115ShareStrm】检测到蓝光原盘标志性目录 '{name}' (路径: {current_path})，跳过该目录的遍历")
                    continue
                # 递归进入子目录，共享同一轮询池
                yield from iter_share_files(
                    client, share_code, receive_code, int(item["id"]), current_path,
                    max_retries, _pool,
                )
            else:
                item["_full_path"] = current_path
                yield item

        offset += limit
        # 如果当前页返回的数据少于 limit，说明已经是最后一页
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
            resp = client.fs_mkdir(f"subtitle-{uuid4()}", headers=custom_headers)
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
            resp = client.share_receive(payload, headers=custom_headers)
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
                    attr = next(
                        _iter_fs_files(
                            client=client,
                            payload=scid,
                            page_size=1,
                            app="web",
                            headers=custom_headers,
                        )
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
                    tmp_files = list(_iter_fs_files(client=client, payload=scid, page_size=10, app="web", headers=custom_headers))
                    logger.warning(f"【P115ShareStrm】临时目录下实际存在的文件列表: {tmp_files}")
                except Exception as list_err:
                    logger.warning(f"【P115ShareStrm】尝试列出临时目录内容失败: {list_err}")
                raise RuntimeError("115 转存文件超时，临时文件夹中未发现字幕文件")

            # 4. 调用 fs_video_subtitle 获取下载链接
            logger.info(f"【P115ShareStrm】通过 fs_video_subtitle 批量获取字幕下载链接，使用 pickcode: {attr['pickcode']}")
            resp = client.fs_video_subtitle(attr["pickcode"], headers=custom_headers)
            check_response(resp)
            logger.debug(f"【P115ShareStrm】fs_video_subtitle 接口返回数据: {resp}")

            # 列出转存到临时文件夹下的所有文件的 sha1 -> pickcode 映射（用于兜底下载）
            tmp_files_map = {}
            try:
                tmp_files = list(_iter_fs_files(client=client, payload=scid, page_size=100, app="web", headers=custom_headers))
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
                            url = str(client.download_url(tmp_pickcode, user_agent=custom_headers.get("User-Agent")))
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

        except Exception as e:
            logger.error(f"【P115ShareStrm】字幕批处理失败: {e}", exc_info=True)
            # 本批次中尚未成功下载的全部计为失败
            batch_downloaded = len([p for p in downloaded_paths if p not in downloaded_paths[:batch_start]])
            fail_count += len(batch_items) - batch_downloaded
        finally:
            # 7. 清理临时文件夹
            if scid:
                try:
                    logger.info(f"【P115ShareStrm】正在清理网盘临时目录 {scid}...")
                    resp_del = client.fs_delete(scid, headers=custom_headers)
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
        resp = client.share_snap_app(payload, app="android", headers=custom_headers, timeout=15)
        data = resp.get("data", {})
        share_info = data.get("shareinfo", data.get("share_info", {}))
        share_state = data.get("share_state", share_info.get("share_state", share_info.get("status")))
        if share_state is not None:
            return int(share_state)
    except Exception as e:
        logger.warning(f"【P115ShareStrm】获取分享链接状态失败: {e}")
    return None


def _background_download_subtitles(
    client: ShareP115Client,
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
    后台轮询等待分享链接审核通过，然后执行字幕下载与整理（防止堵塞主队列工作线程）
    """
    max_attempts = 36  # 36次 * 5分钟 = 3小时
    logger.info(f"【P115ShareStrm】已为 {share_code} 启动字幕后台轮询下载任务...")

    for attempt in range(1, max_attempts + 1):
        # 动态前置等待：前两次分别等 1 分钟、2 分钟，后续每 5 分钟检查一次
        if attempt == 1:
            sleep(60)
        elif attempt == 2:
            sleep(120)
        else:
            sleep(300)

        logger.info(f"【P115ShareStrm】字幕下载后台轮询：正在进行第 {attempt}/{max_attempts} 次状态检查: {share_code}")
        state = _get_share_state(client, share_code, receive_code)

        if state is not None and state != 0:
            if state == 7:
                logger.warning(f"【P115ShareStrm】字幕后台轮询发现链接已过期: {share_code}")
                task_queue._notify(user_id, "【115分享STRM】字幕下载失败", f"❌ {share_code}\n原因: 链接已失效或在审核期间过期")
                return

            logger.info(f"【P115ShareStrm】字幕后台轮询检测到审核已通过！开始下载字幕: {share_code}")
            try:
                downloaded_subtitle_paths, subtitle_fail_count = _download_subtitles_from_share(
                    client, share_code, receive_code, subtitle_files, save_path_obj
                )
                subtitle_count = len(downloaded_subtitle_paths)

                if downloaded_subtitle_paths:
                    # 动态重新尝试从历史数据库中获取未命中的整理目标路径
                    try:
                        from app.db.transferhistory_oper import TransferHistoryOper
                        _th_oper = TransferHistoryOper()
                    except Exception as th_err:
                        logger.warning(f"【P115ShareStrm】后台整理历史数据库连接失败: {th_err}")
                        _th_oper = None

                    # 预建 "分享内父目录" → "该目录下媒体stem列表" 的索引
                    dir_to_media_stems: Dict[str, List[str]] = {}
                    for m_item in media_files:
                        m_path = m_item.get("_full_path", m_item.get("name", ""))
                        m_parent = str(Path(m_path).parent)
                        m_stem = Path(m_item.get("name", "")).stem
                        if m_parent not in dir_to_media_stems:
                            dir_to_media_stems[m_parent] = []
                        dir_to_media_stems[m_parent].append(m_stem)

                    for sub_item, local_path in zip(subtitle_files, downloaded_subtitle_paths):
                        sub_full_path = sub_item.get("_full_path", sub_item.get("name", ""))
                        sub_name = Path(sub_full_path).name
                        sub_parent = str(Path(sub_full_path).parent)

                        strm_target = None
                        candidate_stems = dir_to_media_stems.get(sub_parent, [])

                        # 挑选对应的 media_stem
                        matched_stem = None
                        if len(candidate_stems) == 1:
                            matched_stem = candidate_stems[0]
                        elif len(candidate_stems) > 1:
                            ep_match = re.search(r"[Ss](\d+)[Ee](\d+)", sub_name)
                            if ep_match:
                                for cstem in candidate_stems:
                                    m_ep = re.search(r"[Ss](\d+)[Ee](\d+)", cstem)
                                    if m_ep and m_ep.group(1) == ep_match.group(1) and m_ep.group(2) == ep_match.group(2):
                                        matched_stem = cstem
                                        break
                            if not matched_stem:
                                best = max(
                                    candidate_stems,
                                    key=lambda s: len(s) if s in sub_name else 0,
                                )
                                if best in sub_name:
                                    matched_stem = best
                            if not matched_stem:
                                matched_stem = candidate_stems[0]

                        if matched_stem:
                            strm_target = media_stem_to_target.get(matched_stem)
                            # 如果先前没有获取到整理目标路径，尝试直接到数据库检索
                            if not strm_target and _th_oper:
                                strm_path = media_stem_to_strm_path.get(matched_stem)
                                if strm_path:
                                    history = _get_transfer_history_by_strm_path(_th_oper, strm_path)
                                    if history and history.dest:
                                        strm_target = Path(history.dest)
                                        # 缓存在内存中
                                        media_stem_to_target[matched_stem] = strm_target

                        if strm_target:
                            lang_tag = _extract_subtitle_lang_tag(sub_name)
                            new_name = _truncate_filename(strm_target.stem + lang_tag + local_path.suffix)
                            dest_path = strm_target.parent / new_name
                            try:
                                if dest_path.exists() and dest_path.read_bytes() == local_path.read_bytes():
                                    logger.debug(f"【P115ShareStrm】字幕已存在且内容相同，跳过: {new_name}")
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
                            except Exception as e:
                                logger.warning(f"【P115ShareStrm】字幕放置失败 {sub_name}: {e}")
                        else:
                            logger.warning(f"【P115ShareStrm】未找到对应 STRM 目标路径，跳过字幕: {sub_name}")

                msg = f"✅ 审核通过，字幕已后台自动下载并整理完成: {share_code}\n成功下载: {subtitle_count} 个"
                if subtitle_fail_count:
                    msg += f"，失败 {subtitle_fail_count} 个"

                task_queue._notify(user_id, "【115分享STRM】字幕自动下载成功", msg)
                return
            except Exception as e:
                logger.error(f"【P115ShareStrm】字幕后台下载处理异常: {e}", exc_info=True)

    logger.warning(f"【P115ShareStrm】字幕下载后台轮询超时 (3小时): {share_code}")
    task_queue._notify(user_id, "【115分享STRM】字幕下载失败", f"❌ {share_code}\n原因: 链接审核轮询超时 (3小时)")


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

        # 构建字幕文件后缀集合（按需）
        subtitle_exts: set = set()
        if configer.download_subtitle:
            subtitle_exts = {
                f".{ext.strip().lower()}"
                for ext in configer.user_subtitle_ext.split(",")
                if ext.strip()
            }

        # 1. 第一步：递归扫描获取分享中的所有目标媒体文件和字幕文件
        logger.info(f"【P115ShareStrm】正在扫描分享内容: {share_code} ...")
        subtitle_files: List[dict] = []
        for item in iter_share_files(client, share_code, receive_code):
            filename = item.get("name", "")
            file_ext = Path(filename).suffix.lower()
            if file_ext in media_exts:
                media_files.append(item)
            elif subtitle_exts and file_ext in subtitle_exts:
                subtitle_files.append(item)

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


        # 4. 第四步：全部生成完毕后，如果是全量任务，批量交由 MoviePilot 整理 STRM；如果是字幕重试任务，直接从历史库查出整理目标
        # strm 本地源路径（posix）→ MoviePilot 整理后目标路径，用于字幕直接放置
        strm_source_to_target: Dict[str, Path] = {}
        media_stem_to_target: Dict[str, Path] = {}
        if configer.moviepilot_transfer:
            from app.db.transferhistory_oper import TransferHistoryOper
            _th_oper = TransferHistoryOper()

            if not sub_only and generated_strm_paths:
                logger.info(f"【P115ShareStrm】开始批量整理 STRM，共 {len(generated_strm_paths)} 个文件")
                transfer_chain = TransferChain()
                # 1. 全部异步入队
                for strm_file_path in generated_strm_paths:
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
                logger.info("【P115ShareStrm】STRM 已全部入队，等待整理完成...")

                # 2. 轮询等待所有 STRM 整理完成
                # 优化：只有在需要下载并放置字幕，且本次分享确实包含字幕文件时，才需要等待以获取目标路径
                need_wait = configer.download_subtitle and subtitle_files
                
                pending = set(strm_file_path.as_posix() for strm_file_path in generated_strm_paths)
                
                # 即使不需要等待放置字幕，也可以先通过预扫描清理掉已经整理过的记录（保持状态准确）
                for src in list(pending):
                    history = _get_transfer_history_by_strm_path(_th_oper, Path(src))
                    if history and history.dest:
                        strm_source_to_target[src] = Path(history.dest)
                        pending.remove(src)
                
                if pending:
                    if need_wait:
                        # 动态调整超时时间：如果队列积压严重，缩短等待时间
                        qsize = task_queue.get_queue_size()
                        base_timeout = configer.wait_organize_timeout
                        if qsize > 50:
                            timeout = min(base_timeout, 30)
                        elif qsize > 20:
                            timeout = min(base_timeout, 60)
                        elif qsize > 10:
                            timeout = min(base_timeout, 120)
                        else:
                            timeout = base_timeout

                        logger.info(f"【P115ShareStrm】剩余 {len(pending)} 个文件等待整理 (积压: {qsize}，超时: {timeout}s)...")
                        interval = 2
                        waited = 0
                        while pending and waited < timeout:
                            finished = set()
                            for src in pending:
                                history = _get_transfer_history_by_strm_path(_th_oper, Path(src))
                                if history and history.dest:
                                    strm_source_to_target[src] = Path(history.dest)
                                    finished.add(src)
                            
                            if finished:
                                logger.info(f"【P115ShareStrm】整理完成新进度: {len(finished)} 个")
                                pending -= finished
                            
                            if pending:
                                sleep(interval)
                                waited += interval
                        
                        if pending:
                            if waited >= timeout:
                                logger.warning(f"【P115ShareStrm】STRM 整理等待超时 ({timeout}s)，跳过后续等待（STRM 将在后台继续整理，但字幕可能无法自动归位）: {pending}")
                            else:
                                logger.warning(f"【P115ShareStrm】部分 STRM 整理异常未完成: {pending}")
                        else:
                            logger.info("【P115ShareStrm】STRM 批量整理全部完成")
                    else:
                        logger.info("【P115ShareStrm】无需放置字幕，跳过整理等待")
                else:
                    logger.info("【P115ShareStrm】所有文件均已整理过，无需等待")
            elif sub_only:
                # 在 sub_only 模式下，直接在 TransferHistory 中查找每个 strm_file_path 的整理位置
                logger.info("【P115ShareStrm】仅重试下载字幕模式：直接检索历史整理记录...")
                for stem, strm_path in media_stem_to_strm_path.items():
                    history = _get_transfer_history_by_strm_path(_th_oper, strm_path)
                    if history and history.dest:
                        strm_source_to_target[strm_path.as_posix()] = Path(history.dest)
                logger.info(f"【P115ShareStrm】历史整理记录检索完成，成功匹配到 {len(strm_source_to_target)}/{len(media_stem_to_strm_path)} 个目标路径")

            # 3. 构建 媒体 stem → STRM 整理目标路径 映射，供字幕放置使用
            for stem, strm_path in media_stem_to_strm_path.items():
                target = strm_source_to_target.get(strm_path.as_posix())
                if target:
                    media_stem_to_target[stem] = target

        # 5. 第五步：下载字幕文件并放置（如开启）
        subtitle_count = 0
        subtitle_fail_count = 0
        subtitle_in_background = False
        if configer.download_subtitle and subtitle_files:
            share_state = _get_share_state(client, share_code, receive_code)
            if share_state == 0:
                logger.info(f"【P115ShareStrm】检测到分享链接 {share_code} 处于审核状态中，启动后台轮询下载字幕任务...")
                subtitle_in_background = True
                t = Thread(
                    target=_background_download_subtitles,
                    args=(
                        ShareP115Client(configer.cookies),
                        share_code,
                        receive_code,
                        subtitle_files,
                        save_path_obj,
                        user_id,
                        media_files,
                        media_stem_to_strm_path,
                        media_stem_to_target,
                    ),
                    daemon=True
                )
                t.start()
            else:
                logger.info(f"【P115ShareStrm】开始下载字幕文件，共 {len(subtitle_files)} 个")
                downloaded_subtitle_paths, subtitle_fail_count = _download_subtitles_from_share(
                    client, share_code, receive_code, subtitle_files, save_path_obj
                )
                subtitle_count = len(downloaded_subtitle_paths)
                logger.info(
                    f"【P115ShareStrm】字幕下载完成，成功 {subtitle_count} 个，失败 {subtitle_fail_count} 个"
                )
                if downloaded_subtitle_paths and configer.moviepilot_transfer and media_stem_to_target:
                    # 直接将字幕复制到 STRM 整理目标目录，文件名以整理后的 STRM stem 为前缀，
                    # 避免 ffprobe 等插件造成 STRM 与字幕文件名前缀不一致的问题
                    #
                    # 改进：不再使用基于纯文件名的 name_map（同名字幕会互相覆盖映射），
                    # 而是直接通过字幕条目的 _full_path 父目录匹配同目录的媒体文件 stem，
                    # 从而精确定位每个字幕对应的媒体目标路径。
                    #
                    # 预建 "分享内父目录" → "该目录下媒体stem列表" 的索引
                    dir_to_media_stems: Dict[str, List[str]] = {}
                    for m_item in media_files:
                        m_path = m_item.get("_full_path", m_item.get("name", ""))
                        m_parent = str(Path(m_path).parent)
                        m_stem = Path(m_item.get("name", "")).stem
                        if m_parent not in dir_to_media_stems:
                            dir_to_media_stems[m_parent] = []
                        dir_to_media_stems[m_parent].append(m_stem)

                    for sub_item, local_path in zip(subtitle_files, downloaded_subtitle_paths):
                        sub_full_path = sub_item.get("_full_path", sub_item.get("name", ""))
                        sub_name = Path(sub_full_path).name
                        sub_parent = str(Path(sub_full_path).parent)

                        # 在同目录下寻找媒体文件 stem，确定字幕放置目标
                        strm_target = None
                        candidate_stems = dir_to_media_stems.get(sub_parent, [])
                        if len(candidate_stems) == 1:
                            # 同目录只有一个媒体文件，直接关联
                            strm_target = media_stem_to_target.get(candidate_stems[0])
                        elif len(candidate_stems) > 1:
                            # 多媒体文件：先尝试集号匹配，再尝试公共前缀匹配
                            ep_match = re.search(r"[Ss](\d+)[Ee](\d+)", sub_name)
                            if ep_match:
                                for cstem in candidate_stems:
                                    m_ep = re.search(r"[Ss](\d+)[Ee](\d+)", cstem)
                                    if m_ep and m_ep.group(1) == ep_match.group(1) and m_ep.group(2) == ep_match.group(2):
                                        strm_target = media_stem_to_target.get(cstem)
                                        break
                            if not strm_target:
                                # 回退：选公共前缀最长的
                                best = max(
                                    candidate_stems,
                                    key=lambda s: len(s) if s in sub_name else 0,
                                )
                                if best in sub_name:
                                    strm_target = media_stem_to_target.get(best)
                            if not strm_target:
                                # 最终兜底：取第一个
                                strm_target = media_stem_to_target.get(candidate_stems[0])

                        if strm_target:
                            lang_tag = _extract_subtitle_lang_tag(sub_name)
                            new_name = _truncate_filename(strm_target.stem + lang_tag + local_path.suffix)
                            dest_path = strm_target.parent / new_name
                            # 如果目标已存在且内容完全相同，则跳过（避免重复运行产生大量带序号的重复字幕）
                            try:
                                if dest_path.exists() and dest_path.read_bytes() == local_path.read_bytes():
                                    logger.debug(f"【P115ShareStrm】字幕已存在且内容相同，跳过: {new_name}")
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
                            except Exception as e:
                                logger.warning(f"【P115ShareStrm】字幕放置失败 {sub_name}: {e}")
                        else:
                            logger.warning(f"【P115ShareStrm】未找到对应 STRM 目标路径，跳过字幕: {sub_name}")

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
        self._processing_count: int = 0  # 当前正在处理中的任务数
        self._notify_callback: Optional[Callable[[Optional[str], str, str], None]] = None
        # 正在队列中等待或处理的分享码（全量去重）
        self._pending_codes: Set[str] = set()
        # 去重缓存：{share_code: 最后入队时间戳}，用于短期防抖
        self._recent_tasks: Dict[str, float] = {}
        # 持久化文件路径，在 start() 中通过 settings 初始化
        self._persist_path: Optional[Path] = None

    # ── 持久化 ──────────────────────────────────────────────

    def _get_persist_path(self) -> Path:
        """懒加载持久化文件路径（避免模块加载时 settings 尚未就绪）"""
        if self._persist_path is None:
            from app.core.config import settings
            data_dir = Path(settings.PLUGIN_DATA_PATH) / "P115ShareStrm"
            data_dir.mkdir(parents=True, exist_ok=True)
            self._persist_path = data_dir / "pending_tasks.json"
        return self._persist_path

    def _load_tasks(self) -> List[Dict]:
        """从 JSON 文件加载持久化任务列表，文件不存在或损坏时返回空列表"""
        path = self._get_persist_path()
        try:
            if path.exists():
                return json.loads(path.read_text(encoding="utf-8"))
        except Exception as e:
            logger.warning(f"【P115ShareStrm】读取持久化任务失败，将重置: {e}")
        return []

    def _save_tasks(self, tasks: List[Dict]) -> None:
        """原子写入：先写 .tmp 再 rename，防止崩溃导致文件损坏"""
        path = self._get_persist_path()
        tmp = path.with_suffix(".tmp")
        try:
            tmp.write_text(json.dumps(tasks, ensure_ascii=False, indent=2), encoding="utf-8")
            tmp.replace(path)
        except Exception as e:
            logger.warning(f"【P115ShareStrm】写入持久化任务失败: {e}")

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
            if not self._running:
                self._running = True
                self._worker_thread = Thread(target=self._worker, daemon=True)
                self._worker_thread.start()
                logger.info("【P115ShareStrm】任务队列工作线程已启动")

        # 恢复持久化任务（在 _lock 外执行，避免死锁）
        self._restore_persisted_tasks()

    def stop(self):
        """停止工作线程"""
        self._running = False

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
                            msg = f"⏳ 字幕重新下载已转为后台轮询，等待链接审核通过后自动下载整理: {share_code}"
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
                            msg += f"\n⏳ 分享链接审核中，字幕下载已转为后台排队轮询，审核通过后自动下载放置"

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
                    if result.get("terminal"):
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
