import os
import re
import hashlib
from typing import Dict, Any, List, Optional, Tuple, Set


class OrganizeAnalyzerCore:
    """
    整理异常分析核心逻辑引擎
    """

    @staticmethod
    def _generate_key(rule_type: str, identifier: str) -> str:
        """生成基于规则和标识符的唯一 key"""
        raw = f"{rule_type}:{identifier}"
        return hashlib.md5(raw.encode("utf-8")).hexdigest()

    @classmethod
    def analyze(
        cls,
        histories: List[Any],
        config: Dict[str, Any],
    ) -> Tuple[List[Dict[str, Any]], int]:
        """
        根据配置检测历史记录中的异常
        :param histories: TransferHistory ORM 对象或字典列表
        :param config: 插件配置选项
        :return: (异常对象列表, 本次最高 history ID)
        """
        exceptions: List[Dict[str, Any]] = []
        max_id = 0

        # 配置开关
        min_merged_files = int(config.get("min_merged_files", 2))
        detect_merged = bool(config.get("detect_merged_files", True))
        detect_english = bool(config.get("detect_english_title", True))
        detect_unidentified = bool(config.get("detect_unidentified", True))
        detect_failed = bool(config.get("detect_failed_status", True))
        detect_duplicate = bool(config.get("detect_duplicate_episode", True))
        detect_missing = bool(config.get("detect_missing_dest", False))
        detect_invalid_ep = bool(config.get("detect_invalid_episode", False))

        # 忽略路径白名单
        ignore_paths_raw = config.get("ignore_paths", "")
        ignore_paths = [p.strip() for p in ignore_paths_raw.split(",") if p.strip()]

        # 预处理 history 数据字典，加速归并分组
        records = []
        for h in histories:
            if hasattr(h, "id"):
                hid = getattr(h, "id", 0)
                src = getattr(h, "src", "") or ""
                dest = getattr(h, "dest", "") or ""
                title = getattr(h, "title", "") or ""
                tmdbid = getattr(h, "tmdbid", 0) or 0
                mtype = getattr(h, "type", "") or ""
                seasons = getattr(h, "seasons", "") or ""
                episodes = getattr(h, "episodes", "") or ""
                status = getattr(h, "status", True)
                errmsg = getattr(h, "errmsg", "") or ""
                date = getattr(h, "date", "") or ""
                files = getattr(h, "files", []) or []
            else:
                hid = h.get("id", 0)
                src = h.get("src", "") or ""
                dest = h.get("dest", "") or ""
                title = h.get("title", "") or ""
                tmdbid = h.get("tmdbid", 0) or 0
                mtype = h.get("type", "") or ""
                seasons = h.get("seasons", "") or ""
                episodes = h.get("episodes", "") or ""
                status = h.get("status", True)
                errmsg = h.get("errmsg", "") or ""
                date = h.get("date", "") or ""
                files = h.get("files", []) or []

            if hid > max_id:
                max_id = hid

            # 白名单路径过滤
            if any(p in src or p in dest for p in ignore_paths):
                continue

            records.append({
                "id": hid,
                "src": src,
                "dest": dest,
                "title": title,
                "tmdbid": tmdbid,
                "type": mtype,
                "seasons": seasons,
                "episodes": episodes,
                "status": status,
                "errmsg": errmsg,
                "date": date,
                "files": files,
            })

        # 1. 检测整理失败 (detect_failed_status)
        if detect_failed:
            for r in records:
                if r["status"] is False or r["errmsg"]:
                    key = cls._generate_key("failed_status", str(r["id"]))
                    exceptions.append({
                        "key": key,
                        "type": "failed_status",
                        "type_name": "整理运行失败",
                        "title": r["title"] or "未知标题",
                        "history_id": r["id"],
                        "src": r["src"],
                        "dest": r["dest"],
                        "date": r["date"],
                        "detail": f"错误日志: {r['errmsg'] or '转移状态为失败'}",
                        "status": "active"
                    })

        # 2. 检测未识别 / TMDB缺失 (detect_unidentified)
        if detect_unidentified:
            for r in records:
                is_unk = not r["tmdbid"] or r["tmdbid"] == 0 or "未知" in r["title"] or "Unknown" in r["title"]
                if is_unk:
                    key = cls._generate_key("unidentified", str(r["id"]))
                    exceptions.append({
                        "key": key,
                        "type": "unidentified",
                        "type_name": "未识别/TMDB缺失",
                        "title": r["title"] or "未知",
                        "history_id": r["id"],
                        "src": r["src"],
                        "dest": r["dest"],
                        "date": r["date"],
                        "detail": f"TMDB ID: {r['tmdbid'] or '缺失'}, 整理标题: {r['title']}",
                        "status": "active"
                    })

        # 3. 检测英文/未中文化标题 (detect_english_title)
        if detect_english:
            for r in records:
                title = r["title"].strip()
                if title and not re.search(r"[\u4e00-\u9fff]", title):
                    # 忽略纯数字或短符号
                    if not title.replace(".", "").replace("-", "").isdigit():
                        key = cls._generate_key("english_title", str(r["id"]))
                        exceptions.append({
                            "key": key,
                            "type": "english_title",
                            "type_name": "英文未中文化标题",
                            "title": title,
                            "history_id": r["id"],
                            "src": r["src"],
                            "dest": r["dest"],
                            "date": r["date"],
                            "detail": f"标题 [{title}] 未包含中文，可能识别降级或缺少中文别名",
                            "status": "active"
                        })

        # 4. 检测多文件合并归并到同一个目标文件 (detect_merged_files)
        if detect_merged:
            dest_to_srcs: Dict[str, Set[str]] = {}
            dest_to_histories: Dict[str, List[Dict[str, Any]]] = {}

            for r in records:
                dest = r["dest"].strip()
                if not dest:
                    continue
                if dest not in dest_to_srcs:
                    dest_to_srcs[dest] = set()
                    dest_to_histories[dest] = []

                if r["src"]:
                    dest_to_srcs[dest].add(r["src"])
                # 累加 files 列表里的 src
                if isinstance(r["files"], list):
                    for fitem in r["files"]:
                        if isinstance(fitem, dict) and fitem.get("src"):
                            dest_to_srcs[dest].add(fitem["src"])

                dest_to_histories[dest].append(r)

            for dest, srcs in dest_to_srcs.items():
                if len(srcs) >= min_merged_files:
                    sample_h = dest_to_histories[dest][0]
                    key = cls._generate_key("merged_files", dest)
                    exceptions.append({
                        "key": key,
                        "type": "merged_files",
                        "type_name": "多文件合并覆盖",
                        "title": sample_h["title"] or dest,
                        "history_id": sample_h["id"],
                        "src": f"共有 {len(srcs)} 个源文件指向此目标",
                        "dest": dest,
                        "date": sample_h["date"],
                        "detail": f"检测到 {len(srcs)} 个源文件归并/覆盖到了同一个目标文件: {dest}",
                        "status": "active",
                        "file_count": len(srcs)
                    })

        # 5. 检测重复季集 (detect_duplicate_episode)
        if detect_duplicate:
            ep_map: Dict[str, List[Dict[str, Any]]] = {}
            for r in records:
                # 仅对电视剧生效
                mtype = r["type"].lower() if r["type"] else ""
                if "tv" in mtype or "剧" in mtype or r["seasons"] or r["episodes"]:
                    media_key = r["tmdbid"] if r["tmdbid"] else r["title"]
                    if media_key and r["seasons"] and r["episodes"]:
                        ep_key = f"{media_key}:{r['seasons']}:{r['episodes']}"
                        if ep_key not in ep_map:
                            ep_map[ep_key] = []
                        ep_map[ep_key].append(r)

            for ep_key, r_list in ep_map.items():
                # 多个不同 dest 覆盖
                dests = set(item["dest"] for item in r_list if item["dest"])
                if len(dests) > 1:
                    sample_h = r_list[0]
                    key = cls._generate_key("duplicate_episode", ep_key)
                    exceptions.append({
                        "key": key,
                        "type": "duplicate_episode",
                        "type_name": "重复季集冲突",
                        "title": sample_h["title"],
                        "history_id": sample_h["id"],
                        "src": sample_h["src"],
                        "dest": sample_h["dest"],
                        "date": sample_h["date"],
                        "detail": f"季集 [{sample_h['seasons']}{sample_h['episodes']}] 被多次整理到了 {len(dests)} 个不同的目标文件",
                        "status": "active",
                        "file_count": len(dests)
                    })

        # 6. 检测目标文件缺失/0字节 (detect_missing_dest)
        if detect_missing:
            for r in records:
                dest = r["dest"]
                if dest and (os.path.isabs(dest) or (len(dest) > 1 and dest[1] == ":")):
                    try:
                        if not os.path.exists(dest):
                            key = cls._generate_key("missing_dest", str(r["id"]))
                            exceptions.append({
                                "key": key,
                                "type": "missing_dest",
                                "type_name": "目标文件缺失",
                                "title": r["title"],
                                "history_id": r["id"],
                                "src": r["src"],
                                "dest": dest,
                                "date": r["date"],
                                "detail": f"目标路径物理文件不存在: {dest}",
                                "status": "active"
                            })
                        elif os.path.getsize(dest) == 0:
                            key = cls._generate_key("missing_dest_zero", str(r["id"]))
                            exceptions.append({
                                "key": key,
                                "type": "missing_dest",
                                "type_name": "目标文件0字节",
                                "title": r["title"],
                                "history_id": r["id"],
                                "src": r["src"],
                                "dest": dest,
                                "date": r["date"],
                                "detail": f"目标路径物理文件大小为 0 字节: {dest}",
                                "status": "active"
                            })
                    except Exception:
                        pass

        # 7. 检测离群/格式异常集数 (detect_invalid_episode)
        if detect_invalid_ep:
            invalid_ep_threshold = int(config.get("invalid_episode_threshold", 500))
            
            # 第一步：按媒体分组收集本次扫描到的所有集数
            media_episodes: Dict[str, Set[int]] = {}
            for r in records:
                media_key = r["tmdbid"] if r["tmdbid"] else r["title"]
                if media_key:
                    ep_nums = [int(n) for n in re.findall(r"\d+", r["episodes"] or "")]
                    if media_key not in media_episodes:
                        media_episodes[media_key] = set()
                    media_episodes[media_key].update(ep_nums)

            # 第二步：二次遍历，检测离群集数
            for r in records:
                media_key = r["tmdbid"] if r["tmdbid"] else r["title"]
                ep_nums = [int(n) for n in re.findall(r"\d+", r["episodes"] or "")]
                
                is_invalid = False
                for n in ep_nums:
                    if n > invalid_ep_threshold:
                        # 检查连续性：当前批次中是否有 n-1 或 n+1 的集数存在
                        all_eps = media_episodes.get(media_key, set())
                        has_continuity = (n - 1) in all_eps or (n + 1) in all_eps
                        if not has_continuity:
                            is_invalid = True
                            break
                            
                if is_invalid:
                    key = cls._generate_key("invalid_episode", str(r["id"]))
                    exceptions.append({
                        "key": key,
                        "type": "invalid_episode",
                        "type_name": "离群集数异常",
                        "title": r["title"],
                        "history_id": r["id"],
                        "src": r["src"],
                        "dest": r["dest"],
                        "date": r["date"],
                        "detail": f"解析集数数值过大 [{r['episodes']}] (超阈值 {invalid_ep_threshold}) 且无前后连续集数，疑似误提取了分辨率/日期",
                        "status": "active"
                    })

        return exceptions, max_id
