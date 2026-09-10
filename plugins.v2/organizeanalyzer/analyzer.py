import os
import re
import hashlib
from typing import Dict, Any, List, Optional, Tuple, Set


class OrganizeAnalyzerCore:
    """
    整理异常分析核心逻辑引擎
    """

    # 常见无意义或标签类中文字符串/关键词黑名单
    NOISE_KEYWORDS = {
        "合集", "剧集", "电视剧", "电影", "动漫", "纪录片", "综艺", "系列", "全集",
        "国粤双语", "国语", "粤语", "英语", "韩语", "日语", "双语", "双字", "简繁",
        "简繁双字", "简繁中字", "简日双字", "繁日双字", "简英双字", "繁英双字", "中文字幕",
        "内封简繁", "内封中字", "内封字幕", "内嵌简繁", "内嵌字幕", "外挂字幕", "特效字幕",
        "压制", "重温", "修复", "高清", "超清", "无删减", "未删减", "加长版", "导演剪辑版",
        "特别篇", "剧场版", "最终版", "原盘", "蓝光", "首播", "完结", "更新", "分享",
        "本地", "夸克", "阿里", "百度", "迅雷", "网盘", "转存", "整理", "电影合集", "剧集合集",
        "港剧合集", "华语合集", "国产剧", "欧美剧", "日韩剧", "港台剧", "未命名", "其他"
    }

    # 匹配各类集数、季度、容量、数量噪声的正则
    NOISE_REGEX = re.compile(
        r"^(\d+|全\d+[集话期]?|第\d+[季集话期]|S\d+|E\d+|EP\d+|\d+部|\d+个|\d+(\.\d+)?[TGMK]B?|"
        r"4K|1080P|720P|2160P|H264|H265|x264|x265|HEVC|AVC|AAC|DTS|TrueHD|Atmos|FLAC|"
        r"REMUX|BluRay|WEB-DL|WEBRip|HDTV|DVD|MKV|MP4|STRM|ISO)$",
        re.IGNORECASE
    )

    @staticmethod
    def _generate_key(rule_type: str, identifier: str) -> str:
        """生成基于规则和标识符的唯一 key"""
        raw = f"{rule_type}:{identifier}"
        return hashlib.md5(raw.encode("utf-8")).hexdigest()

    @classmethod
    def extract_source_chinese_titles(cls, src_path: str) -> List[str]:
        """
        从源路径中智能提取候选中文片名
        """
        if not src_path:
            return []

        # 规范化路径分隔符
        norm_path = src_path.replace("\\", "/")
        parts = [p.strip() for p in norm_path.split("/") if p.strip()]
        if not parts:
            return []

        candidates: List[str] = []

        # 从最底层的目录/文件名开始向上回溯（最多回溯 4 层）
        inspect_parts = list(reversed(parts[-4:]))

        for part in inspect_parts:
            # 去除扩展名
            part_stem = os.path.splitext(part)[0]

            # 1. 提取方括号 [ ]、大括号 { }、中文括号 【 】、（ ） 中的内容
            bracket_blocks = re.findall(r"[\[\(\{【（]([^\]\)\}】）]+)[\]\)\}】）]", part_stem)
            for block in bracket_blocks:
                block_clean = block.strip()
                # 检查是否为纯中文或包含中文
                cn_matches = re.findall(r"[\u4e00-\u9fff]+", block_clean)
                for cn_str in cn_matches:
                    cn_str = cn_str.strip()
                    if len(cn_str) >= 2 and cn_str not in cls.NOISE_KEYWORDS and not cls.NOISE_REGEX.match(cn_str):
                        # 排除如 "全20集"、"941部"、"200部" 等复合词
                        if not re.match(r"^(全?\d+[集话期部个]|第\d+[季集话期]|内封.*|内嵌.*|简繁.*|双字.*|中字.*|\d+部.*|\d+个.*)$", cn_str):
                            if cn_str not in candidates:
                                candidates.append(cn_str)

            # 2. 从未被括号包裹的完整名称中分析（例如 中华英雄.1991.1080p 或 中华英雄之中华傲决）
            # 将常见分隔符替换为空格
            cleaned_part = re.sub(r"[\[\]\(\)\{\}【】（）._\-+]+", " ", part_stem)
            for segment in cleaned_part.split():
                cn_matches = re.findall(r"[\u4e00-\u9fff]+", segment)
                for cn_str in cn_matches:
                    cn_str = cn_str.strip()
                    if len(cn_str) >= 2 and cn_str not in cls.NOISE_KEYWORDS and not cls.NOISE_REGEX.match(cn_str):
                        if not re.match(r"^(全?\d+[集话期部个]|第\d+[季集话期]|内封.*|内嵌.*|简繁.*|双字.*|中字.*|\d+部.*|\d+个.*)$", cn_str):
                            if cn_str not in candidates:
                                candidates.append(cn_str)

        return candidates

    @classmethod
    def calc_title_similarity(cls, str1: str, str2: str) -> float:
        """
        计算两个中文标题的字符集合重合度 (Jaccard-like & 包含关系)
        """
        s1 = "".join(re.findall(r"[\u4e00-\u9fff0-9a-zA-Z]+", str1)).lower()
        s2 = "".join(re.findall(r"[\u4e00-\u9fff0-9a-zA-Z]+", str2)).lower()

        if not s1 or not s2:
            return 0.0

        # 完全匹配或互为子串
        if s1 == s2:
            return 1.0
        if s1 in s2 or s2 in s1:
            # 子串匹配赋予高相似度 (例如: "中华英雄" in "中华英雄之中华傲决")
            return min(len(s1), len(s2)) / max(len(s1), len(s2)) * 0.9 + 0.1

        set1 = set(s1)
        set2 = set(s2)
        intersection = set1 & set2
        if not intersection:
            return 0.0

        union = set1 | set2
        return len(intersection) / len(union)

    @classmethod
    def analyze(
        cls,
        histories: List[Any],
        config: Dict[str, Any],
        path_filter: Optional[str] = None,
        path_type: str = "all",
    ) -> Tuple[List[Dict[str, Any]], int]:
        """
        根据配置检测历史记录中的异常
        :param histories: TransferHistory ORM 对象或字典列表
        :param config: 插件配置选项
        :param path_filter: 指定路径过滤关键字
        :param path_type: 指定路径过滤模式 ('src', 'dest', 'all')
        :return: (异常对象列表, 本次最高 history ID)
        """
        exceptions: List[Dict[str, Any]] = []
        max_id = 0

        # 配置开关
        min_merged_files = int(config.get("min_merged_files", 2))
        detect_merged = bool(config.get("detect_merged_files", True))
        detect_english = bool(config.get("detect_english_title", True))
        detect_title_mismatch = bool(config.get("detect_title_mismatch", True))
        title_mismatch_threshold = float(config.get("title_mismatch_threshold", 0.3))
        detect_unidentified = bool(config.get("detect_unidentified", True))
        detect_failed = bool(config.get("detect_failed_status", True))
        detect_duplicate = bool(config.get("detect_duplicate_episode", True))
        detect_missing = bool(config.get("detect_missing_dest", False))
        detect_invalid_ep = bool(config.get("detect_invalid_episode", False))

        # 忽略路径白名单
        ignore_paths_raw = config.get("ignore_paths", "")
        ignore_paths = [p.strip() for p in ignore_paths_raw.split(",") if p.strip()]

        pf = (path_filter or "").strip().lower()

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

            # 指定路径过滤 (支持 src / dest / all)
            if pf:
                src_lower = src.lower()
                dest_lower = dest.lower()
                if path_type == "src" and pf not in src_lower:
                    continue
                elif path_type == "dest" and pf not in dest_lower:
                    continue
                elif path_type == "all" and pf not in src_lower and pf not in dest_lower:
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
                title = r["title"] or ""
                has_tmdbid = r["tmdbid"] and int(r["tmdbid"]) > 0
                if has_tmdbid:
                    is_unk = False
                else:
                    is_unk = True

                if is_unk:
                    key = cls._generate_key("unidentified", str(r["id"]))
                    exceptions.append({
                        "key": key,
                        "type": "unidentified",
                        "type_name": "未识别/TMDB缺失",
                        "title": title or "未知",
                        "history_id": r["id"],
                        "src": r["src"],
                        "dest": r["dest"],
                        "date": r["date"],
                        "detail": f"TMDB ID: {r['tmdbid'] or '缺失'}, 整理标题: {title}",
                        "status": "active"
                    })

        # 3. 检测英文/未中文化标题 (detect_english_title)
        if detect_english:
            for r in records:
                title = r["title"].strip()
                if title and not re.search(r"[\u4e00-\u9fff]", title):
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

        # 4. 检测整理前后中文名差异/错配 (detect_title_mismatch)
        if detect_title_mismatch:
            for r in records:
                target_title = (r["title"] or "").strip()
                # 仅在目标包含中文且整理记录有效的情况下检测
                if not target_title or not re.search(r"[\u4e00-\u9fff]", target_title):
                    continue

                src_path = r["src"]
                if not src_path:
                    continue

                # 提取源路径中的中文片名候选
                candidates = cls.extract_source_chinese_titles(src_path)
                if not candidates:
                    continue

                # 计算与目标标题的最大相似度
                max_sim = 0.0
                best_cand = candidates[0]
                for cand in candidates:
                    sim = cls.calc_title_similarity(cand, target_title)
                    if sim > max_sim:
                        max_sim = sim
                        best_cand = cand

                # 若所有候选的相似度都低于阈值，且目标中文名在源路径中完全不存在
                target_cn_only = "".join(re.findall(r"[\u4e00-\u9fff]+", target_title))
                if max_sim < title_mismatch_threshold and target_cn_only not in src_path:
                    key = cls._generate_key("title_mismatch", str(r["id"]))
                    exceptions.append({
                        "key": key,
                        "type": "title_mismatch",
                        "type_name": "中文名差异/错配",
                        "title": f"{target_title} (源: {best_cand})",
                        "history_id": r["id"],
                        "src": r["src"],
                        "dest": r["dest"],
                        "date": r["date"],
                        "detail": f"源路径提取中文名 [{best_cand}] 与 整理后中文标题 [{target_title}] 差异过大 (相似度: {int(max_sim * 100)}%)，疑似刮削错误",
                        "status": "active"
                    })

        # 5. 检测多文件合并归并到同一个目标文件 (detect_merged_files)
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

        # 6. 检测重复季集 (detect_duplicate_episode)
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

        # 7. 检测目标文件缺失/0字节 (detect_missing_dest)
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

        # 8. 检测离群/格式异常集数 (detect_invalid_episode，仅对电视剧/非电影生效)
        if detect_invalid_ep:
            invalid_ep_threshold = int(config.get("invalid_episode_threshold", 500))
            
            def is_movie_record(rec: dict) -> bool:
                mtype = str(rec.get("type") or "").lower()
                dest = str(rec.get("dest") or "").lower()
                src = str(rec.get("src") or "").lower()
                return "movie" in mtype or "电影" in mtype or "/电影/" in dest or "/电影/" in src

            # 第一步：按媒体分组收集本次扫描到的所有集数（排除电影）
            media_episodes: Dict[str, Set[int]] = {}
            for r in records:
                if is_movie_record(r):
                    continue
                media_key = r["tmdbid"] if r["tmdbid"] else r["title"]
                if media_key:
                    ep_nums = [int(n) for n in re.findall(r"\d+", r["episodes"] or "")]
                    if media_key not in media_episodes:
                        media_episodes[media_key] = set()
                    media_episodes[media_key].update(ep_nums)

            # 第二步：二次遍历，检测离群集数（排除电影）
            for r in records:
                if is_movie_record(r):
                    continue
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
