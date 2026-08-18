import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Set

try:
    import ruamel.yaml as ruamel_yaml
except ImportError:
    import yaml as ruamel_yaml

from app.core.config import settings
from app.log import logger
from app.schemas.types import MediaType


class RuleEngine:
    """
    高级二级分类匹配引擎
    """

    @staticmethod
    def normalize_rule_key(key: str) -> str:
        """剔除键名前的 ? 等兼容前缀"""
        key = str(key).strip()
        if key.startswith('?'):
            key = key[1:].strip()
        return key

    @staticmethod
    def parse_values(val: Any) -> Tuple[List[str], List[str]]:
        """
        解析配置值，返回 (普通包含值列表, 排除!值列表)
        支持 `,` 分隔与 `!`/`-` 排除前缀
        """
        if isinstance(val, list):
            raw_str = ",".join(str(v) for v in val)
        else:
            raw_str = str(val or "")

        values = []
        invert_values = []

        tokens = [t.strip() for t in raw_str.split(",") if t.strip()]
        for token in tokens:
            prefix = ""
            current = token

            # 处理 ! 或 - 排除前缀
            if current.startswith("!"):
                prefix = "!"
                current = current[1:]
            elif current.startswith("-") and not current[1:].isdigit():
                prefix = "!"
                current = current[1:]

            # 处理数字范围 2020-2024
            if "-" in current:
                parts = current.split("-", 1)
                if parts[0].isdigit() and parts[1].isdigit():
                    start, end = int(parts[0]), int(parts[1])
                    for num in range(start, end + 1):
                        if prefix == "!":
                            invert_values.append(str(num).upper())
                        else:
                            values.append(str(num).upper())
                    continue

            # 普通值
            if current.startswith("+"):
                current = current[1:]

            if prefix == "!":
                invert_values.append(current.upper())
            else:
                values.append(current.upper())

        return values, invert_values

    @classmethod
    def match_title_keyword(cls, pattern_word: str, title: str) -> bool:
        """
        片名关键词精准匹配逻辑：
        - pattern_word: 目标词 (大写)
        - title: 片名 (大写)
        - 长词 (>=3字): 允许子串包含或标点归一化匹配
        - 短词 (<=2字，如"本能", "色戒"): 要求全等、标点忽略全等（如"色，戒"与"色戒"）、带序号/边界匹配
        """
        if not pattern_word or not title:
            return False

        # 1. 全等命中
        if title == pattern_word:
            return True

        # 2. 标点符号与空格归一化比对（例如 "色，戒" 与 "色戒"）
        clean_title = re.sub(r"[^\u4e00-\u9fa5A-Z0-9]", "", title)
        clean_pattern = re.sub(r"[^\u4e00-\u9fa5A-Z0-9]", "", pattern_word)
        if clean_pattern and clean_title == clean_pattern:
            return True

        # 中文短词（长度 <= 2）或特定易歧义词
        is_short_cjk = len(clean_pattern) <= 2 and any('\u4e00' <= ch <= '\u9fa5' for ch in clean_pattern)
        if is_short_cjk:
            # 允许前缀带数字/续集，例如 "本能2", "色戒2"
            if clean_title.startswith(clean_pattern):
                rest = clean_title[len(clean_pattern):]
                if not rest or rest.isdigit():
                    return True
            # 允许边界匹配，例如 "本能 2", "【本能】", "色戒 (2007)" 等
            regex_pat = rf"(?:^|[^\u4e00-\u9fa5\w]){re.escape(pattern_word)}(?:$|[^\u4e00-\u9fa5\w]|\d+)"
            if re.search(regex_pat, title, re.IGNORECASE):
                return True
            return False

        # 纯英文短词 (长度 <= 3)
        if pattern_word.isascii() and len(pattern_word) <= 3:
            regex_pat = rf"\b{re.escape(pattern_word)}\b"
            if re.search(regex_pat, title, re.IGNORECASE):
                return True
            return False

        # 长词（>=3字）：子串匹配或归一化子串匹配
        if pattern_word in title or (clean_pattern and clean_pattern in clean_title):
            return True

        return False

    @classmethod
    def match_tag_keyword(cls, pattern_word: str, tags: List[str]) -> bool:
        """
        TMDb 标签 (Tag) 独立比对逻辑
        - pattern_word: 目标关键词 (大写)
        - tags: TMDb 官方标签列表 (全大写)
        """
        if not pattern_word or not tags:
            return False

        for tag in tags:
            tag_upper = tag.strip().upper()
            if not tag_upper:
                continue
            # 单项全字匹配
            if tag_upper == pattern_word:
                return True
            # 多词短语全短语匹配 (如 "unusual sexual practices")
            if " " in pattern_word and pattern_word in tag_upper:
                return True

        return False

    @classmethod
    def match_rule(cls, rule: Dict[str, Any], tmdb_info: Dict[str, Any], extra_data: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """
        比对单个分类规则是否匹配
        :param rule: YAML中定义的某个分类的属性字典
        :param tmdb_info: MoviePilot 基础 tmdb_info 字典
        :param extra_data: 包含 titles, tags, series_names, actors 等扩展字典
        :return: (是否匹配成功, 命中特征描述)
        """
        if not rule:
            return True, "默认规则"

        # 整理要比对的规则条目
        normalized_rule: Dict[str, Any] = {}
        title_kw_list = []
        tag_kw_list = []
        actor_list = []
        series_kw_list = []
        country_list = []
        overview_kw_list = []

        for k, v in rule.items():
            if v is None or v == "":
                continue
            norm_k = cls.normalize_rule_key(k)
            if norm_k in ("keywords", "title_keywords"):
                title_kw_list.append(str(v))
            elif norm_k in ("include_keywords", "tags", "tmdb_keywords"):
                tag_kw_list.append(str(v))
            elif norm_k in ("actors", "series_actors"):
                actor_list.append(str(v))
            elif norm_k in ("series_keywords",):
                series_kw_list.append(str(v))
            elif norm_k in ("origin_country", "production_countries"):
                country_list.append(str(v))
            elif norm_k in ("overview_keywords",):
                overview_kw_list.append(str(v))
            else:
                normalized_rule[norm_k] = v

        if title_kw_list:
            normalized_rule["title_keywords"] = ",".join(title_kw_list)
        if tag_kw_list:
            normalized_rule["tag_keywords"] = ",".join(tag_kw_list)
        if series_kw_list:
            normalized_rule["series_keywords"] = ",".join(series_kw_list)
        if actor_list:
            normalized_rule["actors"] = ",".join(actor_list)
        if country_list:
            normalized_rule["origin_country"] = ",".join(country_list)
        if overview_kw_list:
            normalized_rule["overview_keywords"] = ",".join(overview_kw_list)

        # 提取扩展数据池
        titles = [str(t).upper() for t in extra_data.get("titles", []) if t]
        tags = [str(tg).upper() for tg in extra_data.get("tags", []) if tg]
        series_names = [str(s).upper() for s in extra_data.get("series_names", []) if s]
        actors = [str(a).upper() for a in extra_data.get("actors", []) if a]
        overview_text = str(extra_data.get("overview", "")).upper()

        # 区分硬约束属性与正向内容特征
        has_any_positive_feature = False
        feature_matched = False
        hit_reason = None

        # 遍历规则中的各项要求
        for attr, rule_val in normalized_rule.items():
            if rule_val is None or rule_val == "":
                continue

            # 处理成人标记 adult: true / false
            if attr == "adult":
                is_adult = bool(tmdb_info.get("adult") or extra_data.get("adult"))
                target_adult = str(rule_val).lower() in ("true", "1", "yes")
                if target_adult:
                    has_any_positive_feature = True
                    if is_adult:
                        feature_matched = True
                        hit_reason = hit_reason or "TMDb Adult 纯成人标识"
                else:
                    if is_adult:
                        return False, None
                continue

            values, invert_values = cls.parse_values(rule_val)
            if not values and not invert_values:
                continue

            # 1. 基础属性硬约束: release_year
            if attr == "release_year":
                date_val = tmdb_info.get("release_date") or tmdb_info.get("first_air_date")
                year_str = str(date_val)[:4].upper() if date_val else ""
                if not year_str:
                    return False, None
                if values and year_str not in values:
                    return False, None
                if invert_values and year_str in invert_values:
                    return False, None
                continue

            # 2. 基础属性硬约束: production_countries / origin_country
            if attr in ("production_countries", "origin_country"):
                c_list = []
                prod_countries = tmdb_info.get("production_countries")
                if isinstance(prod_countries, list):
                    for c in prod_countries:
                        if isinstance(c, dict) and c.get("iso_3166_1"):
                            c_list.append(str(c.get("iso_3166_1")).upper())
                        elif isinstance(c, str):
                            c_list.append(c.upper())
                origin_c = tmdb_info.get("origin_country")
                if isinstance(origin_c, list):
                    for c in origin_c:
                        c_list.append(str(c).upper())
                elif isinstance(origin_c, str):
                    c_list.append(origin_c.upper())

                if not c_list:
                    return False, None
                if values and not (set(values) & set(c_list)):
                    return False, None
                if invert_values and (set(invert_values) & set(c_list)):
                    return False, None
                continue

            # 3. 基础属性硬约束: genre_ids
            if attr == "genre_ids":
                genre_ids = tmdb_info.get("genre_ids") or []
                info_genres = [str(g).upper() for g in genre_ids]
                if values and not (set(values) & set(info_genres)):
                    return False, None
                if invert_values and (set(invert_values) & set(info_genres)):
                    return False, None
                continue

            # 4. 基础属性硬约束: original_language
            if attr == "original_language":
                lang = str(tmdb_info.get("original_language") or "").upper()
                if not lang:
                    return False, None
                if values and lang not in values:
                    return False, None
                if invert_values and lang in invert_values:
                    return False, None
                continue

            # 5. 片名关键词匹配: title_keywords / keywords
            if attr == "title_keywords":
                # 排除词一票否决
                if invert_values:
                    for inv_val in invert_values:
                        if any(cls.match_title_keyword(inv_val, t) for t in titles) or cls.match_tag_keyword(inv_val, tags):
                            return False, None

                if values:
                    has_any_positive_feature = True
                    for target_val in values:
                        # 优先在片名池中比对
                        matched_t = next((t for t in titles if cls.match_title_keyword(target_val, t)), None)
                        if matched_t:
                            feature_matched = True
                            hit_reason = hit_reason or f"片名关键词 [{target_val}] (匹配片名: {matched_t})"
                            break
                        # 兼顾在标签池中比对
                        if cls.match_tag_keyword(target_val, tags):
                            feature_matched = True
                            hit_reason = hit_reason or f"关键词标签 [{target_val}]"
                            break
                continue

            # 6. TMDb 官方标签匹配: tag_keywords / include_keywords
            if attr == "tag_keywords":
                # 排除词一票否决
                if invert_values:
                    for inv_val in invert_values:
                        if cls.match_tag_keyword(inv_val, tags):
                            return False, None

                if values:
                    has_any_positive_feature = True
                    for target_val in values:
                        if cls.match_tag_keyword(target_val, tags):
                            feature_matched = True
                            hit_reason = hit_reason or f"TMDb 标签 [{target_val}]"
                            break
                continue

            # 7. 系列关键词特征匹配: series_keywords
            if attr == "series_keywords":
                # 排除词一票否决
                if invert_values:
                    for inv_val in invert_values:
                        if any(cls.match_title_keyword(inv_val, s) for s in series_names + titles):
                            return False, None

                if values:
                    has_any_positive_feature = True
                    for target_val in values:
                        matched_s = next((s for s in series_names + titles if cls.match_title_keyword(target_val, s)), None)
                        if matched_s:
                            feature_matched = True
                            hit_reason = hit_reason or f"系列/合集关键词 [{target_val}] (匹配: {matched_s})"
                            break
                continue

            # 8. 演职员特征匹配: actors / series_actors
            if attr == "actors":
                full_actor_str = " ".join(actors)
                # 排除词一票否决
                if invert_values:
                    for inv_val in invert_values:
                        if inv_val in full_actor_str or any(inv_val in a for a in actors):
                            return False, None

                if values:
                    has_any_positive_feature = True
                    for target_val in values:
                        if target_val in full_actor_str or any(target_val in a for a in actors):
                            feature_matched = True
                            hit_reason = hit_reason or f"演职人员 [{target_val}]"
                            break
                continue

            # 9. 剧情简介匹配: overview_keywords (显式配置时才启用)
            if attr == "overview_keywords":
                if invert_values:
                    for inv_val in invert_values:
                        if inv_val in overview_text:
                            return False, None

                if values:
                    has_any_positive_feature = True
                    for target_val in values:
                        if target_val in overview_text:
                            feature_matched = True
                            hit_reason = hit_reason or f"简介关键词 [{target_val}]"
                            break
                continue

            # 10. 通用字段动态匹配 (如 tmdb_id, id, status 等)
            info_val = tmdb_info.get(attr)
            if info_val is not None:
                info_str = str(info_val).upper()
                if values and info_str not in values:
                    return False, None
                if invert_values and info_str in invert_values:
                    return False, None
            else:
                return False, None

        # 如果规则中定义了正向特征（关键词、系列、演职员等），必须至少命中一个特征
        if has_any_positive_feature and not feature_matched:
            return False, None

        return True, hit_reason or "属性完全吻合"


class TmdbExtraHelper:
    """
    TMDB 扩展信息（关键词、演职员、系列名）查询工具
    """

    def __init__(self):
        self._tmdb_module = None

    def _get_tmdb_module(self):
        if not self._tmdb_module:
            try:
                from app.modules.themoviedb import TheMovieDbModule
                self._tmdb_module = TheMovieDbModule()
            except Exception as e:
                logger.error(f"【高级二级分类】加载 TheMovieDbModule 失败: {e}")
        return self._tmdb_module

    def build_extra_data(self, tmdb_info: Dict[str, Any], mtype: Optional[Any] = None) -> Dict[str, Any]:
        """
        提取/补充片名池、标签池、演职员列表和系列信息
        物理隔离 text_pool，彻底杜绝剧情简介污染片名匹配
        """
        extra_data = {
            "titles": [],
            "tags": [],
            "actors": [],
            "series_names": [],
            "overview": "",
            "tagline": "",
            "adult": bool(tmdb_info.get("adult", False)) if tmdb_info else False,
        }
        if not tmdb_info:
            return extra_data

        if not mtype:
            mtype = tmdb_info.get("media_type") or tmdb_info.get("type") or "movie"
        if hasattr(mtype, "value"):
            mtype = mtype.value
        mtype = str(mtype).lower()

        tmdbid = tmdb_info.get("id")

        # 1. 片名池提取 (主标题、原标题、别名/译名)
        title = tmdb_info.get("title") or tmdb_info.get("name") or ""
        original_title = tmdb_info.get("original_title") or tmdb_info.get("original_name") or ""
        if title:
            extra_data["titles"].append(title)
        if original_title and original_title not in extra_data["titles"]:
            extra_data["titles"].append(original_title)

        names = tmdb_info.get("names") or []
        if isinstance(names, list):
            for n in names:
                n_str = str(n).strip()
                if n_str and n_str not in extra_data["titles"]:
                    extra_data["titles"].append(n_str)

        # 2. 剧情简介与宣传语提取
        overview = tmdb_info.get("overview") or ""
        tagline = tmdb_info.get("tagline") or ""
        extra_data["overview"] = overview
        extra_data["tagline"] = tagline

        # 3. 系列信息 (Collection)
        collection = tmdb_info.get("belongs_to_collection")
        if isinstance(collection, dict) and collection.get("name"):
            extra_data["series_names"].append(str(collection.get("name")))

        # 4. 从现有 tmdb_info 中提取已有的 credits / keywords (若有)
        credits = tmdb_info.get("credits")
        if isinstance(credits, dict):
            cast = credits.get("cast") or []
            crew = credits.get("crew") or []
            for item in cast + crew:
                if isinstance(item, dict):
                    name = item.get("name") or item.get("original_name")
                    if name and str(name) not in extra_data["actors"]:
                        extra_data["actors"].append(str(name))

        keywords_obj = tmdb_info.get("keywords")
        if isinstance(keywords_obj, dict):
            kw_list = keywords_obj.get("keywords") or keywords_obj.get("results") or []
            for item in kw_list:
                if isinstance(item, dict) and item.get("name"):
                    extra_data["tags"].append(str(item.get("name")))
                elif isinstance(item, str):
                    extra_data["tags"].append(item)
        elif isinstance(keywords_obj, list):
            for item in keywords_obj:
                if isinstance(item, dict) and item.get("name"):
                    extra_data["tags"].append(str(item.get("name")))
                elif isinstance(item, str):
                    extra_data["tags"].append(item)

        # 如果已有演员和标签，直接返回
        if extra_data["actors"] and extra_data["tags"]:
            return extra_data

        # 若缺乏 API 数据且有 TMDB ID，尝试通过 TMDB 详情 API 补充抓取
        if tmdbid:
            try:
                tmdb_mod = self._get_tmdb_module()
                if tmdb_mod and hasattr(tmdb_mod, "tmdb"):
                    detail = None
                    if mtype == MediaType.MOVIE or mtype == "movie":
                        detail = tmdb_mod.tmdb._get_movie_detail(tmdbid, append_to_response="credits,keywords")
                    elif mtype == MediaType.TV or mtype == "tv":
                        detail = tmdb_mod.tmdb._get_tv_detail(tmdbid, append_to_response="credits,keywords")

                    if detail and isinstance(detail, dict):
                        # 补充演员
                        c_cast = (detail.get("credits") or {}).get("cast") or []
                        for c in c_cast[:15]:
                            c_name = c.get("name") or c.get("original_name")
                            if c_name and str(c_name) not in extra_data["actors"]:
                                extra_data["actors"].append(str(c_name))

                        # 补充关键词标签
                        k_list = (detail.get("keywords") or {}).get("keywords") or (detail.get("keywords") or {}).get("results") or []
                        for k in k_list:
                            k_name = k.get("name") if isinstance(k, dict) else str(k)
                            if k_name and str(k_name) not in extra_data["tags"]:
                                extra_data["tags"].append(str(k_name))

                        # 补充系列信息
                        coll = detail.get("belongs_to_collection")
                        if isinstance(coll, dict) and coll.get("name"):
                            coll_name = str(coll.get("name"))
                            if coll_name not in extra_data["series_names"]:
                                extra_data["series_names"].append(coll_name)

                        # 补充 adult
                        if detail.get("adult"):
                            extra_data["adult"] = True
            except Exception as e:
                logger.debug(f"【高级二级分类】通过 TMDB API 补充 credits/keywords 失败: {e}")

        return extra_data


class CacheManager:
    """
    分类结果持久化缓存管理器
    """

    def __init__(self, cache_file_path: Path):
        self.cache_file_path = cache_file_path
        self._cache_data: Dict[str, Any] = {}
        self.load()

    def load(self):
        try:
            if self.cache_file_path.exists():
                with open(self.cache_file_path, "r", encoding="utf-8") as f:
                    self._cache_data = json.load(f)
            else:
                self._cache_data = {}
        except Exception as e:
            logger.error(f"【高级二级分类】加载缓存文件 {self.cache_file_path} 失败: {e}")
            self._cache_data = {}

    def save(self):
        try:
            self.cache_file_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.cache_file_path, "w", encoding="utf-8") as f:
                json.dump(self._cache_data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"【高级二级分类】保存缓存文件 {self.cache_file_path} 失败: {e}")

    def get(self, key: str) -> Optional[str]:
        return self._cache_data.get(key)

    def set(self, key: str, category_name: str):
        if key and category_name:
            self._cache_data[key] = category_name
            self.save()

    def clear(self):
        self._cache_data = {}
        self.save()
