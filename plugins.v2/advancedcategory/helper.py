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
    def match_rule(cls, rule: Dict[str, Any], tmdb_info: Dict[str, Any], extra_data: Dict[str, Any]) -> bool:
        """
        比对单个分类规则是否匹配
        :param rule: YAML中定义的某个分类的属性字典
        :param tmdb_info: MoviePilot 基础 tmdb_info 字典
        :param extra_data: 包含 keywords, actors, collection 等扩展文本列表的字典
        """
        if not rule:
            return True

        # 整理要比对的规则条目，将同类型逻辑字段（如 keywords 与 include_keywords）自动合并
        normalized_rule: Dict[str, Any] = {}
        kw_list = []
        actor_list = []
        country_list = []

        for k, v in rule.items():
            if v is None or v == "":
                continue
            norm_k = cls.normalize_rule_key(k)
            if norm_k in ("keywords", "include_keywords"):
                kw_list.append(str(v))
            elif norm_k in ("actors", "series_actors"):
                actor_list.append(str(v))
            elif norm_k in ("origin_country", "production_countries"):
                country_list.append(str(v))
            else:
                normalized_rule[norm_k] = v

        if kw_list:
            normalized_rule["keywords"] = ",".join(kw_list)
        if actor_list:
            normalized_rule["actors"] = ",".join(actor_list)
        if country_list:
            normalized_rule["origin_country"] = ",".join(country_list)

        # 遍历规则中的各项要求
        for attr, rule_val in normalized_rule.items():
            if rule_val is None or rule_val == "":
                continue


            values, invert_values = cls.parse_values(rule_val)
            if not values and not invert_values:
                continue

            # 1. 基础属性: release_year
            if attr == "release_year":
                date_val = tmdb_info.get("release_date") or tmdb_info.get("first_air_date")
                year_str = str(date_val)[:4].upper() if date_val else ""
                if not year_str:
                    return False
                if values and year_str not in values:
                    return False
                if invert_values and year_str in invert_values:
                    return False
                continue

            # 2. 基础属性: production_countries / origin_country
            if attr in ("production_countries", "origin_country"):
                country_list = []
                prod_countries = tmdb_info.get("production_countries")
                if isinstance(prod_countries, list):
                    for c in prod_countries:
                        if isinstance(c, dict) and c.get("iso_3166_1"):
                            country_list.append(str(c.get("iso_3166_1")).upper())
                        elif isinstance(c, str):
                            country_list.append(c.upper())
                origin_c = tmdb_info.get("origin_country")
                if isinstance(origin_c, list):
                    for c in origin_c:
                        country_list.append(str(c).upper())
                elif isinstance(origin_c, str):
                    country_list.append(origin_c.upper())

                if not country_list:
                    return False
                if values and not (set(values) & set(country_list)):
                    return False
                if invert_values and (set(invert_values) & set(country_list)):
                    return False
                continue

            # 3. 基础属性: genre_ids
            if attr == "genre_ids":
                genre_ids = tmdb_info.get("genre_ids") or []
                info_genres = [str(g).upper() for g in genre_ids]
                if values and not (set(values) & set(info_genres)):
                    return False
                if invert_values and (set(invert_values) & set(info_genres)):
                    return False
                continue

            # 4. 基础属性: original_language
            if attr == "original_language":
                lang = str(tmdb_info.get("original_language") or "").upper()
                if not lang:
                    return False
                if values and lang not in values:
                    return False
                if invert_values and lang in invert_values:
                    return False
                continue

            # 5. 高级文本/关键词匹配: keywords, include_keywords
            if attr in ("keywords", "include_keywords"):
                search_text_pool: Set[str] = set()
                # 放入扩展的 TMDB 关键词文本与基础标题简介
                for kw in extra_data.get("keywords", []):
                    search_text_pool.add(str(kw).upper())
                for t in extra_data.get("text_pool", []):
                    search_text_pool.add(str(t).upper())

                full_pool_str = " ".join(search_text_pool)
                matched_any = False

                if values:
                    for target_val in values:
                        if target_val in full_pool_str:
                            matched_any = True
                            break
                    if not matched_any:
                        return False

                if invert_values:
                    for inv_val in invert_values:
                        if inv_val in full_pool_str:
                            return False
                continue

            # 6. 系列关键词匹配: series_keywords
            if attr == "series_keywords":
                series_pool: Set[str] = set()
                for collection_name in extra_data.get("series_names", []):
                    series_pool.add(str(collection_name).upper())
                for t in extra_data.get("text_pool", []):
                    series_pool.add(str(t).upper())

                full_series_str = " ".join(series_pool)
                matched_any = False

                if values:
                    for target_val in values:
                        if target_val in full_series_str:
                            matched_any = True
                            break
                    if not matched_any:
                        return False

                if invert_values:
                    for inv_val in invert_values:
                        if inv_val in full_series_str:
                            return False
                continue

            # 7. 演职员匹配: series_actors, actors
            if attr in ("series_actors", "actors"):
                actor_list = [str(a).upper() for a in extra_data.get("actors", [])]
                full_actor_str = " ".join(actor_list)
                matched_any = False

                if values:
                    for target_val in values:
                        if target_val in full_actor_str or any(target_val in a for a in actor_list):
                            matched_any = True
                            break
                    if not matched_any:
                        return False

                if invert_values:
                    for inv_val in invert_values:
                        if inv_val in full_actor_str:
                            return False
                continue

            # 8. 通用字段动态匹配 (如 tmdb_id, id, status 等)
            info_val = tmdb_info.get(attr)
            if info_val is not None:
                info_str = str(info_val).upper()
                if values and info_str not in values:
                    return False
                if invert_values and info_str in invert_values:
                    return False
            else:
                return False

        return True


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

    def build_extra_data(self, tmdb_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        提取/补充文本池、演职员列表和关键词列表
        """
        extra_data = {
            "text_pool": [],
            "keywords": [],
            "actors": [],
            "series_names": [],
        }
        if not tmdb_info:
            return extra_data

        mtype = tmdb_info.get("media_type") or tmdb_info.get("type")
        tmdbid = tmdb_info.get("id")

        # 基础文本池提取
        title = tmdb_info.get("title") or tmdb_info.get("name") or ""
        original_title = tmdb_info.get("original_title") or tmdb_info.get("original_name") or ""
        overview = tmdb_info.get("overview") or ""
        tagline = tmdb_info.get("tagline") or ""

        if title:
            extra_data["text_pool"].append(title)
        if original_title:
            extra_data["text_pool"].append(original_title)
        if tagline:
            extra_data["text_pool"].append(tagline)
        if overview:
            extra_data["text_pool"].append(overview)

        # 别名/译名列表
        names = tmdb_info.get("names") or []
        if isinstance(names, list):
            extra_data["text_pool"].extend([str(n) for n in names])

        # 系列信息 (Collection)
        collection = tmdb_info.get("belongs_to_collection")
        if isinstance(collection, dict) and collection.get("name"):
            extra_data["series_names"].append(str(collection.get("name")))

        # 从现有 tmdb_info 中提取已有的 credits / keywords (若有)
        credits = tmdb_info.get("credits")
        if isinstance(credits, dict):
            cast = credits.get("cast") or []
            crew = credits.get("crew") or []
            for item in cast + crew:
                if isinstance(item, dict):
                    name = item.get("name") or item.get("original_name")
                    if name:
                        extra_data["actors"].append(str(name))

        keywords_obj = tmdb_info.get("keywords")
        if isinstance(keywords_obj, dict):
            kw_list = keywords_obj.get("keywords") or keywords_obj.get("results") or []
            for item in kw_list:
                if isinstance(item, dict) and item.get("name"):
                    extra_data["keywords"].append(str(item.get("name")))
                elif isinstance(item, str):
                    extra_data["keywords"].append(item)
        elif isinstance(keywords_obj, list):
            for item in keywords_obj:
                if isinstance(item, dict) and item.get("name"):
                    extra_data["keywords"].append(str(item.get("name")))
                elif isinstance(item, str):
                    extra_data["keywords"].append(item)

        # 如果已有演员和关键词，直接返回
        if extra_data["actors"] and extra_data["keywords"]:
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
                            if c_name and c_name not in extra_data["actors"]:
                                extra_data["actors"].append(str(c_name))

                        # 补充关键词
                        k_list = (detail.get("keywords") or {}).get("keywords") or (detail.get("keywords") or {}).get("results") or []
                        for k in k_list:
                            k_name = k.get("name") if isinstance(k, dict) else str(k)
                            if k_name and k_name not in extra_data["keywords"]:
                                extra_data["keywords"].append(str(k_name))
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

