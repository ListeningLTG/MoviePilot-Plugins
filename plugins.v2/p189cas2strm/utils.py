import re
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple
from urllib.parse import quote, urlparse, parse_qs

from jinja2 import Environment, Template, select_autoescape
from jinja2.exceptions import TemplateError

from app.log import logger

# 189 分享链接正则
_P189_URL_PATTERN = re.compile(
    r"https?://(?:cloud\.189\.cn)/(?:t/|web/share\?code=)([a-zA-Z0-9]+)(?:\?[\w=&%-]*)?",
    re.I,
)

# 提取码正则（按优先级排序）
_PWD_PATTERNS = [
    re.compile(r'(?:pwd|password|提取码|访问码|密码|码)[:：\s=]*([a-zA-Z0-9]{4,6})', re.I),
    re.compile(r'^[:：\s=]*([a-zA-Z0-9]{4,6})\b'),
    re.compile(r'[\(\uff08]([a-zA-Z0-9]{4,6})[\)\uff09]'),
]


class StrmUrlTemplateResolver:
    """
    基于 Jinja2 的 STRM URL 模板解析器
    """
    def __init__(
        self,
        base_template: Optional[str] = None,
        auto_escape: bool = False,
    ):
        self.env = Environment(
            autoescape=select_autoescape(["html", "xml"]) if auto_escape else False,
            trim_blocks=True,
            lstrip_blocks=True,
        )
        self.env.filters["urlencode"] = lambda v: quote(str(v), safe="")
        
        self.base_template: Optional[Template] = None
        if base_template:
            try:
                self.base_template = self.env.from_string(base_template)
            except TemplateError as e:
                logger.error(f"【P189Cas2Strm】模板解析失败: {e}")
                raise

    def render(self, **kwargs: Any) -> Optional[str]:
        if not self.base_template:
            return None
        try:
            return self.base_template.render(**kwargs)
        except Exception as e:
            logger.error(f"【P189Cas2Strm】模板渲染失败: {e}")
            return None


def extract_189_links_from_text(text: str) -> List[Dict[str, str]]:
    """
    从纯文本中提取 189 分享链接及提取码
    """
    matches = list(_P189_URL_PATTERN.finditer(text))
    results = []
    seen_codes = set()

    for i, match in enumerate(matches):
        full_url = match.group(0)
        share_code = match.group(1)
        access_code = ""

        # 从 URL 参数中提取 accessCode
        parsed_url = urlparse(full_url)
        qs = parse_qs(parsed_url.query)
        if "accessCode" in qs:
            access_code = qs["accessCode"][0]

        if not share_code:
            continue

        if share_code in seen_codes:
            continue
        seen_codes.add(share_code)

        # 如果没有提取码，在文本中检索
        if not access_code:
            search_start = match.end()
            search_end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
            sub_text = text[search_start:search_end].strip()
            for pat in _PWD_PATTERNS:
                m = pat.search(sub_text)
                if m:
                    access_code = m.group(1)
                    break

        results.append({"share_code": share_code, "access_code": access_code})

    return results


def extract_189_links(event_data: Dict[str, Any]) -> List[Dict[str, str]]:
    """
    从事件数据中提取 189 分享信息（支持富文本）
    """
    text = event_data.get("text") or event_data.get("caption") or event_data.get("arg_str") or ""
    entities = event_data.get("entities") or []

    search_text = text
    if entities:
        sorted_entities = sorted(
            entities, key=lambda x: x.get("offset", 0), reverse=True
        )
        for entity in sorted_entities:
            if entity.get("type") == "text_link":
                offset = entity.get("offset", 0)
                length = entity.get("length", 0)
                url = entity.get("url", "")
                if url:
                    search_text = search_text[:offset] + url + search_text[offset + length:]

    return extract_189_links_from_text(search_text)

def extract_tmdb_info(text: str) -> Tuple[Optional[int], Optional[str]]:
    """
    从文本中提取 TMDB ID 和媒体类型
    """
    tmdbid = None
    mtype = None
    
    # 匹配 TMDB 链接
    tmdb_url_match = re.search(r'themoviedb\.org/(tv|movie|collection)/(\d+)', text, re.IGNORECASE)
    if tmdb_url_match:
        mtype = tmdb_url_match.group(1).lower()
        tmdbid = int(tmdb_url_match.group(2))
    else:
        # 匹配文本格式: TMDB ID: 123
        tmdb_match = re.search(r'TMDB(?:[\s\-_]*ID)?\s*[：:= \-]+(\d+)', text, re.IGNORECASE)
        if tmdb_match:
            tmdbid = int(tmdb_match.group(1))
            
            # 手动提取可能的媒体类型
            clean_text = re.sub(r'https?://\S+', '', text)
            if re.search(r'电视剧|剧集|番剧|[美日韩台港英泰]剧|动漫|综艺|Season|S[0-9]+E[0-9]+|S[0-9]+|第[0-9]+[季集]', clean_text, re.I):
                mtype = "tv"
            elif re.search(r'系列|合集|Collection', clean_text, re.I):
                mtype = "collection"
            elif re.search(r'电影|Movie', clean_text, re.I):
                mtype = "movie"
                
    return tmdbid, mtype
