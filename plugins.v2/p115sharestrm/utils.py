import re
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple
from urllib.parse import quote

from jinja2 import Environment, Template, select_autoescape
from jinja2.exceptions import TemplateError
from p115client.util import share_extract_payload

from app.log import logger

# 115 分享链接正则
_P115_URL_PATTERN = re.compile(
    r"https?://(?:115\.com|115cdn\.com|anxia\.com)/s/[a-zA-Z0-9]+(?:\?[\w=&%-]*)?",
    re.I,
)

# 提取码正则（按优先级排序）
_PWD_PATTERNS = [
    re.compile(r'(?:pwd|password|提取码|码|访问码|密码)[:：\s=]*([a-zA-Z0-9]{4,6})', re.I),
    re.compile(r'^[:：\s=]*([a-zA-Z0-9]{4,6})\b'),
    re.compile(r'[\(\uff08]([a-zA-Z0-9]{4,6})[\)\uff09]'),
]


class StrmUrlTemplateResolver:
    """
    基于 Jinja2 的 STRM URL 模板解析器，支持扩展名特定规则和自定义保存路径。

    custom_rules 格式（每行一条）：
        ext1,ext2 => url_template [=> /custom/save/path]

    示例：
        iso => {{ base_url }}/iso?id={{ file_id }}&share_code={{ share_code }} => /data/isoshare
        mkv,mp4 => {{ base_url }}/redirect?id={{ file_id }}
    """

    def __init__(
        self,
        base_template: Optional[str] = None,
        custom_rules: Optional[str] = None,
        auto_escape: bool = False,
    ):
        self.env = Environment(
            autoescape=select_autoescape(["html", "xml"]) if auto_escape else False,
            trim_blocks=True,
            lstrip_blocks=True,
        )
        self._register_filters()

        # 基础模板（无扩展名匹配时使用）
        self.base_template: Optional[Template] = None
        if base_template:
            try:
                self.base_template = self.env.from_string(base_template)
            except TemplateError as e:
                logger.error(f"【P115ShareStrm】基础模板解析失败: {e}")
                raise

        # 扩展名 → (url_template, save_path_override)
        self._extension_rules: Dict[str, Tuple[Template, Optional[str]]] = {}
        if custom_rules:
            self._parse_custom_rules(custom_rules)

    def _register_filters(self):
        self.env.filters["urlencode"] = lambda v: quote(str(v), safe="")
        self.env.filters["path_encode"] = lambda v: quote(str(v), safe="/")
        self.env.filters["upper"] = lambda v: str(v).upper() if v else ""
        self.env.filters["lower"] = lambda v: str(v).lower() if v else ""

    def _parse_custom_rules(self, config_str: str):
        """
        解析扩展名特定规则。

        格式：ext1,ext2 => url_template [=> /save/path]
        """
        for line in config_str.strip().splitlines():
            line = line.strip()
            if not line or "=>" not in line:
                continue
            parts = line.split("=>")
            if len(parts) < 2:
                continue
            try:
                exts_part = parts[0].strip()
                url_tpl_str = parts[1].strip()
                save_path_override = parts[2].strip() if len(parts) >= 3 else None

                if not url_tpl_str:
                    logger.warning(f"【P115ShareStrm】扩展名规则模板为空，跳过: {line}")
                    continue

                try:
                    tpl = self.env.from_string(url_tpl_str)
                except TemplateError as e:
                    logger.error(f"【P115ShareStrm】扩展名规则模板解析失败: {line}: {e}")
                    continue

                for ext in exts_part.split(","):
                    ext = ext.strip().lower()
                    if not ext:
                        continue
                    if not ext.startswith("."):
                        ext = "." + ext
                    self._extension_rules[ext] = (tpl, save_path_override or None)
                    logger.debug(
                        f"【P115ShareStrm】注册扩展名规则: {ext} => "
                        f"{url_tpl_str[:40]}... "
                        f"(保存路径: {save_path_override or '默认'})"
                    )
            except Exception as e:
                logger.error(f"【P115ShareStrm】解析扩展名规则失败: {line}: {e}")

    def get_save_path_override(self, file_name: str) -> Optional[str]:
        """
        返回该文件扩展名对应的自定义保存路径，无则返回 None。
        """
        ext = Path(file_name).suffix.lower()
        rule = self._extension_rules.get(ext)
        return rule[1] if rule else None

    def render(self, file_name: str = "", **kwargs: Any) -> Optional[str]:
        """
        渲染模板，返回最终 URL。

        优先匹配扩展名规则，无匹配则使用基础模板。
        """
        ext = Path(file_name).suffix.lower() if file_name else ""
        rule = self._extension_rules.get(ext)
        tpl = rule[0] if rule else self.base_template

        if not tpl:
            return None

        ctx = {"file_name": file_name, **kwargs}
        try:
            return tpl.render(**ctx)
        except Exception as e:
            logger.error(f"【P115ShareStrm】模板渲染失败: {e}")
            return None


def extract_115_links_from_text(text: str) -> List[Dict[str, str]]:
    """
    从纯文本中提取 115 分享链接及提取码

    :param text: 原始文本（可含多个链接）
    :return: list of {"share_code": ..., "receive_code": ...}
    """
    matches = list(_P115_URL_PATTERN.finditer(text))
    results = []
    seen_codes = set()

    for i, match in enumerate(matches):
        full_url = match.group(0)

        # 尝试用 p115client 内置解析器解析
        try:
            data = share_extract_payload(full_url)
        except Exception as e:
            logger.warning(f"【P115ShareStrm】share_extract_payload 异常: {e}，URL: {full_url}")
            data = None

        if not data:
            logger.warning(f"【P115ShareStrm】无法解析 115 链接: {full_url}")
            continue

        share_code = data.get("share_code") or ""
        receive_code = data.get("receive_code") or ""

        if not share_code:
            continue

        if share_code in seen_codes:
            continue
        seen_codes.add(share_code)

        # 如果链接自带提取码（password=xxx），优先使用
        if not receive_code:
            # 在下一个链接出现前的文本中检索提取码
            search_start = match.end()
            search_end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
            sub_text = text[search_start:search_end].strip()
            for pat in _PWD_PATTERNS:
                m = pat.search(sub_text)
                if m:
                    receive_code = m.group(1)
                    break

        logger.debug(
            f"【P115ShareStrm】解析链接 share_code={share_code}, receive_code={receive_code}"
        )
        results.append({"share_code": share_code, "receive_code": receive_code})

    return results


def extract_115_links(event_data: Dict[str, Any]) -> List[Dict[str, str]]:
    """
    从事件数据中提取 115 分享信息（支持富文本/Entities）

    :param event_data: MoviePilot 事件 event_data 字典
    :return: list of {"share_code": ..., "receive_code": ...}
    """
    text = event_data.get("text") or event_data.get("caption") or event_data.get("arg_str") or ""
    entities = event_data.get("entities") or []

    # 将 text_link 实体替换为实际 URL（富文本场景）
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

    return extract_115_links_from_text(search_text)
