#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
短剧批量识别与离线整理脚本 (short-move.py)
--------------------------------------------------
功能：
1. 交互式引导用户输入【源扫描目录】和【短剧目标目录】。
2. 继承 plugins.v2/shortdramacompilation 插件的 4 重识别管道：
   策略 0: 本地 cache.json 缓存 (0ms 响应)
   策略 1: TMDB 播出平台 Network ID (如 8020 红果短剧)
   策略 2: TMDB 官方单集片长 (episode_run_time)
   策略 3: 豆瓣单集片长 (解析网页 '单集片长:')
   策略 4: FFprobe 文件/STRM 网络流真实片长探测 (兜底)
3. 生成详细 Markdown 分析报告文件 short_drama_scan_report.md 供人工核对。
4. 用户二次确认后，执行批量移动并同步更新 cache.json 缓存文件。
"""

import json
import os
import re
import shutil
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

# ==================== 全局默认配置 ====================
DEFAULT_EPISODE_DURATION = 8.0  # 单集时长阈值（分钟）
DEFAULT_SHORT_NETWORKS = ["8020"]  # TMDB 短剧平台 ID (8020: 红果短剧)
TMDB_API_KEY = "b2405786a34c264fb9795e1e1279589d"  # 默认 TMDB API Key
TMDB_BASE_URL = "https://api.themoviedb.org/3"

# 尝试寻找 MP 配置文件中的缓存路径
POSSIBLE_CACHE_PATHS = [
    Path("/vol1/1000/data/config/plugins/shortdramacompilation/cache.json"),
    Path("/root/.config/moviepilot/plugins/shortdramacompilation/cache.json"),
    Path.home() / ".config" / "moviepilot" / "plugins" / "shortdramacompilation" / "cache.json",
    Path("cache.json"),
]


class ShortDramaScanner:
    def __init__(
        self,
        source_dir: str,
        target_dir: str,
        anime_target_dir: Optional[str] = None,
        episode_duration: float = DEFAULT_EPISODE_DURATION,
        short_networks: List[str] = None,
    ):
        self.source_dir = Path(source_dir).resolve()
        self.target_dir = Path(target_dir).resolve()
        self.anime_target_dir = Path(anime_target_dir).resolve() if anime_target_dir else None
        self.episode_duration = float(episode_duration)
        self.short_networks = short_networks or DEFAULT_SHORT_NETWORKS
        self.cache_file = self._find_cache_file()
        self.cache_data: Dict[str, dict] = self._load_cache()

    def check_is_anime(self, tmdb_id: Optional[str], folder_path: Path, tmdb_info: Optional[dict] = None) -> bool:
        """检查剧集是否属于动画类型（包含 24 小时 TTL 缓存刷新机制）"""
        now = datetime.now()

        # Step A: 检查缓存
        if tmdb_id and str(tmdb_id) in self.cache_data:
            cache_item = self.cache_data[str(tmdb_id)]
            if isinstance(cache_item, dict) and "is_anime" in cache_item:
                checked_at_str = cache_item.get("anime_checked_at")
                if checked_at_str:
                    try:
                        checked_at = datetime.strptime(checked_at_str, "%Y-%m-%d %H:%M:%S")
                        if (now - checked_at).total_seconds() < 86400:
                            return bool(cache_item["is_anime"])
                    except Exception:
                        pass

        # Step B: 24 小时超时或首次获取
        is_anime = False
        if tmdb_info and tmdb_info.get("genres"):
            for g in tmdb_info["genres"]:
                g_id = g.get("id")
                g_name = str(g.get("name", "")).lower()
                if g_id == 16 or "动画" in g_name or "animation" in g_name:
                    is_anime = True
                    break

        if not is_anime and tmdb_id:
            info = tmdb_info or self.fetch_tmdb_info(tmdb_id)
            if info and info.get("genres"):
                for g in info["genres"]:
                    g_id = g.get("id")
                    g_name = str(g.get("name", "")).lower()
                    if g_id == 16 or "动画" in g_name or "animation" in g_name:
                        is_anime = True
                        break

        # Step C: 更新 cache 中的 is_anime 和 anime_checked_at
        if tmdb_id and str(tmdb_id) in self.cache_data:
            self.cache_data[str(tmdb_id)]["is_anime"] = is_anime
            self.cache_data[str(tmdb_id)]["anime_checked_at"] = now.strftime("%Y-%m-%d %H:%M:%S")
            self._save_cache()

        return is_anime

    def _find_cache_file(self) -> Path:
        """选择可写的缓存文件路径"""
        for p in POSSIBLE_CACHE_PATHS:
            if p.exists() and p.is_file():
                return p
        # 默认尝试使用第一个存在父目录的路径，或者存本地
        for p in POSSIBLE_CACHE_PATHS:
            if p.parent.exists():
                return p
        return Path("cache.json").resolve()

    def _load_cache(self) -> Dict[str, dict]:
        """读取缓存"""
        if self.cache_file.exists():
            try:
                data = json.loads(self.cache_file.read_text(encoding="utf-8"))
                print(f"[*] 成功加载本地缓存文件 [{self.cache_file}]，已记录 {len(data)} 条剧集信息。")
                return data
            except Exception as e:
                print(f"[!] 读取缓存文件 {self.cache_file} 失败: {e}")
        return {}

    def _save_cache(self):
        """保存缓存"""
        try:
            self.cache_file.parent.mkdir(parents=True, exist_ok=True)
            self.cache_file.write_text(
                json.dumps(self.cache_data, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            print(f"[*] 已同步更新本地缓存文件: {self.cache_file}")
        except Exception as e:
            print(f"[!] 保存缓存文件失败: {e}")

    def _update_cache(
        self,
        tmdb_id: Union[int, str],
        title: str,
        is_short: bool,
        strategy: str = "",
        strategy_type: str = "runtime",
        runtime: float = 0.0,
    ):
        """更新条目到缓存"""
        if not tmdb_id:
            return
        key = str(tmdb_id)
        self.cache_data[key] = {
            "title": title or "",
            "is_short_drama": bool(is_short),
            "strategy": strategy or "",
            "strategy_type": strategy_type or "runtime",
            "runtime": round(float(runtime), 1) if runtime else 0.0,
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        self._save_cache()

    @staticmethod
    def extract_show_info(folder_path: Path) -> Tuple[Optional[str], str]:
        """
        从文件夹名称或内部 tvshow.nfo 中解析 TMDB ID 和剧集名称
        """
        folder_name = folder_path.name

        # 优先由文件夹名称正则表达式抽取: [tmdbid=329721]
        match = re.search(r"\[tmdbid=(\d+)\]", folder_name, re.IGNORECASE)
        if match:
            tmdb_id = match.group(1)
            # 清理标题名称 (去除 (2026) [tmdbid=...] 等)
            clean_title = re.sub(r"\(\d{4}\)", "", folder_name)
            clean_title = re.sub(r"\[tmdbid=\d+\]", "", clean_title, flags=re.IGNORECASE).strip()
            return tmdb_id, clean_title

        # 备选：尝试解析目录下的 tvshow.nfo
        nfo_path = folder_path / "tvshow.nfo"
        if nfo_path.exists():
            try:
                content = nfo_path.read_text(encoding="utf-8", errors="replace")
                # 寻找 <tmdbid>XXXX</tmdbid> 或 <id>XXXX</id>
                tmdb_match = re.search(r"<tmdbid>(\d+)</tmdbid>", content, re.IGNORECASE)
                if not tmdb_match:
                    tmdb_match = re.search(r"<id>(\d+)</id>", content, re.IGNORECASE)
                if tmdb_match:
                    # 提取标题
                    title_match = re.search(r"<title>(.*?)</title>", content, re.IGNORECASE)
                    title = title_match.group(1).strip() if title_match else folder_name
                    return tmdb_match.group(1), title
            except Exception:
                pass

        return None, folder_name

    def fetch_tmdb_info(self, tmdb_id: str) -> Optional[dict]:
        """请求 TMDB 电视剧详情 API"""
        url = f"{TMDB_BASE_URL}/tv/{tmdb_id}?api_key={TMDB_API_KEY}&language=zh-CN"
        headers = {"User-Agent": "Mozilla/5.0"}
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=8) as response:
                if response.status == 200:
                    return json.loads(response.read().decode("utf-8"))
        except Exception as e:
            pass
        return None

    def fetch_douban_runtime(self, douban_id: str) -> float:
        """请求豆瓣网页解析单集片长"""
        url = f"https://movie.douban.com/subject/{douban_id}/"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=5) as response:
                html = response.read().decode("utf-8", errors="ignore")
                match = re.search(r"单集片长:?\s*</span>\s*([^<]+)", html)
                if match:
                    text = match.group(1).strip()
                    min_match = re.search(r"(\d+)\s*分(?:\s*(\d+)\s*秒)?", text)
                    if min_match:
                        mins = float(min_match.group(1))
                        secs = float(min_match.group(2)) if min_match.group(2) else 0.0
                        return mins + (secs / 60.0)
                    digit_match = re.search(r"(\d+(?:\.\d+)?)", text)
                    if digit_match:
                        return float(digit_match.group(1))
        except Exception:
            pass
        return 0.0

    def probe_video_duration(self, folder_path: Path) -> float:
        """在剧集目录下寻找视频文件/STRM文件并使用 FFprobe 测量时长"""
        media_files = []
        for root, _, files in os.walk(folder_path):
            for file in files:
                ext = Path(file).suffix.lower()
                if ext in [".strm", ".mp4", ".mkv", ".ts", ".flv", ".mov", ".avi"]:
                    media_files.append(Path(root) / file)

        if not media_files:
            return 0.0

        # 取样本文件（优先找 Season 01 内的文件）
        sample_file = media_files[0]
        for f in media_files:
            if "season" in f.parent.name.lower() or "s01" in f.name.lower():
                sample_file = f
                break

        probe_target = str(sample_file)
        # 如果是 .strm 文件，解析里面的网络 URL
        if sample_file.suffix.lower() == ".strm":
            try:
                content = sample_file.read_text(encoding="utf-8-sig", errors="replace")
                for line in content.splitlines():
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    if len(line) >= 2 and line[0] == line[-1] and line[0] in "\"'":
                        line = line[1:-1].strip()
                    if "%" in line:
                        try:
                            line = urllib.parse.unquote(line)
                        except Exception:
                            pass
                    if line:
                        probe_target = line
                        break
            except Exception:
                pass

        cmd = [
            "ffprobe",
            "-v",
            "error",
            "-probesize",
            "1000000",
            "-analyzeduration",
            "2000000",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            probe_target,
        ]
        try:
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            output, _ = process.communicate(timeout=25)
            duration_str = output.decode("utf-8", errors="ignore").strip()
            if duration_str:
                return round(float(duration_str) / 60.0, 1)
        except Exception:
            pass

        return 0.0

    def identify_show(self, folder_path: Path) -> Tuple[bool, str, Optional[str], str]:
        """
        多策略管道识别核心方法
        返回: (is_short_drama, strategy_name, tmdb_id, title)
        """
        tmdb_id, title = self.extract_show_info(folder_path)

        # 策略 0: 本地缓存判断 (支持动态片长比对)
        if tmdb_id and str(tmdb_id) in self.cache_data:
            cache_item = self.cache_data[str(tmdb_id)]
            if isinstance(cache_item, dict) and "is_short_drama" in cache_item:
                st_type = cache_item.get("strategy_type", "runtime")
                cached_runtime = float(cache_item.get("runtime", 0.0))
                cached_strategy = cache_item.get("strategy", "策略0: 本地 JSON 缓存")

                if st_type in ["network", "manual"]:
                    is_short = bool(cache_item["is_short_drama"])
                    return is_short, cached_strategy or "策略0: 本地缓存 (平台/手动)", tmdb_id, title

                if cached_runtime > 0:
                    dynamic_is_short = (cached_runtime <= self.episode_duration)
                    strategy_label = f"策略0: 本地缓存动态比对 ({cached_runtime}m {'≤' if dynamic_is_short else '>'} 阈值 {self.episode_duration}m)"
                    return dynamic_is_short, strategy_label, tmdb_id, title

                is_short = bool(cache_item["is_short_drama"])
                return is_short, cached_strategy or "策略0: 本地 JSON 缓存", tmdb_id, title
            elif isinstance(cache_item, bool):
                return cache_item, "策略0: 本地 JSON 缓存", tmdb_id, title

        tmdb_info = self.fetch_tmdb_info(tmdb_id) if tmdb_id else None

        # 策略 1: TMDB 播出平台 Network ID 匹配
        if tmdb_info:
            networks = tmdb_info.get("networks") or []
            for net in networks:
                net_id = str(net.get("id"))
                if net_id in self.short_networks:
                    net_name = net.get("name") or net_id
                    strategy = f"策略1: TMDB 播出平台 ({net_name})"
                    self._update_cache(tmdb_id, title, True, strategy=strategy, strategy_type="network", runtime=0.0)
                    return True, strategy, tmdb_id, title

        # 策略 2: TMDB 官方片长匹配
        if tmdb_info:
            runtimes = tmdb_info.get("episode_run_time") or []
            valid_runtimes = [float(r) for r in runtimes if float(r) > 0]
            if valid_runtimes:
                rt = valid_runtimes[0]
                if all(r <= self.episode_duration for r in valid_runtimes):
                    strategy = f"策略2: TMDB 标注片长 ({rt}m ≤ 阈值)"
                    self._update_cache(tmdb_id, title, True, strategy=strategy, strategy_type="runtime", runtime=rt)
                    return True, strategy, tmdb_id, title
                elif any(r > self.episode_duration for r in valid_runtimes):
                    strategy = f"策略2: TMDB 标注片长 ({rt}m > 阈值)"
                    self._update_cache(tmdb_id, title, False, strategy=strategy, strategy_type="runtime", runtime=rt)
                    return False, strategy, tmdb_id, title

        # 策略 4: FFprobe 文件真实片长探测 (兜底)
        duration = self.probe_video_duration(folder_path)
        if duration > self.episode_duration:
            strategy = f"策略4: FFprobe 探测 ({duration}m > 阈值)"
            if tmdb_id:
                self._update_cache(tmdb_id, title, False, strategy=strategy, strategy_type="runtime", runtime=duration)
            return False, strategy, tmdb_id, title
        elif 0 < duration <= self.episode_duration:
            strategy = f"策略4: FFprobe 探测 ({duration}m ≤ 阈值)"
            if tmdb_id:
                self._update_cache(tmdb_id, title, True, strategy=strategy, strategy_type="runtime", runtime=duration)
            return True, strategy, tmdb_id, title

        # 若未识别出为短剧，默认记为普通长剧
        if tmdb_id:
            self._update_cache(tmdb_id, title, False, strategy="未满足短剧条件", strategy_type="runtime", runtime=0.0)
        return False, "未满足短剧条件", tmdb_id, title

    def scan(self) -> List[Dict[str, Any]]:
        """扫描全目录并返回匹配到的短剧结果清单"""
        results = []
        subdirs = [p for p in self.source_dir.iterdir() if p.is_dir()]
        total = len(subdirs)

        print(f"\n[*] 开始扫描目录: {self.source_dir}")
        print(f"[*] 共发现 {total} 个剧集子文件夹，正在逐个识别，请稍候...\n")

        for idx, folder in enumerate(subdirs, 1):
            sys.stdout.write(f"\r处理中 [{idx}/{total}]: {folder.name[:35]}...".ljust(60))
            sys.stdout.flush()

            is_short, strategy, tmdb_id, title = self.identify_show(folder)
            if is_short:
                # 检查动画类型并路由目标路径
                is_anime = self.check_is_anime(tmdb_id, folder)
                if is_anime and self.anime_target_dir:
                    dest_path = self.anime_target_dir / folder.name
                    category_label = "动画短剧"
                else:
                    dest_path = self.target_dir / folder.name
                    category_label = "普通短剧"

                results.append(
                    {
                        "folder_name": folder.name,
                        "title": title,
                        "tmdb_id": tmdb_id,
                        "tmdb_url": f"https://www.themoviedb.org/tv/{tmdb_id}" if tmdb_id else "N/A",
                        "strategy": f"{strategy} [{category_label}]",
                        "src_path": str(folder),
                        "dest_path": str(dest_path),
                    }
                )

        sys.stdout.write("\r" + " " * 70 + "\r")
        sys.stdout.flush()
        print(f"[✓] 扫描完成！在 {total} 个剧集中共检测到 {len(results)} 部微短剧。")
        return results

    def generate_report(self, results: List[Dict[str, Any]], report_file: Path = Path("short_drama_scan_report.md")):
        """生成 Markdown 分析报告"""
        lines = []
        lines.append("# 短剧扫描分析报告\n")
        lines.append(f"- **扫描源目录**: `{self.source_dir}`")
        lines.append(f"- **短剧目标目录**: `{self.target_dir}`")
        if self.anime_target_dir:
            lines.append(f"- **动画短剧独立目录**: `{self.anime_target_dir}`")
        lines.append(f"- **片长判定阈值**: `{self.episode_duration}` 分钟")
        lines.append(f"- **生成时间**: `{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`")
        lines.append(f"- **检测到短剧数量**: **{len(results)}** 部\n")

        lines.append("## 拟移动短剧清单\n")
        if not results:
            lines.append("> 提示：未检测到需要移动的短剧。")
        else:
            lines.append("| 序号 | 剧集文件夹名称 | TMDB ID & 链接 | 识别策略 | 目标存放路径 |")
            lines.append("|---|---|---|---|---|")
            for idx, item in enumerate(results, 1):
                tmdb_link = (
                    f"[{item['tmdb_id']}]({item['tmdb_url']})" if item["tmdb_id"] else "无"
                )
                lines.append(
                    f"| {idx} | `{item['folder_name']}` | {tmdb_link} | {item['strategy']} | `{item['dest_path']}` |"
                )

        report_file.write_text("\n".join(lines), encoding="utf-8")
        print(f"\n[★] 分析报告已成功生成: {report_file.resolve()}")

    def execute_move(self, results: List[Dict[str, Any]]):
        """执行批量移动"""
        if not results:
            print("[!] 没有需要移动的短剧。")
            return

        total = len(results)
        print(f"\n[*] 开始执行文件移动操作 (共 {total} 部)...")

        success_count = 0
        for idx, item in enumerate(results, 1):
            src = Path(item["src_path"])
            dest = Path(item["dest_path"])

            print(f"[{idx}/{total}] 移动: {src.name}")
            print(f"       -> {dest}")

            try:
                dest.parent.mkdir(parents=True, exist_ok=True)
                if dest.exists():
                    # 如果目标位置已存在同名目录，合并内容
                    for sub_item in src.iterdir():
                        target_sub = dest / sub_item.name
                        if sub_item.is_file():
                            shutil.move(str(sub_item), str(target_sub))
                        else:
                            shutil.copytree(str(sub_item), str(target_sub), dirs_exist_ok=True)
                            shutil.rmtree(str(sub_item), ignore_errors=True)
                    shutil.rmtree(str(src), ignore_errors=True)
                else:
                    shutil.move(str(src), str(dest))

                success_count += 1
                print("      [✓] 移动成功")
            except Exception as e:
                print(f"      [✗] 移动失败: {e}")

        print(f"\n[✓] 批量移动完成！成功: {success_count} / {total}")


def prompt_user():
    """交互式引导与可编辑核对函数"""
    print("\n================================================================================")
    print("           MoviePilot 存量剧集 - 短剧批量识别与离线整理工具 (v0.2.0)           ")
    print("================================================================================\n")

    # 1. 引导用户输入路径
    while True:
        src_path_str = input("1. 请输入【需要识别短剧的电视剧目录】绝对路径:\n   例如: /vol1/1000/data/strm/shareStrm/电视剧/国产剧\n   路径: ").strip()
        if not src_path_str:
            print("[!] 路径不能为空，请重新输入。")
            continue
        src_path = Path(src_path_str)
        if not src_path.exists():
            print(f"[!] 找不到路径: {src_path.resolve()}，请确认目录是否存在。")
            continue
        break

    while True:
        dest_path_str = input("\n2. 请输入【短剧存放的目标目录】绝对路径:\n   例如: /vol1/1000/data/strm/shareStrm/电视剧/短剧\n   路径: ").strip()
        if not dest_path_str:
            print("[!] 路径不能为空，请重新输入。")
            continue
        dest_path = Path(dest_path_str)
        break

    anime_dest_path_str = input("\n3. 请输入【动画短剧独立目标目录】绝对路径 (直接回车表示不开启独立动画短剧目录):\n   例如: /vol1/1000/data/strm/shareStrm/动漫/短剧\n   路径: ").strip()
    anime_dest_path = Path(anime_dest_path_str) if anime_dest_path_str else None

    while True:
        duration_str = input(f"\n4. 请输入【单集片长判定阈值（分钟）】(直接回车默认使用 {DEFAULT_EPISODE_DURATION}): ").strip()
        if not duration_str:
            episode_duration = float(DEFAULT_EPISODE_DURATION)
            break
        try:
            episode_duration = float(duration_str)
            if episode_duration <= 0:
                print("[!] 阈值必须为大于 0 的数字，请重新输入。")
                continue
            break
        except ValueError:
            print("[!] 输入无效，请输入有效的数字（如 8 或 10）。")

    # 2. 确认配置信息
    print("\n================================================================================")
    print("                              配置二次确认                                      ")
    print("--------------------------------------------------------------------------------")
    print(f"  源 扫 描 目录:  {src_path.resolve()}")
    print(f"  普通短剧目录:  {dest_path.resolve()}")
    print(f"  动画短剧目录:  {anime_dest_path.resolve() if anime_dest_path else '未开启 (合并存入普通短剧目录)'}")
    print(f"  单集片长阈值:  {episode_duration} 分钟")
    print("================================================================================")

    confirm = input("是否确认以上目录配置并开始扫描分析？[y/N]: ").strip().lower()
    if confirm not in ["y", "yes"]:
        print("[!] 用户取消操作，程序退出。")
        sys.exit(0)

    # 3. 执行扫描
    scanner = ShortDramaScanner(
        source_dir=str(src_path),
        target_dir=str(dest_path),
        anime_target_dir=str(anime_dest_path) if anime_dest_path else None,
        episode_duration=episode_duration,
    )
    results = scanner.scan()

    # 4. 生成分析报告
    report_file = Path("short_drama_scan_report.md").resolve()
    scanner.generate_report(results, report_file=report_file)

    if not results:
        print("\n[*] 扫描未发现任何短剧，无需移动。程序结束。")
        sys.exit(0)

    # 5. 可编辑交互逻辑菜单
    while True:
        print("\n================================================================================")
        print("                            短剧分析结果核对与可编辑交互                        ")
        print("--------------------------------------------------------------------------------")
        print(f" 当前拟移动的短剧数量: {len(results)} 部")
        print(f" 详细分析报告已生成至: {report_file}")
        print("--------------------------------------------------------------------------------")
        print(" [1] 确认无误，立即执行全量移动")
        print(" [2] 在终端直接排除指定剧集 (输入序号如: 2, 4)")
        print(" [3] 从您修改后的 short_drama_scan_report.md 重新同步读取")
        print(" [4] 从您修改后的 cache.json 本地缓存重新同步读取")
        print(" [0] 取消并退出程序")
        print("================================================================================")

        choice = input("请选择操作 [1/2/3/4/0]: ").strip()

        if choice == "1":
            break
        elif choice == "2":
            # 终端模式按序号排除
            print("\n当前拟移动剧集列表：")
            for idx, r in enumerate(results, 1):
                print(f"  [{idx}] {r['folder_name']} (TMDB ID: {r['tmdb_id'] or '无'}) - {r['strategy']}")
            ex_input = input("\n请输入要排除的剧集序号（多个用逗号隔开，如 2,4）: ").strip()
            if not ex_input:
                continue
            ex_indices = set()
            for part in re.split(r"[,\s]+", ex_input):
                if part.isdigit():
                    ex_indices.add(int(part))

            excluded_items = []
            new_results = []
            for idx, r in enumerate(results, 1):
                if idx in ex_indices:
                    excluded_items.append(r)
                else:
                    new_results.append(r)

            if excluded_items:
                for item in excluded_items:
                    print(f"[+] 已将《{item['folder_name']}》从移动队列中排除，并写回 cache.json 为非短剧。")
                    if item.get("tmdb_id"):
                        scanner._update_cache(item["tmdb_id"], item["title"], False)
                results = new_results
                scanner.generate_report(results, report_file=report_file)
                if not results:
                    print("\n[*] 排除后拟移动列表为空，程序退出。")
                    sys.exit(0)
            else:
                print("[!] 未匹配到有效序号，未做修改。")

        elif choice == "3":
            # 从修改后的 Markdown 报告重新解析
            print(f"\n[*] 正在重新解析 Markdown 报告: {report_file} ...")
            if not report_file.exists():
                print(f"[!] 找不到报告文件: {report_file}")
                continue
            content = report_file.read_text(encoding="utf-8")
            kept_names = set()
            for line in content.splitlines():
                if line.startswith("|") and not line.startswith("| 序号") and not line.startswith("|---|"):
                    parts = [p.strip() for p in line.split("|") if p.strip()]
                    if len(parts) >= 2:
                        folder_name = parts[1].strip("`")
                        kept_names.add(folder_name)

            new_results = []
            excluded_items = []
            for r in results:
                if r["folder_name"] in kept_names:
                    new_results.append(r)
                else:
                    excluded_items.append(r)

            for item in excluded_items:
                print(f"[+] 报告中已删去 《{item['folder_name']}》，已将其移除，并更新 cache.json 为非短剧。")
                if item.get("tmdb_id"):
                    scanner._update_cache(item["tmdb_id"], item["title"], False)

            results = new_results
            print(f"[✓] 同步完成！当前剩余 {len(results)} 部短剧。")
            if not results:
                print("\n[*] 报告中已清空所有短剧，程序退出。")
                sys.exit(0)

        elif choice == "4":
            # 从 cache.json 重新加载
            scanner.cache_data = scanner._load_cache()
            new_results = []
            for r in results:
                tid = str(r.get("tmdb_id"))
                if tid and tid in scanner.cache_data:
                    c_item = scanner.cache_data[tid]
                    is_short = c_item.get("is_short_drama", True) if isinstance(c_item, dict) else c_item
                    if is_short:
                        new_results.append(r)
                    else:
                        print(f"[+] cache.json 中已将 《{r['folder_name']}》 标记为非短剧，已移除。")
                else:
                    new_results.append(r)
            results = new_results
            scanner.generate_report(results, report_file=report_file)
            print(f"[✓] 缓存重新加载完成！当前剩余 {len(results)} 部短剧。")

        elif choice == "0":
            print("[!] 用户取消操作，未移动任何文件。程序退出。")
            sys.exit(0)

    # 6. 执行批量移动
    scanner.execute_move(results)


if __name__ == "__main__":
    try:
        prompt_user()
    except KeyboardInterrupt:
        print("\n\n[!] 用户中断程序执行。")
        sys.exit(0)
