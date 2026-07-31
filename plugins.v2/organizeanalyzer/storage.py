import json
import time
from pathlib import Path
from typing import Dict, Any, List, Optional


class AnalyzerStorage:
    """
    整理异常分析结果与忽略白名单持久化存储
    """

    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        self.data_file = self.data_dir / "exceptions.json"

    def _ensure_dir(self):
        if not self.data_dir.exists():
            self.data_dir.mkdir(parents=True, exist_ok=True)

    def load_data(self) -> Dict[str, Any]:
        """
        读取数据
        """
        self._ensure_dir()
        if not self.data_file.exists():
            return {
                "exceptions": [],
                "ignored_keys": [],
                "last_run_time": "",
                "last_analyzed_id": 0,
                "summary": {
                    "total": 0,
                    "merged_files": 0,
                    "english_title": 0,
                    "unidentified": 0,
                    "failed_status": 0,
                    "duplicate_episode": 0,
                    "missing_dest": 0,
                    "invalid_episode": 0,
                }
            }
        try:
            with open(self.data_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data
        except Exception as e:
            return {
                "exceptions": [],
                "ignored_keys": [],
                "last_run_time": "",
                "last_analyzed_id": 0,
                "summary": {},
                "error": str(e)
            }

    def save_data(self, data: Dict[str, Any]) -> bool:
        """
        保存数据
        """
        self._ensure_dir()
        try:
            with open(self.data_file, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            return True
        except Exception:
            return False

    def update_analysis_results(self, new_exceptions: List[Dict[str, Any]], mode: str, max_history_id: int = 0) -> Dict[str, Any]:
        """
        更新分析结果
        :param new_exceptions: 新扫描出的异常列表
        :param mode: 'full' 或 'incremental'
        :param max_history_id: 本次扫描用到的最大 history ID
        """
        data = self.load_data()
        existing = data.get("exceptions", [])
        ignored_keys = set(data.get("ignored_keys", []))

        now_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

        if mode == "full":
            # 全量分析：保留忽略标记，重置其余列表
            merged_list = []
            for item in new_exceptions:
                key = item.get("key")
                if key in ignored_keys:
                    item["status"] = "ignored"
                merged_list.append(item)
            data["exceptions"] = merged_list
        else:
            # 增量分析：增量覆盖/追加
            existing_map = {item["key"]: item for item in existing}
            for item in new_exceptions:
                key = item.get("key")
                if key in ignored_keys:
                    item["status"] = "ignored"
                existing_map[key] = item
            data["exceptions"] = list(existing_map.values())

        data["last_run_time"] = now_str
        if max_history_id > data.get("last_analyzed_id", 0):
            data["last_analyzed_id"] = max_history_id

        # 重新统计 summary
        summary = {
            "total": 0,
            "merged_files": 0,
            "english_title": 0,
            "unidentified": 0,
            "failed_status": 0,
            "duplicate_episode": 0,
            "missing_dest": 0,
            "invalid_episode": 0,
            "ignored": 0,
        }
        for item in data["exceptions"]:
            if item.get("status") == "ignored":
                summary["ignored"] += 1
                continue
            summary["total"] += 1
            cat = item.get("type", "")
            if cat in summary:
                summary[cat] += 1

        data["summary"] = summary
        self.save_data(data)
        return data

    def ignore_exception(self, key: str) -> bool:
        """
        标记某项异常为已忽略
        """
        data = self.load_data()
        ignored_keys = set(data.get("ignored_keys", []))
        ignored_keys.add(key)
        data["ignored_keys"] = list(ignored_keys)

        for item in data.get("exceptions", []):
            if item.get("key") == key:
                item["status"] = "ignored"

        # 更新 summary
        summary = data.get("summary", {})
        total_active = 0
        ignored_cnt = 0
        for item in data.get("exceptions", []):
            if item.get("status") == "ignored":
                ignored_cnt += 1
            else:
                total_active += 1
        summary["total"] = total_active
        summary["ignored"] = ignored_cnt
        data["summary"] = summary

        return self.save_data(data)

    def clear_ignored(self) -> bool:
        """
        清空忽略白名单
        """
        data = self.load_data()
        data["ignored_keys"] = []
        for item in data.get("exceptions", []):
            item["status"] = "active"

        # 更新 summary
        summary = {
            "total": 0,
            "merged_files": 0,
            "english_title": 0,
            "unidentified": 0,
            "failed_status": 0,
            "duplicate_episode": 0,
            "missing_dest": 0,
            "invalid_episode": 0,
            "ignored": 0,
        }
        for item in data.get("exceptions", []):
            summary["total"] += 1
            cat = item.get("type", "")
            if cat in summary:
                summary[cat] += 1

        data["summary"] = summary
        return self.save_data(data)
