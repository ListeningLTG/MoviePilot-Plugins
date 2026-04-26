from typing import Optional, Dict, Any
from pydantic import (
    BaseModel,
    ValidationError,
    ConfigDict,
    Field,
)

from app.log import logger
from app.core.config import settings
from app.db.systemconfig_oper import SystemConfigOper


class ConfigManager(BaseModel):
    """
    p189cas2strm 插件配置管理器
    """
    model_config = ConfigDict(
        extra="ignore",
        arbitrary_types_allowed=True,
        validate_assignment=True,
    )

    PLUGIN_NAME: str = Field(default="P189Cas2Strm", description="插件名称")

    enabled: bool = Field(default=False, description="插件开启状态")
    username: str = Field(default="", description="天翼云盘账号")
    password: str = Field(default="", description="天翼云盘密码")
    strm_save_path: str = Field(default="", description="STRM 本地保存路径")
    p189_target_path: str = Field(default="/p189cas2strm_cache", description="网盘秒传缓存目录")
    cleanup_cron: str = Field(default="0 2 * * *", description="定时清理 Cron 表达式")
    moviepilot_transfer: bool = Field(default=True, description="STRM 交由 MP 整理")
    tmdb_extract: bool = Field(default=False, description="自动提取 TMDB ID")
    bulk_save_enabled: bool = Field(default=False, description="是否启用整目录批量转存")
    max_concurrency: int = Field(default=2, description="CAS 并发处理数")
    moviepilot_address_custom: Optional[str] = Field(default=None, description="MoviePilot 地址 (手动配置优先)")
    play_proxy_enabled: bool = Field(default=False, description="是否启用直链代理加速")
    play_proxy_prefix: str = Field(default="", description="直链代理地址前缀")
    
    @property
    def plugin_data_path(self) -> str:
        """获取插件数据存储目录"""
        import os

        base_path = (
            getattr(settings, "PLUGIN_CONFIG_PATH", None)
            or getattr(settings, "INNER_CONFIG_PATH", None)
            or getattr(settings, "CONFIG_PATH", None)
            or "."
        )
        data_path = os.path.join(str(base_path), "p189cas2strm")
        if not os.path.exists(data_path):
            os.makedirs(data_path, exist_ok=True)
        return data_path

    @property
    def cas_record_path(self) -> str:
        """CAS 记录持久化路径"""
        import os
        return os.path.join(self.plugin_data_path, "cas_records.json")

    @property
    def cookie_store_path(self) -> str:
        """登录会话 Cookie 持久化路径"""
        import os
        return os.path.join(self.plugin_data_path, "session_cookies.txt")

    @property
    def moviepilot_address(self) -> str:
        """
        获取 MoviePilot 访问地址
        """
        if self.moviepilot_address_custom:
            return self.moviepilot_address_custom.rstrip("/")

        app_domain = getattr(settings, "APP_DOMAIN", None)
        if app_domain:
            return str(app_domain).rstrip("/")

        port = getattr(settings, "PORT", 3001)
        return f"http://localhost:{port}"

    def load_from_dict(self, config_dict: Dict[str, Any]) -> bool:
        """从字典加载配置"""
        try:
            for key, value in config_dict.items():
                if hasattr(self, key):
                    setattr(self, key, value)
            return True
        except ValidationError as e:
            logger.error(f"【P189Cas2Strm】配置验证失败: {e}")
            return False

    def update_plugin_config(self) -> Optional[bool]:
        """持久化配置到数据库"""
        systemconfig = SystemConfigOper()
        return systemconfig.set(f"plugin.{self.PLUGIN_NAME}", self.model_dump(mode="json"))

configer = ConfigManager()
