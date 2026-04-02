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
    p115sharestrm 插件配置管理器
    """
    model_config = ConfigDict(
        extra="ignore",
        arbitrary_types_allowed=True,
        validate_assignment=True,
    )

    PLUGIN_NAME: str = Field(default="P115ShareStrm", description="插件名称")

    enabled: bool = Field(default=False, description="插件总开关")
    cookies: Optional[str] = Field(default=None, description="115 Cookie")
    strm_save_path: str = Field(default="", description="STRM 保存路径")
    moviepilot_transfer: bool = Field(default=True, description="STRM 交由 MoviePilot 整理")

    strm_url_template_enabled: bool = Field(default=False, description="是否启用 STRM URL 自定义模板")
    strm_url_template: Optional[str] = Field(default=None, description="STRM URL 基础模板")

    user_rmt_mediaext: str = Field(
        default="mp4,mkv,ts,iso,rmvb,avi,mov,mpeg,mpg,wmv,3gp,asf,m4v,flv,m2ts,tp,f4v",
        description="可识别媒体后缀",
    )

    @property
    def moviepilot_address(self) -> str:
        """
        获取 MoviePilot 对外访问地址，用于构造默认 STRM redirect URL

        优先使用 APP_DOMAIN（如 http://192.168.100.139:3066），
        fallback 到 http://localhost:{PORT}
        """
        app_domain = getattr(settings, "APP_DOMAIN", None)
        if app_domain:
            return str(app_domain).rstrip("/")
        port = getattr(settings, "PORT", 3001)
        return f"http://localhost:{port}"

    def load_from_dict(self, config_dict: Dict[str, Any]) -> bool:
        """
        从字典加载配置
        """
        try:
            for key, value in config_dict.items():
                if hasattr(self, key):
                    setattr(self, key, value)
            return True
        except ValidationError as e:
            logger.error(f"【P115ShareStrm】配置验证失败: {e}")
            return False

    def update_plugin_config(self) -> Optional[bool]:
        """
        持久化配置到数据库
        """
        systemconfig = SystemConfigOper()
        return systemconfig.set(f"plugin.{self.PLUGIN_NAME}", self.model_dump(mode="json"))

    def get_config(self, key: str) -> Optional[Any]:
        return getattr(self, key, None)


configer = ConfigManager()
