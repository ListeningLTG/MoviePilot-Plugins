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
    tmdb_extract: bool = Field(default=False, description="从文本中自动提取 TMDB ID")
    imdb_extract: bool = Field(default=False, description="从文本中自动提取 IMDB ID")
    extract_blacklist: Optional[str] = Field(default=None, description="提取识别黑名单")
    moviepilot_address_custom: Optional[str] = Field(default=None, description="MoviePilot 地址 (手动配置优先)")

    strm_url_template_enabled: bool = Field(default=False, description="是否启用 STRM URL 自定义模板")
    strm_url_template: Optional[str] = Field(default=None, description="STRM URL 基础模板")
    strm_url_template_custom: Optional[str] = Field(
        default=None,
        description="STRM URL 扩展名特定规则，格式：ext1,ext2 => url_template [=> /save/path]",
    )

    user_rmt_mediaext: str = Field(
        default="mp4,mkv,ts,iso,rmvb,avi,mov,mpeg,mpg,wmv,3gp,asf,m4v,flv,m2ts,tp,f4v",
        description="可识别媒体后缀",
    )
    download_subtitle: bool = Field(default=False, description="同步下载分享中的字幕文件")
    user_subtitle_ext: str = Field(
        default="srt,ass,ssa",
        description="字幕文件后缀",
    )
    wait_organize_timeout: int = Field(
        default=120,
        description="STRM 整理等待超时时间（秒），若队列任务过多建议调低"
    )
    subtitle_audit_poll_timeout_hours: int = Field(
        default=6,
        description="字幕审核轮询超时时间（小时），分享链接审核中时后台最长等待时长",
    )
    subtitle_finalize_timeout_hours: int = Field(
        default=6,
        description="字幕后台收尾总超时（小时）：等整理映射+下载放置",
    )
    skip_wait_pending_threshold: int = Field(
        default=500,
        description="待整理文件数超过此值时跳过同步等待（主路径已后台化，作兜底/日志分级）",
    )
    skip_wait_pending_when_queued: int = Field(
        default=100,
        description="队列有积压且待整理数超过此值时跳过同步等待",
    )
    share_snap_speed_mode: int = Field(
        default=3,
        ge=0,
        le=3,
        description="分享扫描速度 0最快~3最慢（对齐 p115strmhelper，默认3最安全）",
    )
    scan_cache_ttl_hours: int = Field(
        default=72,
        ge=1,
        description="分享扫描结果本地缓存有效期（小时）",
    )
    reuse_scan_cache_for_sharestrm: bool = Field(
        default=True,
        description="全量 /sharestrm 在缓存未过期时复用扫描结果，跳过 115 列举",
    )
    audit_poll_min_sec: int = Field(
        default=60,
        ge=10,
        description="字幕审核轮询最小间隔（秒）",
    )
    audit_poll_max_sec: int = Field(
        default=300,
        ge=30,
        description="字幕审核轮询最大间隔（秒）",
    )
    share_receive_retry_hours: int = Field(
        default=3,
        ge=1,
        description="share_receive 限制接收(4200041)后首次自动重试等待（小时）",
    )

    @property
    def moviepilot_address(self) -> str:
        """
        获取 MoviePilot 访问地址，用于构造默认 STRM redirect URL

        优先级：手动配置 > APP_DOMAIN (系统设置) > localhost
        """
        if self.moviepilot_address_custom:
            return self.moviepilot_address_custom.rstrip("/")

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
