"""
影视洗版插件 — UI 渲染模块
================================
配置表单和结果展示页的 Vuetify JSON 结构构建。
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from .quality import SCORE_PROFILES


# ============================================================
# CSS 样式常量
# ============================================================

WASH_STYLES = '''
    .wash-stats-card {
        border-radius: 16px;
        transition: all 0.3s ease;
    }
    .wash-stats-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 20px rgba(0,0,0,0.08) !important;
    }
    .wash-version-card {
        border-radius: 12px;
        transition: all 0.3s ease;
        margin-bottom: 8px;
        border: 1px solid rgba(0,0,0,0.04);
    }
    .wash-version-card:hover {
        transform: translateX(4px);
        box-shadow: 0 2px 12px rgba(0,0,0,0.06) !important;
    }
    .wash-best-badge {
        background: linear-gradient(135deg, #4CAF50, #66BB6A);
        color: white;
        border-radius: 12px;
        padding: 2px 12px;
        font-size: 12px;
        font-weight: 500;
        display: inline-block;
    }
    .wash-delete-badge {
        background: linear-gradient(135deg, #EF5350, #E57373);
        color: white;
        border-radius: 12px;
        padding: 2px 12px;
        font-size: 12px;
        font-weight: 500;
        display: inline-block;
    }
    .wash-deleted-badge {
        background: #9E9E9E;
        color: white;
        border-radius: 12px;
        padding: 2px 12px;
        font-size: 12px;
        font-weight: 500;
        display: inline-block;
        text-decoration: line-through;
    }
    .wash-score-high { color: #4CAF50; font-weight: 600; }
    .wash-score-mid { color: #FF9800; font-weight: 600; }
    .wash-score-low { color: #F44336; font-weight: 600; }
    .wash-guide-step { border-left: 3px solid #4CAF50; padding-left: 16px; margin: 12px 0; }
    .wash-guide-step-2 { border-left: 3px solid #FF9800; padding-left: 16px; margin: 12px 0; }
    .wash-guide-step-3 { border-left: 3px solid #2196F3; padding-left: 16px; margin: 12px 0; }
    .wash-guide-step-4 { border-left: 3px solid #9C27B0; padding-left: 16px; margin: 12px 0; }
    .wash-guide-step-5 { border-left: 3px solid #F44336; padding-left: 16px; margin: 12px 0; }
'''

EMPTY_STATE_STYLES = '''
    .wash-empty-state {
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: center;
        padding: 60px 20px;
        text-align: center;
    }
    .wash-empty-icon {
        font-size: 64px;
        opacity: 0.3;
        margin-bottom: 16px;
    }
    .wash-empty-text {
        font-size: 16px;
        color: rgba(0,0,0,0.45);
        line-height: 1.6;
    }
''' + WASH_STYLES


# ============================================================
# 共享 UI 组件
# ============================================================

def _build_guide_step(cls: str, title: str, text: str) -> dict:
    """构建单个使用说明步骤。"""
    return {
        'component': 'div',
        'props': {'class': f'{cls} mb-3'},
        'content': [
            {'component': 'div', 'props': {'class': 'text-body-2 font-weight-medium'}, 'text': title},
            {'component': 'div', 'props': {'class': 'text-caption text-grey mt-1'}, 'text': text}
        ]
    }


def build_usage_guide_section(title: str = "📖 使用说明") -> list:
    """
    构建使用说明折叠面板。
    修复 Bug B4: 提取为共享方法，避免空状态和结果页重复。
    """
    return [
        {
            'component': 'VExpansionPanels',
            'props': {'variant': 'accordion', 'class': 'mt-2'},
            'content': [
                {
                    'component': 'VExpansionPanel',
                    'content': [
                        {
                            'component': 'VExpansionPanelTitle',
                            'content': [
                                {
                                    'component': 'div',
                                    'props': {'class': 'd-flex align-center'},
                                    'content': [
                                        {'component': 'span', 'props': {'class': 'font-weight-medium'}, 'text': title},
                                        {'component': 'VSpacer'},
                                        {'component': 'VChip', 'props': {'size': 'x-small', 'color': 'primary', 'variant': 'flat'}, 'text': '点击展开'}
                                    ]
                                }
                            ]
                        },
                        {
                            'component': 'VExpansionPanelText',
                            'content': [
                                _build_guide_step(
                                    'wash-guide-step', '1. 扫描目标',
                                    '扫描 .strm 和视频文件，从文件名解析分辨率、片源、音频、HDR 等质量信息'
                                ),
                                _build_guide_step(
                                    'wash-guide-step-2', '2. 精准去重',
                                    '按标题+年份+TMDB+季+集精确分组，同集多版本才对比去重，单版本每集自动保留。集数按升序排列方便翻阅。'
                                ),
                                _build_guide_step(
                                    'wash-guide-step-3', '3. 评分规则详解',
                                    '插件从文件名解析5个质量维度。默认规则如下（可在配置页「自定义评分规则」中完全自定义）：'
                                ),
                                {
                                    'component': 'div', 'props': {'class': 'text-caption mt-1'},
                                    'text': '📺 分辨率: 8K=50 | 4K=40 | 1080p=30 | 720p=20 | 540p=15 | 480p=10 | 360p=5'
                                },
                                {
                                    'component': 'div', 'props': {'class': 'text-caption mt-1'},
                                    'text': '🎞️ 片源: Remux=35 | BluRay=30 | WEB-DL=25 | WEBRip=20 | HDTV=15 | HDRip=10 | DVDRip=5 | CAM=0'
                                },
                                {
                                    'component': 'div', 'props': {'class': 'text-caption mt-1'},
                                    'text': '🔊 音频: Atmos/DTS-HD=15 | DTS=12 | TrueHD=11 | FLAC=10 | AC3/DD+=8 | AAC=5 | MP3=2'
                                },
                                {
                                    'component': 'div', 'props': {'class': 'text-caption mt-1'},
                                    'text': '🌈 HDR: DV/HDR10+/HDR10=15(同分) | HLG=5 | 未识别=SDR=0'
                                },
                                {
                                    'component': 'div', 'props': {'class': 'text-caption mt-1'},
                                    'text': '⭐ 加分: CC版=3 | Complete/3D/IMAX/导演剪辑/加长版 各=2 (可累加)'
                                },
                                {
                                    'component': 'div', 'props': {'class': 'text-caption text-grey mt-2'},
                                    'text': '各维度权重可调，调整后各档位按比例缩放。也可在配置页用JSON完全自定义模式和分值。'
                                },
                                _build_guide_step(
                                    'wash-guide-step-4', '4. 操作方式',
                                    '手动: 配置"立即运行一次" | 定时: Cron 表达式 | 命令: /media_wash | API: GET /scan'
                                ),
                                _build_guide_step(
                                    'wash-guide-step-5', '5. 清理操作',
                                    '永久删除。配置页勾选「确认清理」→ 保存即执行。自动清理需在配置中开启。'
                                ),
                                _build_guide_step(
                                    'wash-guide-step-4', '6. 高级自定义评分',
                                    '可在插件配置 → 「自定义评分规则(高级)」中，使用 JSON 格式完全自定义各维度的模式匹配和分值。'
                                ),
                                {
                                    'component': 'div', 'props': {'class': 'text-caption text-grey mt-1'},
                                    'text': '例如：让 HDR10 和 DV 不同分、增加新的音频编码档位、调整加分项分值等。留空即使用默认规则。'
                                }
                            ]
                        }
                    ]
                }
            ]
        }
    ]


def build_json_tutorial_section() -> list:
    """
    构建 JSON 评分规则自定义教程折叠面板。
    修复 Bug B1: 提取为共享方法，空状态和结果页均可引用。
    """
    return [
        {
            'component': 'VExpansionPanels',
            'props': {'variant': 'accordion'},
            'content': [
                {
                    'component': 'VExpansionPanel',
                    'content': [
                        {
                            'component': 'VExpansionPanelTitle',
                            'content': [
                                {
                                    'component': 'div',
                                    'props': {'class': 'd-flex align-center'},
                                    'content': [
                                        {'component': 'span', 'props': {'class': 'font-weight-bold'}, 'text': '📖 JSON评分规则自定义教程'},
                                        {'component': 'VSpacer'},
                                        {'component': 'VChip', 'props': {'size': 'x-small', 'color': 'primary', 'variant': 'flat'}, 'text': '点击展开'}
                                    ]
                                }
                            ]
                        },
                        {
                            'component': 'VExpansionPanelText',
                            'content': [
                                {'component': 'div', 'props': {'class': 'mb-3'},
                                 'content': [
                                     {'component': 'div', 'props': {'class': 'text-h6 font-weight-bold mb-2'}, 'text': '1. 什么是 custom_rules？'},
                                     {'component': 'div', 'props': {'class': 'text-body-2 text-grey'},
                                      'text': 'custom_rules 是一段 JSON 格式的配置，让你完全自定义评分规则中每个维度的匹配模式和分值。留空 = 使用默认规则。'}
                                 ]},
                                {'component': 'div', 'props': {'class': 'mb-3'},
                                 'content': [
                                     {'component': 'div', 'props': {'class': 'text-h6 font-weight-bold mb-2'}, 'text': '2. JSON 结构'},
                                     {'component': 'div', 'props': {'class': 'text-body-2 text-grey'}, 'text': '5个可选维度键（不填则使用默认值）：resolution, source, audio, hdr, bonus'},
                                     {'component': 'div', 'props': {'class': 'text-body-2 mt-1 pa-2', 'style': 'background:rgba(0,0,0,0.03);border-radius:8px;font-family:monospace;'},
                                      'text': '{\n  "resolution": [...],  "source": [...],  "audio": [...],\n  "hdr": [...],  "bonus": [...]\n}'}
                                 ]},
                                {'component': 'div', 'props': {'class': 'mb-3'},
                                 'content': [
                                     {'component': 'div', 'props': {'class': 'text-h6 font-weight-bold mb-2'}, 'text': '3. 每个条目的3个字段'},
                                     {'component': 'div', 'props': {'class': 'text-body-2 mt-1 pa-2', 'style': 'background:rgba(0,0,0,0.03);border-radius:8px;font-family:monospace;'},
                                      'text': '[\n  {\n    "pattern": "2160p|4K|UHD",  // 正则（必填，不区分大小写）\n    "score": 40,                 // 分值（必填）\n    "label": "4K"                // 显示标签（可选）\n  }\n]'},
                                     {'component': 'div', 'props': {'class': 'text-body-2 text-grey mt-1'}, 'text': '⚠️ 反斜杠需双写：HDR10+ 应写为 HDR10\\\\+'}
                                 ]},
                                {'component': 'div', 'props': {'class': 'mb-3'},
                                 'content': [
                                     {'component': 'div', 'props': {'class': 'text-h6 font-weight-bold mb-2'}, 'text': '4. 示例：HDR10/DV不同分'},
                                     {'component': 'div', 'props': {'class': 'text-body-2 mt-1 pa-2', 'style': 'background:rgba(0,0,0,0.03);border-radius:8px;font-family:monospace;'},
                                      'text': '{\n  "hdr": [\n    {"pattern": "DV|Dolby.?Vision|DoVi", "score": 20, "label": "Dolby Vision"},\n    {"pattern": "HDR10\\\\+|HDR10P", "score": 12, "label": "HDR10+"},\n    {"pattern": "HDR10|HDR", "score": 10, "label": "HDR10"},\n    {"pattern": "HLG", "score": 5, "label": "HLG"}\n  ]\n}'}
                                 ]},
                                {'component': 'div', 'props': {'class': 'mb-3'},
                                 'content': [
                                     {'component': 'div', 'props': {'class': 'text-h6 font-weight-bold mb-2'}, 'text': '5. 示例：音频增加DTS:X'},
                                     {'component': 'div', 'props': {'class': 'text-body-2 mt-1 pa-2', 'style': 'background:rgba(0,0,0,0.03);border-radius:8px;font-family:monospace;'},
                                      'text': '{\n  "audio": [\n    {"pattern": "DTS.?X|DTSX", "score": 15, "label": "DTS:X"},\n    {"pattern": "DTS|DTS.?ES", "score": 12, "label": "DTS"},\n    {"pattern": "FLAC|LPCM|PCM", "score": 10, "label": "无损"},\n    {"pattern": "AC3|DD|Dolby.?Digital", "score": 8, "label": "AC3"}\n  ]\n}'}
                                 ]},
                                {'component': 'div', 'props': {'class': 'mb-2'},
                                 'content': [
                                     {'component': 'div', 'props': {'class': 'text-h6 font-weight-bold mb-2'}, 'text': '6. 使用步骤'},
                                     {'component': 'div', 'props': {'class': 'text-body-2 text-grey'}, 'text': '① 打开插件配置 → 「自定义评分规则(JSON)」文本框\n② 粘贴 JSON（留空 = 默认）\n③ 保存配置 → ④ 手动触发扫描 → ⑤ 查看新评分结果'}
                                 ]}
                            ]
                        }
                    ]
                }
            ]
        }
    ]


def build_score_chip(score: int) -> dict:
    """根据分数值构建颜色芯片。"""
    if score >= 70:
        cls = 'wash-score-high'
    elif score >= 40:
        cls = 'wash-score-mid'
    else:
        cls = 'wash-score-low'
    return {
        'component': 'span',
        'props': {'class': cls},
        'text': str(score)
    }


def build_stat_card(icon: str, title: str, value: str, color: str) -> dict:
    """构建单个统计卡片。"""
    return {
        'component': 'VCol',
        'props': {'cols': 6, 'md': 3, 'sm': 6, 'xs': 6},
        'content': [
            {
                'component': 'VCard',
                'props': {
                    'class': 'wash-stats-card pa-3',
                    'variant': 'tonal',
                    'color': color
                },
                'content': [
                    {'component': 'div', 'props': {'class': 'text-h5 font-weight-bold'}, 'text': value},
                    {'component': 'div', 'props': {'class': 'text-caption text-grey mt-1'}, 'text': f'{icon} {title}'}
                ]
            }
        ]
    }


# ============================================================
# 配置表单构建
# ============================================================

# ── 表单组件辅助函数 ──

def _vswitch_col(size: str, model: str, label: str,
                 color: str = None, hint: str = None) -> dict:
    """构建一个VSwitch组件（包在VCol内）"""
    props = {'model': model, 'label': label}
    if color:
        props['color'] = color
    if hint:
        props['hint'] = hint
    return {
        'component': 'VCol',
        'props': {'cols': 12, 'md': int(size.replace('md', ''))},
        'content': [{'component': 'VSwitch', 'props': props}]
    }


def _textfield_col(size: str, model: str, label: str,
                   hint: str = None, placeholder: str = None,
                   type: str = 'text', min_val: int = None) -> dict:
    """构建一个VTextField组件（包在VCol内）"""
    props = {'model': model, 'label': label, 'type': type}
    if hint:
        props['hint'] = hint
    if placeholder:
        props['placeholder'] = placeholder
    if min_val is not None:
        props['min'] = min_val
    return {
        'component': 'VCol',
        'props': {'cols': 12, 'md': int(size.replace('md', ''))},
        'content': [{'component': 'VTextField', 'props': props}]
    }


def _weight_input(size: str, model: str, label: str,
                  default: str, max_val: int, hint: str = None) -> dict:
    """构建一个权重输入组件"""
    props = {
        'model': model, 'label': label, 'type': 'number',
        'suffix': '分', 'min': 0, 'max': max_val,
        'hint': f'默认{default}' if not hint else hint,
    }
    return {
        'component': 'VCol',
        'props': {'cols': 12, 'md': int(size.replace('md', '')), 'sm': 4, 'xs': 6},
        'content': [{'component': 'VTextField', 'props': props}]
    }


def _tier_input_row(tiers: list) -> dict:
    """构建一行紧凑型档位分值输入"""
    cols = []
    for model, label, max_val in tiers:
        cols.append({
            'component': 'VCol',
            'props': {'cols': 4, 'md': 2},
            'content': [{
                'component': 'VTextField',
                'props': {
                    'model': model, 'label': label, 'type': 'number',
                    'min': 0, 'max': max_val,
                    'hideDetails': True, 'density': 'compact'
                }
            }]
        })
    return {'component': 'VRow', 'props': {'dense': True}, 'content': cols}


def _tier_section_header(text: str) -> dict:
    """构建档位区域标题行"""
    return {
        'component': 'VRow', 'props': {'class': 'mt-2'},
        'content': [{
            'component': 'VCol', 'props': {'cols': 12},
            'content': [{
                'component': 'div',
                'props': {'class': 'font-weight-medium text-body-2 mb-1'},
                'text': text
            }]
        }]
    }


def _divider_row() -> dict:
    """构建分隔线行"""
    return {
        'component': 'VRow',
        'content': [{
            'component': 'VCol', 'props': {'cols': 12},
            'content': [{'component': 'VDivider', 'props': {'class': 'my-4'}}]
        }]
    }


def _section_title_row(title: str, desc: str) -> dict:
    """构建区域标题+描述行"""
    return {
        'component': 'VRow',
        'content': [{
            'component': 'VCol', 'props': {'cols': 12},
            'content': [
                {'component': 'div', 'props': {'class': 'text-h6 font-weight-bold mb-2'}, 'text': title},
                {'component': 'div', 'props': {'class': 'text-caption text-grey mb-4'}, 'text': desc}
            ]
        }]
    }


def _info_alert_row(text: str) -> dict:
    """构建信息提示行"""
    return {
        'component': 'VRow',
        'content': [{
            'component': 'VCol', 'props': {'cols': 12},
            'content': [{
                'component': 'VAlert',
                'props': {'type': 'info', 'variant': 'tonal', 'text': text}
            }]
        }]
    }


def _warning_alert_row(text: str) -> dict:
    """构建警告提示行"""
    return {
        'component': 'VRow',
        'content': [{
            'component': 'VCol', 'props': {'cols': 12},
            'content': [{
                'component': 'VAlert',
                'props': {'type': 'warning', 'variant': 'tonal', 'text': text}
            }]
        }]
    }


def _build_basic_settings() -> list:
    """构建基本设置区域：开关、目录、Cron、保留数等"""
    return [
        # 第1行: 开关
        {
            'component': 'VRow',
            'content': [
                _vswitch_col('md3', 'enabled', '启用插件'),
                _vswitch_col('md3', 'onlyonce', '立即运行一次'),
                _vswitch_col('md3', 'notify', '发送通知'),
            ]
        },
        # 第2行: 确认清理 + 自动清理
        {
            'component': 'VRow',
            'content': [
                _vswitch_col('md3', 'trigger_cleanup', '确认清理', 'error'),
                _vswitch_col('md3', 'auto_cleanup', '自动清理', 'warning'),
            ]
        },
        # 第2行: 输入框
        {
            'component': 'VRow',
            'content': [
                {
                    'component': 'VCol',
                    'props': {'cols': 12, 'md': 6},
                    'content': [
                        {
                            'component': 'VTextarea',
                            'props': {
                                'model': 'media_dirs',
                                'label': '媒体目录',
                                'rows': 4,
                                'placeholder': '/media/movies\n/media/tv\n/media/anime',
                                'hint': '每行一个目录路径，留空则使用系统媒体库目录',
                                'persistent-hint': True,
                                'clearable': True,
                            }
                        }
                    ]
                },
                _textfield_col('md3', 'cron', '执行周期(Cron)',
                               '5位cron表达式', '留空则仅手动执行'),
                _textfield_col('md3', 'min_size', '最小文件(MB)',
                               '小于此大小的视频文件将被跳过（.strm 不受限制）',
                               type='number'),
            ]
        },
        # 第3行: 保留版本数 + 保留策略 + 最低分数
        {
            'component': 'VRow',
            'content': [
                _textfield_col('md3', 'keep_count', '保留版本数',
                               '每组保留的最佳版本数量', type='number'),
                {
                    'component': 'VCol',
                    'props': {'cols': 12, 'md': 3},
                    'content': [
                        {
                            'component': 'VSelect',
                            'props': {
                                'model': 'keep_mode',
                                'label': '保留策略',
                                'items': [
                                    {'title': '按总分保留前N个', 'value': 'top_n'},
                                    {'title': '每种HDR各保留1个最高分', 'value': 'per_hdr'},
                                    {'title': '每种HDR+分辨率各保留1个最高分', 'value': 'per_hdr_res'},
                                ],
                                'hint': 'top_n=兼容旧版行为; per_hdr=按HDR分组; per_hdr_res=按HDR+分辨率分组'
                            }
                        }
                    ]
                },
                _textfield_col('md3', 'min_score', '最低保留分数',
                               '低于此分的版本强制删除，不占用保留名额。0=关闭',
                               type='number', min_val=0),
            ]
        },
    ]


def _build_scoring_settings() -> list:
    """构建评分权重设置区域"""
    return [
        _divider_row(),
        _section_title_row('🎯 自定义评分机制',
                           '调整每个质量维度的权重分值。数值越大，该维度在总分中的占比越高。'
                           '默认值=原始评分行为，可依据自己的偏好调整。'),
        # 评分机制说明
        _info_alert_row(
            '📖 评分机制说明：\n'
            '1. 每个维度（分辨率/片源/音频/HDR/视频编码）独立评分\n'
            '2. 各维度得分 × 权重 = 加权得分\n'
            '3. 综合得分 = 各维度加权得分之和 + 加分项\n'
            '4. 同集多版本按综合得分排序，保留前 N 个最佳版本\n'
            '5. 各档位分值可在下方「各档位分值微调」区域调整'
        ),
        # 预设方案选择
        {
            'component': 'VRow',
            'content': [
                {
                    'component': 'VCol',
                    'props': {'cols': 12, 'md': 4},
                    'content': [
                        {
                            'component': 'VSelect',
                            'props': {
                                'model': 'score_profile',
                                'label': '预设评分方案',
                                'items': [
                                    {'title': '自定义', 'value': 'custom'},
                                    {'title': '电影(偏重片源)', 'value': 'movie_source'},
                                    {'title': '电影(偏重画质)', 'value': 'movie_resolution'},
                                    {'title': '电视剧(均衡)', 'value': 'tv_balanced'},
                                    {'title': '动漫(偏重画质)', 'value': 'anime_resolution'}
                                ],
                                'hint': '选择预设方案后自动填入下方权重值（需手动保存）'
                            }
                        }
                    ]
                }
            ]
        },
        # 6个权重输入
        {
            'component': 'VRow',
            'content': [
                _weight_input('md2', 'res_weight', '📺 分辨率', '40', 100),
                _weight_input('md2', 'src_weight', '🎞️ 片源类型', '35', 100),
                _weight_input('md2', 'aud_weight', '🔊 音频编码', '15', 50),
                _weight_input('md2', 'hdr_weight', '🌈 HDR类型', '15', 50),
                _weight_input('md2', 'vid_weight', '🎞️ 视频编码', '12', 50),
                _weight_input('md2', 'bonus_weight', '⭐ 加分特征', '6', 30, hint='设为0关闭'),
            ]
        },
    ]


def _build_tier_adjustments() -> list:
    """构建各档位分值微调区域"""
    return [
        _divider_row(),
        _section_title_row('🎯 各档位分值微调',
                           '直接修改各档次得分，无需编写JSON。修改保存后立即生效，下次扫描使用新分值。'),
        # 微调说明
        _info_alert_row(
            '📖 各档位分值微调说明：\n'
            '• 修改某档位的分值后保存，下次扫描生效\n'
            '• 分值仅为同维度内排序依据，不影响其他维度\n'
            '• 建议保持各维度内最高档 ≈ 权重值，最低档 = 0\n'
            '• 例如：分辨率权重=40 时，建议 8K≈50, 4K=40, 1080p=30 ...'
        ),
        # 分辨率
        _tier_section_header('📺 分辨率各档分值'),
        _tier_input_row([
            ('tier_res_8k', '8K', 100), ('tier_res_4k', '4K', 100),
            ('tier_res_fhd', '1080p', 100), ('tier_res_hd', '720p', 100),
            ('tier_res_540p', '540p', 100), ('tier_res_480p', '480p', 100),
            ('tier_res_360p', '360p', 100),
        ]),
        # 片源
        _tier_section_header('🎞️ 片源各档分值'),
        _tier_input_row([
            ('tier_src_remux', 'Remux', 100), ('tier_src_bluray', 'BluRay', 100),
            ('tier_src_webdl', 'WEB-DL', 100), ('tier_src_webrip', 'WEBRip', 100),
            ('tier_src_hdtv', 'HDTV', 100), ('tier_src_hdrip', 'HDRip', 100),
        ]),
        _tier_input_row([
            ('tier_src_dvdrip', 'DVDRip', 100), ('tier_src_cam', 'CAM', 100),
        ]),
        # 音频
        _tier_section_header('🔊 音频各档分值'),
        _tier_input_row([
            ('tier_aud_atmos', 'Atmos/HDMA', 50), ('tier_aud_dts', 'DTS', 50),
            ('tier_aud_truehd', 'TrueHD', 50), ('tier_aud_flac', 'FLAC', 50),
            ('tier_aud_ac3', 'AC3/DD+', 50), ('tier_aud_aac', 'AAC', 50),
        ]),
        _tier_input_row([
            ('tier_aud_mp3', 'MP3', 50),
        ]),
        # HDR
        _tier_section_header('🌈 HDR各档分值'),
        _tier_input_row([
            ('tier_hdr_dv', 'Dolby Vision', 50), ('tier_hdr_10p', 'HDR10+', 50),
            ('tier_hdr_10', 'HDR10', 50), ('tier_hdr_hlg', 'HLG', 50),
            ('tier_hdr_sdr', 'SDR', 50),
        ]),
        # 视频编码
        _tier_section_header('🎞️ 视频编码各档分值'),
        _tier_input_row([
            ('tier_vid_av1', 'AV1', 50), ('tier_vid_hevc', 'HEVC/H265', 50),
            ('tier_vid_h264', 'H264/AVC', 50), ('tier_vid_other', '其他', 50),
        ]),
        # 加分
        _tier_section_header('⭐ 加分各档分值（可累加）'),
        _tier_input_row([
            ('tier_bonus_cc', 'CC版', 20), ('tier_bonus_complete', 'Complete', 20),
            ('tier_bonus_3d', '3D', 20), ('tier_bonus_imax', 'IMAX', 20),
            ('tier_bonus_director', '导演版', 20), ('tier_bonus_extended', '加长版', 20),
        ]),
    ]


def _build_advanced_section() -> list:
    """构建高级JSON设置和信息提示"""
    return [
        # 高级JSON（折叠）
        {
            'component': 'VRow',
            'content': [
                {
                    'component': 'VCol',
                    'props': {'cols': 12},
                    'content': [
                        {
                            'component': 'VExpansionPanels',
                            'props': {'variant': 'accordion', 'class': 'mt-3'},
                            'content': [
                                {
                                    'component': 'VExpansionPanel',
                                    'content': [
                                        {
                                            'component': 'VExpansionPanelTitle',
                                            'content': [
                                                {'component': 'span', 'props': {'class': 'font-weight-medium text-caption'}, 'text': '⚙️ 高级自定义(JSON) — 非技术人员请忽略'}
                                            ]
                                        },
                                        {
                                            'component': 'VExpansionPanelText',
                                            'content': [
                                                {
                                                    'component': 'VTextarea',
                                                    'props': {
                                                        'model': 'custom_rules',
                                                        'label': '评分规则 JSON (覆盖上方所有设置)',
                                                        'rows': 6,
                                                        'no-resize': True,
                                                        'hint': '填写后将完全替代上面的分档设置。留空则使用上方UI设置。',
                                                        'placeholder': '{\n  "resolution": [\n    {"pattern": "2160p|4K|UHD", "score": 40, "label": "4K"}\n  ],\n  ...\n}'
                                                    }
                                                }
                                            ]
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]
        },
        # 提示信息
        _info_alert_row('💡 各维度值决定此类别识别到的最高档位能得多少分。'
                        '例如「分辨率权重=40」时，4K=40分、1080p=30分、720p=20分。'
                        '如果改为60，则4K=60分、1080p=45分、720p=30分。'),
        _info_alert_row('💡 使用说明：\n'
                        '1. 启用插件后，点击"立即运行一次"手动扫描，仅展示对比结果，不会删除文件\n'
                        '2. 扫描完成后进入插件详情页查看结果，确认无误后点击「确认删除」按钮执行清理\n'
                        '3. 定时任务（Cron）会自动扫描并直接删除重复的低质量文件\n'
                        '4. 单版本文件始终保留，只处理存在重复的影视\n'
                        '5. 在「自定义评分机制」区域可调整各质量维度的权重\n'
                        '6. "保留版本数"控制每组保留几个最佳版本，其余标记为删除'),
        _warning_alert_row('⚠️ 清理操作会永久删除低质量文件！勾选「确认清理」开关后保存配置，'
                           '插件将自动执行清理。推荐先在详情页查看对比结果，确认无误后再操作。'),
    ]


def build_form() -> Tuple[List[dict], Dict[str, Any]]:
    """
    构建插件配置表单。
    """
    return [
        {
            'component': 'VForm',
            'content': (
                _build_basic_settings()
                + _build_scoring_settings()
                + _build_tier_adjustments()
                + _build_advanced_section()
            )
        }
    ], {
        "enabled": False, "onlyonce": False, "notify": True, "auto_cleanup": False,
        "media_dirs": "", "cron": "", "min_size": 100, "keep_count": 1,
        "min_score": 0,
        "score_profile": "custom",
        "res_weight": 40, "src_weight": 35, "aud_weight": 15,
        "hdr_weight": 15, "vid_weight": 12, "bonus_weight": 6,
        "trigger_cleanup": False, "custom_rules": "",
    }


# ============================================================
# 详情页构建
# ============================================================

# 详情页构建
def _build_quality_detail_row(quality_details: dict) -> list:
    """构建单条质量详情行。"""
    d = quality_details
    rows = []
    # 分辨率
    if d.get("resolution", {}).get("label", "未知") != "未知":
        label = d["resolution"]["label"]
        score = d.get("weighted_details", {}).get("resolution", d["resolution"]["value"])
        rows.append(f"📺 {label}({score}分)")
    # 片源
    if d.get("source", {}).get("label", "未知") != "未知":
        label = d["source"]["label"]
        score = d.get("weighted_details", {}).get("source", d["source"]["value"])
        rows.append(f"🎞️ {label}({score}分)")
    # 音频
    if d.get("audio", {}).get("label", "未知") != "未知":
        label = d["audio"]["label"]
        score = d.get("weighted_details", {}).get("audio", d["audio"]["value"])
        rows.append(f"🔊 {label}({score}分)")
    # HDR
    if d.get("hdr", {}).get("label", "SDR") != "SDR":
        label = d["hdr"]["label"]
        score = d.get("weighted_details", {}).get("hdr", d["hdr"]["value"])
        rows.append(f"🌈 {label}({score}分)")
    return rows


def build_page(results: Optional[Dict], action_msg: str, progress: Optional[Dict] = None) -> list:
    """
    构建插件详情页。

    P1 增强: 支持扫描进度条展示。

    Args:
        results: 扫描结果字典（来自 get_data("wash_results")）
        action_msg: 操作反馈消息
        progress: 扫描进度数据（来自 get_data("_scan_progress")）

    Returns:
        Vuetify JSON 页面结构
    """
    # 操作反馈消息横幅
    action_banner = []
    if action_msg:
        action_banner.append({
            'component': 'VRow',
            'props': {'class': 'mb-3'},
            'content': [
                {
                    'component': 'VCol',
                    'props': {'cols': 12},
                    'content': [
                        {
                            'component': 'VAlert',
                            'props': {
                                'type': 'success', 'variant': 'tonal',
                                'closable': True, 'text': action_msg
                            }
                        }
                    ]
                }
            ]
        })

    # P1: 扫描进度条
    progress_banner = []
    if progress and progress.get("scanning"):
        pct = progress.get("pct", 0)
        stage = progress.get("stage", "扫描中...")
        current = progress.get("current", 0)
        total = progress.get("total", 0)
        progress_banner = [
            {
                'component': 'VRow',
                'props': {'class': 'mb-3'},
                'content': [
                    {
                        'component': 'VCol',
                        'props': {'cols': 12},
                        'content': [
                            {
                                'component': 'VCard',
                                'props': {'variant': 'tonal', 'color': 'primary', 'class': 'pa-3'},
                                'content': [
                                    {
                                        'component': 'div',
                                        'props': {'class': 'd-flex align-center mb-1'},
                                        'content': [
                                            {'component': 'VIcon', 'props': {'class': 'mr-2'}, 'text': 'mdi-loading mdi-spin'},
                                            {'component': 'span', 'props': {'class': 'text-subtitle-2 font-weight-medium'}, 'text': f'⏳ {stage}'},
                                            {'component': 'VSpacer'},
                                            {'component': 'span', 'props': {'class': 'text-caption'}, 'text': f'{current}/{total} ({pct}%)'}
                                        ]
                                    },
                                    {
                                        'component': 'VProgressLinear',
                                        'props': {
                                            'model-value': pct,
                                            'color': 'primary',
                                            'height': 6,
                                            'striped': True,
                                            'rounded': True
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ]

    # ====== 空状态 ======
    if not results or not results.get("items"):
        return _build_empty_page(action_banner)

    # ====== 结果状态 ======
    return _build_results_page(results, action_banner, progress_banner)


def _build_empty_page(action_banner: list) -> list:
    """构建空状态页面。"""
    return [
        {
            'component': 'style',
            'props': {'text': EMPTY_STATE_STYLES}
        },
        *action_banner,
        # 空状态
        {
            'component': 'VRow',
            'content': [
                {
                    'component': 'VCol',
                    'props': {'cols': 12},
                    'content': [
                        {
                            'component': 'VCard',
                            'props': {'variant': 'tonal', 'class': 'pa-6'},
                            'content': [
                                {
                                    'component': 'div',
                                    'props': {'class': 'wash-empty-state'},
                                    'content': [
                                        {'component': 'div', 'props': {'class': 'wash-empty-icon'}, 'text': '🎬'},
                                        {'component': 'div', 'props': {'class': 'wash-empty-text'}, 'text': '暂无扫描结果'},
                                        {'component': 'div', 'props': {'class': 'text-caption text-grey mt-2'}, 'text': '请在插件配置中启用并点击「立即运行一次」开始扫描'},
                                        {'component': 'div', 'props': {'class': 'text-caption text-grey mt-1'}, 'text': '或设置 Cron 定时任务自动扫描'}
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]
        },
        # 使用说明（修复 Bug B4: 使用共享方法）
        *build_usage_guide_section(),
        # JSON教程（修复 Bug B1: 空状态同样展示）
        *build_json_tutorial_section(),
    ]


def _build_results_page(results: Dict, action_banner: list, progress_banner: list = None) -> list:
    """构建有扫描结果时的详情页。"""
    progress_banner = progress_banner or []  # 防御 None
    items = results.get("items", {})
    shows = results.get("shows", {})
    stats = _compute_stats(results)
    pending_delete_count = _compute_pending_delete(items)
    has_deleted = _has_deleted_versions(items)

    # 统计卡片
    stat_cards = [
        build_stat_card("🎬", "总文件", str(stats["total_items"]), "primary"),
        build_stat_card("📁", "重复组", str(stats["duplicates"]), "warning"),
        build_stat_card("🗑️", "待删", str(stats.get("pending_delete_count", 0)), "error"),
        build_stat_card("💾", "可节省", f"{stats['savings_gb']}GB", "success"),
    ]

    # 如果有多季，按"all"汇总
    show_seasons = {
        show_key: sorted(s_data["seasons"].keys())
        for show_key, s_data in shows.items()
    }

    # 构建媒体列表面板（每部剧为一个折叠面板）
    media_panels = _build_media_panels(shows, items, stats, filter_duplicates_only=True)

    # v2.7.0: 无重复项的空状态提示
    no_dup_alert = []
    if not media_panels and stats["total_items"] > 0:
        no_dup_alert = [
            {
                'component': 'VAlert',
                'props': {
                    'type': 'success',
                    'variant': 'tonal',
                    'class': 'mb-4',
                },
                'content': [
                    {
                        'component': 'span',
                        'text': f'扫描 {stats["total_items"]:,} 个文件，未发现重复版本，无需清理'
                    }
                ]
            }
        ]

    cleanup_info = ""
    if stats["last_cleanup"]:
        cleanup_info = f'上次清理: {stats["last_cleanup"]}, 共 {stats["last_cleanup_count"]} 个文件'

    # P1: 搜索/筛选栏 + 批量操作
    search_bar = _build_search_filter_bar(stats, pending_delete_count)

    return [
        {
            'component': 'style',
            'props': {'text': WASH_STYLES}
        },
        *action_banner,
        *progress_banner,
        # v2.7.0: 无重复项空状态提示
        *no_dup_alert,
        # 统计卡片
        {
            'component': 'VRow',
            'props': {'class': 'mb-4'},
            'content': stat_cards
        },
        # P1: 搜索/筛选栏
        *search_bar,
        # 扫描结果标题行
        {
            'component': 'VRow',
            'props': {'class': 'mb-2'},
            'content': [
                {
                    'component': 'VCol',
                    'props': {'cols': 12},
                    'content': [
                        {
                            'component': 'div',
                            'props': {'class': 'd-flex align-center flex-wrap'},
                            'content': [
                                {'component': 'span', 'props': {'class': 'text-h6 font-weight-bold'}, 'text': '📋 扫描结果'},
                                {'component': 'VSpacer'},
                                {
                                    'component': 'VChip',
                                    'props': {'size': 'small', 'color': 'grey-lighten-3', 'variant': 'flat', 'class': 'ml-2'},
                                    'text': f'🕐 {stats["last_scan"]}'
                                },
                                {
                                    'component': 'div',
                                    'props': {'class': 'd-flex align-center flex-wrap ml-2'},
                                    'content': [
                                        {
                                            'component': 'VChip',
                                            'props': {'size': 'small', 'color': 'green-lighten-4', 'variant': 'flat', 'class': 'ml-1'},
                                            'text': f'保留{stats["total_items"] - stats["duplicates"]}组'
                                        },
                                        {
                                            'component': 'VChip',
                                            'props': {'size': 'small', 'color': 'orange-lighten-4', 'variant': 'flat', 'class': 'ml-1'},
                                            'text': f'重复{stats["duplicates"]}组'
                                        },
                                    ]
                                },
                            ]
                        }
                    ]
                }
            ]
        },
        # v2.6.0: 无重复项隐藏提示
    ] + (_hidden_no_dup_alert(stats) if stats.get("hidden_no_dup_count", 0) > 0 else []) + [
        # 媒体列表面板
        {
            'component': 'VRow',
            'props': {'class': 'mt-4'},
            'content': [
                {
                    'component': 'VCol',
                    'props': {'cols': 12},
                    'content': [
                        {
                            'component': 'div',
                            'props': {'class': 'd-flex align-center mb-2'},
                            'content': [
                                {
                                    'component': 'span',
                                    'props': {'class': 'text-subtitle-2 font-weight-bold text-grey'},
                                    'text': f'共 {stats["total_groups"]} 组 — 同集多版本才清理。配置页操作: 立即扫描 / 确认清理'
                                }
                            ]
                        },
                        {
                            'component': 'VExpansionPanels',
                            'props': {'variant': 'accordion', 'class': 'mb-2'},
                            'content': [
                                {
                                    'component': 'VExpansionPanel',
                                    'content': [
                                        {
                                            'component': 'VExpansionPanelTitle',
                                            'content': [
                                                {
                                                    'component': 'div',
                                                    'props': {'class': 'd-flex align-center w-100'},
                                                    'content': [
                                                        {'component': 'span', 'props': {'class': 'font-weight-bold text-h6'}, 'text': '📋 扫描结果详情'},
                                                        {'component': 'VSpacer'},
                                                        {
                                                            'component': 'VChip',
                                                            'props': {'size': 'x-small', 'color': 'primary', 'variant': 'flat'},
                                                            'text': f'{stats["total_groups"]}组, 点击展开'
                                                        }
                                                    ]
                                                }
                                            ]
                                        },
                                        {
                                            'component': 'VExpansionPanelText',
                                            'content': [
                                                {
                                                    'component': 'VExpansionPanels',
                                                    'props': {'variant': 'accordion', 'multiple': True, 'class': 'mb-2'},
                                                    'content': media_panels
                                                }
                                            ]
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]
        },
        # 使用说明（修复 Bug B4: 使用共享方法）
        *build_usage_guide_section(),
        # JSON教程
        *build_json_tutorial_section(),
    ]


def _compute_stats(results: Dict) -> dict:
    """v2.7.0: 从扫描结果中计算统计汇总（total_items 为扫描的总文件数）。"""
    return {
        "last_scan": results.get("last_scan", "未知"),
        "total_items": results.get("total_items", 0),
        "total_groups": results.get("total_groups", 0),
        "duplicates": results.get("groups_with_duplicates", 0),
        "savings_gb": results.get("potential_savings_gb", 0),
        "last_cleanup": results.get("last_cleanup", None),
        "last_cleanup_count": results.get("last_cleanup_count", 0),
        "pending_delete_count": results.get("pending_delete_count", 0),
    }


def _compute_pending_delete(items: Dict) -> int:
    """计算待删除文件数。"""
    return sum(
        1 for g in items.values()
        for v in g.get("versions", [])
        if not v.get("is_best") and not v.get("deleted")
    )


def _has_deleted_versions(items: Dict) -> bool:
    """检查是否有已删除的版本。"""
    return any(
        v.get("deleted") for g in items.values()
        for v in g.get("versions", [])
    )


def _build_version_card(show_items: list, items: Dict) -> list:
    """构建版本卡片列表（单集的多版本对比展示）。"""
    version_cards = []
    for v in show_items:
        is_best = v.get("is_best", False)
        deleted = v.get("deleted", False)
        score = v.get("quality_score", 0)

        if is_best:
            badge = {'component': 'span', 'props': {'class': 'wash-best-badge'}, 'text': '✅ 最佳'}
        elif deleted:
            badge = {'component': 'span', 'props': {'class': 'wash-deleted-badge'}, 'text': '🗑️ 已删除'}
        else:
            badge = {'component': 'span', 'props': {'class': 'wash-delete-badge'}, 'text': '⚠️ 待删除'}

        score_color = 'wash-score-high' if score >= 70 else ('wash-score-mid' if score >= 40 else 'wash-score-low')

        quality_detail_rows = _build_quality_detail_row(v.get("quality_details", {}))

        version_cards.append({
            'component': 'VCard',
            'props': {'variant': 'outlined', 'class': 'wash-version-card pa-3'},
            'content': [
                {
                    'component': 'div',
                    'props': {'class': 'd-flex align-center flex-wrap'},
                    'content': [
                        badge,
                        {
                            'component': 'span',
                            'props': {'class': f'ml-2 font-weight-bold {score_color}'},
                            'text': f'综合: {score}分'
                        },
                        {'component': 'VSpacer'},
                        {'component': 'span', 'props': {'class': 'text-caption text-grey'}, 'text': f'#{v.get("rank", "?")}'},
                    ]
                },
                {
                    'component': 'div',
                    'props': {'class': 'text-caption mt-1'},
                    'text': v.get("filename", "")
                },
                # P0: 文件完整路径显示
                {
                    'component': 'div',
                    'props': {'class': 'text-caption wash-text-dim mt-0'},
                    'text': v.get("filepath", "")
                },
                {
                    'component': 'div',
                    'props': {'class': 'text-caption text-grey mt-1'},
                    'text': ' | '.join(quality_detail_rows) if quality_detail_rows else v.get("quality_details", {}).get("details", "")
                },
                {
                    'component': 'div',
                    'props': {'class': 'd-flex flex-wrap mt-1'},
                    'content': [
                        {
                            'component': 'VChip',
                            'props': {'size': 'x-small', 'variant': 'flat', 'color': 'grey-lighten-3', 'class': 'mr-1'},
                            'text': v.get("size_display", "")
                        }
                    ]
                }
            ]
        })
    return version_cards


def _build_episode_panels(show_key: str, s_data: dict, items: Dict) -> list:
    """构建某部剧的集折叠面板列表。"""
    episode_panels = []
    for ep_key in s_data["episode_keys"]:
        ep_data = items.get(ep_key)
        if not ep_data:
            continue

        se_display = ep_data.get("episode_display", "")
        has_duplicates = ep_data.get("has_duplicates", False)
        versions = ep_data.get("versions", [])
        best_score = max((v["quality_score"] for v in versions), default=0)

        # 集标题
        title_parts = [f"{se_display}"]
        best_scores = [v["quality_score"] for v in versions if v.get("is_best")]
        if best_scores:
            title_parts.append(f"⭐{best_scores[0]}分")
        if has_duplicates:
            title_parts.append("🔄 多版本")

        version_cards = _build_version_card(versions, items)

        episode_panels.append({
            'component': 'VExpansionPanel',
            'content': [
                {
                    'component': 'VExpansionPanelTitle',
                    'props': {'class': 'pa-2'},
                    'content': [
                        {
                            'component': 'div',
                            'props': {'class': 'd-flex align-center w-100'},
                            'content': [
                                {'component': 'span', 'props': {'class': 'text-body-2 font-weight-medium'}, 'text': ' | '.join(title_parts)},
                                {'component': 'VSpacer'},
                                {
                                    'component': 'VChip',
                                    'props': {'size': 'x-small', 'color': 'grey-lighten-3', 'variant': 'flat'},
                                    'text': f'{len(versions)}版本'
                                }
                            ]
                        }
                    ]
                },
                {
                    'component': 'VExpansionPanelText',
                    'props': {'class': 'pa-2'},
                    'content': version_cards
                }
            ]
        })

    return episode_panels


def _build_search_filter_bar(stats: dict, pending_delete_count: int) -> list:
    """
    构建搜索/筛选栏。

    ⚠️ 修复: MoviePilot 的 Vuetify JSON 渲染不支持 v-model 双向绑定。
    改为使用静态筛选标签 + 统计概览，保证渲染正确性。
    """
    return [
        {
            'component': 'VRow',
            'props': {'class': 'mb-2'},
            'content': [
                {
                    'component': 'VCol',
                    'props': {'cols': 12},
                    'content': [
                        {
                            'component': 'div',
                            'props': {'class': 'd-flex align-center flex-wrap'},
                            'content': [
                                # 统计概览标签 - 显示各状态数量
                                {
                                    'component': 'VChip',
                                    'props': {
                                        'size': 'small',
                                        'color': 'primary',
                                        'variant': 'tonal',
                                        'class': 'mr-1'
                                    },
                                    'text': f'共 {stats["total_groups"]} 组'
                                },
                                {
                                    'component': 'VChip',
                                    'props': {
                                        'size': 'small',
                                        'color': 'warning',
                                        'variant': 'tonal',
                                        'class': 'mr-1'
                                    },
                                    'text': f'重复 {stats["duplicates"]} 组'
                                },
                                {
                                    'component': 'VChip',
                                    'props': {
                                        'size': 'small',
                                        'color': 'error',
                                        'variant': 'tonal',
                                        'class': 'mr-1'
                                    },
                                    'text': f'待删 {pending_delete_count}'
                                },
                                {'component': 'VSpacer'},
                                {
                                    'component': 'VChip',
                                    'props': {
                                        'size': 'small',
                                        'color': 'grey-lighten-3',
                                        'variant': 'flat'
                                    },
                                    'text': '配置页勾选确认清理并保存'
                                }
                            ]
                        }
                    ]
                }
            ]
        }
    ]


def _hidden_no_dup_alert(stats: dict) -> list:
    """v2.6.0: 构建无重复项已隐藏的提示组件。"""
    hidden = stats.get("hidden_no_dup_count", 0)
    if hidden <= 0:
        return []
    return [
        {
            'component': 'VAlert',
            'props': {
                'type': 'info',
                'variant': 'tonal',
                'density': 'compact',
                'class': 'mb-2',
            },
            'content': [
                {
                    'component': 'span',
                    'text': f'另有 {hidden} 部影视无重复版本（已隐藏），仅展示有重复的项目'
                }
            ]
        }
    ]


def _build_media_panels(shows: Dict, items: Dict, stats: dict, filter_duplicates_only: bool = True) -> list:
    """构建媒体（剧集/电影）折叠面板列表。

    Args:
        shows: show 索引字典
        items: item 索引字典
        stats: 统计信息
        filter_duplicates_only: True 时仅展示有重复的 show
    """
    media_panels = []
    show_keys = sorted(
        shows.keys(),
        key=lambda k: (
            0 if any(
                items.get(ek, {}).get("has_duplicates")
                for ek in shows[k]["episode_keys"]
            ) else 1,
            shows[k]["title"].lower(),
        )
    )

    # v2.6.0: 默认过滤掉无重复的 show
    if filter_duplicates_only:
        original_count = len(show_keys)
        show_keys = [
            k for k in show_keys
            if any(items.get(ek, {}).get("has_duplicates") for ek in shows[k]["episode_keys"])
        ]
        stats["hidden_no_dup_count"] = original_count - len(show_keys)
    else:
        stats["hidden_no_dup_count"] = 0

    for idx, show_key in enumerate(show_keys):

        s_data = shows[show_key]
        show_total = len(s_data["episode_keys"])
        show_dup = sum(1 for ek in s_data["episode_keys"] if items.get(ek, {}).get("has_duplicates"))

        episode_panels = _build_episode_panels(show_key, s_data, items)

        media_panels.append({
            'component': 'VExpansionPanel',
            'content': [
                {
                    'component': 'VExpansionPanelTitle',
                    'content': [
                        {
                            'component': 'div',
                            'props': {'class': 'd-flex align-center w-100'},
                            'content': [
                                {
                                    'component': 'span',
                                    'props': {'class': 'font-weight-bold text-body-1'},
                                    'text': f'{s_data["title"]} ({s_data.get("year", "?")})'
                                },
                                {'component': 'VSpacer'},
                                {
                                    'component': 'VChip',
                                    'props': {'size': 'x-small', 'color': 'green-lighten-4', 'variant': 'flat', 'class': 'mr-1'},
                                    'text': f'{show_total}集'
                                },
                                {
                                    'component': 'VChip',
                                    'props': {'size': 'x-small', 'color': 'orange-lighten-4', 'variant': 'flat'},
                                    'text': f'{show_dup}组重复'
                                }
                            ]
                        }
                    ]
                },
                {
                    'component': 'VExpansionPanelText',
                    'content': [
                        {
                            'component': 'VExpansionPanels',
                            'props': {'variant': 'accordion', 'multiple': True, 'class': 'ml-0'},
                            'content': episode_panels
                        }
                    ]
                }
            ]
        })

    return media_panels
