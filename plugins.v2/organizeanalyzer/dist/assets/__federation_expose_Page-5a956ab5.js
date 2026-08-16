import { importShared } from './__federation_fn_import-054b33c3.js';
import { _ as _export_sfc } from './_plugin-vue_export-helper-c4c0bc37.js';

const Page_vue_vue_type_style_index_0_scoped_3514b6fe_lang = '';

const {resolveComponent:_resolveComponent,createVNode:_createVNode,withCtx:_withCtx,createElementVNode:_createElementVNode,toDisplayString:_toDisplayString,createTextVNode:_createTextVNode,openBlock:_openBlock,createElementBlock:_createElementBlock,createCommentVNode:_createCommentVNode,createBlock:_createBlock} = await importShared('vue');


const _hoisted_1 = { class: "d-flex align-center justify-space-between flex-wrap gap-2 mb-4" };
const _hoisted_2 = { class: "d-flex align-center" };
const _hoisted_3 = { class: "d-flex align-center gap-2" };
const _hoisted_4 = { class: "text-caption text-medium-emphasis mt-1 d-flex align-center flex-wrap" };
const _hoisted_5 = { class: "d-flex align-center gap-2" };
const _hoisted_6 = {
  key: 0,
  class: "py-6 text-center"
};
const _hoisted_7 = { key: 1 };
const _hoisted_8 = { class: "text-h4 font-weight-bold mt-1" };
const _hoisted_9 = { class: "text-caption mt-1 opacity-80" };
const _hoisted_10 = { class: "text-h4 font-weight-bold mt-1" };
const _hoisted_11 = { class: "text-subtitle-2 font-weight-bold mb-2 d-flex align-center" };
const _hoisted_12 = { class: "d-flex flex-wrap gap-2" };
const _hoisted_13 = { class: "d-flex align-center justify-space-between mb-3" };
const _hoisted_14 = { class: "text-subtitle-2 font-weight-bold d-flex align-center" };
const _hoisted_15 = { class: "text-body-2 font-weight-medium mt-1" };
const _hoisted_16 = { class: "text-body-2 font-weight-medium mt-1" };
const _hoisted_17 = ["title"];
const _hoisted_18 = { class: "d-flex flex-wrap gap-2" };

const {ref,computed,onMounted} = await importShared('vue');



const _sfc_main = {
  __name: 'Page',
  props: {
  initialConfig: Object,
  api: Object,
  pluginId: {
    type: String,
    default: 'OrganizeAnalyzer'
  }
},
  emits: ['action', 'switch', 'close'],
  setup(__props, { emit: __emit }) {

const props = __props;

const emit = __emit;

const loading = ref(true);
const analyzing = ref(false);
const stats = ref({});
const fetchedConfig = ref({});
const snackbar = ref({ show: false, text: '', color: 'success' });

// 合并配置，优先使用接口实时返回的配置，以 props.initialConfig 为底
const effectiveConfig = computed(() => {
  const cfg = {
    ...(props.initialConfig || {}),
    ...(fetchedConfig.value || {})
  };
  if (cfg.min_merged_files === undefined) cfg.min_merged_files = 2;
  if (cfg.cron_mode === undefined) cfg.cron_mode = 'incremental';
  if (cfg.cron === undefined) cfg.cron = '0 3 * * *';
  if (cfg.invalid_episode_threshold === undefined) cfg.invalid_episode_threshold = 500;
  return cfg
});

const currentEnabled = computed(() => {
  if (stats.value.enabled !== undefined) return Boolean(stats.value.enabled)
  return Boolean(effectiveConfig.value.enabled)
});

const summary = computed(() => stats.value.summary || {});

// 获取数据统计和配置
const fetchStats = async () => {
  loading.value = true;
  try {
    const pluginKey = props.pluginId || 'OrganizeAnalyzer';
    let res = null;
    if (props.api && props.api.get) {
      res = await props.api.get(`plugin/${pluginKey}/stats`);
    } else {
      // 备用 fetch
      const resp = await fetch(`/api/v1/plugin/${pluginKey}/stats`);
      res = await resp.json();
    }

    const data = res?.data || res || {};
    stats.value = data;
    if (data.config) {
      fetchedConfig.value = data.config;
    }
  } catch (err) {
    console.error('[OrganizeAnalyzer] 获取统计数据失败:', err);
  } finally {
    loading.value = false;
  }
};

// 快速发起增量分析
const runQuickAnalyze = async () => {
  analyzing.value = true;
  try {
    const pluginKey = props.pluginId || 'OrganizeAnalyzer';
    if (props.api && props.api.post) {
      await props.api.post(`plugin/${pluginKey}/analyze?mode=incremental`);
    } else {
      await fetch(`/api/v1/plugin/${pluginKey}/analyze?mode=incremental`, { method: 'POST' });
    }
    snackbar.value = { show: true, text: '增量分析已完成！', color: 'success' };
    await fetchStats();
  } catch (err) {
    console.error('[OrganizeAnalyzer] 触发分析失败:', err);
    snackbar.value = { show: true, text: '触发分析失败，请检查插件状态', color: 'error' };
  } finally {
    analyzing.value = false;
  }
};

// 跳转至设置 Tab
const goToConfig = () => {
  emit('switch', 'config');
  emit('action', { action: 'switch', target: 'config' });
};

onMounted(() => {
  fetchStats();
});

return (_ctx, _cache) => {
  const _component_v_icon = _resolveComponent("v-icon");
  const _component_v_avatar = _resolveComponent("v-avatar");
  const _component_v_chip = _resolveComponent("v-chip");
  const _component_v_btn = _resolveComponent("v-btn");
  const _component_v_progress_circular = _resolveComponent("v-progress-circular");
  const _component_v_card = _resolveComponent("v-card");
  const _component_v_col = _resolveComponent("v-col");
  const _component_v_row = _resolveComponent("v-row");
  const _component_v_divider = _resolveComponent("v-divider");
  const _component_v_alert = _resolveComponent("v-alert");
  const _component_v_card_actions = _resolveComponent("v-card-actions");
  const _component_v_snackbar = _resolveComponent("v-snackbar");

  return (_openBlock(), _createBlock(_component_v_card, {
    flat: "",
    class: "organize-analyzer-modal-page pa-4",
    "min-width": "320"
  }, {
    default: _withCtx(() => [
      _createElementVNode("div", _hoisted_1, [
        _createElementVNode("div", _hoisted_2, [
          _createVNode(_component_v_avatar, {
            color: "primary",
            variant: "tonal",
            size: "44",
            class: "mr-3"
          }, {
            default: _withCtx(() => [
              _createVNode(_component_v_icon, {
                icon: "mdi-file-find-outline",
                size: "28"
              })
            ]),
            _: 1
          }),
          _createElementVNode("div", null, [
            _createElementVNode("div", _hoisted_3, [
              _cache[2] || (_cache[2] = _createElementVNode("span", { class: "text-h6 font-weight-bold" }, "媒体整理异常分析", -1)),
              _createVNode(_component_v_chip, {
                size: "small",
                color: currentEnabled.value ? 'success' : 'grey',
                variant: "flat",
                class: "font-weight-medium"
              }, {
                default: _withCtx(() => [
                  _createTextVNode(_toDisplayString(currentEnabled.value ? '运行中' : '已禁用'), 1)
                ]),
                _: 1
              }, 8, ["color"])
            ]),
            _createElementVNode("div", _hoisted_4, [
              _createVNode(_component_v_icon, {
                icon: "mdi-clock-outline",
                size: "14",
                class: "mr-1"
              }),
              _createTextVNode(" 上次分析: " + _toDisplayString(stats.value.last_run_time || '尚未运行'), 1)
            ])
          ])
        ]),
        _createElementVNode("div", _hoisted_5, [
          _createVNode(_component_v_btn, {
            color: "primary",
            variant: "tonal",
            size: "small",
            loading: analyzing.value,
            "prepend-icon": "mdi-play",
            onClick: runQuickAnalyze
          }, {
            default: _withCtx(() => [...(_cache[3] || (_cache[3] = [
              _createTextVNode(" 立即增量分析 ", -1)
            ]))]),
            _: 1
          }, 8, ["loading"]),
          _createVNode(_component_v_btn, {
            color: "secondary",
            variant: "tonal",
            size: "small",
            "prepend-icon": "mdi-cog-outline",
            onClick: goToConfig
          }, {
            default: _withCtx(() => [...(_cache[4] || (_cache[4] = [
              _createTextVNode(" 前往设置 ", -1)
            ]))]),
            _: 1
          })
        ])
      ]),
      (loading.value)
        ? (_openBlock(), _createElementBlock("div", _hoisted_6, [
            _createVNode(_component_v_progress_circular, {
              indeterminate: "",
              color: "primary",
              size: "36"
            }),
            _cache[5] || (_cache[5] = _createElementVNode("div", { class: "text-caption text-medium-emphasis mt-2" }, "正在获取分析数据与生效配置...", -1))
          ]))
        : (_openBlock(), _createElementBlock("div", _hoisted_7, [
            _createVNode(_component_v_row, {
              dense: "",
              class: "mb-4"
            }, {
              default: _withCtx(() => [
                _createVNode(_component_v_col, {
                  cols: "12",
                  sm: "6"
                }, {
                  default: _withCtx(() => [
                    _createVNode(_component_v_card, {
                      variant: "tonal",
                      color: summary.value.total > 0 ? 'error' : 'success',
                      class: "pa-4 d-flex align-center justify-space-between rounded-lg"
                    }, {
                      default: _withCtx(() => [
                        _createElementVNode("div", null, [
                          _cache[6] || (_cache[6] = _createElementVNode("div", { class: "text-caption font-weight-medium text-uppercase" }, "未处理异常", -1)),
                          _createElementVNode("div", _hoisted_8, _toDisplayString(summary.value.total || 0), 1),
                          _createElementVNode("div", _hoisted_9, _toDisplayString(summary.value.total > 0 ? '存在需要关注的整理冲突或未中文化' : '媒体库整理记录状态良好'), 1)
                        ]),
                        _createVNode(_component_v_icon, {
                          icon: summary.value.total > 0 ? 'mdi-alert-circle-outline' : 'mdi-check-circle-outline',
                          size: "48",
                          class: "opacity-70"
                        }, null, 8, ["icon"])
                      ]),
                      _: 1
                    }, 8, ["color"])
                  ]),
                  _: 1
                }),
                _createVNode(_component_v_col, {
                  cols: "12",
                  sm: "6"
                }, {
                  default: _withCtx(() => [
                    _createVNode(_component_v_card, {
                      variant: "tonal",
                      color: "primary",
                      class: "pa-4 d-flex align-center justify-space-between rounded-lg"
                    }, {
                      default: _withCtx(() => [
                        _createElementVNode("div", null, [
                          _cache[7] || (_cache[7] = _createElementVNode("div", { class: "text-caption font-weight-medium text-uppercase" }, "已忽略标记", -1)),
                          _createElementVNode("div", _hoisted_10, _toDisplayString(summary.value.ignored || 0), 1),
                          _cache[8] || (_cache[8] = _createElementVNode("div", { class: "text-caption mt-1 opacity-80" }, " 已人工标记排除的非异常条目 ", -1))
                        ]),
                        _createVNode(_component_v_icon, {
                          icon: "mdi-eye-off-outline",
                          size: "48",
                          class: "opacity-70"
                        })
                      ]),
                      _: 1
                    })
                  ]),
                  _: 1
                })
              ]),
              _: 1
            }),
            _createVNode(_component_v_card, {
              variant: "outlined",
              class: "pa-3 mb-4 rounded-lg"
            }, {
              default: _withCtx(() => [
                _createElementVNode("div", _hoisted_11, [
                  _createVNode(_component_v_icon, {
                    icon: "mdi-chart-box-outline",
                    size: "18",
                    class: "mr-1 text-primary"
                  }),
                  _cache[9] || (_cache[9] = _createTextVNode(" 最近分析异常细项分布 ", -1))
                ]),
                _createElementVNode("div", _hoisted_12, [
                  _createVNode(_component_v_chip, {
                    size: "small",
                    variant: "tonal",
                    color: "info",
                    class: "font-weight-medium"
                  }, {
                    default: _withCtx(() => [
                      _createVNode(_component_v_icon, {
                        start: "",
                        icon: "mdi-translate",
                        size: "14"
                      }),
                      _createTextVNode(" 英文/数字未中文化: " + _toDisplayString(summary.value.english_title || 0), 1)
                    ]),
                    _: 1
                  }),
                  _createVNode(_component_v_chip, {
                    size: "small",
                    variant: "tonal",
                    color: "warning",
                    class: "font-weight-medium"
                  }, {
                    default: _withCtx(() => [
                      _createVNode(_component_v_icon, {
                        start: "",
                        icon: "mdi-file-multiple-outline",
                        size: "14"
                      }),
                      _createTextVNode(" 多文件合并覆盖: " + _toDisplayString(summary.value.merged_files || 0), 1)
                    ]),
                    _: 1
                  }),
                  _createVNode(_component_v_chip, {
                    size: "small",
                    variant: "tonal",
                    color: "purple",
                    class: "font-weight-medium"
                  }, {
                    default: _withCtx(() => [
                      _createVNode(_component_v_icon, {
                        start: "",
                        icon: "mdi-help-circle-outline",
                        size: "14"
                      }),
                      _createTextVNode(" 未识别/TMDB缺失: " + _toDisplayString(summary.value.unidentified || 0), 1)
                    ]),
                    _: 1
                  }),
                  _createVNode(_component_v_chip, {
                    size: "small",
                    variant: "tonal",
                    color: "error",
                    class: "font-weight-medium"
                  }, {
                    default: _withCtx(() => [
                      _createVNode(_component_v_icon, {
                        start: "",
                        icon: "mdi-close-circle-outline",
                        size: "14"
                      }),
                      _createTextVNode(" 整理运行失败: " + _toDisplayString(summary.value.failed_status || 0), 1)
                    ]),
                    _: 1
                  }),
                  _createVNode(_component_v_chip, {
                    size: "small",
                    variant: "tonal",
                    color: "deep-orange",
                    class: "font-weight-medium"
                  }, {
                    default: _withCtx(() => [
                      _createVNode(_component_v_icon, {
                        start: "",
                        icon: "mdi-numeric",
                        size: "14"
                      }),
                      _createTextVNode(" 重复季集冲突: " + _toDisplayString(summary.value.duplicate_episode || 0), 1)
                    ]),
                    _: 1
                  }),
                  _createVNode(_component_v_chip, {
                    size: "small",
                    variant: "tonal",
                    color: "grey-darken-1",
                    class: "font-weight-medium"
                  }, {
                    default: _withCtx(() => [
                      _createVNode(_component_v_icon, {
                        start: "",
                        icon: "mdi-file-remove-outline",
                        size: "14"
                      }),
                      _createTextVNode(" 目标缺失/0字节: " + _toDisplayString(summary.value.missing_dest || 0), 1)
                    ]),
                    _: 1
                  }),
                  _createVNode(_component_v_chip, {
                    size: "small",
                    variant: "tonal",
                    color: "teal",
                    class: "font-weight-medium"
                  }, {
                    default: _withCtx(() => [
                      _createVNode(_component_v_icon, {
                        start: "",
                        icon: "mdi-chart-line-variant",
                        size: "14"
                      }),
                      _createTextVNode(" 离群集数异常: " + _toDisplayString(summary.value.invalid_episode || 0), 1)
                    ]),
                    _: 1
                  })
                ])
              ]),
              _: 1
            }),
            _createVNode(_component_v_card, {
              variant: "outlined",
              class: "pa-3 mb-4 rounded-lg"
            }, {
              default: _withCtx(() => [
                _createElementVNode("div", _hoisted_13, [
                  _createElementVNode("div", _hoisted_14, [
                    _createVNode(_component_v_icon, {
                      icon: "mdi-tune",
                      size: "18",
                      class: "mr-1 text-primary"
                    }),
                    _cache[10] || (_cache[10] = _createTextVNode(" 当前生效设置项 ", -1))
                  ]),
                  _createVNode(_component_v_btn, {
                    size: "x-small",
                    variant: "text",
                    color: "primary",
                    "prepend-icon": "mdi-square-edit-outline",
                    onClick: goToConfig
                  }, {
                    default: _withCtx(() => [...(_cache[11] || (_cache[11] = [
                      _createTextVNode(" 修改设置 ", -1)
                    ]))]),
                    _: 1
                  })
                ]),
                _createVNode(_component_v_row, {
                  dense: "",
                  class: "mb-2"
                }, {
                  default: _withCtx(() => [
                    _createVNode(_component_v_col, {
                      cols: "12",
                      sm: "4"
                    }, {
                      default: _withCtx(() => [
                        _cache[12] || (_cache[12] = _createElementVNode("div", { class: "text-caption text-medium-emphasis" }, "定时任务巡检", -1)),
                        _createElementVNode("div", _hoisted_15, [
                          _createVNode(_component_v_icon, {
                            icon: effectiveConfig.value.cron_enabled ? 'mdi-check-circle' : 'mdi-minus-circle',
                            color: effectiveConfig.value.cron_enabled ? 'success' : 'grey',
                            size: "16",
                            class: "mr-1"
                          }, null, 8, ["icon", "color"]),
                          _createTextVNode(" " + _toDisplayString(effectiveConfig.value.cron_enabled ? `开启 [${effectiveConfig.value.cron_mode === 'incremental' ? '增量' : '全量'}] (${effectiveConfig.value.cron || '0 3 * * *'})` : '未开启'), 1)
                        ])
                      ]),
                      _: 1
                    }),
                    _createVNode(_component_v_col, {
                      cols: "12",
                      sm: "4"
                    }, {
                      default: _withCtx(() => [
                        _cache[13] || (_cache[13] = _createElementVNode("div", { class: "text-caption text-medium-emphasis" }, "系统消息通知", -1)),
                        _createElementVNode("div", _hoisted_16, [
                          _createVNode(_component_v_icon, {
                            icon: effectiveConfig.value.notify ? 'mdi-bell-check' : 'mdi-bell-off',
                            color: effectiveConfig.value.notify ? 'success' : 'grey',
                            size: "16",
                            class: "mr-1"
                          }, null, 8, ["icon", "color"]),
                          _createTextVNode(" " + _toDisplayString(effectiveConfig.value.notify ? '分析完成后推送报告' : '未开启通知'), 1)
                        ])
                      ]),
                      _: 1
                    }),
                    _createVNode(_component_v_col, {
                      cols: "12",
                      sm: "4"
                    }, {
                      default: _withCtx(() => [
                        _cache[14] || (_cache[14] = _createElementVNode("div", { class: "text-caption text-medium-emphasis" }, "忽略路径白名单", -1)),
                        _createElementVNode("div", {
                          class: "text-body-2 font-weight-medium mt-1 text-truncate",
                          title: effectiveConfig.value.ignore_paths || '未设置'
                        }, [
                          _createVNode(_component_v_icon, {
                            icon: "mdi-filter-outline",
                            color: "primary",
                            size: "16",
                            class: "mr-1"
                          }),
                          _createTextVNode(" " + _toDisplayString(effectiveConfig.value.ignore_paths || '未设置'), 1)
                        ], 8, _hoisted_17)
                      ]),
                      _: 1
                    })
                  ]),
                  _: 1
                }),
                _createVNode(_component_v_divider, { class: "my-2" }),
                _cache[20] || (_cache[20] = _createElementVNode("div", { class: "text-caption text-medium-emphasis mb-2" }, "异常检测规则生效状态：", -1)),
                _createElementVNode("div", _hoisted_18, [
                  _createVNode(_component_v_chip, {
                    size: "small",
                    color: effectiveConfig.value.detect_english_title !== false ? 'success' : 'grey',
                    variant: effectiveConfig.value.detect_english_title !== false ? 'tonal' : 'outlined'
                  }, {
                    default: _withCtx(() => [
                      _createVNode(_component_v_icon, {
                        start: "",
                        icon: effectiveConfig.value.detect_english_title !== false ? 'mdi-check' : 'mdi-close',
                        size: "14"
                      }, null, 8, ["icon"]),
                      _cache[15] || (_cache[15] = _createTextVNode(" 英文/纯数字未中文化检测 ", -1))
                    ]),
                    _: 1
                  }, 8, ["color", "variant"]),
                  _createVNode(_component_v_chip, {
                    size: "small",
                    color: effectiveConfig.value.detect_merged_files !== false ? 'success' : 'grey',
                    variant: effectiveConfig.value.detect_merged_files !== false ? 'tonal' : 'outlined'
                  }, {
                    default: _withCtx(() => [
                      _createVNode(_component_v_icon, {
                        start: "",
                        icon: effectiveConfig.value.detect_merged_files !== false ? 'mdi-check' : 'mdi-close',
                        size: "14"
                      }, null, 8, ["icon"]),
                      _createTextVNode(" 多文件归并 (≥ " + _toDisplayString(effectiveConfig.value.min_merged_files || 2) + ") ", 1)
                    ]),
                    _: 1
                  }, 8, ["color", "variant"]),
                  _createVNode(_component_v_chip, {
                    size: "small",
                    color: effectiveConfig.value.detect_unidentified !== false ? 'success' : 'grey',
                    variant: effectiveConfig.value.detect_unidentified !== false ? 'tonal' : 'outlined'
                  }, {
                    default: _withCtx(() => [
                      _createVNode(_component_v_icon, {
                        start: "",
                        icon: effectiveConfig.value.detect_unidentified !== false ? 'mdi-check' : 'mdi-close',
                        size: "14"
                      }, null, 8, ["icon"]),
                      _cache[16] || (_cache[16] = _createTextVNode(" 未识别 / TMDB缺失 ", -1))
                    ]),
                    _: 1
                  }, 8, ["color", "variant"]),
                  _createVNode(_component_v_chip, {
                    size: "small",
                    color: effectiveConfig.value.detect_failed_status !== false ? 'success' : 'grey',
                    variant: effectiveConfig.value.detect_failed_status !== false ? 'tonal' : 'outlined'
                  }, {
                    default: _withCtx(() => [
                      _createVNode(_component_v_icon, {
                        start: "",
                        icon: effectiveConfig.value.detect_failed_status !== false ? 'mdi-check' : 'mdi-close',
                        size: "14"
                      }, null, 8, ["icon"]),
                      _cache[17] || (_cache[17] = _createTextVNode(" 整理运行失败记录 ", -1))
                    ]),
                    _: 1
                  }, 8, ["color", "variant"]),
                  _createVNode(_component_v_chip, {
                    size: "small",
                    color: effectiveConfig.value.detect_duplicate_episode !== false ? 'success' : 'grey',
                    variant: effectiveConfig.value.detect_duplicate_episode !== false ? 'tonal' : 'outlined'
                  }, {
                    default: _withCtx(() => [
                      _createVNode(_component_v_icon, {
                        start: "",
                        icon: effectiveConfig.value.detect_duplicate_episode !== false ? 'mdi-check' : 'mdi-close',
                        size: "14"
                      }, null, 8, ["icon"]),
                      _cache[18] || (_cache[18] = _createTextVNode(" 重复季集冲突 ", -1))
                    ]),
                    _: 1
                  }, 8, ["color", "variant"]),
                  _createVNode(_component_v_chip, {
                    size: "small",
                    color: effectiveConfig.value.detect_missing_dest ? 'success' : 'grey',
                    variant: effectiveConfig.value.detect_missing_dest ? 'tonal' : 'outlined'
                  }, {
                    default: _withCtx(() => [
                      _createVNode(_component_v_icon, {
                        start: "",
                        icon: effectiveConfig.value.detect_missing_dest ? 'mdi-check' : 'mdi-close',
                        size: "14"
                      }, null, 8, ["icon"]),
                      _cache[19] || (_cache[19] = _createTextVNode(" 目标物理文件缺失/0字节 ", -1))
                    ]),
                    _: 1
                  }, 8, ["color", "variant"]),
                  _createVNode(_component_v_chip, {
                    size: "small",
                    color: effectiveConfig.value.detect_invalid_episode ? 'success' : 'grey',
                    variant: effectiveConfig.value.detect_invalid_episode ? 'tonal' : 'outlined'
                  }, {
                    default: _withCtx(() => [
                      _createVNode(_component_v_icon, {
                        start: "",
                        icon: effectiveConfig.value.detect_invalid_episode ? 'mdi-check' : 'mdi-close',
                        size: "14"
                      }, null, 8, ["icon"]),
                      _createTextVNode(" 离群集数异常 (> " + _toDisplayString(effectiveConfig.value.invalid_episode_threshold || 500) + ") ", 1)
                    ]),
                    _: 1
                  }, 8, ["color", "variant"])
                ])
              ]),
              _: 1
            }),
            _createVNode(_component_v_alert, {
              type: "info",
              variant: "tonal",
              icon: "mdi-monitor-dashboard",
              class: "mb-4 text-body-2 rounded-lg"
            }, {
              default: _withCtx(() => [...(_cache[21] || (_cache[21] = [
                _createElementVNode("div", { class: "font-weight-medium" }, "💡 想要查看全部异常文件明细或批量处理？", -1),
                _createElementVNode("div", { class: "text-caption text-medium-emphasis mt-1" }, [
                  _createTextVNode(" 本插件已在 MoviePilot 左侧主导航栏"),
                  _createElementVNode("strong", null, "【整理】"),
                  _createTextVNode("分类下注册了"),
                  _createElementVNode("strong", null, "【异常整理分析】"),
                  _createTextVNode("独立大屏，支持分页筛选、复制路径、直达 TMDB 与一键忽略异常等完整操作。 ")
                ], -1)
              ]))]),
              _: 1
            })
          ])),
      _createVNode(_component_v_card_actions, { class: "px-0 pb-0 pt-2 d-flex justify-end gap-2" }, {
        default: _withCtx(() => [
          _createVNode(_component_v_btn, {
            variant: "outlined",
            color: "secondary",
            "prepend-icon": "mdi-cog",
            onClick: goToConfig
          }, {
            default: _withCtx(() => [...(_cache[22] || (_cache[22] = [
              _createTextVNode(" 前往设置 ", -1)
            ]))]),
            _: 1
          }),
          _createVNode(_component_v_btn, {
            variant: "elevated",
            color: "primary",
            "prepend-icon": "mdi-check",
            onClick: _cache[0] || (_cache[0] = $event => (_ctx.$emit('close')))
          }, {
            default: _withCtx(() => [...(_cache[23] || (_cache[23] = [
              _createTextVNode(" 我知道了，关闭 ", -1)
            ]))]),
            _: 1
          })
        ]),
        _: 1
      }),
      _createVNode(_component_v_snackbar, {
        modelValue: snackbar.value.show,
        "onUpdate:modelValue": _cache[1] || (_cache[1] = $event => ((snackbar.value.show) = $event)),
        color: snackbar.value.color,
        timeout: "3000",
        location: "top"
      }, {
        default: _withCtx(() => [
          _createTextVNode(_toDisplayString(snackbar.value.text), 1)
        ]),
        _: 1
      }, 8, ["modelValue", "color"])
    ]),
    _: 1
  }))
}
}

};
const Page = /*#__PURE__*/_export_sfc(_sfc_main, [['__scopeId',"data-v-3514b6fe"]]);

export { Page as default };
