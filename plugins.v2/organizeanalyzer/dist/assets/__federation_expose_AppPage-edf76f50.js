import { importShared } from './__federation_fn_import-054b33c3.js';

const {resolveComponent:_resolveComponent,createVNode:_createVNode,createTextVNode:_createTextVNode,createElementVNode:_createElementVNode,toDisplayString:_toDisplayString,withCtx:_withCtx,openBlock:_openBlock,createBlock:_createBlock,createCommentVNode:_createCommentVNode,withKeys:_withKeys,createElementBlock:_createElementBlock,renderList:_renderList,Fragment:_Fragment} = await importShared('vue');


const _hoisted_1 = { class: "pa-4 organize-analyzer-page" };
const _hoisted_2 = { class: "d-flex align-center justify-space-between mb-4" };
const _hoisted_3 = { class: "text-h5 font-weight-bold d-flex align-center" };
const _hoisted_4 = { class: "text-caption text-medium-emphasis mt-1" };
const _hoisted_5 = { class: "d-flex ga-2" };
const _hoisted_6 = { class: "text-h4 font-weight-bold mt-1" };
const _hoisted_7 = { class: "text-h5 font-weight-bold mt-1" };
const _hoisted_8 = { class: "text-h5 font-weight-bold mt-1" };
const _hoisted_9 = { class: "text-h5 font-weight-bold mt-1" };
const _hoisted_10 = { class: "text-h5 font-weight-bold mt-1" };
const _hoisted_11 = { class: "text-h5 font-weight-bold mt-1" };
const _hoisted_12 = { class: "text-h5 font-weight-bold mt-1" };
const _hoisted_13 = { key: 0 };
const _hoisted_14 = {
  colspan: "6",
  class: "text-center text-medium-emphasis py-6"
};
const _hoisted_15 = { class: "font-weight-medium" };
const _hoisted_16 = { class: "text-caption text-medium-emphasis" };
const _hoisted_17 = {
  class: "text-caption text-truncate",
  style: {"max-width":"200px"}
};
const _hoisted_18 = {
  class: "text-caption text-truncate",
  style: {"max-width":"200px"}
};
const _hoisted_19 = { class: "text-body-2 text-warning" };
const _hoisted_20 = { class: "text-center" };

const {ref,computed,onMounted} = await importShared('vue');



const _sfc_main = {
  __name: 'AppPage',
  props: {
  api: { type: Object, required: true },
  pluginId: { type: String, default: 'OrganizeAnalyzer' },
  navKey: { type: String, default: 'main' }
},
  setup(__props) {

const props = __props;

const loading = ref(false);
const analyzing = ref(false);
const showCronDialog = ref(false);
const keyword = ref('');
const statusFilter = ref('active');
const typeFilter = ref('');

const stats = ref({
  summary: {
    total: 0,
    merged_files: 0,
    english_title: 0,
    unidentified: 0,
    failed_status: 0,
    duplicate_episode: 0,
    missing_dest: 0,
    invalid_episode: 0,
    ignored: 0
  },
  last_run_time: '尚未运行',
  cron_enabled: true,
  cron: '0 3 * * *',
  cron_mode: 'incremental',
  notify: false
});

const exceptions = ref([]);

const cronForm = ref({
  cron_enabled: true,
  cron: '0 3 * * *',
  cron_mode: 'incremental',
  notify: false
});

const typeOptions = [
  { title: '全部类型', value: '' },
  { title: '多文件合并覆盖', value: 'merged_files' },
  { title: '英文标题未中文化', value: 'english_title' },
  { title: '未识别/TMDB缺失', value: 'unidentified' },
  { title: '整理运行失败', value: 'failed_status' },
  { title: '重复季集冲突', value: 'duplicate_episode' },
  { title: '目标文件缺失/0字节', value: 'missing_dest' },
  { title: '离群/格式异常集数', value: 'invalid_episode' }
];

const cronModeOptions = [
  { title: '增量分析 (推荐，高速高效)', value: 'incremental' },
  { title: '全量分析 (重新完整扫描)', value: 'full' }
];

const summary = computed(() => stats.value.summary || {});

const fetchStats = async () => {
  try {
    const res = await props.api.get(`plugin/${props.pluginId}/stats`);
    if (res && res.data) {
      stats.value = res.data;
      cronForm.value = {
        cron_enabled: res.data.cron_enabled ?? true,
        cron: res.data.cron || '0 3 * * *',
        cron_mode: res.data.cron_mode || 'incremental',
        notify: res.data.notify ?? false
      };
    }
  } catch (err) {
    console.error('[OrganizeAnalyzer] Fetch stats failed', err);
  }
};

const fetchExceptions = async () => {
  loading.value = true;
  try {
    const res = await props.api.get(`plugin/${props.pluginId}/exceptions`, {
      params: {
        status: statusFilter.value,
        type_filter: typeFilter.value,
        keyword: keyword.value
      }
    });
    if (res && res.data) {
      exceptions.value = res.data;
    }
  } catch (err) {
    console.error('[OrganizeAnalyzer] Fetch exceptions failed', err);
  } finally {
    loading.value = false;
  }
};

const triggerAnalyze = async (mode = 'incremental') => {
  analyzing.value = true;
  try {
    await props.api.post(`plugin/${props.pluginId}/analyze?mode=${mode}`);
    await fetchStats();
    await fetchExceptions();
  } catch (err) {
    console.error('[OrganizeAnalyzer] Trigger analyze failed', err);
  } finally {
    analyzing.value = false;
  }
};

const ignoreItem = async (key) => {
  try {
    await props.api.post(`plugin/${props.pluginId}/ignore?key=${encodeURIComponent(key)}`);
    await fetchStats();
    await fetchExceptions();
  } catch (err) {
    console.error('[OrganizeAnalyzer] Ignore item failed', err);
  }
};

const clearIgnored = async () => {
  try {
    await props.api.post(`plugin/${props.pluginId}/clear_ignored`);
    await fetchStats();
    await fetchExceptions();
  } catch (err) {
    console.error('[OrganizeAnalyzer] Clear ignored failed', err);
  }
};

const saveCronConfig = async () => {
  try {
    await props.api.post(`plugin/${props.pluginId}/save_cron_config`, cronForm.value);
    showCronDialog.value = false;
    await fetchStats();
  } catch (err) {
    console.error('[OrganizeAnalyzer] Save cron config failed', err);
  }
};

const getTypeColor = (type) => {
  switch (type) {
    case 'merged_files': return 'warning';
    case 'english_title': return 'info';
    case 'unidentified': return 'purple';
    case 'failed_status': return 'error';
    case 'duplicate_episode': return 'deep-orange';
    case 'missing_dest': return 'grey-darken-1';
    default: return 'primary';
  }
};

onMounted(() => {
  fetchStats();
  fetchExceptions();
});

return (_ctx, _cache) => {
  const _component_v_icon = _resolveComponent("v-icon");
  const _component_v_chip = _resolveComponent("v-chip");
  const _component_v_btn = _resolveComponent("v-btn");
  const _component_v_card = _resolveComponent("v-card");
  const _component_v_col = _resolveComponent("v-col");
  const _component_v_row = _resolveComponent("v-row");
  const _component_v_btn_toggle = _resolveComponent("v-btn-toggle");
  const _component_v_select = _resolveComponent("v-select");
  const _component_v_text_field = _resolveComponent("v-text-field");
  const _component_v_table = _resolveComponent("v-table");
  const _component_v_card_title = _resolveComponent("v-card-title");
  const _component_v_switch = _resolveComponent("v-switch");
  const _component_v_card_text = _resolveComponent("v-card-text");
  const _component_v_spacer = _resolveComponent("v-spacer");
  const _component_v_card_actions = _resolveComponent("v-card-actions");
  const _component_v_dialog = _resolveComponent("v-dialog");

  return (_openBlock(), _createElementBlock("div", _hoisted_1, [
    _createElementVNode("div", _hoisted_2, [
      _createElementVNode("div", null, [
        _createElementVNode("h2", _hoisted_3, [
          _createVNode(_component_v_icon, {
            icon: "mdi-file-find-outline",
            class: "mr-2",
            color: "primary"
          }),
          _cache[12] || (_cache[12] = _createTextVNode(" 媒体整理异常分析仪表盘 ", -1))
        ]),
        _createElementVNode("div", _hoisted_4, [
          _createTextVNode(" 上次运行时间: " + _toDisplayString(stats.value.last_run_time || '尚未运行') + " | 定时分析状态: ", 1),
          (stats.value.cron_enabled)
            ? (_openBlock(), _createBlock(_component_v_chip, {
                key: 0,
                size: "small",
                color: "success",
                variant: "tonal",
                class: "ml-1"
              }, {
                default: _withCtx(() => [
                  _createTextVNode(" 开启 [" + _toDisplayString(stats.value.cron_mode === 'incremental' ? '增量' : '全量') + "] (" + _toDisplayString(stats.value.cron) + ") ", 1)
                ]),
                _: 1
              }))
            : (_openBlock(), _createBlock(_component_v_chip, {
                key: 1,
                size: "small",
                color: "grey",
                variant: "tonal",
                class: "ml-1"
              }, {
                default: _withCtx(() => [...(_cache[13] || (_cache[13] = [
                  _createTextVNode(" 已禁用 ", -1)
                ]))]),
                _: 1
              }))
        ])
      ]),
      _createElementVNode("div", _hoisted_5, [
        _createVNode(_component_v_btn, {
          color: "primary",
          loading: analyzing.value,
          "prepend-icon": "mdi-play",
          onClick: _cache[0] || (_cache[0] = $event => (triggerAnalyze('incremental')))
        }, {
          default: _withCtx(() => [...(_cache[14] || (_cache[14] = [
            _createTextVNode("立即增量分析", -1)
          ]))]),
          _: 1
        }, 8, ["loading"]),
        _createVNode(_component_v_btn, {
          color: "secondary",
          loading: analyzing.value,
          "prepend-icon": "mdi-refresh",
          onClick: _cache[1] || (_cache[1] = $event => (triggerAnalyze('full')))
        }, {
          default: _withCtx(() => [...(_cache[15] || (_cache[15] = [
            _createTextVNode("立即全量分析", -1)
          ]))]),
          _: 1
        }, 8, ["loading"]),
        _createVNode(_component_v_btn, {
          color: "info",
          variant: "tonal",
          "prepend-icon": "mdi-clock-outline",
          onClick: _cache[2] || (_cache[2] = $event => (showCronDialog.value = true))
        }, {
          default: _withCtx(() => [...(_cache[16] || (_cache[16] = [
            _createTextVNode("定时配置", -1)
          ]))]),
          _: 1
        }),
        _createVNode(_component_v_btn, {
          color: "warning",
          variant: "outlined",
          "prepend-icon": "mdi-delete-sweep",
          onClick: clearIgnored
        }, {
          default: _withCtx(() => [...(_cache[17] || (_cache[17] = [
            _createTextVNode("清空忽略", -1)
          ]))]),
          _: 1
        })
      ])
    ]),
    _createVNode(_component_v_row, { class: "mb-4" }, {
      default: _withCtx(() => [
        _createVNode(_component_v_col, {
          cols: "12",
          sm: "6",
          md: "3"
        }, {
          default: _withCtx(() => [
            _createVNode(_component_v_card, {
              variant: "tonal",
              color: summary.value.total > 0 ? 'error' : 'success',
              class: "pa-3"
            }, {
              default: _withCtx(() => [
                _cache[18] || (_cache[18] = _createElementVNode("div", { class: "text-subtitle-2 font-weight-medium" }, "未处理异常总数", -1)),
                _createElementVNode("div", _hoisted_6, _toDisplayString(summary.value.total || 0), 1)
              ]),
              _: 1
            }, 8, ["color"])
          ]),
          _: 1
        }),
        _createVNode(_component_v_col, {
          cols: "12",
          sm: "6",
          md: "3"
        }, {
          default: _withCtx(() => [
            _createVNode(_component_v_card, {
              variant: "tonal",
              color: "warning",
              class: "pa-3"
            }, {
              default: _withCtx(() => [
                _cache[19] || (_cache[19] = _createElementVNode("div", { class: "text-subtitle-2" }, "多文件覆盖冲突", -1)),
                _createElementVNode("div", _hoisted_7, _toDisplayString(summary.value.merged_files || 0), 1)
              ]),
              _: 1
            })
          ]),
          _: 1
        }),
        _createVNode(_component_v_col, {
          cols: "12",
          sm: "6",
          md: "3"
        }, {
          default: _withCtx(() => [
            _createVNode(_component_v_card, {
              variant: "tonal",
              color: "info",
              class: "pa-3"
            }, {
              default: _withCtx(() => [
                _cache[20] || (_cache[20] = _createElementVNode("div", { class: "text-subtitle-2" }, "英文标题未中文化", -1)),
                _createElementVNode("div", _hoisted_8, _toDisplayString(summary.value.english_title || 0), 1)
              ]),
              _: 1
            })
          ]),
          _: 1
        }),
        _createVNode(_component_v_col, {
          cols: "12",
          sm: "6",
          md: "3"
        }, {
          default: _withCtx(() => [
            _createVNode(_component_v_card, {
              variant: "tonal",
              color: "purple",
              class: "pa-3"
            }, {
              default: _withCtx(() => [
                _cache[21] || (_cache[21] = _createElementVNode("div", { class: "text-subtitle-2" }, "未识别 / TMDB缺失", -1)),
                _createElementVNode("div", _hoisted_9, _toDisplayString(summary.value.unidentified || 0), 1)
              ]),
              _: 1
            })
          ]),
          _: 1
        }),
        _createVNode(_component_v_col, {
          cols: "12",
          sm: "6",
          md: "3"
        }, {
          default: _withCtx(() => [
            _createVNode(_component_v_card, {
              variant: "tonal",
              color: "error",
              class: "pa-3"
            }, {
              default: _withCtx(() => [
                _cache[22] || (_cache[22] = _createElementVNode("div", { class: "text-subtitle-2" }, "整理运行失败", -1)),
                _createElementVNode("div", _hoisted_10, _toDisplayString(summary.value.failed_status || 0), 1)
              ]),
              _: 1
            })
          ]),
          _: 1
        }),
        _createVNode(_component_v_col, {
          cols: "12",
          sm: "6",
          md: "3"
        }, {
          default: _withCtx(() => [
            _createVNode(_component_v_card, {
              variant: "tonal",
              color: "deep-orange",
              class: "pa-3"
            }, {
              default: _withCtx(() => [
                _cache[23] || (_cache[23] = _createElementVNode("div", { class: "text-subtitle-2" }, "重复季集冲突", -1)),
                _createElementVNode("div", _hoisted_11, _toDisplayString(summary.value.duplicate_episode || 0), 1)
              ]),
              _: 1
            })
          ]),
          _: 1
        }),
        _createVNode(_component_v_col, {
          cols: "12",
          sm: "6",
          md: "3"
        }, {
          default: _withCtx(() => [
            _createVNode(_component_v_card, {
              variant: "tonal",
              color: "grey-darken-1",
              class: "pa-3"
            }, {
              default: _withCtx(() => [
                _cache[24] || (_cache[24] = _createElementVNode("div", { class: "text-subtitle-2" }, "目标缺失/0字节", -1)),
                _createElementVNode("div", _hoisted_12, _toDisplayString(summary.value.missing_dest || 0), 1)
              ]),
              _: 1
            })
          ]),
          _: 1
        })
      ]),
      _: 1
    }),
    _createVNode(_component_v_card, { class: "mb-4 pa-3" }, {
      default: _withCtx(() => [
        _createVNode(_component_v_row, {
          align: "center",
          dense: ""
        }, {
          default: _withCtx(() => [
            _createVNode(_component_v_col, {
              cols: "12",
              sm: "4",
              md: "3"
            }, {
              default: _withCtx(() => [
                _createVNode(_component_v_btn_toggle, {
                  modelValue: statusFilter.value,
                  "onUpdate:modelValue": [
                    _cache[3] || (_cache[3] = $event => ((statusFilter).value = $event)),
                    fetchExceptions
                  ],
                  mandatory: "",
                  color: "primary",
                  density: "compact"
                }, {
                  default: _withCtx(() => [
                    _createVNode(_component_v_btn, { value: "active" }, {
                      default: _withCtx(() => [...(_cache[25] || (_cache[25] = [
                        _createTextVNode("未处理", -1)
                      ]))]),
                      _: 1
                    }),
                    _createVNode(_component_v_btn, { value: "ignored" }, {
                      default: _withCtx(() => [...(_cache[26] || (_cache[26] = [
                        _createTextVNode("已忽略", -1)
                      ]))]),
                      _: 1
                    }),
                    _createVNode(_component_v_btn, { value: "all" }, {
                      default: _withCtx(() => [...(_cache[27] || (_cache[27] = [
                        _createTextVNode("全部", -1)
                      ]))]),
                      _: 1
                    })
                  ]),
                  _: 1
                }, 8, ["modelValue"])
              ]),
              _: 1
            }),
            _createVNode(_component_v_col, {
              cols: "12",
              sm: "4",
              md: "4"
            }, {
              default: _withCtx(() => [
                _createVNode(_component_v_select, {
                  modelValue: typeFilter.value,
                  "onUpdate:modelValue": [
                    _cache[4] || (_cache[4] = $event => ((typeFilter).value = $event)),
                    fetchExceptions
                  ],
                  label: "筛选异常类型",
                  density: "compact",
                  "hide-details": "",
                  items: typeOptions
                }, null, 8, ["modelValue"])
              ]),
              _: 1
            }),
            _createVNode(_component_v_col, {
              cols: "12",
              sm: "4",
              md: "5"
            }, {
              default: _withCtx(() => [
                _createVNode(_component_v_text_field, {
                  modelValue: keyword.value,
                  "onUpdate:modelValue": _cache[5] || (_cache[5] = $event => ((keyword).value = $event)),
                  onKeyup: _withKeys(fetchExceptions, ["enter"]),
                  "onClick:appendInner": fetchExceptions,
                  label: "搜索标题/路径关键字",
                  density: "compact",
                  "hide-details": "",
                  "append-inner-icon": "mdi-magnify"
                }, null, 8, ["modelValue"])
              ]),
              _: 1
            })
          ]),
          _: 1
        })
      ]),
      _: 1
    }),
    _createVNode(_component_v_card, null, {
      default: _withCtx(() => [
        _createVNode(_component_v_table, { hover: "" }, {
          default: _withCtx(() => [
            _cache[28] || (_cache[28] = _createElementVNode("thead", null, [
              _createElementVNode("tr", null, [
                _createElementVNode("th", {
                  class: "text-left",
                  style: {"width":"140px"}
                }, "异常类型"),
                _createElementVNode("th", { class: "text-left" }, "标题 / 整理信息"),
                _createElementVNode("th", { class: "text-left" }, "源路径 src"),
                _createElementVNode("th", { class: "text-left" }, "目标路径 dest"),
                _createElementVNode("th", { class: "text-left" }, "异常原因明细"),
                _createElementVNode("th", {
                  class: "text-center",
                  style: {"width":"110px"}
                }, "操作")
              ])
            ], -1)),
            _createElementVNode("tbody", null, [
              (exceptions.value.length === 0)
                ? (_openBlock(), _createElementBlock("tr", _hoisted_13, [
                    _createElementVNode("td", _hoisted_14, _toDisplayString(loading.value ? '加载中...' : '暂无相关异常记录 🎉'), 1)
                  ]))
                : _createCommentVNode("", true),
              (_openBlock(true), _createElementBlock(_Fragment, null, _renderList(exceptions.value, (item) => {
                return (_openBlock(), _createElementBlock("tr", {
                  key: item.key
                }, [
                  _createElementVNode("td", null, [
                    _createVNode(_component_v_chip, {
                      size: "small",
                      color: getTypeColor(item.type),
                      variant: "tonal"
                    }, {
                      default: _withCtx(() => [
                        _createTextVNode(_toDisplayString(item.type_name), 1)
                      ]),
                      _: 2
                    }, 1032, ["color"])
                  ]),
                  _createElementVNode("td", null, [
                    _createElementVNode("div", _hoisted_15, _toDisplayString(item.title || '未知'), 1),
                    _createElementVNode("div", _hoisted_16, _toDisplayString(item.date), 1)
                  ]),
                  _createElementVNode("td", _hoisted_17, _toDisplayString(item.src || '-'), 1),
                  _createElementVNode("td", _hoisted_18, _toDisplayString(item.dest || '-'), 1),
                  _createElementVNode("td", _hoisted_19, _toDisplayString(item.detail || '-'), 1),
                  _createElementVNode("td", _hoisted_20, [
                    _createVNode(_component_v_btn, {
                      size: "x-small",
                      variant: "text",
                      color: item.status === 'ignored' ? 'primary' : 'grey',
                      onClick: $event => (ignoreItem(item.key))
                    }, {
                      default: _withCtx(() => [
                        _createTextVNode(_toDisplayString(item.status === 'ignored' ? '取消忽略' : '忽略'), 1)
                      ]),
                      _: 2
                    }, 1032, ["color", "onClick"])
                  ])
                ]))
              }), 128))
            ])
          ]),
          _: 1
        })
      ]),
      _: 1
    }),
    _createVNode(_component_v_dialog, {
      modelValue: showCronDialog.value,
      "onUpdate:modelValue": _cache[11] || (_cache[11] = $event => ((showCronDialog).value = $event)),
      "max-width": "550px"
    }, {
      default: _withCtx(() => [
        _createVNode(_component_v_card, null, {
          default: _withCtx(() => [
            _createVNode(_component_v_card_title, { class: "text-h6 pa-4" }, {
              default: _withCtx(() => [...(_cache[29] || (_cache[29] = [
                _createTextVNode("定时分析详细配置", -1)
              ]))]),
              _: 1
            }),
            _createVNode(_component_v_card_text, { class: "pa-4" }, {
              default: _withCtx(() => [
                _createVNode(_component_v_switch, {
                  modelValue: cronForm.value.cron_enabled,
                  "onUpdate:modelValue": _cache[6] || (_cache[6] = $event => ((cronForm.value.cron_enabled) = $event)),
                  label: "开启后台定时自动分析",
                  color: "primary"
                }, null, 8, ["modelValue"]),
                _createVNode(_component_v_select, {
                  modelValue: cronForm.value.cron_mode,
                  "onUpdate:modelValue": _cache[7] || (_cache[7] = $event => ((cronForm.value.cron_mode) = $event)),
                  label: "定时分析执行模式",
                  class: "mt-2",
                  items: cronModeOptions
                }, null, 8, ["modelValue"]),
                _createVNode(_component_v_text_field, {
                  modelValue: cronForm.value.cron,
                  "onUpdate:modelValue": _cache[8] || (_cache[8] = $event => ((cronForm.value.cron) = $event)),
                  label: "Cron 表达式",
                  placeholder: "0 3 * * *",
                  hint: "默认 0 3 * * * 代表每天凌晨 3:00 执行",
                  "persistent-hint": "",
                  class: "mt-2"
                }, null, 8, ["modelValue"]),
                _createVNode(_component_v_switch, {
                  modelValue: cronForm.value.notify,
                  "onUpdate:modelValue": _cache[9] || (_cache[9] = $event => ((cronForm.value.notify) = $event)),
                  label: "分析完成后发送 Telegram/系统通知报告",
                  color: "primary",
                  class: "mt-2"
                }, null, 8, ["modelValue"])
              ]),
              _: 1
            }),
            _createVNode(_component_v_card_actions, { class: "pa-4 pt-0" }, {
              default: _withCtx(() => [
                _createVNode(_component_v_spacer),
                _createVNode(_component_v_btn, {
                  variant: "text",
                  onClick: _cache[10] || (_cache[10] = $event => (showCronDialog.value = false))
                }, {
                  default: _withCtx(() => [...(_cache[30] || (_cache[30] = [
                    _createTextVNode("取消", -1)
                  ]))]),
                  _: 1
                }),
                _createVNode(_component_v_btn, {
                  color: "primary",
                  variant: "elevated",
                  onClick: saveCronConfig
                }, {
                  default: _withCtx(() => [...(_cache[31] || (_cache[31] = [
                    _createTextVNode("保存生效", -1)
                  ]))]),
                  _: 1
                })
              ]),
              _: 1
            })
          ]),
          _: 1
        })
      ]),
      _: 1
    }, 8, ["modelValue"])
  ]))
}
}

};

export { _sfc_main as default };
