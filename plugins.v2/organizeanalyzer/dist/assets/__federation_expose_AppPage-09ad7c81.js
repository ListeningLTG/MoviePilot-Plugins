import { importShared } from './__federation_fn_import-054b33c3.js';
import { _ as _export_sfc } from './_plugin-vue_export-helper-c4c0bc37.js';

const AppPage_vue_vue_type_style_index_0_scoped_7063a80a_lang = '';

const {resolveComponent:_resolveComponent,createVNode:_createVNode,createTextVNode:_createTextVNode,createElementVNode:_createElementVNode,toDisplayString:_toDisplayString,withCtx:_withCtx,openBlock:_openBlock,createBlock:_createBlock,createCommentVNode:_createCommentVNode,withKeys:_withKeys,createElementBlock:_createElementBlock,renderList:_renderList,Fragment:_Fragment,normalizeClass:_normalizeClass,mergeProps:_mergeProps} = await importShared('vue');


const _hoisted_1 = { class: "pa-4 organize-analyzer-page" };
const _hoisted_2 = { class: "d-flex align-center justify-space-between mb-4 flex-wrap ga-2" };
const _hoisted_3 = { class: "text-h5 font-weight-bold d-flex align-center" };
const _hoisted_4 = { class: "text-caption text-medium-emphasis mt-1" };
const _hoisted_5 = { class: "d-flex ga-2 flex-wrap" };
const _hoisted_6 = { class: "text-h4 font-weight-bold mt-1" };
const _hoisted_7 = { class: "text-h5 font-weight-bold mt-1" };
const _hoisted_8 = { class: "text-h5 font-weight-bold mt-1" };
const _hoisted_9 = { class: "text-h5 font-weight-bold mt-1" };
const _hoisted_10 = { class: "text-h5 font-weight-bold mt-1" };
const _hoisted_11 = { class: "text-h5 font-weight-bold mt-1" };
const _hoisted_12 = { class: "text-h5 font-weight-bold mt-1" };
const _hoisted_13 = { class: "text-h5 font-weight-bold mt-1" };
const _hoisted_14 = { key: 0 };
const _hoisted_15 = {
  colspan: "6",
  class: "text-center text-medium-emphasis py-6"
};
const _hoisted_16 = { key: 1 };
const _hoisted_17 = { class: "font-weight-medium" };
const _hoisted_18 = { class: "text-caption text-medium-emphasis" };
const _hoisted_19 = ["title", "onClick"];
const _hoisted_20 = ["title", "onClick"];
const _hoisted_21 = { class: "text-center" };
const _hoisted_22 = { class: "d-flex align-center justify-center ga-1" };
const _hoisted_23 = {
  key: 0,
  class: "d-flex align-center justify-space-between px-4 py-3"
};
const _hoisted_24 = { class: "text-caption text-medium-emphasis" };
const _hoisted_25 = { class: "d-flex align-center ga-2" };

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
const analyzeScope = ref('');
const showCronDialog = ref(false);
const showPathDialog = ref(false);
const keyword = ref('');
const statusFilter = ref('active');
const typeFilter = ref('');
const pagination = ref({ page: 1, page_size: 50, total: 0, total_pages: 1 });

const pathForm = ref({
  path: '',
  path_type: 'src',
  mode: 'full'
});

const stats = ref({
  summary: {
    total: 0,
    merged_files: 0,
    english_title: 0,
    title_mismatch: 0,
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
  { title: '中文名差异/错配', value: 'title_mismatch' },
  { title: '英文标题未中文化', value: 'english_title' },
  { title: '未识别/TMDB缺失', value: 'unidentified' },
  { title: '整理运行失败', value: 'failed_status' },
  { title: '重复季集冲突', value: 'duplicate_episode' },
  { title: '目标文件缺失/0字节', value: 'missing_dest' },
  { title: '离群/格式异常集数', value: 'invalid_episode' }
];

const sortBy = ref('date_desc');

const sortOptions = [
  { title: '默认排序 (时间最新)', value: 'date_desc' },
  { title: '覆盖文件数倒序 (多 ➔ 少)', value: 'file_count_desc' },
  { title: '覆盖文件数正序 (少 ➔ 多)', value: 'file_count_asc' },
  { title: '时间正序 (最早)', value: 'date_asc' },
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
  let sort_by = '';
  let sort_order = 'desc';
  if (sortBy.value === 'file_count_desc') {
    sort_by = 'file_count';
    sort_order = 'desc';
  } else if (sortBy.value === 'file_count_asc') {
    sort_by = 'file_count';
    sort_order = 'asc';
  } else if (sortBy.value === 'date_asc') {
    sort_by = 'date';
    sort_order = 'asc';
  } else if (sortBy.value === 'date_desc') {
    sort_by = 'date';
    sort_order = 'desc';
  }

  try {
    const res = await props.api.get(`plugin/${props.pluginId}/exceptions`, {
      params: {
        status: statusFilter.value,
        type_filter: typeFilter.value,
        keyword: keyword.value,
        page: pagination.value.page,
        page_size: pagination.value.page_size,
        sort_by: sort_by,
        sort_order: sort_order
      }
    });
    if (res && res.data !== undefined) {
      exceptions.value = res.data;
      pagination.value.total = res.total || 0;
      pagination.value.total_pages = res.total_pages || 1;
      pagination.value.page = res.page || 1;
    }
  } catch (err) {
    console.error('[OrganizeAnalyzer] Fetch exceptions failed', err);
  } finally {
    loading.value = false;
  }
};

const triggerAnalyze = async (mode = 'incremental') => {
  analyzing.value = true;
  analyzeScope.value = mode;
  try {
    await props.api.post(`plugin/${props.pluginId}/analyze?mode=${mode}`);
    await fetchStats();
    await fetchExceptions();
    snackbar.value = { show: true, text: `[${mode === 'full' ? '全量' : '增量'}]分析完成！`, color: 'success' };
  } catch (err) {
    console.error('[OrganizeAnalyzer] Trigger analyze failed', err);
    snackbar.value = { show: true, text: '分析请求失败', color: 'error' };
  } finally {
    analyzing.value = false;
    analyzeScope.value = '';
  }
};

const triggerPathAnalyze = async () => {
  const searchPath = pathForm.value.path ? pathForm.value.path.trim() : '';
  if (!searchPath) return;
  analyzing.value = true;
  analyzeScope.value = 'path';
  try {
    const pathEncoded = encodeURIComponent(searchPath);
    const res = await props.api.post(
      `plugin/${props.pluginId}/analyze?mode=${pathForm.value.mode}&path=${pathEncoded}&path_type=${pathForm.value.path_type}`
    );
    showPathDialog.value = false;
    
    // 自动将搜索栏填充为该指定路径，并重置分页，让表格立即展现该路径下的异常结果
    keyword.value = searchPath;
    pagination.value.page = 1;

    await fetchStats();
    await fetchExceptions();

    const pathCnt = res?.path_summary?.total ?? res?.data?.total ?? 0;
    snackbar.value = { 
      show: true, 
      text: `指定路径 [${searchPath}] 分析完成！已为您自动筛选展示该路径异常。`, 
      color: 'success' 
    };
  } catch (err) {
    console.error('[OrganizeAnalyzer] Trigger path analyze failed', err);
    snackbar.value = { show: true, text: '指定路径分析请求失败', color: 'error' };
  } finally {
    analyzing.value = false;
    analyzeScope.value = '';
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
    snackbar.value = { show: true, text: '已清空全部忽略记录！', color: 'success' };
  } catch (err) {
    console.error('[OrganizeAnalyzer] Clear ignored failed', err);
  }
};

const saveCronConfig = async () => {
  try {
    await props.api.post(`plugin/${props.pluginId}/save_cron_config`, cronForm.value);
    showCronDialog.value = false;
    await fetchStats();
    snackbar.value = { show: true, text: '定时分析配置已保存！', color: 'success' };
  } catch (err) {
    console.error('[OrganizeAnalyzer] Save cron config failed', err);
    snackbar.value = { show: true, text: '保存定时配置失败', color: 'error' };
  }
};

const getTypeColor = (type) => {
  switch (type) {
    case 'merged_files': return 'warning';
    case 'title_mismatch': return 'deep-purple-accent-2';
    case 'english_title': return 'info';
    case 'unidentified': return 'purple';
    case 'failed_status': return 'error';
    case 'duplicate_episode': return 'deep-orange';
    case 'missing_dest': return 'grey-darken-1';
    default: return 'primary';
  }
};

const getTmdbInfo = (item) => {
  let tmdbid = item.tmdbid || '';
  if (!tmdbid) {
    const text = (item.dest || '') + ' ' + (item.detail || '');
    const m = text.match(/tmdbid[=:\s]*(\d+)/i) || text.match(/tmdb_id[=:\s]*(\d+)/i) || text.match(/TMDB\s*ID[:\s]*(\d+)/i);
    if (m) {
      tmdbid = m[1];
    }
  }

  if (!tmdbid || tmdbid === '0') {
    return { tmdbid: '', url: '' };
  }

  const dest = (item.dest || '').toLowerCase();
  const src = (item.src || '').toLowerCase();
  const mtype = (item.type || '').toLowerCase();
  const isTv = mtype.includes('tv') || mtype.includes('剧') || dest.includes('/电视剧/') || dest.includes('/动漫/') || src.includes('/电视剧/');

  const mediaType = isTv ? 'tv' : 'movie';
  return {
    tmdbid: String(tmdbid),
    mediaType: mediaType,
    url: `https://www.themoviedb.org/${mediaType}/${tmdbid}`
  };
};

const snackbar = ref({ show: false, text: '', color: 'success' });

const copyText = (text, label = '路径') => {
  if (!text || text === '-') return;
  if (navigator.clipboard && navigator.clipboard.writeText) {
    navigator.clipboard.writeText(text).then(() => {
      snackbar.value = { show: true, text: `已成功复制${label}到剪贴板！`, color: 'success' };
    }).catch(() => {
      fallbackCopy(text, label);
    });
  } else {
    fallbackCopy(text, label);
  }
};

const fallbackCopy = (text, label = '路径') => {
  const textArea = document.createElement('textarea');
  textArea.value = text;
  document.body.appendChild(textArea);
  textArea.select();
  try {
    document.execCommand('copy');
    snackbar.value = { show: true, text: `已成功复制${label}到剪贴板！`, color: 'success' };
  } catch (err) {
    snackbar.value = { show: true, text: '复制失败，请手动复制', color: 'error' };
  }
  document.body.removeChild(textArea);
};

onMounted(() => {
  fetchStats();
  fetchExceptions();
});

return (_ctx, _cache) => {
  const _component_v_icon = _resolveComponent("v-icon");
  const _component_v_chip = _resolveComponent("v-chip");
  const _component_v_progress_circular = _resolveComponent("v-progress-circular");
  const _component_v_btn = _resolveComponent("v-btn");
  const _component_v_card = _resolveComponent("v-card");
  const _component_v_col = _resolveComponent("v-col");
  const _component_v_row = _resolveComponent("v-row");
  const _component_v_btn_toggle = _resolveComponent("v-btn-toggle");
  const _component_v_select = _resolveComponent("v-select");
  const _component_v_text_field = _resolveComponent("v-text-field");
  const _component_v_tooltip = _resolveComponent("v-tooltip");
  const _component_v_table = _resolveComponent("v-table");
  const _component_v_pagination = _resolveComponent("v-pagination");
  const _component_v_card_title = _resolveComponent("v-card-title");
  const _component_v_card_text = _resolveComponent("v-card-text");
  const _component_v_spacer = _resolveComponent("v-spacer");
  const _component_v_card_actions = _resolveComponent("v-card-actions");
  const _component_v_dialog = _resolveComponent("v-dialog");
  const _component_v_switch = _resolveComponent("v-switch");
  const _component_v_snackbar = _resolveComponent("v-snackbar");

  return (_openBlock(), _createElementBlock("div", _hoisted_1, [
    _createElementVNode("div", _hoisted_2, [
      _createElementVNode("div", null, [
        _createElementVNode("h2", _hoisted_3, [
          _createVNode(_component_v_icon, {
            icon: "mdi-file-find-outline",
            class: "mr-2",
            color: "primary"
          }),
          _cache[29] || (_cache[29] = _createTextVNode(" 媒体整理异常分析仪表盘 ", -1))
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
                default: _withCtx(() => [...(_cache[30] || (_cache[30] = [
                  _createTextVNode(" 已禁用 ", -1)
                ]))]),
                _: 1
              }))
        ])
      ]),
      _createElementVNode("div", _hoisted_5, [
        _createVNode(_component_v_btn, {
          color: "primary",
          disabled: analyzing.value,
          onClick: _cache[0] || (_cache[0] = $event => (triggerAnalyze('incremental')))
        }, {
          prepend: _withCtx(() => [
            (analyzing.value && analyzeScope.value === 'incremental')
              ? (_openBlock(), _createBlock(_component_v_progress_circular, {
                  key: 0,
                  indeterminate: "",
                  size: "20",
                  width: "2"
                }))
              : (_openBlock(), _createBlock(_component_v_icon, { key: 1 }, {
                  default: _withCtx(() => [...(_cache[31] || (_cache[31] = [
                    _createTextVNode("mdi-play", -1)
                  ]))]),
                  _: 1
                }))
          ]),
          default: _withCtx(() => [
            _cache[32] || (_cache[32] = _createTextVNode(" 立即增量分析 ", -1))
          ]),
          _: 1
        }, 8, ["disabled"]),
        _createVNode(_component_v_btn, {
          color: "secondary",
          disabled: analyzing.value,
          onClick: _cache[1] || (_cache[1] = $event => (triggerAnalyze('full')))
        }, {
          prepend: _withCtx(() => [
            (analyzing.value && analyzeScope.value === 'full')
              ? (_openBlock(), _createBlock(_component_v_progress_circular, {
                  key: 0,
                  indeterminate: "",
                  size: "20",
                  width: "2"
                }))
              : (_openBlock(), _createBlock(_component_v_icon, { key: 1 }, {
                  default: _withCtx(() => [...(_cache[33] || (_cache[33] = [
                    _createTextVNode("mdi-refresh", -1)
                  ]))]),
                  _: 1
                }))
          ]),
          default: _withCtx(() => [
            _cache[34] || (_cache[34] = _createTextVNode(" 立即全量分析 ", -1))
          ]),
          _: 1
        }, 8, ["disabled"]),
        _createVNode(_component_v_btn, {
          color: "teal",
          variant: "elevated",
          disabled: analyzing.value,
          "prepend-icon": "mdi-folder-search-outline",
          onClick: _cache[2] || (_cache[2] = $event => (showPathDialog.value = true))
        }, {
          prepend: _withCtx(() => [
            (analyzing.value && analyzeScope.value === 'path')
              ? (_openBlock(), _createBlock(_component_v_progress_circular, {
                  key: 0,
                  indeterminate: "",
                  size: "20",
                  width: "2"
                }))
              : (_openBlock(), _createBlock(_component_v_icon, { key: 1 }, {
                  default: _withCtx(() => [...(_cache[35] || (_cache[35] = [
                    _createTextVNode("mdi-folder-search-outline", -1)
                  ]))]),
                  _: 1
                }))
          ]),
          default: _withCtx(() => [
            _cache[36] || (_cache[36] = _createTextVNode(" 按路径分析 ", -1))
          ]),
          _: 1
        }, 8, ["disabled"]),
        _createVNode(_component_v_btn, {
          color: "info",
          variant: "tonal",
          "prepend-icon": "mdi-clock-outline",
          onClick: _cache[3] || (_cache[3] = $event => (showCronDialog.value = true))
        }, {
          default: _withCtx(() => [...(_cache[37] || (_cache[37] = [
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
          default: _withCtx(() => [...(_cache[38] || (_cache[38] = [
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
                _cache[39] || (_cache[39] = _createElementVNode("div", { class: "text-subtitle-2 font-weight-medium" }, "未处理异常总数", -1)),
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
                _cache[40] || (_cache[40] = _createElementVNode("div", { class: "text-subtitle-2" }, "多文件覆盖冲突", -1)),
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
              color: "deep-purple-accent-2",
              class: "pa-3"
            }, {
              default: _withCtx(() => [
                _cache[41] || (_cache[41] = _createElementVNode("div", { class: "text-subtitle-2 font-weight-bold" }, "中文名差异 / 错配", -1)),
                _createElementVNode("div", _hoisted_8, _toDisplayString(summary.value.title_mismatch || 0), 1)
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
                _cache[42] || (_cache[42] = _createElementVNode("div", { class: "text-subtitle-2" }, "英文标题未中文化", -1)),
                _createElementVNode("div", _hoisted_9, _toDisplayString(summary.value.english_title || 0), 1)
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
                _cache[43] || (_cache[43] = _createElementVNode("div", { class: "text-subtitle-2" }, "未识别 / TMDB缺失", -1)),
                _createElementVNode("div", _hoisted_10, _toDisplayString(summary.value.unidentified || 0), 1)
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
                _cache[44] || (_cache[44] = _createElementVNode("div", { class: "text-subtitle-2" }, "整理运行失败", -1)),
                _createElementVNode("div", _hoisted_11, _toDisplayString(summary.value.failed_status || 0), 1)
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
                _cache[45] || (_cache[45] = _createElementVNode("div", { class: "text-subtitle-2" }, "重复季集冲突", -1)),
                _createElementVNode("div", _hoisted_12, _toDisplayString(summary.value.duplicate_episode || 0), 1)
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
                _cache[46] || (_cache[46] = _createElementVNode("div", { class: "text-subtitle-2" }, "目标缺失/0字节", -1)),
                _createElementVNode("div", _hoisted_13, _toDisplayString(summary.value.missing_dest || 0), 1)
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
              sm: "3",
              md: "2"
            }, {
              default: _withCtx(() => [
                _createVNode(_component_v_btn_toggle, {
                  modelValue: statusFilter.value,
                  "onUpdate:modelValue": [
                    _cache[4] || (_cache[4] = $event => ((statusFilter).value = $event)),
                    _cache[5] || (_cache[5] = () => { pagination.value.page = 1; fetchExceptions(); })
                  ],
                  mandatory: "",
                  color: "primary",
                  density: "compact"
                }, {
                  default: _withCtx(() => [
                    _createVNode(_component_v_btn, { value: "active" }, {
                      default: _withCtx(() => [...(_cache[47] || (_cache[47] = [
                        _createTextVNode("未处理", -1)
                      ]))]),
                      _: 1
                    }),
                    _createVNode(_component_v_btn, { value: "ignored" }, {
                      default: _withCtx(() => [...(_cache[48] || (_cache[48] = [
                        _createTextVNode("已忽略", -1)
                      ]))]),
                      _: 1
                    }),
                    _createVNode(_component_v_btn, { value: "all" }, {
                      default: _withCtx(() => [...(_cache[49] || (_cache[49] = [
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
              sm: "3",
              md: "3"
            }, {
              default: _withCtx(() => [
                _createVNode(_component_v_select, {
                  modelValue: typeFilter.value,
                  "onUpdate:modelValue": [
                    _cache[6] || (_cache[6] = $event => ((typeFilter).value = $event)),
                    _cache[7] || (_cache[7] = () => { pagination.value.page = 1; fetchExceptions(); })
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
              sm: "3",
              md: "3"
            }, {
              default: _withCtx(() => [
                _createVNode(_component_v_select, {
                  modelValue: sortBy.value,
                  "onUpdate:modelValue": [
                    _cache[8] || (_cache[8] = $event => ((sortBy).value = $event)),
                    _cache[9] || (_cache[9] = () => { pagination.value.page = 1; fetchExceptions(); })
                  ],
                  label: "排序规则",
                  density: "compact",
                  "hide-details": "",
                  items: sortOptions
                }, null, 8, ["modelValue"])
              ]),
              _: 1
            }),
            _createVNode(_component_v_col, {
              cols: "12",
              sm: "3",
              md: "4"
            }, {
              default: _withCtx(() => [
                _createVNode(_component_v_text_field, {
                  modelValue: keyword.value,
                  "onUpdate:modelValue": _cache[10] || (_cache[10] = $event => ((keyword).value = $event)),
                  onKeyup: _cache[11] || (_cache[11] = _withKeys(() => { pagination.value.page = 1; fetchExceptions(); }, ["enter"])),
                  "onClick:appendInner": _cache[12] || (_cache[12] = () => { pagination.value.page = 1; fetchExceptions(); }),
                  "onClick:clear": _cache[13] || (_cache[13] = () => { keyword.value = ''; pagination.value.page = 1; fetchExceptions(); }),
                  clearable: "",
                  label: "搜索标题/路径关键字 (回车筛选)",
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
            _cache[52] || (_cache[52] = _createElementVNode("thead", null, [
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
                  style: {"width":"150px"}
                }, "操作")
              ])
            ], -1)),
            _createElementVNode("tbody", null, [
              (loading.value)
                ? (_openBlock(), _createElementBlock("tr", _hoisted_14, [
                    _createElementVNode("td", _hoisted_15, [
                      _createVNode(_component_v_progress_circular, {
                        indeterminate: "",
                        size: "24",
                        width: "2",
                        class: "mr-2"
                      }),
                      _cache[50] || (_cache[50] = _createTextVNode(" 加载中... ", -1))
                    ])
                  ]))
                : (exceptions.value.length === 0)
                  ? (_openBlock(), _createElementBlock("tr", _hoisted_16, [...(_cache[51] || (_cache[51] = [
                      _createElementVNode("td", {
                        colspan: "6",
                        class: "text-center text-medium-emphasis py-6"
                      }, " 暂无相关异常记录 🎉 ", -1)
                    ]))]))
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
                    _createElementVNode("div", _hoisted_17, _toDisplayString(item.title || '未知'), 1),
                    _createElementVNode("div", _hoisted_18, _toDisplayString(item.date), 1)
                  ]),
                  _createElementVNode("td", {
                    class: "text-caption text-truncate path-cell",
                    style: {"max-width":"200px"},
                    title: item.src ? `${item.src} (点击复制)` : '',
                    onClick: $event => (copyText(item.src))
                  }, _toDisplayString(item.src || '-'), 9, _hoisted_19),
                  _createElementVNode("td", {
                    class: "text-caption text-truncate path-cell",
                    style: {"max-width":"200px"},
                    title: item.dest ? `${item.dest} (点击复制)` : '',
                    onClick: $event => (copyText(item.dest))
                  }, _toDisplayString(item.dest || '-'), 9, _hoisted_20),
                  _createElementVNode("td", {
                    class: _normalizeClass(["text-body-2", item.type === 'title_mismatch' ? 'text-deep-purple-accent-2' : 'text-warning'])
                  }, _toDisplayString(item.detail || '-'), 3),
                  _createElementVNode("td", _hoisted_21, [
                    _createElementVNode("div", _hoisted_22, [
                      (getTmdbInfo(item).tmdbid)
                        ? (_openBlock(), _createBlock(_component_v_tooltip, {
                            key: 0,
                            text: "复制 TMDB ID",
                            location: "top"
                          }, {
                            activator: _withCtx(({ props }) => [
                              _createVNode(_component_v_btn, _mergeProps({ ref_for: true }, props, {
                                icon: "mdi-identifier",
                                size: "x-small",
                                variant: "text",
                                color: "primary",
                                onClick: $event => (copyText(getTmdbInfo(item).tmdbid, 'TMDB ID'))
                              }), null, 16, ["onClick"])
                            ]),
                            _: 2
                          }, 1024))
                        : _createCommentVNode("", true),
                      (getTmdbInfo(item).url)
                        ? (_openBlock(), _createBlock(_component_v_tooltip, {
                            key: 1,
                            text: "在 TMDB 中打开网页",
                            location: "top"
                          }, {
                            activator: _withCtx(({ props }) => [
                              _createVNode(_component_v_btn, _mergeProps({ ref_for: true }, props, {
                                icon: "mdi-open-in-new",
                                size: "x-small",
                                variant: "text",
                                color: "info",
                                href: getTmdbInfo(item).url,
                                target: "_blank"
                              }), null, 16, ["href"])
                            ]),
                            _: 2
                          }, 1024))
                        : _createCommentVNode("", true),
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
                  ])
                ]))
              }), 128))
            ])
          ]),
          _: 1
        }),
        (pagination.value.total > 0)
          ? (_openBlock(), _createElementBlock("div", _hoisted_23, [
              _createElementVNode("div", _hoisted_24, [
                _cache[53] || (_cache[53] = _createTextVNode(" 共 ", -1)),
                _createElementVNode("strong", null, _toDisplayString(pagination.value.total), 1),
                _createTextVNode(" 条，当前第 " + _toDisplayString(pagination.value.page) + " / " + _toDisplayString(pagination.value.total_pages) + " 页 ", 1)
              ]),
              _createElementVNode("div", _hoisted_25, [
                _createVNode(_component_v_select, {
                  modelValue: pagination.value.page_size,
                  "onUpdate:modelValue": [
                    _cache[14] || (_cache[14] = $event => ((pagination.value.page_size) = $event)),
                    _cache[15] || (_cache[15] = () => { pagination.value.page = 1; fetchExceptions(); })
                  ],
                  items: [20, 50, 100, 200],
                  label: "每页条数",
                  density: "compact",
                  "hide-details": "",
                  style: {"width":"110px"}
                }, null, 8, ["modelValue"]),
                _createVNode(_component_v_pagination, {
                  modelValue: pagination.value.page,
                  "onUpdate:modelValue": [
                    _cache[16] || (_cache[16] = $event => ((pagination.value.page) = $event)),
                    fetchExceptions
                  ],
                  length: pagination.value.total_pages,
                  "total-visible": 7,
                  density: "compact"
                }, null, 8, ["modelValue", "length"])
              ])
            ]))
          : _createCommentVNode("", true)
      ]),
      _: 1
    }),
    _createVNode(_component_v_dialog, {
      modelValue: showPathDialog.value,
      "onUpdate:modelValue": _cache[21] || (_cache[21] = $event => ((showPathDialog).value = $event)),
      "max-width": "600px"
    }, {
      default: _withCtx(() => [
        _createVNode(_component_v_card, null, {
          default: _withCtx(() => [
            _createVNode(_component_v_card_title, { class: "text-h6 pa-4 d-flex align-center" }, {
              default: _withCtx(() => [
                _createVNode(_component_v_icon, {
                  icon: "mdi-folder-search-outline",
                  class: "mr-2",
                  color: "teal"
                }),
                _cache[54] || (_cache[54] = _createTextVNode(" 指定路径异常分析 ", -1))
              ]),
              _: 1
            }),
            _createVNode(_component_v_card_text, { class: "pa-4" }, {
              default: _withCtx(() => [
                _cache[55] || (_cache[55] = _createElementVNode("div", { class: "text-caption text-medium-emphasis mb-3" }, " 指定整理前（源路径）或整理后（目标路径）的文件夹/关键字，针对性扫描该目录下的整理异常记录。 ", -1)),
                _createVNode(_component_v_text_field, {
                  modelValue: pathForm.value.path,
                  "onUpdate:modelValue": _cache[17] || (_cache[17] = $event => ((pathForm.value.path) = $event)),
                  label: "输入待分析的目标路径 / 目录关键字",
                  placeholder: "/mnt/data/mp2/shareStrm/港剧合集",
                  clearable: "",
                  density: "compact",
                  "persistent-hint": "",
                  hint: "例如: /mnt/data/mp2/shareStrm/港剧合集 或 港剧合集",
                  class: "mb-3"
                }, null, 8, ["modelValue"]),
                _createVNode(_component_v_select, {
                  modelValue: pathForm.value.path_type,
                  "onUpdate:modelValue": _cache[18] || (_cache[18] = $event => ((pathForm.value.path_type) = $event)),
                  label: "路径匹配范围",
                  density: "compact",
                  class: "mb-3",
                  items: [
              { title: '匹配整理前源路径 (src)', value: 'src' },
              { title: '匹配整理后目标路径 (dest)', value: 'dest' },
              { title: '匹配任意路径 (源或目标均可)', value: 'all' }
            ]
                }, null, 8, ["modelValue"]),
                _createVNode(_component_v_select, {
                  modelValue: pathForm.value.mode,
                  "onUpdate:modelValue": _cache[19] || (_cache[19] = $event => ((pathForm.value.mode) = $event)),
                  label: "分析执行模式",
                  density: "compact",
                  items: [
              { title: '全量扫描指定路径 (推荐)', value: 'full' },
              { title: '增量扫描指定路径', value: 'incremental' }
            ]
                }, null, 8, ["modelValue"])
              ]),
              _: 1
            }),
            _createVNode(_component_v_card_actions, { class: "pa-4 pt-0" }, {
              default: _withCtx(() => [
                _createVNode(_component_v_spacer),
                _createVNode(_component_v_btn, {
                  variant: "text",
                  onClick: _cache[20] || (_cache[20] = $event => (showPathDialog.value = false))
                }, {
                  default: _withCtx(() => [...(_cache[56] || (_cache[56] = [
                    _createTextVNode("取消", -1)
                  ]))]),
                  _: 1
                }),
                _createVNode(_component_v_btn, {
                  color: "teal",
                  variant: "elevated",
                  loading: analyzing.value,
                  disabled: !pathForm.value.path,
                  onClick: triggerPathAnalyze
                }, {
                  default: _withCtx(() => [...(_cache[57] || (_cache[57] = [
                    _createTextVNode(" 开始路径分析 ", -1)
                  ]))]),
                  _: 1
                }, 8, ["loading", "disabled"])
              ]),
              _: 1
            })
          ]),
          _: 1
        })
      ]),
      _: 1
    }, 8, ["modelValue"]),
    _createVNode(_component_v_dialog, {
      modelValue: showCronDialog.value,
      "onUpdate:modelValue": _cache[27] || (_cache[27] = $event => ((showCronDialog).value = $event)),
      "max-width": "550px"
    }, {
      default: _withCtx(() => [
        _createVNode(_component_v_card, null, {
          default: _withCtx(() => [
            _createVNode(_component_v_card_title, { class: "text-h6 pa-4" }, {
              default: _withCtx(() => [...(_cache[58] || (_cache[58] = [
                _createTextVNode("定时分析详细配置", -1)
              ]))]),
              _: 1
            }),
            _createVNode(_component_v_card_text, { class: "pa-4" }, {
              default: _withCtx(() => [
                _createVNode(_component_v_switch, {
                  modelValue: cronForm.value.cron_enabled,
                  "onUpdate:modelValue": _cache[22] || (_cache[22] = $event => ((cronForm.value.cron_enabled) = $event)),
                  label: "开启后台定时自动分析",
                  color: "primary"
                }, null, 8, ["modelValue"]),
                _createVNode(_component_v_select, {
                  modelValue: cronForm.value.cron_mode,
                  "onUpdate:modelValue": _cache[23] || (_cache[23] = $event => ((cronForm.value.cron_mode) = $event)),
                  label: "定时分析执行模式",
                  class: "mt-2",
                  items: cronModeOptions
                }, null, 8, ["modelValue"]),
                _createVNode(_component_v_text_field, {
                  modelValue: cronForm.value.cron,
                  "onUpdate:modelValue": _cache[24] || (_cache[24] = $event => ((cronForm.value.cron) = $event)),
                  label: "Cron 表达式",
                  placeholder: "0 3 * * *",
                  hint: "默认 0 3 * * * 代表每天凌晨 3:00 执行",
                  "persistent-hint": "",
                  class: "mt-2"
                }, null, 8, ["modelValue"]),
                _createVNode(_component_v_switch, {
                  modelValue: cronForm.value.notify,
                  "onUpdate:modelValue": _cache[25] || (_cache[25] = $event => ((cronForm.value.notify) = $event)),
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
                  onClick: _cache[26] || (_cache[26] = $event => (showCronDialog.value = false))
                }, {
                  default: _withCtx(() => [...(_cache[59] || (_cache[59] = [
                    _createTextVNode("取消", -1)
                  ]))]),
                  _: 1
                }),
                _createVNode(_component_v_btn, {
                  color: "primary",
                  variant: "elevated",
                  onClick: saveCronConfig
                }, {
                  default: _withCtx(() => [...(_cache[60] || (_cache[60] = [
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
    }, 8, ["modelValue"]),
    _createVNode(_component_v_snackbar, {
      modelValue: snackbar.value.show,
      "onUpdate:modelValue": _cache[28] || (_cache[28] = $event => ((snackbar.value.show) = $event)),
      color: snackbar.value.color,
      timeout: "3000",
      location: "top"
    }, {
      default: _withCtx(() => [
        _createTextVNode(_toDisplayString(snackbar.value.text), 1)
      ]),
      _: 1
    }, 8, ["modelValue", "color"])
  ]))
}
}

};
const AppPage = /*#__PURE__*/_export_sfc(_sfc_main, [['__scopeId',"data-v-7063a80a"]]);

export { AppPage as default };
