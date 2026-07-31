import { importShared } from './__federation_fn_import-054b33c3.js';

const {createElementVNode:_createElementVNode,resolveComponent:_resolveComponent,createVNode:_createVNode,withCtx:_withCtx,createTextVNode:_createTextVNode,openBlock:_openBlock,createBlock:_createBlock} = await importShared('vue');


const {ref,watch} = await importShared('vue');



const _sfc_main = {
  __name: 'Config',
  props: {
  initialConfig: Object,
  api: Object
},
  emits: ['save', 'close', 'switch'],
  setup(__props, { emit: __emit }) {

const props = __props;

const emit = __emit;

// 深度克隆一份配置供双向绑定
const config = ref(JSON.parse(JSON.stringify(props.initialConfig || {})));

// 确保默认值
if (config.value.min_merged_files === undefined) config.value.min_merged_files = 2;
if (config.value.cron_mode === undefined) config.value.cron_mode = 'incremental';
if (config.value.cron === undefined) config.value.cron = '0 3 * * *';
if (config.value.invalid_episode_threshold === undefined) config.value.invalid_episode_threshold = 500;

const save = () => {
  // 转换部分类型
  config.value.min_merged_files = parseInt(config.value.min_merged_files) || 2;
  config.value.invalid_episode_threshold = parseInt(config.value.invalid_episode_threshold) || 500;
  emit('save', config.value);
};

return (_ctx, _cache) => {
  const _component_v_switch = _resolveComponent("v-switch");
  const _component_v_col = _resolveComponent("v-col");
  const _component_v_text_field = _resolveComponent("v-text-field");
  const _component_v_select = _resolveComponent("v-select");
  const _component_v_row = _resolveComponent("v-row");
  const _component_v_divider = _resolveComponent("v-divider");
  const _component_v_textarea = _resolveComponent("v-textarea");
  const _component_v_card_text = _resolveComponent("v-card-text");
  const _component_v_spacer = _resolveComponent("v-spacer");
  const _component_v_btn = _resolveComponent("v-btn");
  const _component_v_card_actions = _resolveComponent("v-card-actions");
  const _component_v_card = _resolveComponent("v-card");

  return (_openBlock(), _createBlock(_component_v_card, { flat: "" }, {
    default: _withCtx(() => [
      _createVNode(_component_v_card_text, null, {
        default: _withCtx(() => [
          _cache[16] || (_cache[16] = _createElementVNode("div", { class: "text-subtitle-1 mb-4 font-weight-bold" }, " 全局设置 ", -1)),
          _createVNode(_component_v_row, { dense: "" }, {
            default: _withCtx(() => [
              _createVNode(_component_v_col, {
                cols: "12",
                md: "4"
              }, {
                default: _withCtx(() => [
                  _createVNode(_component_v_switch, {
                    modelValue: config.value.enabled,
                    "onUpdate:modelValue": _cache[0] || (_cache[0] = $event => ((config.value.enabled) = $event)),
                    label: "启用插件",
                    color: "primary",
                    density: "compact",
                    "hide-details": ""
                  }, null, 8, ["modelValue"])
                ]),
                _: 1
              }),
              _createVNode(_component_v_col, {
                cols: "12",
                md: "4"
              }, {
                default: _withCtx(() => [
                  _createVNode(_component_v_switch, {
                    modelValue: config.value.notify,
                    "onUpdate:modelValue": _cache[1] || (_cache[1] = $event => ((config.value.notify) = $event)),
                    label: "分析完成后发送系统通知",
                    color: "primary",
                    density: "compact",
                    "hide-details": ""
                  }, null, 8, ["modelValue"])
                ]),
                _: 1
              }),
              _createVNode(_component_v_col, {
                cols: "12",
                md: "4"
              }, {
                default: _withCtx(() => [
                  _createVNode(_component_v_switch, {
                    modelValue: config.value.cron_enabled,
                    "onUpdate:modelValue": _cache[2] || (_cache[2] = $event => ((config.value.cron_enabled) = $event)),
                    label: "开启后台定时分析",
                    color: "primary",
                    density: "compact",
                    "hide-details": ""
                  }, null, 8, ["modelValue"])
                ]),
                _: 1
              }),
              _createVNode(_component_v_col, {
                cols: "12",
                md: "6"
              }, {
                default: _withCtx(() => [
                  _createVNode(_component_v_text_field, {
                    modelValue: config.value.cron,
                    "onUpdate:modelValue": _cache[3] || (_cache[3] = $event => ((config.value.cron) = $event)),
                    label: "定时 Cron 表达式",
                    placeholder: "0 3 * * *",
                    density: "compact",
                    class: "mt-2",
                    "hide-details": ""
                  }, null, 8, ["modelValue"])
                ]),
                _: 1
              }),
              _createVNode(_component_v_col, {
                cols: "12",
                md: "6"
              }, {
                default: _withCtx(() => [
                  _createVNode(_component_v_select, {
                    modelValue: config.value.cron_mode,
                    "onUpdate:modelValue": _cache[4] || (_cache[4] = $event => ((config.value.cron_mode) = $event)),
                    label: "定时分析执行模式",
                    items: [{title: '增量分析 (推荐，高效速度快)', value: 'incremental'}, {title: '全量分析 (重新完整检索)', value: 'full'}],
                    density: "compact",
                    class: "mt-2",
                    "hide-details": ""
                  }, null, 8, ["modelValue"])
                ]),
                _: 1
              })
            ]),
            _: 1
          }),
          _createVNode(_component_v_divider, { class: "my-4" }),
          _cache[17] || (_cache[17] = _createElementVNode("div", { class: "text-subtitle-1 mb-4 font-weight-bold" }, " 异常检测规则开关及参数 ", -1)),
          _createVNode(_component_v_row, { dense: "" }, {
            default: _withCtx(() => [
              _createVNode(_component_v_col, {
                cols: "12",
                md: "6"
              }, {
                default: _withCtx(() => [
                  _createVNode(_component_v_switch, {
                    modelValue: config.value.detect_merged_files,
                    "onUpdate:modelValue": _cache[5] || (_cache[5] = $event => ((config.value.detect_merged_files) = $event)),
                    label: "检测多文件归并/覆盖同一目标",
                    color: "primary",
                    density: "compact",
                    "hide-details": ""
                  }, null, 8, ["modelValue"])
                ]),
                _: 1
              }),
              _createVNode(_component_v_col, {
                cols: "12",
                md: "6"
              }, {
                default: _withCtx(() => [
                  _createVNode(_component_v_text_field, {
                    modelValue: config.value.min_merged_files,
                    "onUpdate:modelValue": _cache[6] || (_cache[6] = $event => ((config.value.min_merged_files) = $event)),
                    type: "number",
                    label: "归并文件最小数量阈值",
                    density: "compact",
                    "hide-details": ""
                  }, null, 8, ["modelValue"])
                ]),
                _: 1
              }),
              _createVNode(_component_v_col, {
                cols: "12",
                md: "6"
              }, {
                default: _withCtx(() => [
                  _createVNode(_component_v_switch, {
                    modelValue: config.value.detect_english_title,
                    "onUpdate:modelValue": _cache[7] || (_cache[7] = $event => ((config.value.detect_english_title) = $event)),
                    label: "检测英文未中文化标题",
                    color: "primary",
                    density: "compact",
                    "hide-details": ""
                  }, null, 8, ["modelValue"])
                ]),
                _: 1
              }),
              _createVNode(_component_v_col, {
                cols: "12",
                md: "6"
              }, {
                default: _withCtx(() => [
                  _createVNode(_component_v_switch, {
                    modelValue: config.value.detect_unidentified,
                    "onUpdate:modelValue": _cache[8] || (_cache[8] = $event => ((config.value.detect_unidentified) = $event)),
                    label: "检测未识别 / TMDB 缺失",
                    color: "primary",
                    density: "compact",
                    "hide-details": ""
                  }, null, 8, ["modelValue"])
                ]),
                _: 1
              }),
              _createVNode(_component_v_col, {
                cols: "12",
                md: "6"
              }, {
                default: _withCtx(() => [
                  _createVNode(_component_v_switch, {
                    modelValue: config.value.detect_failed_status,
                    "onUpdate:modelValue": _cache[9] || (_cache[9] = $event => ((config.value.detect_failed_status) = $event)),
                    label: "检测整理状态失败记录",
                    color: "primary",
                    density: "compact",
                    "hide-details": ""
                  }, null, 8, ["modelValue"])
                ]),
                _: 1
              }),
              _createVNode(_component_v_col, {
                cols: "12",
                md: "6"
              }, {
                default: _withCtx(() => [
                  _createVNode(_component_v_switch, {
                    modelValue: config.value.detect_duplicate_episode,
                    "onUpdate:modelValue": _cache[10] || (_cache[10] = $event => ((config.value.detect_duplicate_episode) = $event)),
                    label: "检测重复季集冲突",
                    color: "primary",
                    density: "compact",
                    "hide-details": ""
                  }, null, 8, ["modelValue"])
                ]),
                _: 1
              }),
              _createVNode(_component_v_col, {
                cols: "12",
                md: "6"
              }, {
                default: _withCtx(() => [
                  _createVNode(_component_v_switch, {
                    modelValue: config.value.detect_missing_dest,
                    "onUpdate:modelValue": _cache[11] || (_cache[11] = $event => ((config.value.detect_missing_dest) = $event)),
                    label: "检测目标物理文件缺失/0字节 (本地路径)",
                    color: "primary",
                    density: "compact",
                    "hide-details": ""
                  }, null, 8, ["modelValue"])
                ]),
                _: 1
              }),
              _createVNode(_component_v_col, {
                cols: "12",
                md: "6"
              }, {
                default: _withCtx(() => [
                  _createVNode(_component_v_switch, {
                    modelValue: config.value.detect_invalid_episode,
                    "onUpdate:modelValue": _cache[12] || (_cache[12] = $event => ((config.value.detect_invalid_episode) = $event)),
                    label: `检测离群/格式异常集数 (>${config.value.invalid_episode_threshold || 500})`,
                    color: "primary",
                    density: "compact",
                    "hide-details": ""
                  }, null, 8, ["modelValue", "label"])
                ]),
                _: 1
              }),
              _createVNode(_component_v_col, {
                cols: "12",
                md: "6"
              }, {
                default: _withCtx(() => [
                  _createVNode(_component_v_text_field, {
                    modelValue: config.value.invalid_episode_threshold,
                    "onUpdate:modelValue": _cache[13] || (_cache[13] = $event => ((config.value.invalid_episode_threshold) = $event)),
                    type: "number",
                    label: "离群集数判断阈值",
                    density: "compact",
                    "hide-details": ""
                  }, null, 8, ["modelValue"])
                ]),
                _: 1
              }),
              _createVNode(_component_v_col, { cols: "12" }, {
                default: _withCtx(() => [
                  _createVNode(_component_v_textarea, {
                    modelValue: config.value.ignore_paths,
                    "onUpdate:modelValue": _cache[14] || (_cache[14] = $event => ((config.value.ignore_paths) = $event)),
                    label: "忽略路径关键词白名单 (英文逗号分隔)",
                    placeholder: "/downloads/, /temp/",
                    rows: "2",
                    density: "compact",
                    class: "mt-2",
                    "hide-details": ""
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
      _createVNode(_component_v_card_actions, { class: "px-4 pb-4 pt-0" }, {
        default: _withCtx(() => [
          _createVNode(_component_v_spacer),
          _createVNode(_component_v_btn, {
            variant: "text",
            onClick: _cache[15] || (_cache[15] = $event => (_ctx.$emit('close')))
          }, {
            default: _withCtx(() => [...(_cache[18] || (_cache[18] = [
              _createTextVNode("取消", -1)
            ]))]),
            _: 1
          }),
          _createVNode(_component_v_btn, {
            color: "primary",
            variant: "elevated",
            onClick: save
          }, {
            default: _withCtx(() => [...(_cache[19] || (_cache[19] = [
              _createTextVNode("保存设置", -1)
            ]))]),
            _: 1
          })
        ]),
        _: 1
      })
    ]),
    _: 1
  }))
}
}

};

export { _sfc_main as default };
