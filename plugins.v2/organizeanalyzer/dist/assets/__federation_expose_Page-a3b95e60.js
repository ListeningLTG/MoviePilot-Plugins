import { importShared } from './__federation_fn_import-054b33c3.js';

const {resolveComponent:_resolveComponent,createVNode:_createVNode,createElementVNode:_createElementVNode,createTextVNode:_createTextVNode,withCtx:_withCtx,openBlock:_openBlock,createBlock:_createBlock} = await importShared('vue');



const _sfc_main = {
  __name: 'Page',
  emits: ['action', 'switch', 'close'],
  setup(__props, { emit: __emit }) {

return (_ctx, _cache) => {
  const _component_v_icon = _resolveComponent("v-icon");
  const _component_v_btn = _resolveComponent("v-btn");
  const _component_v_card = _resolveComponent("v-card");

  return (_openBlock(), _createBlock(_component_v_card, {
    flat: "",
    class: "text-center pa-10"
  }, {
    default: _withCtx(() => [
      _createVNode(_component_v_icon, {
        icon: "mdi-open-in-new",
        size: "64",
        color: "primary",
        class: "mb-4"
      }),
      _cache[2] || (_cache[2] = _createElementVNode("div", { class: "text-h6 mb-2" }, "插件数据已迁移", -1)),
      _cache[3] || (_cache[3] = _createElementVNode("div", { class: "text-body-1 text-medium-emphasis mb-6" }, [
        _createTextVNode(" 本插件的异常数据展示已经升级为全屏独立大屏仪表盘，无法在小窗口中完美展示。 请关闭本窗口，然后点击 MoviePilot 主界面左侧导航栏中的 "),
        _createElementVNode("strong", null, "【异常整理分析】"),
        _createTextVNode(" 菜单进行查看！ ")
      ], -1)),
      _createVNode(_component_v_btn, {
        color: "primary",
        variant: "elevated",
        onClick: _cache[0] || (_cache[0] = $event => (_ctx.$emit('close'))),
        "prepend-icon": "mdi-close"
      }, {
        default: _withCtx(() => [...(_cache[1] || (_cache[1] = [
          _createTextVNode(" 我知道了，关闭窗口 ", -1)
        ]))]),
        _: 1
      })
    ]),
    _: 1
  }))
}
}

};

export { _sfc_main as default };
