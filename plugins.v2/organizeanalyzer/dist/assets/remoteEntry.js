// MoviePilot V2 Vue Remote Module Federation Entry for OrganizeAnalyzer
import { h, ref, onMounted, defineComponent } from 'vue';

const AppPage = defineComponent({
  name: 'OrganizeAnalyzerAppPage',
  props: {
    api: { type: Object, required: true },
    pluginId: { type: String, default: 'OrganizeAnalyzer' },
    navKey: { type: String, default: 'main' }
  },
  setup(props) {
    const loading = ref(false);
    const analyzing = ref(false);
    const showCronDialog = ref(false);
    const keyword = ref('');
    const statusFilter = ref('active'); // 'active', 'ignored', 'all'
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

    // Cron Form Data
    const cronForm = ref({
      cron_enabled: true,
      cron: '0 3 * * *',
      cron_mode: 'incremental',
      notify: false
    });

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

    onMounted(() => {
      fetchStats();
      fetchExceptions();
    });

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

    return () => {
      const summary = stats.value.summary || {};

      return h('div', { class: 'pa-4 organize-analyzer-page' }, [
        // 顶部工具栏 Header
        h('div', { class: 'd-flex align-center justify-space-between mb-4' }, [
          h('div', {}, [
            h('h2', { class: 'text-h5 font-weight-bold d-flex align-center' }, [
              h('v-icon', { icon: 'mdi-file-find-outline', class: 'mr-2', color: 'primary' }),
              '媒体整理异常分析仪表盘'
            ]),
            h('div', { class: 'text-caption text-medium-emphasis mt-1' }, [
              `上次运行时间: ${stats.value.last_run_time || '尚未运行'} | `,
              `定时分析状态: `,
              stats.value.cron_enabled
                ? h('v-chip', { size: 'small', color: 'success', variant: 'tonal', class: 'ml-1' }, `开启 [${stats.value.cron_mode === 'incremental' ? '增量' : '全量'}] (${stats.value.cron})`)
                : h('v-chip', { size: 'small', color: 'grey', variant: 'tonal', class: 'ml-1' }, '已禁用')
            ])
          ]),
          h('div', { class: 'd-flex ga-2' }, [
            h('v-btn', {
              color: 'primary',
              loading: analyzing.value,
              prependIcon: 'mdi-play',
              onClick: () => triggerAnalyze('incremental')
            }, () => '立即增量分析'),
            h('v-btn', {
              color: 'secondary',
              loading: analyzing.value,
              prependIcon: 'mdi-refresh',
              onClick: () => triggerAnalyze('full')
            }, () => '立即全量分析'),
            h('v-btn', {
              color: 'info',
              variant: 'tonal',
              prependIcon: 'mdi-clock-outline',
              onClick: () => { showCronDialog.value = true; }
            }, () => '定时配置'),
            h('v-btn', {
              color: 'warning',
              variant: 'outlined',
              prependIcon: 'mdi-delete-sweep',
              onClick: clearIgnored
            }, () => '清空忽略')
          ])
        ]),

        // 7 大统计卡片 Metrics Row
        h('v-row', { class: 'mb-4' }, [
          h('v-col', { cols: 12, sm: 6, md: 3 }, [
            h('v-card', { variant: 'tonal', color: summary.total > 0 ? 'error' : 'success', class: 'pa-3' }, [
              h('div', { class: 'text-subtitle-2 font-weight-medium' }, '未处理异常总数'),
              h('div', { class: 'text-h4 font-weight-bold mt-1' }, summary.total || 0)
            ])
          ]),
          h('v-col', { cols: 12, sm: 6, md: 3 }, [
            h('v-card', { variant: 'tonal', color: 'warning', class: 'pa-3' }, [
              h('div', { class: 'text-subtitle-2' }, '多文件覆盖冲突'),
              h('div', { class: 'text-h5 font-weight-bold mt-1' }, summary.merged_files || 0)
            ])
          ]),
          h('v-col', { cols: 12, sm: 6, md: 3 }, [
            h('v-card', { variant: 'tonal', color: 'info', class: 'pa-3' }, [
              h('div', { class: 'text-subtitle-2' }, '英文标题未中文化'),
              h('div', { class: 'text-h5 font-weight-bold mt-1' }, summary.english_title || 0)
            ])
          ]),
          h('v-col', { cols: 12, sm: 6, md: 3 }, [
            h('v-card', { variant: 'tonal', color: 'purple', class: 'pa-3' }, [
              h('div', { class: 'text-subtitle-2' }, '未识别 / TMDB缺失'),
              h('div', { class: 'text-h5 font-weight-bold mt-1' }, summary.unidentified || 0)
            ])
          ]),
          h('v-col', { cols: 12, sm: 6, md: 3 }, [
            h('v-card', { variant: 'tonal', color: 'error', class: 'pa-3' }, [
              h('div', { class: 'text-subtitle-2' }, '整理运行失败'),
              h('div', { class: 'text-h5 font-weight-bold mt-1' }, summary.failed_status || 0)
            ])
          ]),
          h('v-col', { cols: 12, sm: 6, md: 3 }, [
            h('v-card', { variant: 'tonal', color: 'deep-orange', class: 'pa-3' }, [
              h('div', { class: 'text-subtitle-2' }, '重复季集冲突'),
              h('div', { class: 'text-h5 font-weight-bold mt-1' }, summary.duplicate_episode || 0)
            ])
          ]),
          h('v-col', { cols: 12, sm: 6, md: 3 }, [
            h('v-card', { variant: 'tonal', color: 'grey-darken-1', class: 'pa-3' }, [
              h('div', { class: 'text-subtitle-2' }, '目标缺失/0字节'),
              h('div', { class: 'text-h5 font-weight-bold mt-1' }, summary.missing_dest || 0)
            ])
          ])
        ]),

        // 筛选与搜索工具条 Filters Bar
        h('v-card', { class: 'mb-4 pa-3' }, [
          h('v-row', { align: 'center', dense: true }, [
            h('v-col', { cols: 12, sm: 4, md: 3 }, [
              h('v-btn-toggle', {
                modelValue: statusFilter.value,
                'onUpdate:modelValue': (val) => { statusFilter.value = val; fetchExceptions(); },
                mandatory: true,
                color: 'primary',
                density: 'compact'
              }, () => [
                h('v-btn', { value: 'active' }, () => '未处理'),
                h('v-btn', { value: 'ignored' }, () => '已忽略'),
                h('v-btn', { value: 'all' }, () => '全部')
              ])
            ]),
            h('v-col', { cols: 12, sm: 4, md: 4 }, [
              h('v-select', {
                modelValue: typeFilter.value,
                'onUpdate:modelValue': (val) => { typeFilter.value = val; fetchExceptions(); },
                label: '筛选异常类型',
                density: 'compact',
                hideDetails: true,
                items: [
                  { title: '全部类型', value: '' },
                  { title: '多文件合并覆盖', value: 'merged_files' },
                  { title: '英文标题未中文化', value: 'english_title' },
                  { title: '未识别/TMDB缺失', value: 'unidentified' },
                  { title: '整理运行失败', value: 'failed_status' },
                  { title: '重复季集冲突', value: 'duplicate_episode' },
                  { title: '目标文件缺失/0字节', value: 'missing_dest' },
                  { title: '离群/格式异常集数', value: 'invalid_episode' }
                ]
              })
            ]),
            h('v-col', { cols: 12, sm: 4, md: 5 }, [
              h('v-text-field', {
                modelValue: keyword.value,
                'onUpdate:modelValue': (val) => { keyword.value = val; },
                onKeyupEnter: fetchExceptions,
                label: '搜索标题/路径关键字',
                density: 'compact',
                hideDetails: true,
                appendInnerIcon: 'mdi-magnify',
                'onClick:appendInner': fetchExceptions
              })
            ])
          ])
        ]),

        // 异常数据列表 Data Table Card
        h('v-card', {}, [
          h('v-table', { hover: true }, {
            default: () => [
              h('thead', {}, [
                h('tr', {}, [
                  h('th', { class: 'text-left', style: 'width: 140px;' }, '异常类型'),
                  h('th', { class: 'text-left' }, '标题 / 整理信息'),
                  h('th', { class: 'text-left' }, '源路径 src'),
                  h('th', { class: 'text-left' }, '目标路径 dest'),
                  h('th', { class: 'text-left' }, '异常原因明细'),
                  h('th', { class: 'text-center', style: 'width: 110px;' }, '操作')
                ])
              ]),
              h('tbody', {}, exceptions.value.length === 0
                ? [
                    h('tr', {}, [
                      h('td', { colspan: 6, class: 'text-center text-medium-emphasis py-6' },
                        loading.value ? '加载中...' : '暂无相关异常记录 🎉'
                      )
                    ])
                  ]
                : exceptions.value.map(item =>
                    h('tr', { key: item.key }, [
                      h('td', {}, [
                        h('v-chip', { size: 'small', color: getTypeColor(item.type), variant: 'tonal' }, () => item.type_name)
                      ]),
                      h('td', {}, [
                        h('div', { class: 'font-weight-medium' }, item.title || '未知'),
                        h('div', { class: 'text-caption text-medium-emphasis' }, item.date)
                      ]),
                      h('td', { class: 'text-caption text-truncate', style: 'max-width: 200px;' }, item.src || '-'),
                      h('td', { class: 'text-caption text-truncate', style: 'max-width: 200px;' }, item.dest || '-'),
                      h('td', { class: 'text-body-2 text-warning' }, item.detail || '-'),
                      h('td', { class: 'text-center' }, [
                        item.status === 'ignored'
                          ? h('v-btn', { size: 'x-small', variant: 'text', color: 'primary', onClick: () => ignoreItem(item.key) }, () => '取消忽略')
                          : h('v-btn', { size: 'x-small', variant: 'text', color: 'grey', onClick: () => ignoreItem(item.key) }, () => '忽略')
                      ])
                    ])
                  )
            ]
          })
        ]),

        // 定时分析配置弹窗 Cron Config Dialog
        h('v-dialog', {
          modelValue: showCronDialog.value,
          'onUpdate:modelValue': (val) => { showCronDialog.value = val; },
          maxWidth: '550px'
        }, () => [
          h('v-card', {}, [
            h('v-card-title', { class: 'text-h6 pa-4' }, '定时分析详细配置'),
            h('v-card-text', { class: 'pa-4' }, [
              h('v-switch', {
                modelValue: cronForm.value.cron_enabled,
                'onUpdate:modelValue': (val) => { cronForm.value.cron_enabled = val; },
                label: '开启后台定时自动分析',
                color: 'primary'
              }),
              h('v-select', {
                modelValue: cronForm.value.cron_mode,
                'onUpdate:modelValue': (val) => { cronForm.value.cron_mode = val; },
                label: '定时分析执行模式',
                class: 'mt-2',
                items: [
                  { title: '增量分析 (推荐，高速高效)', value: 'incremental' },
                  { title: '全量分析 (重新完整扫描)', value: 'full' }
                ]
              }),
              h('v-text-field', {
                modelValue: cronForm.value.cron,
                'onUpdate:modelValue': (val) => { cronForm.value.cron = val; },
                label: 'Cron 表达式',
                placeholder: '0 3 * * *',
                hint: '默认 0 3 * * * 代表每天凌晨 3:00 执行',
                persistentHint: true,
                class: 'mt-2'
              }),
              h('v-switch', {
                modelValue: cronForm.value.notify,
                'onUpdate:modelValue': (val) => { cronForm.value.notify = val; },
                label: '分析完成后发送 Telegram/系统通知报告',
                color: 'primary',
                class: 'mt-2'
              })
            ]),
            h('v-card-actions', { class: 'pa-4 pt-0' }, [
              h('v-spacer'),
              h('v-btn', { variant: 'text', onClick: () => { showCronDialog.value = false; } }, () => '取消'),
              h('v-btn', { color: 'primary', variant: 'elevated', onClick: saveCronConfig }, () => '保存生效')
            ])
          ])
        ])
      ]);
    };
  }
});

// Module Factory matching Vite / Module Federation contract:
// container.get(module) returns a Promise resolving to a factory function () => Promise<{ default: Component }>
const moduleFactory = () => Promise.resolve({ default: AppPage });

const moduleMap = {
  './AppPage': () => Promise.resolve(moduleFactory),
  './AppPageMain': () => Promise.resolve(moduleFactory),
  './AppPageOrganizeAnalyzer': () => Promise.resolve(moduleFactory),
  './AppPageStart': () => Promise.resolve(moduleFactory),
  './Page': () => Promise.resolve(moduleFactory),
  './Config': () => Promise.resolve(moduleFactory),
  './Dashboard': () => Promise.resolve(moduleFactory),
  'AppPage': () => Promise.resolve(moduleFactory),
  'Page': () => Promise.resolve(moduleFactory)
};

export const get = (module) => {
  const getter = moduleMap[module] || (() => Promise.resolve(moduleFactory));
  return getter();
};

export const init = (shareScope) => {
  return Promise.resolve();
};

export default {
  get,
  init
};
