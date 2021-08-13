import { defineConfig } from 'umi';

export default defineConfig({
  title: '蜻蜓-文件分发',
  nodeModulesTransform: {
    type: 'none',
  },
  routes: [
    { exact: false, path: '/', component: '@/layouts/index',
      routes: [
        { exact: true, path: '/', component: '@/pages/index' },
        { exact: true, path: '/config', component: '@/pages/config' },
      ],
    },
  ],
  fastRefresh: {},
});
