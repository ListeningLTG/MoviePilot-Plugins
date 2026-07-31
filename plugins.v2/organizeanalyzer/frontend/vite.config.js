import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import federation from '@originjs/vite-plugin-federation'

export default defineConfig({
  plugins: [
    vue(),
    federation({
      name: 'organizeanalyzer',
      filename: 'remoteEntry.js',
      exposes: {
        './AppPage': './src/AppPage.vue',
        './Page': './src/Page.vue',
        './Config': './src/Config.vue',
        './Dashboard': './src/AppPage.vue'
      },
      shared: {
        vue: { requiredVersion: false, generate: false },
        vuetify: { requiredVersion: false, generate: false, singleton: true },
        'vuetify/styles': { requiredVersion: false, generate: false, singleton: true }
      },
      format: 'esm'
    })
  ],
  build: {
    target: 'esnext',
    minify: false,
    cssCodeSplit: true,
    outDir: 'dist',
    rollupOptions: {
      output: {
        format: 'esm'
      }
    }
  }
})
