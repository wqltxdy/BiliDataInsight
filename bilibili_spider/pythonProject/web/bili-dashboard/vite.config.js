import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'

// https://vite.dev/config/
export default defineConfig({
  plugins: [vue()],
  server: {
    open: true, // 在服务器启动时自动在浏览器中打开应用程序
  },
})
