import { defineConfig } from '@playwright/test';
export default defineConfig({testDir:'./e2e',use:{baseURL:'http://127.0.0.1:3100'},projects:[{name:'phone',use:{viewport:{width:390,height:844}}},{name:'desktop',use:{viewport:{width:1440,height:900}}}],webServer:{command:'npm run dev -- --port 3100',url:'http://127.0.0.1:3100',reuseExistingServer:!process.env.CI}});
