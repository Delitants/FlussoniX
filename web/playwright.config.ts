import {defineConfig} from '@playwright/test';
export default defineConfig({testDir:'./tests',workers:1,use:{baseURL:process.env.FLUSSONIX_TEST_URL||'http://127.0.0.1:18210',headless:true},outputDir:'../test-results'});
