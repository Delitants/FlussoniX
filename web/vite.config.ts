import {defineConfig} from 'vite';
export default defineConfig({base:'/admin/',server:{proxy:{'/streamer':'http://127.0.0.1:18210','/flussonix':'http://127.0.0.1:18210'}}});
