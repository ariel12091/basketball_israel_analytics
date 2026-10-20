import { defineConfig } from 'vitest/config';
import path from 'node:path';
export default defineConfig({resolve:{alias:{'server-only':path.resolve('test/server-only.ts'),'@':path.resolve('src')}},test:{include:['src/**/*.test.ts','test/**/*.test.ts'],exclude:['**/node_modules/**','**/*.int.test.ts']}});
