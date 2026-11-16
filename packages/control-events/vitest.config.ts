import { defineConfig } from 'vitest/config';
import path from 'path';

export default defineConfig({
  test: {
    root: path.resolve(import.meta.dirname),
    include: ['src/**/*.test.ts'],
    environment: 'node',
  },
});
