import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    root: './services/bootstrap-server',
    globals: false,
    environment: 'node',
    include: ['src/**/*.test.ts', 'src/**/__tests__/**/*.test.ts'],
  },
});
