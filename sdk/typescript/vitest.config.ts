import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["tests/**/*.test.ts"],
    testTimeout: 30_000,
    hookTimeout: 320_000,
    // Boot one broker per file and share it across tests in that file.
    fileParallelism: false,
  },
});
