import { defineConfig } from "vitest/config";
import path from "node:path";

export default defineConfig({
  resolve: {
    alias: {
      "@relaymesh/relaybus-core": path.resolve(
        __dirname,
        "sdk/core/typescript/src/index.ts"
      )
    }
  }
});
