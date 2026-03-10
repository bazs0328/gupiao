import react from "@vitejs/plugin-react";
import path from "node:path";
import { defineConfig } from "vite";

export default defineConfig({
  root: path.resolve(__dirname, "src/frontend"),
  plugins: [react()],
  build: {
    outDir: path.resolve(__dirname, ".build/frontend"),
    emptyOutDir: true
  }
});
