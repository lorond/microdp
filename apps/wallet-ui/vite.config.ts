import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/api/clickstream": "http://localhost:8001",
      "/api/users": "http://localhost:8000"
    }
  }
});

