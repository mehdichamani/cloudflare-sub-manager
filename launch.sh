#!/usr/bin/env bash
# ==============================================================================
# cloudflare-sub-manager Web Launcher
# Launches local web GUI & manager in browser
# ==============================================================================
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PORT=3000
URL="http://localhost:${PORT}"

echo -e "\033[0;34m==================================================\033[0m"
echo -e "\033[1;36m🚀 Launching Cloudflare Sub Manager & Web GUI...\033[0m"
echo -e "\033[0;34m==================================================\033[0m"

# Open URL in default browser after server boots up
(
  sleep 1.2
  if command -v xdg-open >/dev/null 2>&1; then
    xdg-open "$URL" >/dev/null 2>&1 || true
  elif command -v open >/dev/null 2>&1; then
    open "$URL" >/dev/null 2>&1 || true
  fi
) &

# Run GUI server
exec pnpm run gui
