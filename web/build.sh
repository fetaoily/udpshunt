#!/bin/sh
# Rebuilds the web SPA and installs it into the Go embed directory.
# Requires node >= 20 and npm.
set -e
cd "$(dirname "$0")"
npm ci
npm run build
rm -rf ../internal/webui/dist
mkdir -p ../internal/webui/dist
cp -r dist/* ../internal/webui/dist/
echo "installed $(find ../internal/webui/dist -type f | wc -l) files into internal/webui/dist"
