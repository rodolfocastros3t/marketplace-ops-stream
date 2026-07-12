#!/usr/bin/env bash
# Gera o dashboard React e copia para backend/static (servido no Render e na API local).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT/frontend"
npm ci
npm run build
rm -rf "$ROOT/backend/static"
mkdir -p "$ROOT/backend/static"
cp -R dist/. "$ROOT/backend/static/"
echo "OK → backend/static (index + assets)"
