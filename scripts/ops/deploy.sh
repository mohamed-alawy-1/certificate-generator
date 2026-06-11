#!/bin/bash
set -euo pipefail

REPO_URL="https://github.com/mohamed-alawy-1/certificate-generator.git"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_APP_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
APP_DIR="${APP_DIR:-$DEFAULT_APP_DIR}"
BRANCH="${BRANCH:-main}"

echo "==> Deploying ${BRANCH} to ${APP_DIR}"

if ! command -v git >/dev/null 2>&1; then
    echo "==> Installing git..."
    sudo apt-get update
    sudo apt-get install -y git
fi

if [ ! -d "$APP_DIR/.git" ]; then
    echo "==> Cloning repository..."
    rm -rf "$APP_DIR"
    git clone "$REPO_URL" "$APP_DIR"
fi

cd "$APP_DIR"

echo "==> Pulling latest changes..."
git fetch origin "$BRANCH"
git checkout "$BRANCH" || git checkout -b "$BRANCH"
git pull --ff-only origin "$BRANCH"

chmod +x scripts/ops/*.sh

echo "==> Rebuilding and restarting Docker container..."
docker compose build app
docker compose up -d --force-recreate app

echo "==> Waiting for container to start..."
sleep 8
docker compose ps

echo "==> Deploy completed"
