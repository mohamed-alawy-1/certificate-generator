#!/bin/bash
set -euo pipefail

REPO_URL="https://github.com/mohamed-alawy-1/certificate-generator.git"
APP_DIR="${APP_DIR:-$HOME/certificate-dashboard}"
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

chmod +x setup-systemd.sh restart-service.sh deploy.sh

echo "==> Applying service setup and restart..."
./setup-systemd.sh

echo "==> Deploy completed"
