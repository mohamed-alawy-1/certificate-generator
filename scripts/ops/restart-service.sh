#!/bin/bash
# Script to restart certificate dashboard service

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK_DIR="${APP_DIR:-$(cd "$SCRIPT_DIR/../.." && pwd)}"
VENV_PYTHON="$WORK_DIR/venv/bin/python3"
VENV_GUNICORN="$WORK_DIR/venv/bin/gunicorn"
SERVICE_NAME="certificate-dashboard"

echo "🔍 Checking current service status..."
ps aux | grep -v grep | grep -E "gunicorn.*app:app|python.*app.py" || true

echo ""
if systemctl list-unit-files | grep -q "^${SERVICE_NAME}\.service"; then
	echo "🔄 Restarting systemd service: ${SERVICE_NAME}"
	sudo systemctl restart "${SERVICE_NAME}"
else
	echo "🛑 Stopping any running instances..."
	pkill -f 'python.*app.py|gunicorn.*app:app' || true
	sleep 2

	echo ""
	echo "🚀 Starting service with virtualenv Gunicorn..."
	cd "$WORK_DIR"
	if [ ! -f "$VENV_GUNICORN" ]; then
		echo "❌ Gunicorn not found at $VENV_GUNICORN"
		exit 1
	fi
	nohup "$VENV_GUNICORN" --worker-class geventwebsocket.gunicorn.workers.GeventWebSocketWorker --workers 1 --bind 127.0.0.1:5000 app:app > app.log 2>&1 &
fi

echo ""
echo "⏳ Waiting for service to start..."
sleep 3

echo ""
echo "✅ Service status:"
ps aux | grep -v grep | grep -E "gunicorn.*app:app|python.*app.py" || true

echo ""
echo "📊 Checking port 5000..."
netstat -tlnp 2>/dev/null | grep :5000 || lsof -i :5000 2>/dev/null

echo ""
echo "📝 Last 20 lines of log:"
tail -20 "$WORK_DIR/app.log"

echo ""
echo "✅ Done! Service should be running now."
echo "   Monitor logs with: tail -f app.log"
