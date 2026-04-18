#!/bin/bash
# Setup systemd service for certificate dashboard

set -e

echo "🔧 Setting up systemd service..."

# Get current user and paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CURRENT_USER=$(whoami)
WORK_DIR="${APP_DIR:-$(cd "$SCRIPT_DIR/../.." && pwd)}"
VENV_PYTHON="$WORK_DIR/venv/bin/python3"
VENV_PIP="$WORK_DIR/venv/bin/pip"
VENV_GUNICORN="$WORK_DIR/venv/bin/gunicorn"
LOGROTATE_FILE="/etc/logrotate.d/certificate-dashboard"

# Check if virtual environment exists
if [ ! -f "$VENV_PYTHON" ]; then
    echo "⚠️  Virtual environment not found at $VENV_PYTHON"
    echo "   Creating virtual environment..."
    python3 -m venv "$WORK_DIR/venv"
fi

echo "📦 Installing/updating dependencies..."
"$VENV_PIP" install -r "$WORK_DIR/requirements.txt"

if ! command -v logrotate >/dev/null 2>&1; then
    echo "📦 Installing logrotate..."
    sudo apt-get update
    sudo apt-get install -y logrotate
fi

# Create systemd service file
echo "📝 Creating systemd service..."
sudo tee /etc/systemd/system/certificate-dashboard.service > /dev/null <<EOF
[Unit]
Description=Certificate Dashboard Service
After=network.target

[Service]
Type=simple
User=$CURRENT_USER
Group=$CURRENT_USER
WorkingDirectory=$WORK_DIR
Environment="PATH=$WORK_DIR/venv/bin"
Environment="SERVICE_ACCOUNTS_DIR=$WORK_DIR/config/service-accounts"

# Run the application
ExecStart=$VENV_GUNICORN --worker-class geventwebsocket.gunicorn.workers.GeventWebSocketWorker --workers 1 --bind 127.0.0.1:5000 app:app

# Restart policy
Restart=always
RestartSec=10

# Logging
StandardOutput=append:$WORK_DIR/app.log
StandardError=append:$WORK_DIR/app.log

[Install]
WantedBy=multi-user.target
EOF

# Configure log rotation (trim app.log automatically)
echo "🧹 Configuring logrotate..."
sudo tee "$LOGROTATE_FILE" > /dev/null <<EOF
$WORK_DIR/app.log {
    daily
    rotate 14
    minsize 20M
    missingok
    notifempty
    compress
    delaycompress
    copytruncate
    create 0644 $CURRENT_USER $CURRENT_USER
}
EOF

# Stop any running instances
echo "🛑 Stopping any running instances..."
pkill -f 'python.*app.py|gunicorn.*app:app' 2>/dev/null || true
sleep 2

# Reload systemd
echo "🔄 Reloading systemd..."
sudo systemctl daemon-reload

# Enable and start service
echo "🚀 Enabling and starting service..."
sudo systemctl enable certificate-dashboard.service
sudo systemctl start certificate-dashboard.service

# Wait until service stabilizes (avoids transient auto-restart race in CI)
echo "⏳ Waiting for service to stabilize..."
for _ in $(seq 1 15); do
    STATE="$(sudo systemctl is-active certificate-dashboard.service || true)"
    if [ "$STATE" = "active" ]; then
        break
    fi
    sleep 2
done

# Check status
echo ""
echo "═══════════════════════════════════════════════"
echo "📊 Service Status:"
echo "═══════════════════════════════════════════════"

FINAL_STATE="$(sudo systemctl is-active certificate-dashboard.service || true)"
if [ "$FINAL_STATE" != "active" ]; then
    echo "❌ Service is not active (state: $FINAL_STATE)"
    sudo systemctl status certificate-dashboard.service --no-pager || true
    echo ""
    echo "🧾 Last logs:"
    sudo journalctl -u certificate-dashboard.service -n 80 --no-pager || true
    exit 1
fi

sudo systemctl status certificate-dashboard.service --no-pager || true

echo ""
echo "═══════════════════════════════════════════════"
echo "✅ Systemd service setup complete!"
echo "═══════════════════════════════════════════════"
echo ""
echo "📋 Useful commands:"
echo "   sudo systemctl status certificate-dashboard   # Check status"
echo "   sudo systemctl restart certificate-dashboard  # Restart service"
echo "   sudo systemctl stop certificate-dashboard     # Stop service"
echo "   sudo systemctl start certificate-dashboard    # Start service"
echo "   sudo journalctl -u certificate-dashboard -f   # View live logs"
echo "   tail -f $WORK_DIR/app.log                     # View app logs"
echo ""
echo "🎯 Benefits:"
echo "   ✓ Auto-start on server reboot"
echo "   ✓ Auto-restart on crashes"
echo "   ✓ Better process management"
echo "   ✓ Centralized logging"
echo ""
