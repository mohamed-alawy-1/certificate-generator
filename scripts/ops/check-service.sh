#!/bin/bash
# Script to check certificate dashboard service status

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK_DIR="${APP_DIR:-$(cd "$SCRIPT_DIR/../.." && pwd)}"

echo "═══════════════════════════════════════════════"
echo "📊 Certificate Dashboard Service Check"
echo "═══════════════════════════════════════════════"
echo ""

echo "1️⃣  Process Status:"
if ps aux | grep -v grep | grep -E "gunicorn.*app:app|python.*app.py" > /dev/null; then
    echo "   ✅ Service is RUNNING"
    ps aux | grep -v grep | grep -E "gunicorn.*app:app|python.*app.py"
else
    echo "   ❌ Service is NOT running"
fi

echo ""
echo "2️⃣  Port Status:"
if netstat -tlnp 2>/dev/null | grep :5000 > /dev/null || lsof -i :5000 2>/dev/null > /dev/null; then
    echo "   ✅ Port 5000 is OPEN"
    netstat -tlnp 2>/dev/null | grep :5000 || lsof -i :5000 2>/dev/null
else
    echo "   ❌ Port 5000 is NOT listening"
fi

echo ""
echo "3️⃣  Last 10 Log Lines:"
if [ -f "$WORK_DIR/app.log" ]; then
    tail -10 "$WORK_DIR/app.log"
else
    echo "   ⚠️  Log file not found"
fi

echo ""
echo "4️⃣  Disk Space:"
df -h | grep -E '(Filesystem|/$)'

echo ""
echo "5️⃣  Memory Usage:"
free -h

echo ""
echo "═══════════════════════════════════════════════"
