#!/bin/bash
# Setup Nginx as reverse proxy for certificate dashboard

echo "🔧 Setting up Nginx reverse proxy..."

# Install Nginx
echo "📦 Installing Nginx..."
sudo apt update
sudo apt install nginx -y

# Backup default config
sudo cp /etc/nginx/sites-available/default /etc/nginx/sites-available/default.backup

# Create new config
echo "📝 Creating Nginx configuration..."
sudo tee /etc/nginx/sites-available/certificate-dashboard > /dev/null <<'EOF'
server {
    listen 80;
    server_name _;

    # Security headers
    add_header X-Frame-Options "SAMEORIGIN" always;
    add_header X-Content-Type-Options "nosniff" always;
    add_header X-XSS-Protection "1; mode=block" always;

    # Max upload size
    client_max_body_size 10M;

    # Block bad requests
    if ($request_method !~ ^(GET|POST|HEAD|PUT|DELETE|OPTIONS)$ ) {
        return 444;
    }

    location / {
        proxy_pass http://127.0.0.1:5000;
        proxy_http_version 1.1;
        
        # WebSocket support
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        
        # Proxy headers
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        
        # Timeouts
        proxy_connect_timeout 60s;
        proxy_send_timeout 60s;
        proxy_read_timeout 60s;
    }

    # Block access to hidden files
    location ~ /\. {
        deny all;
        access_log off;
        log_not_found off;
    }
}
EOF

# Enable site
echo "✅ Enabling site..."
sudo ln -sf /etc/nginx/sites-available/certificate-dashboard /etc/nginx/sites-enabled/

# Remove default site
sudo rm -f /etc/nginx/sites-enabled/default

# Test configuration
echo "🧪 Testing Nginx configuration..."
sudo nginx -t

if [ $? -eq 0 ]; then
    echo "✅ Configuration is valid!"
    
    # Restart Nginx
    echo "🔄 Restarting Nginx..."
    sudo systemctl restart nginx
    sudo systemctl enable nginx
    
    echo ""
    echo "═══════════════════════════════════════════════"
    echo "✅ Nginx setup complete!"
    echo "═══════════════════════════════════════════════"
    echo ""
    echo "📋 Next steps:"
    echo "   1. Make sure app.py runs on 127.0.0.1:5000"
    echo "   2. Access your app via: http://YOUR_SERVER_IP"
    echo "   3. Nginx will handle all external traffic"
    echo ""
    echo "🛡️  Benefits:"
    echo "   ✓ Protection from malicious requests"
    echo "   ✓ Better performance"
    echo "   ✓ Easy SSL/HTTPS setup later"
    echo ""
else
    echo "❌ Configuration test failed!"
    echo "Please check the errors above."
fi
