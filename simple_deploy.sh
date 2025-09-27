#!/bin/bash
# Simple Secure S3 OData Server Setup for Portfolio Projects
# Essential security measures only - no overkill

set -e

echo "🔒 Setting up Simple Secure S3 OData Server..."

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Update system
print_status "Updating system packages..."
sudo apt-get update -y

# Install essential packages only
print_status "Installing essential dependencies..."
sudo apt-get install -y \
    python3 \
    python3-pip \
    nginx \
    ufw

# Install Python dependencies
print_status "Installing Python packages..."
pip3 install \
    fastapi \
    uvicorn \
    boto3 \
    pandas \
    python-dotenv \
    passlib[bcrypt]

# Create application directory
APP_DIR="/opt/s3-odata-server"
print_status "Creating application directory: $APP_DIR"
sudo mkdir -p $APP_DIR
sudo chown $USER:$USER $APP_DIR

# Copy application files
print_status "Copying application files..."
cp simple_secure_s3_odata_server.py $APP_DIR/
cp simple_env.example $APP_DIR/.env.example

# Update environment file for production (bind to all interfaces)
sed -i 's/ODATA_HOST=localhost/ODATA_HOST=ALL_INTERFACES/' $APP_DIR/.env.example

# Create simple systemd service
print_status "Creating systemd service..."
sudo tee /etc/systemd/system/s3-odata-server.service > /dev/null <<EOF
[Unit]
Description=S3 OData Server
After=network.target

[Service]
Type=simple
User=$USER
WorkingDirectory=$APP_DIR
Environment=PATH=/home/$USER/.local/bin:/usr/local/bin:/usr/bin:/bin
ExecStart=/home/$USER/.local/bin/python3 $APP_DIR/simple_secure_s3_odata_server.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

# Configure basic firewall
print_status "Configuring firewall..."
sudo ufw --force reset
sudo ufw default deny incoming
sudo ufw default allow outgoing
sudo ufw allow ssh
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp
sudo ufw --force enable

# Configure Nginx with basic security
print_status "Configuring Nginx..."
sudo tee /etc/nginx/sites-available/s3-odata-server > /dev/null <<EOF
server {
    listen 80;
    server_name _;
    
    # Basic security headers
    add_header X-Frame-Options "SAMEORIGIN" always;
    add_header X-Content-Type-Options "nosniff" always;
    add_header X-XSS-Protection "1; mode=block" always;
    
    # Hide nginx version
    server_tokens off;
    
    location / {
        proxy_pass http://localhost:8000;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
        
        # Basic timeouts
        proxy_connect_timeout 30s;
        proxy_send_timeout 30s;
        proxy_read_timeout 30s;
    }
    
    # Block common attack patterns
    location ~* \.(php|asp|aspx|jsp)$ {
        return 444;
    }
}
EOF

sudo ln -sf /etc/nginx/sites-available/s3-odata-server /etc/nginx/sites-enabled/
sudo rm -f /etc/nginx/sites-enabled/default
sudo nginx -t
sudo systemctl restart nginx
sudo systemctl enable nginx

# Copy Cloudflare setup script
print_status "Copying Cloudflare setup script..."
cp setup_cloudflare.sh $APP_DIR/
chmod +x $APP_DIR/setup_cloudflare.sh

# Create simple monitoring script
print_status "Creating monitoring script..."
tee $APP_DIR/check_status.sh > /dev/null <<EOF
#!/bin/bash
echo "📊 S3 OData Server Status"
echo "========================="

echo "Service Status:"
systemctl is-active s3-odata-server

echo ""
echo "Recent Logs:"
journalctl -u s3-odata-server --since "1 hour ago" --no-pager | tail -5

echo ""
echo "Network Connections:"
ss -tuln | grep :8000
EOF

chmod +x $APP_DIR/check_status.sh

print_status "Reloading systemd..."
sudo systemctl daemon-reload

print_status "✅ Simple Secure S3 OData Server setup complete!"
echo ""
echo "🔧 Next steps:"
echo "1. Edit $APP_DIR/.env with your configuration"
echo "2. Run: sudo systemctl start s3-odata-server"
echo "3. Run: sudo systemctl enable s3-odata-server"
echo "4. For HTTPS: Run $APP_DIR/setup_cloudflare.sh"
echo ""
echo "🔒 Essential security features enabled:"
echo "- Basic firewall (UFW)"
echo "- Input validation"
echo "- IP-based lockout protection"
echo "- CORS restrictions to Tableau Public only"
echo "- Cloudflare HTTPS ready"
echo ""
print_warning "Remember to:"
echo "1. Configure your .env file with actual values"
echo "2. Set up Cloudflare for HTTPS"
echo "3. Test the server before going live"
