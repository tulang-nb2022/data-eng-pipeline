#!/bin/bash
# Cloudflare Setup Script for S3 OData Server
# Provides HTTPS for Tableau Public OData connector

set -e

echo "☁️ Setting up Cloudflare for S3 OData Server..."

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Get EC2 public IP
EC2_PUBLIC_IP=$(curl -s http://checkip.amazonaws.com/)
print_status "EC2 Public IP: $EC2_PUBLIC_IP"

echo ""
echo "🔧 Cloudflare Setup Steps:"
echo "=========================="
echo ""
echo "1. 🌐 Get a Domain Name"
echo "   - Free options: Freenom (.tk, .ml, .ga), Namecheap"
echo "   - Or use a subdomain from existing domain"
echo ""
echo "2. ☁️ Add Domain to Cloudflare"
echo "   - Go to: https://dash.cloudflare.com"
echo "   - Click 'Add a Site'"
echo "   - Enter your domain name"
echo "   - Choose Free plan"
echo ""
echo "3. 🔗 Configure DNS Records"
echo "   - Add A record: your-domain.com → $EC2_PUBLIC_IP"
echo "   - Add CNAME record: www.your-domain.com → your-domain.com"
echo ""
echo "4. 🔒 Enable SSL/TLS"
echo "   - Go to SSL/TLS → Overview"
echo "   - Set encryption mode to 'Full (strict)'"
echo "   - Enable 'Always Use HTTPS'"
echo ""
echo "5. 🛡️ Configure Security Settings"
echo "   - Security Level: Medium"
echo "   - Bot Fight Mode: On"
echo "   - DDoS Protection: On"
echo ""
echo "6. ⚡ Performance Settings"
echo "   - Caching Level: Standard"
echo "   - Browser Cache TTL: 4 hours"
echo "   - Auto Minify: CSS, HTML, JavaScript"
echo ""

# Create Cloudflare configuration script
print_status "Creating Cloudflare configuration script..."

cat > setup_cloudflare.sh << 'EOF'
#!/bin/bash
# Cloudflare Configuration Helper

echo "☁️ Cloudflare Configuration Helper"
echo "=================================="
echo ""

read -p "Enter your domain name (e.g., your-domain.com): " DOMAIN_NAME

if [ -z "$DOMAIN_NAME" ]; then
    echo "Error: Domain name is required"
    exit 1
fi

echo ""
echo "📋 Configuration Summary:"
echo "Domain: $DOMAIN_NAME"
echo "EC2 IP: $EC2_PUBLIC_IP"
echo ""

echo "🔧 DNS Records to Add in Cloudflare:"
echo "====================================="
echo "Type: A"
echo "Name: @"
echo "Content: $EC2_PUBLIC_IP"
echo "TTL: Auto"
echo "Proxy: ✅ (Orange cloud)"
echo ""
echo "Type: CNAME"
echo "Name: www"
echo "Content: $DOMAIN_NAME"
echo "TTL: Auto"
echo "Proxy: ✅ (Orange cloud)"
echo ""

echo "🔒 SSL/TLS Settings:"
echo "===================="
echo "Encryption Mode: Full (strict)"
echo "Edge Certificates: Universal SSL"
echo "Always Use HTTPS: On"
echo "HTTP Strict Transport Security: On"
echo ""

echo "🛡️ Security Settings:"
echo "====================="
echo "Security Level: Medium"
echo "Bot Fight Mode: On"
echo "DDoS Protection: On"
echo "Web Application Firewall: On"
echo ""

echo "⚡ Performance Settings:"
echo "======================="
echo "Caching Level: Standard"
echo "Browser Cache TTL: 4 hours"
echo "Auto Minify: CSS, HTML, JavaScript"
echo "Brotli Compression: On"
echo ""

echo "✅ Configuration complete!"
echo ""
echo "🌐 Your OData server will be available at:"
echo "https://$DOMAIN_NAME"
echo ""
echo "📊 Tableau Public Connection:"
echo "1. Open Tableau Public"
echo "2. Connect to Data → More Servers → OData"
echo "3. URL: https://$DOMAIN_NAME"
echo "4. Enter your username and password"
echo ""

# Test connectivity
echo "🧪 Testing connectivity..."
echo "Testing HTTP (should redirect to HTTPS):"
curl -I "http://$DOMAIN_NAME/health" 2>/dev/null | head -5 || echo "Domain not yet active"

echo ""
echo "Testing HTTPS:"
curl -I "https://$DOMAIN_NAME/health" 2>/dev/null | head -5 || echo "HTTPS not yet active"

echo ""
echo "⏳ Note: DNS propagation may take 5-15 minutes"
echo "🔄 Run this script again to test connectivity"
EOF

chmod +x setup_cloudflare.sh

print_status "Created setup_cloudflare.sh script"

echo ""
echo "🚀 Quick Start:"
echo "==============="
echo "1. Get a domain name"
echo "2. Add domain to Cloudflare"
echo "3. Run: ./setup_cloudflare.sh"
echo "4. Configure DNS records as shown"
echo "5. Wait 5-15 minutes for DNS propagation"
echo "6. Test: curl https://your-domain.com/health"
echo ""

echo "📚 Cloudflare Benefits:"
echo "======================="
echo "✅ Free HTTPS/SSL certificates"
echo "✅ DDoS protection"
echo "✅ Global CDN (faster loading)"
echo "✅ Web Application Firewall"
echo "✅ Bot protection"
echo "✅ Always-on HTTPS"
echo "✅ DNS management"
echo ""

echo "🔍 Troubleshooting:"
echo "==================="
echo "• DNS not working: Wait 15 minutes, check DNS records"
echo "• SSL errors: Ensure 'Full (strict)' mode"
echo "• 502 errors: Check EC2 security group (port 80/443)"
echo "• Connection refused: Verify server is running"
echo ""

print_warning "Important Notes:"
echo "• Keep your EC2 instance running"
echo "• Ensure security group allows ports 80/443"
echo "• Cloudflare will proxy traffic to your EC2"
echo "• SSL certificates are automatically managed"
echo ""

echo "🎯 Next Steps:"
echo "=============="
echo "1. Run: ./setup_cloudflare.sh"
echo "2. Follow the configuration steps"
echo "3. Test your OData server"
echo "4. Connect from Tableau Public"
echo ""

print_status "Cloudflare setup guide complete!"
