# Simple Secure S3 OData Server - Portfolio Project Guide

## 🎯 **Streamlined Security for Portfolio Projects**

This is a simplified version focused on **essential security measures** to prevent reverse shells and unauthorized access, without the enterprise overkill.

## 🔒 **Core Security Features (Anti-Reverse Shell Focus)**

### **1. Input Validation & Injection Prevention**
- **Strict regex patterns** for all user inputs
- **File name sanitization** prevents path traversal
- **Column name validation** prevents SQL injection
- **Row limits** (50,000 max) prevent memory exhaustion

### **2. Authentication Security**
- **IP-based lockout** after 5 failed attempts (5-minute lockout)
- **Strong password hashing** with bcrypt
- **Security event logging** for monitoring

### **3. Network Security**
- **CORS restrictions** to Tableau Public only
- **Basic firewall** (UFW) with minimal open ports
- **HTTPS/SSL** support for encrypted communication

### **4. Process Security**
- **Disabled API docs** (no `/docs` endpoint)
- **Limited error information** (no internal details exposed)
- **Request size limits** and timeouts

## 🚀 **Quick Setup (5 Minutes)**

### **1. Upload Files**
```bash
scp simple_secure_s3_odata_server.py simple_deploy.sh simple_env.example user@your-ec2-ip:/home/user/
```

### **2. Run Setup**
```bash
chmod +x simple_deploy.sh
./simple_deploy.sh
```

### **3. Configure**
```bash
cd /opt/s3-odata-server
cp .env.example .env
nano .env  # Add your S3 bucket, username, password
```

### **4. Start Service**
```bash
sudo systemctl start s3-odata-server
sudo systemctl enable s3-odata-server
```

### **5. Add SSL (Optional but Recommended)**
```bash
./setup_ssl.sh
# Enter your domain name when prompted
```

## 📋 **Minimal Configuration**

```bash
# .env file - only essential settings
S3_BUCKET=your-portfolio-bucket
ODATA_USERNAME=tableau_user
ODATA_PASSWORD=your-secure-password-123
ODATA_HOST=localhost  # Use ALL_INTERFACES for production to bind to all interfaces
ODATA_PORT=8000
```

**Note:** For production deployment, change `ODATA_HOST=localhost` to `ODATA_HOST=ALL_INTERFACES` to allow external connections.

## 🔗 **Tableau Public Connection**

1. Open Tableau Public
2. Connect to Data → More Servers → OData
3. URL: `https://your-domain.com` (or `http://your-ip:8000` for testing)
4. Username/Password: Your configured credentials

### **For Partitioned Data (Hive-style partitions)**

If your S3 data is partitioned like:
```
s3://your-bucket/gold/weather/processed/year=2025/month=9/day=19/data_0.parquet
```

The server will:
- **Automatically detect** partitioned data
- **Group partitions** by dataset name (e.g., `weather_processed_partitioned`)
- **Combine all partitions** into a single dataset
- **Add partition columns** (`year`, `month`, `day`) to your data
- **Show partition info** via `/partitions/{dataset_name}` endpoint

### **Available Endpoints**

- `/files` - List all available datasets (partitioned and non-partitioned)
- `/data/{file_name}` - Get data from a specific dataset
- `/partitions/{dataset_name}` - Get partition information for a dataset
- `/health` - Health check

## 🛡️ **Security Measures Explained**

### **Prevents Reverse Shells By:**
- **No command execution** - Only reads S3 data
- **No file uploads** - Read-only access
- **No shell access** - Pure API endpoints
- **Input validation** - Blocks malicious payloads
- **Process isolation** - Runs as non-root user

### **Prevents Unauthorized Access By:**
- **Authentication required** for all endpoints
- **IP lockout** after failed attempts
- **CORS restrictions** to Tableau Public only
- **Firewall** blocks unauthorized ports

### **Prevents Data Exposure By:**
- **Row limits** prevent large data dumps
- **Error sanitization** hides internal details
- **No debug endpoints** in production
- **HTTPS encryption** for data in transit

## 📊 **Monitoring**

### **Check Status**
```bash
./check_status.sh
```

### **View Logs**
```bash
journalctl -u s3-odata-server -f
```

### **Test Security**
```bash
# Test authentication
curl -u username:password https://your-domain.com/files

# Test invalid input (should be blocked)
curl -u username:password https://your-domain.com/data/../../../etc/passwd

# Test partitioned data
curl -u username:password https://your-domain.com/files
curl -u username:password https://your-domain.com/data/weather_processed_partitioned
curl -u username:password https://your-domain.com/partitions/weather_processed
```

## ⚠️ **Security Checklist**

- [ ] Strong password set (12+ characters)
- [ ] SSL certificate configured
- [ ] Firewall enabled (ports 22, 80, 443 only)
- [ ] Service running as non-root user
- [ ] Input validation working (test with malicious inputs)
- [ ] IP lockout working (test with wrong password 5 times)
- [ ] CORS restrictions active (test from non-Tableau origin)

## 🔧 **Troubleshooting**

### **Service Won't Start**
```bash
sudo systemctl status s3-odata-server
journalctl -u s3-odata-server --no-pager
```

### **Can't Connect from Tableau**
- Check firewall: `sudo ufw status`
- Check SSL: `curl -I https://your-domain.com/health`
- Check logs: `journalctl -u s3-odata-server | tail -20`

### **Authentication Issues**
- Verify credentials in `.env` file
- Check for IP lockout: `journalctl -u s3-odata-server | grep "SECURITY_EVENT"`

## 🎯 **What This Protects Against**

✅ **Reverse Shells** - No command execution capabilities  
✅ **SQL Injection** - Input validation prevents malicious queries  
✅ **Path Traversal** - File name validation blocks `../` attacks  
✅ **Brute Force** - IP lockout after 5 failed attempts  
✅ **Data Dumps** - Row limits prevent large data extraction  
✅ **Unauthorized Access** - Authentication required for all endpoints  
✅ **Man-in-the-Middle** - HTTPS encryption for data in transit  

## 🚫 **What This Doesn't Include (Enterprise Features)**

❌ Redis-based rate limiting (uses simple in-memory tracking)  
❌ Fail2ban intrusion prevention (basic firewall only)  
❌ Complex monitoring (simple status check only)  
❌ Automated backups (manual process)  
❌ Multi-user support (single username/password)  
❌ Advanced logging (basic security events only)  

## 💡 **Perfect for Portfolio Projects**

This streamlined version gives you:
- **Essential security** without complexity
- **Quick setup** (5 minutes)
- **Minimal maintenance** 
- **Professional appearance** for Tableau Public
- **Security confidence** for public-facing deployment

The focus is on **preventing the most common attacks** (reverse shells, injection, unauthorized access) while keeping the solution **simple and maintainable** for portfolio purposes.
