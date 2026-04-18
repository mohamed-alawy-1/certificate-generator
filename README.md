# Certificate Generator

<div align="center">

![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)
![Flask](https://img.shields.io/badge/Flask-2.0+-green.svg)
![License](https://img.shields.io/badge/License-MIT-yellow.svg)
![Status](https://img.shields.io/badge/Status-Production-success.svg)

**Advanced system for automatic PDF certificate generation from Google Docs/Slides templates with a professional web interface**

[Features](#features) • [Installation](#installation) • [Usage](#usage) • [Deployment](#deployment-on-server)

</div>

---

## Overview

A comprehensive and integrated system for automatically generating certificates from Google Docs or Google Slides templates, with full support for Arabic and English variables, and a modern web interface for operation management.

### Performance
- **High Speed**: Up to 50 certificates/minute per account
- **Parallel Processing**: Automatic distribution across multiple service accounts
- **Smart Retry**: Automatic retry for failed certificates

## Requirements

- Python 3.8+
- Google Service Accounts (5-10 accounts recommended for optimal performance)
- Google Drive Shared Folder

## Installation

### 1. Clone the Repository
```bash
git clone https://github.com/mohamed-alawy-1/certificate-generator.git
cd certificate-generator
```

### 2. Create Virtual Environment
```bash
python -m venv venv
source venv/bin/activate  # On Linux/Mac
# or
.\venv\Scripts\activate  # On Windows
```

### 3. Install Requirements
```bash
pip install -r requirements.txt
```

### 4. Add Service Accounts
- Create service accounts from Google Cloud Console
- Download JSON files and save them in the project folder
- Name them as: `service-account-1.json`, `service-account-2.json`, etc.
- See `config/examples/example-service-account.json` for the required format

### 5. Run the Application
```bash
python app.py
```

Open browser at: `http://localhost:5000`

## Deployment on Server

### Quick Start (Manual)

```bash
# Run in background
nohup python app.py > app.log 2>&1 &

# Stop the service
pkill -f 'python.*app.py'

# Check logs
tail -f app.log
```

### Professional Setup (Recommended)

The repository includes automated scripts for production deployment:

#### 1. **Check Service Status**
```bash
./scripts/ops/check-service.sh
```
Displays comprehensive service information:
- Process status (running/stopped)
- Port status (5000)
- Recent log entries
- Disk space and memory usage

#### 2. **Restart Service**
```bash
./scripts/ops/restart-service.sh
```
Safely restarts the application service.

#### 3. Setup Systemd Service (Recommended)
```bash
./scripts/ops/setup-systemd.sh
```
Creates a systemd service with:
- Auto-start on server reboot
- Auto-restart on crashes
- Centralized logging
- Better process management

**Useful systemd commands:**
```bash
sudo systemctl status certificate-dashboard    # Check status
sudo systemctl restart certificate-dashboard   # Restart
sudo systemctl stop certificate-dashboard      # Stop
sudo systemctl start certificate-dashboard     # Start
sudo journalctl -u certificate-dashboard -f    # View logs
```

#### 4. Setup Nginx Reverse Proxy (Security)
```bash
./scripts/ops/setup-nginx.sh
```
Installs and configures Nginx as a reverse proxy:
- Protection from malicious requests
- Better performance and caching
- Easy SSL/HTTPS setup
- Security headers
- WebSocket support

After setup, access via: `http://YOUR_SERVER_IP` (port 80)

### Deployment Steps Summary

```bash
# 1. Clone and setup
git clone https://github.com/mohamed-alawy-1/certificate-generator.git
cd certificate-generator
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 2. Add service accounts (*.json files)

# 3. Make scripts executable
chmod +x scripts/ops/*.sh

# 4. Setup systemd service (auto-start & auto-restart)
./scripts/ops/setup-systemd.sh

# 5. Setup Nginx (security & performance)
./scripts/ops/setup-nginx.sh

# Done! Service is now production-ready
```

### Server Requirements
- **OS**: Ubuntu 20.04+ / Debian 11+
- **RAM**: 512 MB minimum, 1 GB recommended
- **Disk**: 1 GB free space
- **Python**: 3.8+
- **Access**: sudo privileges for systemd/nginx setup

## Usage

### Web Interface

1. **Select Template**
   - Choose a Google Docs or Google Slides template
   - Template must contain variables in format `<<VARIABLE>>`
   - Example: `<<Name>>`, `<<Date>>`, `<<Course Name>>`

2. **Select Folder**
   - Choose Google Drive folder to save final certificates (PDF)

3. **Select Google Sheet**
   - Choose a spreadsheet containing trainee data
   - System will automatically detect:
     - Name column (searches for "name" or "اسم")
     - Link column (creates "Certificate Link" column if not found)

4. **Start Generation**
   - Click "Start Generation"
   - System will process all names without links
   - Track progress through the interface

### Auto-Watch Mode
- Enable "Auto-Watch" to automatically process new names
- Checks spreadsheet every 30 seconds (customizable)
- Perfect for live events

## Features

### High Performance
- **Parallel Processing**: Automatic distribution across multiple service accounts
- **Speed**: Up to 50 certificates/minute per account
- **Smart Retry**: Automatic retry for failed certificates

### Automatic Name Cleaning
System automatically removes titles and prefixes:
- **Arabic**: استاذ، دكتور، مهندس، محامي، الأستاذ، الدكتور, etc.
- **English**: Dr, Eng, Mr, Mrs, Prof, Captain, etc.
- **Customizable**: Add or remove words from the list
- **Note (Dec 2025)**: Fixed handling of feminine/alternate suffixes for titles (e.g., removing `شيخة`/`شيخه` without leaving a stray `ه` or `ة`).

### Comprehensive Template Support
- **Google Docs**: Text documents
- **Google Slides**: Presentations
- **Auto-Detection**: Automatically recognizes template type

### Advanced Variable Support
- **Variable Format**: `<<VARIABLE>>`
- **Arabic Support**: `<<الاسم>>`, `<<التاريخ>>`
- **English Support**: `<<NAME>>`, `<<DATE>>`
- **Space Support**: `<<اسم المتدرب>>`, `<<Course Name>>`

### Monitoring and Tracking
- **Live Dashboard**: Real-time progress tracking
- **Detailed Logs**: Track all operations
- **Statistics**: Total, Completed, Failed, Speed

## Project Structure

```
certificate-generator/
├── app.py                                  # Main Flask + SocketIO server
├── templates/
│   └── index.html                          # User interface (SPA)
├── config/
│   ├── examples/
│   │   └── example-service-account.json    # Service account example
│   └── service-accounts/                   # Local service accounts (ignored)
├── scripts/
│   ├── check_clean.py                      # Utility check script
│   └── ops/
│       ├── deploy.sh                       # CI/CD deploy script
│       ├── check-service.sh                # Check service status
│       ├── restart-service.sh              # Restart service
│       ├── setup-systemd.sh                # Setup systemd service
│       └── setup-nginx.sh                  # Setup Nginx reverse proxy
├── requirements.txt                        # Dependencies
├── .github/workflows/deploy.yml            # GitHub Actions deployment
├── .gitignore                              # Files excluded from Git
└── README.md                               # This file
```

## Security and Privacy

- **Protected Service Files**: `.gitignore` prevents uploading service accounts
- **Limited Permissions**: Service accounts only have Drive permissions
- **No Local Storage**: No sensitive data saved locally
- **HTTPS Ready**: Can run behind Nginx with SSL

## Troubleshooting

### Service Accounts Not Working?
```bash
# Verify files are loaded correctly
ls -la config/service-accounts/*.json

# Check file permissions
chmod 600 config/service-accounts/*.json
```

### Slow Generation?
- Add more service accounts (5-10 recommended)
- Check Google API limits for accounts

### Permission Errors?
- Ensure template and folders are shared with all service accounts
- Check read/write permissions

### Service Not Working?
```bash
# Quick check with automated script
./scripts/ops/check-service.sh

# Or manual check
tail -50 app.log
ps aux | grep python

# Restart service
./scripts/ops/restart-service.sh

# If using systemd
sudo systemctl restart certificate-dashboard
sudo systemctl status certificate-dashboard
```

### Port Already in Use?
```bash
# Find process using port 5000
sudo lsof -i :5000
# or
sudo netstat -tlnp | grep :5000

# Kill the process
sudo kill -9 <PID>
```

### Nginx Issues?
```bash
# Test configuration
sudo nginx -t

# Check status
sudo systemctl status nginx

# Restart
sudo systemctl restart nginx

# View error logs
sudo tail -f /var/log/nginx/error.log
```

## Use Cases

- **Training Course Certificates**: Generate thousands of certificates for trainees
- **Recognition Certificates**: Thank you and appreciation certificates for employees
- **Graduation Certificates**: University or school certificates
- **Bulk Documents**: Any document requiring individual data customization

## Contributing

Contributions are welcome! Open an Issue or Pull Request.

## License

MIT License - Use freely in personal and commercial projects

## ⭐ Client Reviews

### 5/5 Stars Rating on Mostaql

> **Excellent experience with a professional developer**
> 
> Dealt with complete professionalism and flexibility. Engineer Mohamed executed the project with high precision and was responsive to all modifications and additional requirements.
> 
> **Highlights:**
> - Precise requirements implementation
> - Fast and continuous communication
> - High code quality
> - Post-delivery support
> - Flexibility in modifications
> 
> **Result:** A complete system working with high efficiency that saved us hours of manual work.
> 
> Highly recommend working with him! 👍
> 
> — **Client from Mostaql Platform** | [Review Link](https://mostaql.com/u/mohamed_alawy/reviews/9436518)

---

<div align="center">

**Made with ❤️ in Egypt**

If you like this project, don't forget to give it a ⭐

[Contact me on Mostaql](https://mostaql.com/u/mohamed_alawy) • [GitHub](https://github.com/mohamed-alawy)

</div>
