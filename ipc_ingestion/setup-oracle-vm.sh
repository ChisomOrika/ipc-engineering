#!/bin/bash
# =============================================================================
# IPC Analytics — Oracle Cloud VM Setup
# Run this ONCE after SSH-ing into your new Oracle Cloud VM.
# =============================================================================

set -e  # Stop on any error

echo "========================================="
echo "  IPC Analytics — VM Setup"
echo "========================================="

# 1. System updates & Python
echo ""
echo ">>> Step 1: Installing system packages..."
sudo apt update && sudo apt upgrade -y
sudo apt install -y python3.12 python3.12-venv python3-pip git

# 2. Clone the repo
echo ""
echo ">>> Step 2: Cloning repository..."
cd ~
if [ -d "ipc_analytics" ]; then
    echo "Repo already exists, pulling latest..."
    cd ipc_analytics && git pull
else
    echo "Enter your GitHub repo URL (e.g. https://github.com/youruser/ipc_analytics.git):"
    read REPO_URL
    git clone "$REPO_URL" ipc_analytics
    cd ipc_analytics
fi

# 3. Python virtual environment
echo ""
echo ">>> Step 3: Setting up Python environment..."
python3.12 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r ipc_ingestion/requirements.txt

# 4. Environment variables
echo ""
echo ">>> Step 4: Setting up .env file..."
if [ ! -f .env ]; then
    cat > .env << 'ENVTEMPLATE'
# PostgreSQL (DigitalOcean)
PG_HOST=postgres-db-do-user-14235175-0.c.db.ondigitalocean.com
PG_PORT=25060
PG_USER=doadmin
PG_PASSWORD=YOUR_PG_PASSWORD_HERE

# MongoDB
DASH_URL=YOUR_DASH_MONGO_URI_HERE
GOSOURCE_URL=YOUR_GOSOURCE_MONGO_URI_HERE

# Payment APIs
PAYSTACK_SECRET_KEY=YOUR_PAYSTACK_KEY_HERE
LENCO_API_TOKEN=YOUR_LENCO_TOKEN_HERE
9JAPAY_API_KEY=YOUR_9JAPAY_KEY_HERE
9JAPAY_SECRET=YOUR_9JAPAY_SECRET_HERE
ENVTEMPLATE
    echo ""
    echo ">>> .env file created at ~/ipc_analytics/.env"
    echo ">>> EDIT IT NOW with your real credentials:"
    echo ">>>   nano ~/ipc_analytics/.env"
    echo ""
    echo "Press ENTER after you've edited the .env file..."
    read
else
    echo ".env already exists, skipping."
fi

# 5. Quick test — verify connections
echo ""
echo ">>> Step 5: Testing connections..."
source venv/bin/activate
python3 -c "
from dotenv import load_dotenv; load_dotenv()
import os, psycopg2
try:
    conn = psycopg2.connect(host=os.getenv('PG_HOST'), port=os.getenv('PG_PORT'),
                            dbname='PROD_ANALYTICS_DB', user=os.getenv('PG_USER'),
                            password=os.getenv('PG_PASSWORD'))
    conn.cursor().execute('SELECT 1')
    print('  ✅ PostgreSQL: connected')
    conn.close()
except Exception as e:
    print(f'  ❌ PostgreSQL: {e}')

from pymongo import MongoClient
for name, key in [('DAASH MongoDB', 'DASH_URL'), ('GoSource MongoDB', 'GOSOURCE_URL')]:
    try:
        uri = os.getenv(key)
        client = MongoClient(uri, serverSelectionTimeoutMS=10000, tls=True, tlsAllowInvalidCertificates=True)
        client.admin.command('ping')
        print(f'  ✅ {name}: connected')
        client.close()
    except Exception as e:
        print(f'  ❌ {name}: {e}')
        print(f'     → Make sure this VM IP is whitelisted in DigitalOcean MongoDB trusted sources!')
"

# 6. Install systemd service (runs scheduler on boot, restarts on crash)
echo ""
echo ">>> Step 6: Installing systemd service..."
sudo tee /etc/systemd/system/ipc-ingestion.service > /dev/null << EOF
[Unit]
Description=IPC Analytics Ingestion Scheduler
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$USER
WorkingDirectory=$HOME/ipc_analytics
ExecStart=$HOME/ipc_analytics/venv/bin/python ipc_ingestion/scheduler.py
Restart=always
RestartSec=30
StandardOutput=append:$HOME/ipc_analytics/scheduler.log
StandardError=append:$HOME/ipc_analytics/scheduler.log

# Load .env file
EnvironmentFile=$HOME/ipc_analytics/.env

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable ipc-ingestion
sudo systemctl start ipc-ingestion

echo ""
echo ">>> Checking service status..."
sleep 3
sudo systemctl status ipc-ingestion --no-pager

echo ""
echo "========================================="
echo "  ✅ Setup complete!"
echo "========================================="
echo ""
echo "  Scheduler runs at 07:00, 14:00, 19:00 WAT"
echo ""
echo "  Useful commands:"
echo "    sudo systemctl status ipc-ingestion   # Check status"
echo "    sudo systemctl restart ipc-ingestion   # Restart"
echo "    tail -f ~/ipc_analytics/scheduler.log  # Watch logs"
echo "    sudo systemctl stop ipc-ingestion      # Stop"
echo ""
