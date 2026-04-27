# Nifty50 Options Scalper — Trading Engine

**Automated Nifty50 options scalping system for Zerodha Kite Connect and Angel One SmartAPI.**

Built as described in the GeoBiz Blueprint (GBZ-Y4KD0O-2026).

---

## File Structure

```
nifty_scalper/
├── requirements.txt          # Python dependencies
├── .env.example              # Environment variables template
├── config.py                 # All config, risk params, subscriber profiles
├── broker_base.py            # Abstract broker interface
├── broker_zerodha.py         # Zerodha Kite Connect implementation
├── broker_angel.py           # Angel One SmartAPI implementation
├── strategy.py               # Core RSI + trend signal logic
├── backtest_engine.py        # Backtrader historical backtesting
├── live_main.py              # Multi-tenant live trading engine
├── telegram_reporter.py      # Automated P&L Telegram reports
├── database.py               # SQLite trade log
├── encrypt_config.py         # Subscriber config encryption utility
├── tests/
│   └── test_strategy.py      # Unit tests
└── .github/
    └── workflows/deploy.yml  # CI/CD pipeline for GCP
```

---

## Quick Start (Development)

### 1. Install Python dependencies
```bash
python -m pip install -r requirements.txt
```

### 2. Set up environment variables
```bash
cp .env.example .env
# Edit .env with your real API keys
```

### 3. Generate your master encryption key
```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
# Paste the output into .env as MASTER_ENCRYPTION_KEY
```

### 4. Create subscriber config
```bash
# Create data/ directory
mkdir -p data logs

# Add your first subscriber interactively
python encrypt_config.py add-subscriber
```

### 5. Run the backtest (with synthetic data first)
```bash
python backtest_engine.py
# Results saved to backtest_results/
```

### 6. Test Telegram notifications
```bash
python telegram_reporter.py --test
```

### 7. Run live engine (paper trading / beta)
```bash
python live_main.py
```

---

## GCP Server Setup (Production)

### One-time server setup
```bash
# SSH into your GCP VM
ssh user@YOUR_GCP_IP

# Install Python 3.11
sudo apt update && sudo apt install -y python3.11 python3-pip git

# Create app directory
mkdir -p /home/kiran/nifty_scalper/data /home/kiran/nifty_scalper/logs
```

### Create systemd service (so it restarts automatically)
```bash
sudo nano /etc/systemd/system/nifty-scalper.service
```

Paste:
```ini
[Unit]
Description=Nifty50 Options Scalper
After=network.target

[Service]
Type=simple
User=kiran
WorkingDirectory=/home/kiran/nifty_scalper
ExecStart=/usr/bin/python3 live_main.py
Restart=on-failure
RestartSec=30
StandardOutput=append:/home/kiran/nifty_scalper/logs/service.log
StandardError=append:/home/kiran/nifty_scalper/logs/service_error.log

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable nifty-scalper
sudo systemctl start nifty-scalper

# View status
sudo systemctl status nifty-scalper

# View live logs
tail -f /home/kiran/nifty_scalper/logs/live_trading_*.log
```

---

## Daily Zerodha Token Refresh

Zerodha access tokens expire daily. Automate the refresh:

1. Set up Zerodha's **redirect URL** in your Kite developer app to a URL you control.
2. Each morning at ~7:45 AM, visit the Zerodha login URL for each subscriber.
3. Extract the `request_token` from the redirect URL.
4. Call `broker.exchange_request_token(request_token)` to get the access token.
5. Update the encrypted subscriber config with `encrypt_config.py`.

For full automation, use `playwright` to headlessly complete the login flow.

---

## Risk Management Summary

| Parameter | Value |
|-----------|-------|
| Profit target per trade | +15% of premium |
| Stop loss per trade | -10% of premium |
| Daily circuit breaker | -3% of account |
| Max trades per day | 20 |
| VIX halt threshold | 25 |
| Max capital per trade | 10% of account |
| EOD position flatten | 15:20 IST |

---

## ⚠️ Legal Disclaimer

This software is for educational purposes only. Nothing in this codebase constitutes financial advice. Options trading involves substantial risk. Always consult a SEBI-registered advisor before deploying capital. The authors accept no liability for trading losses.
