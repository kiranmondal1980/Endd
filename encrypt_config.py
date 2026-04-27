"""
encrypt_config.py — Subscriber Config Encryption Utility
==========================================================
Use this utility to encrypt your subscribers.json into
the encrypted subscribers.enc.json that live_main.py reads.

USAGE:
  # First time: encrypt your plain config
  python encrypt_config.py encrypt --input data/subscribers.json

  # To view decrypted content (for debugging)
  python encrypt_config.py decrypt --output data/subscribers_decrypted.json

  # Add a single new subscriber interactively
  python encrypt_config.py add-subscriber

IMPORTANT:
  - Run this on your LOCAL machine, not the server.
  - Upload ONLY the .enc.json file to the server.
  - NEVER upload subscribers.json to GitHub or the server.

SAMPLE subscribers.json structure:
[
  {
    "subscriber_id": "SUB_001",
    "name": "Subhajit Das",
    "broker": "zerodha",
    "active": true,
    "zerodha_api_key": "abc123",
    "zerodha_api_secret": "secret_key_here",
    "zerodha_access_token": "",
    "max_lots": 1,
    "max_daily_drawdown_pct": 3.0,
    "telegram_chat_id": "123456789"
  }
]
"""

import json
import argparse
import getpass
from pathlib import Path
from cryptography.fernet import Fernet
from config import MASTER_ENCRYPTION_KEY, SUBSCRIBER_CONFIG_PATH


def get_fernet() -> Fernet:
    if not MASTER_ENCRYPTION_KEY:
        print("❌ MASTER_ENCRYPTION_KEY not set in .env")
        print("   Generate one with:")
        print("   python -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\"")
        exit(1)
    return Fernet(MASTER_ENCRYPTION_KEY)


def encrypt_file(input_path: str):
    """Encrypt a plain JSON subscriber config file."""
    f = get_fernet()
    data = Path(input_path).read_bytes()
    # Validate JSON first
    json.loads(data)
    encrypted = f.encrypt(data)
    output_path = SUBSCRIBER_CONFIG_PATH
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(encrypted)
    print(f"✅ Encrypted config saved to: {output_path}")
    print(f"   Source: {input_path}")
    print(f"   ⚠️  Delete {input_path} after verifying the encrypted file works.")


def decrypt_file(output_path: str):
    """Decrypt subscriber config to a plain JSON file (for debugging)."""
    f = get_fernet()
    encrypted = SUBSCRIBER_CONFIG_PATH.read_bytes()
    decrypted  = f.decrypt(encrypted)
    data = json.loads(decrypted)
    Path(output_path).write_text(json.dumps(data, indent=2))
    print(f"✅ Decrypted config saved to: {output_path}")
    print(f"   ⚠️  Delete {output_path} after use!")


def add_subscriber_interactive():
    """Interactively add a new subscriber to the encrypted config."""
    f = get_fernet()

    # Load existing config
    if SUBSCRIBER_CONFIG_PATH.exists():
        existing_data = json.loads(f.decrypt(SUBSCRIBER_CONFIG_PATH.read_bytes()))
    else:
        existing_data = []

    print("\n=== Add New Subscriber ===\n")
    sub = {
        "subscriber_id": input("Subscriber ID (e.g. SUB_005): ").strip(),
        "name":          input("Full Name: ").strip(),
        "broker":        input("Broker (zerodha/angel): ").strip().lower(),
        "active":        True,
        "max_lots":      int(input("Max lots per trade [1]: ").strip() or "1"),
        "max_daily_drawdown_pct": float(
            input("Max daily drawdown % [3.0]: ").strip() or "3.0"
        ),
        "telegram_chat_id": input("Telegram chat_id (or blank): ").strip() or None,
    }

    if sub["broker"] == "zerodha":
        sub["zerodha_api_key"]     = getpass.getpass("Zerodha API Key: ")
        sub["zerodha_api_secret"]  = getpass.getpass("Zerodha API Secret: ")
        sub["zerodha_access_token"] = ""  # Will be set daily
    elif sub["broker"] == "angel":
        sub["angel_api_key"]    = getpass.getpass("Angel API Key: ")
        sub["angel_client_id"]  = input("Angel Client ID: ").strip()
        sub["angel_password"]   = getpass.getpass("Angel MPIN: ")
        sub["angel_totp_secret"] = getpass.getpass("Angel TOTP Secret (base32): ")

    existing_data.append(sub)

    encrypted = f.encrypt(json.dumps(existing_data).encode())
    SUBSCRIBER_CONFIG_PATH.write_bytes(encrypted)
    print(f"\n✅ Subscriber '{sub['name']}' added and config saved.")
    print(f"   Total subscribers: {len(existing_data)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Subscriber config encryption utility")
    sub = parser.add_subparsers(dest="command")

    enc_p = sub.add_parser("encrypt", help="Encrypt a plain JSON config file")
    enc_p.add_argument("--input", required=True, help="Path to plain subscribers.json")

    dec_p = sub.add_parser("decrypt", help="Decrypt config to plain JSON (debug only)")
    dec_p.add_argument("--output", default="data/subscribers_debug.json")

    sub.add_parser("add-subscriber", help="Add a subscriber interactively")

    args = parser.parse_args()

    if args.command == "encrypt":
        encrypt_file(args.input)
    elif args.command == "decrypt":
        decrypt_file(args.output)
    elif args.command == "add-subscriber":
        add_subscriber_interactive()
    else:
        parser.print_help()
