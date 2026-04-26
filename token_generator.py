"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                         TOKEN GENERATOR                                      ║
║           Run locally once per Gmail account to generate OAuth tokens        ║
╚══════════════════════════════════════════════════════════════════════════════╝

HOW TO USE:
  1. Set CREDENTIALS_PATH below.
  2. Change TOKEN_OUTPUT_NAME for each account:
       token_account_1.json → GitHub Secret: GMAIL_TOKEN_1
       token_account_2.json → GitHub Secret: GMAIL_TOKEN_2
  3. Run: python token_generator.py
  4. Browser opens → log in with the Gmail account you want to add.
  5. Copy the printed JSON → paste into GitHub Secret and Railway Variable.

Dependencies:
    pip install google-auth-oauthlib google-auth-httplib2 google-api-python-client
"""

# ==============================================================================
#   CONFIGURATION  ——  EDIT ONLY THESE TWO LINES
# ==============================================================================

CREDENTIALS_PATH  = r"C:\Users\Tonmoy\Documents\solar-email-automation\credentials.json"
TOKEN_OUTPUT_NAME = "token_account_1.json"   # Change to _2, _3 etc. for each account

# ==============================================================================
#   END OF CONFIGURATION  ——  do not edit below this line
# ==============================================================================

import os
import json

from google.oauth2.credentials          import Credentials
from google.auth.transport.requests     import Request
from google_auth_oauthlib.flow          import InstalledAppFlow

SCOPES = ["https://www.googleapis.com/auth/gmail.send"]
BORDER = "=" * 72


def generate_token(credentials_path: str, output_name: str) -> None:

    # Step 1: Validate credentials.json
    if not os.path.exists(credentials_path):
        print(f"\n❌  credentials.json not found at:\n    {credentials_path}")
        print("\n    How to get credentials.json:")
        print("    1. Go to https://console.cloud.google.com")
        print("    2. APIs & Services → Credentials")
        print("    3. Create Credentials → OAuth Client ID → Desktop App")
        print("    4. Download and save as credentials.json")
        return

    creds = None

    # Step 2: Try to refresh existing token silently
    if os.path.exists(output_name):
        print(f"\n📂  Existing token found: '{output_name}' — attempting refresh...")
        try:
            creds = Credentials.from_authorized_user_file(output_name, SCOPES)
        except Exception:
            print("    ⚠️  Could not read existing token — will re-authenticate.")
            creds = None

    if creds and creds.valid:
        print("    ✅  Token still valid. No browser login needed.")

    elif creds and creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
            print("    ✅  Token refreshed silently.")
        except Exception as exc:
            print(f"    ⚠️  Refresh failed ({exc}). Opening browser...")
            creds = None

    # Step 3: Browser OAuth flow if needed
    if not creds or not creds.valid:
        print(f"\n🌐  Opening browser for Google login...")
        print("    ➜  Sign in with the Gmail account you want as a sender.")
        print("    ➜  Use incognito if the wrong account is pre-selected.\n")
        flow  = InstalledAppFlow.from_client_secrets_file(credentials_path, SCOPES)
        creds = flow.run_local_server(port=0)
        print("\n    ✅  Authentication successful.")

    # Step 4: Save token locally
    token_json_str = creds.to_json()
    with open(output_name, "w") as f:
        f.write(token_json_str)
    print(f"\n💾  Token saved to: '{output_name}'")

    # Step 5: Print for GitHub Secrets / Railway Variables
    account_number = "".join(filter(str.isdigit, output_name)) or "N"

    print("\n" + BORDER)
    print(f"  COPY THE JSON BELOW")
    print(f"  → Paste into GitHub Secret:    GMAIL_TOKEN_{account_number}")
    print(f"  → Paste into Railway Variable: GMAIL_TOKEN_{account_number}")
    print(BORDER)
    print()
    print(token_json_str)
    print()
    print(BORDER)

    # Summary
    try:
        parsed = json.loads(token_json_str)
        print("\n📋  Token summary:")
        print(f"    Has access token  : {'token' in parsed}")
        print(f"    Has refresh token : {'refresh_token' in parsed}")
        print(f"    Expiry            : {parsed.get('expiry', 'N/A')}")
    except Exception:
        pass

    next_num = int(account_number) + 1 if account_number.isdigit() else 2
    print(f"\n✅  Done! For the next account change TOKEN_OUTPUT_NAME to 'token_account_{next_num}.json' and run again.\n")


if __name__ == "__main__":
    generate_token(CREDENTIALS_PATH, TOKEN_OUTPUT_NAME)
