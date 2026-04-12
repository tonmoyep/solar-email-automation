"""
Gmail API — Multi-Sender Round-Robin Campaign Script
------------------------------------------------------
Designed to run in GitHub Actions. All configuration lives in the
Master Configuration section below. Do not edit anything else.

No credentials.json file is required. Authentication is built entirely
from the Token JSON stored in GitHub Secrets via environment variables.

Dependencies:
    pip install google-auth google-auth-httplib2 google-api-python-client
"""

# =============================================================================
# ── MASTER CONFIGURATION — All edits go here, nowhere else ───────────────────
# =============================================================================

# "TEST" → sends only to TEST_RECIPIENTS using the first sender account.
# "LIVE" → reads the CSV and runs the full round-robin campaign.
RUN_MODE = "TEST"

# Used only in TEST mode. Every address in this list receives one email.
TEST_RECIPIENTS = ["tonmoy.ep@example.com" "tonmoy.bold@gmail.com" "teacherstoday.ctg@gmail.com"]

# Each sender needs a Gmail address and the name of the GitHub Secret that
# holds its Token JSON. The secret_name must exactly match your GitHub
# Secret name, including capitalisation.
# To add more senders later, add more lines in the same format:
#   {"email": "sender2@gmail.com", "secret_name": "TOKEN_2"},
SENDER_CONFIG = [
    {"email": "rakibtonmoy007@gmail.com", "secret_name": "RAKIBTONMOY007"},
]

# Email content — use {placeholder} syntax to personalise.
# Placeholders must match the keys defined in CSV_MAPPING below.
# If a placeholder value is missing from the CSV, it is silently replaced
# with an empty string. The script will never crash on a missing field.
EMAIL_SUBJECT = "Your Subject Here"
EMAIL_BODY    = """Hi {first_name},

I noticed that {company} is doing great work and wanted to reach out.

[Your value proposition here.]

Would you be open to a quick chat this week?

Best regards,
{sender_name}"""

# Maximum number of emails each sender account is allowed to send per run.
# Total emails sent per run = EMAILS_PER_ACCOUNT_LIMIT x number of senders.
EMAILS_PER_ACCOUNT_LIMIT = 3

# Seconds to wait between every send. Applied after both successes and
# failures to maintain a consistent timing rhythm throughout the run.
DELAY_SECONDS = 5

# Path to the CSV file inside your GitHub repository.
CSV_FILE_PATH = "Solar_Email_Leads.csv"

# Maps {placeholder} names used in EMAIL_BODY to exact CSV column headers.
# Add or remove entries here to match your CSV and your email body.
CSV_MAPPING = {
    "first_name": "First Name",
    "company":    "Company Name",
    "email":      "Email",
}


# =============================================================================
# ── SCRIPT LOGIC — Do not edit below this line ────────────────────────────────
# =============================================================================

import csv
import os
import sys
import time
import base64
import json

from email.mime.multipart           import MIMEMultipart
from email.mime.text                import MIMEText
from google.oauth2.credentials      import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery      import build
from googleapiclient.errors         import HttpError


GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.send"]


# ── Authentication ─────────────────────────────────────────────────────────────

def load_all_secrets() -> dict:
    """
    Reads the ALL_SECRETS environment variable — a JSON string containing
    every GitHub Secret in the repository, injected by the YAML as:
        env:
          ALL_SECRETS: ${{ toJSON(secrets) }}

    Returns a plain dict of { SECRET_NAME: secret_value_string }.
    This is called once at startup and the result is passed around,
    so the JSON is only parsed a single time per run.
    """
    raw = os.environ.get("ALL_SECRETS")
    if not raw:
        print("  ERROR: ALL_SECRETS environment variable is not set.")
        print("         Make sure the campaign.yml env block contains:")
        print("           ALL_SECRETS: ${{ toJSON(secrets) }}")
        sys.exit(1)
    try:
        return json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"  ERROR: ALL_SECRETS is not valid JSON. Detail: {e}")
        sys.exit(1)


def load_credentials_from_secret(secret_name: str, all_secrets: dict) -> Credentials:
    """
    Looks up secret_name inside the all_secrets dict (pre-loaded from
    ALL_SECRETS) and builds a Credentials object from the token JSON string.

    Because secret names are resolved at runtime from the dict, you only
    ever need to update SENDER_CONFIG in campaign.py — the YAML never
    needs to change when you rename or add secrets.

    Automatically refreshes the short-lived access token if it has expired.
    """
    token_json = all_secrets.get(secret_name)

    if not token_json:
        print(f"  ERROR: Secret '{secret_name}' was not found.")
        print(f"         Confirm it exists at:")
        print(f"         GitHub repo -> Settings -> Secrets -> Actions")
        sys.exit(1)

    try:
        token_data = json.loads(token_json)
    except json.JSONDecodeError as e:
        print(f"  ERROR: Secret '{secret_name}' contains invalid JSON.")
        print(f"         Detail: {e}")
        print(f"         Re-run token_generator.py and re-paste the output into the secret.")
        sys.exit(1)

    creds = Credentials.from_authorized_user_info(token_data, GMAIL_SCOPES)

    # Silently refresh if the access token has expired using the refresh token
    if creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
        except Exception as e:
            print(f"  ERROR: Token refresh failed for secret '{secret_name}'.")
            print(f"         Detail: {e}")
            print(f"         Re-run token_generator.py and update the GitHub Secret.")
            sys.exit(1)

    return creds


def build_gmail_service(creds: Credentials):
    """Returns an authenticated Gmail API service object."""
    return build("gmail", "v1", credentials=creds)


# ── Email Construction ─────────────────────────────────────────────────────────

def build_mime_message(sender_email: str, recipient_email: str,
                       subject: str, body: str) -> dict:
    """
    Constructs a MIME email and base64url-encodes it for the Gmail API.
    Both plain text and HTML parts are attached so any email client can
    render it correctly. The Gmail API requires the message in a 'raw' key.
    """
    msg = MIMEMultipart("alternative")
    msg["From"]    = sender_email
    msg["To"]      = recipient_email
    msg["Subject"] = subject

    msg.attach(MIMEText(body, "plain"))
    msg.attach(MIMEText(body.replace("\n", "<br>"), "html"))

    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")
    return {"raw": raw}


def send_email(service, sender_email: str, recipient_email: str,
               subject: str, body: str) -> bool:
    """
    Sends one email via the Gmail API.
    Returns True on success, False on failure.
    Logs the error clearly but never raises — the main loop always
    continues and the delay is always applied regardless of outcome.
    """
    try:
        message = build_mime_message(sender_email, recipient_email, subject, body)
        result  = service.users().messages().send(userId="me", body=message).execute()
        print(f"   OK    -> {recipient_email}  (Message ID: {result.get('id')})")
        return True
    except HttpError as e:
        print(f"   FAIL  -> {recipient_email}  |  Gmail API error: {e}")
        return False
    except Exception as e:
        print(f"   FAIL  -> {recipient_email}  |  Unexpected error: {e}")
        return False


# ── Personalisation ───────────────────────────────────────────────────────────

def personalise_body(body_template: str, row: dict,
                     mapping: dict, sender_name: str) -> str:
    """
    Replaces every {placeholder} in the body template with values from
    the CSV row (via CSV_MAPPING) and the built-in {sender_name} token.

    Placeholder safety: every placeholder in CSV_MAPPING is pre-filled
    with "" before reading the CSV row. If a column is absent or blank,
    that placeholder becomes an empty string — never a KeyError or crash.

    format_map is used instead of format() so any placeholder NOT listed
    in CSV_MAPPING is left untouched rather than raising an error.
    """
    # Pre-fill every mapped placeholder with "" as a guaranteed safe default
    values = {placeholder: "" for placeholder in mapping}
    values["sender_name"] = sender_name

    # Overwrite defaults with real CSV values where the column exists
    for placeholder, column_header in mapping.items():
        raw = row.get(column_header, "")
        values[placeholder] = raw.strip() if isinstance(raw, str) else ""

    return body_template.format_map(values)


# ── CSV Loading ───────────────────────────────────────────────────────────────

def load_csv(filepath: str) -> list:
    """
    Reads the CSV using csv.DictReader and returns a list of row dicts.
    Exits with a clear message if the file is missing or has no data rows.
    """
    if not os.path.exists(filepath):
        print(f"  ERROR: CSV file not found at '{filepath}'.")
        print(f"         Make sure '{filepath}' is committed to your repository.")
        sys.exit(1)

    with open(filepath, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    if not rows:
        print(f"  ERROR: CSV file '{filepath}' is empty or has no data rows.")
        sys.exit(1)

    return rows


# ── Config Validation ─────────────────────────────────────────────────────────

def validate_config() -> None:
    """Catches obvious configuration mistakes before any API calls are made."""
    if RUN_MODE not in ("TEST", "LIVE"):
        print(f"  ERROR: RUN_MODE must be 'TEST' or 'LIVE'. Got: '{RUN_MODE}'")
        sys.exit(1)
    if not SENDER_CONFIG:
        print("  ERROR: SENDER_CONFIG is empty. Add at least one sender.")
        sys.exit(1)
    if RUN_MODE == "TEST" and not TEST_RECIPIENTS:
        print("  ERROR: TEST_RECIPIENTS is empty. Add at least one test email address.")
        sys.exit(1)


# ── Test Mode ─────────────────────────────────────────────────────────────────

def run_test_mode() -> None:
    """
    Sends one email to each address in TEST_RECIPIENTS using only the first
    sender in SENDER_CONFIG. The CSV is never opened or read in this mode.
    Placeholder values use dummy text so the email body renders in full
    and you can verify exactly how a real personalised email will look.
    """
    sender      = SENDER_CONFIG[0]
    sender_name = sender["email"].split("@")[0]

    print(f"\n  TEST MODE")
    print(f"  Sender     : {sender['email']}")
    print(f"  Secret     : {sender['secret_name']}")
    print(f"  Recipients : {', '.join(TEST_RECIPIENTS)}\n")

    all_secrets = load_all_secrets()
    creds       = load_credentials_from_secret(sender["secret_name"], all_secrets)
    service     = build_gmail_service(creds)

    for i, recipient in enumerate(TEST_RECIPIENTS):
        print(f"  Sending test email {i + 1}/{len(TEST_RECIPIENTS)}  ->  {recipient}")

        # Pre-fill every CSV_MAPPING placeholder with dummy values so the
        # full email body renders even though no CSV row is being used.
        dummy_values = {placeholder: "" for placeholder in CSV_MAPPING}
        dummy_values.update({
            "first_name":  "Test Recipient",
            "company":     "Test Company",
            "sender_name": sender_name,
        })
        body = EMAIL_BODY.format_map(dummy_values)

        send_email(service, sender["email"], recipient, EMAIL_SUBJECT, body)

        if i < len(TEST_RECIPIENTS) - 1:
            print(f"  Waiting {DELAY_SECONDS}s before next send...")
            time.sleep(DELAY_SECONDS)

    print("\n  Test mode complete.")


# ── Live Mode ─────────────────────────────────────────────────────────────────

def run_live_mode() -> None:
    """
    Sends personalised emails from the CSV in round-robin order.

    Round-robin behaviour (example with 2 senders):
      Email 1  ->  Sender 1  ->  wait 120s
      Email 2  ->  Sender 2  ->  wait 120s
      Email 3  ->  Sender 1  ->  wait 120s
      ...continues until every sender hits EMAILS_PER_ACCOUNT_LIMIT
      or the CSV is exhausted, whichever comes first.

    With a single sender, every email goes through that one account with
    the 120s delay between each send. Round-robin logic still works correctly
    and is already ready for additional senders whenever you add them.

    The delay is always applied after each attempt — success or failure —
    so the timing rhythm is never broken by an individual email error.
    """
    rows        = load_csv(CSV_FILE_PATH)
    num_senders = len(SENDER_CONFIG)
    total_cap   = EMAILS_PER_ACCOUNT_LIMIT * num_senders

    print(f"\n  LIVE MODE")
    print(f"  Senders             : {num_senders}")
    print(f"  Limit per account   : {EMAILS_PER_ACCOUNT_LIMIT}")
    print(f"  Max emails this run : {total_cap}")
    print(f"  CSV rows loaded     : {len(rows)}")
    print(f"  Delay between sends : {DELAY_SECONDS}s\n")

    # Authenticate all senders upfront so auth errors surface before any
    # emails are sent rather than failing halfway through the campaign.
    all_secrets = load_all_secrets()

    print("  Authenticating sender accounts...")
    services = []
    for sender in SENDER_CONFIG:
        try:
            creds   = load_credentials_from_secret(sender["secret_name"], all_secrets)
            service = build_gmail_service(creds)
            services.append(service)
            print(f"   OK  {sender['email']}")
        except SystemExit:
            raise
        except Exception as e:
            print(f"   FAIL  {sender['email']} -- {e}")
            sys.exit(1)
    print()

    send_counts  = [0] * num_senders  # per-sender email count tracker
    row_index    = 0
    total_sent   = 0
    total_failed = 0
    sender_index = 0                  # round-robin pointer

    while row_index < len(rows):

        # Stop if every sender has reached its individual cap
        if all(count >= EMAILS_PER_ACCOUNT_LIMIT for count in send_counts):
            print(f"  All senders have reached the limit of {EMAILS_PER_ACCOUNT_LIMIT}. Stopping.")
            break

        # Advance the round-robin pointer past any sender already at cap
        skipped = 0
        while send_counts[sender_index] >= EMAILS_PER_ACCOUNT_LIMIT:
            sender_index = (sender_index + 1) % num_senders
            skipped += 1
            if skipped > num_senders:
                break  # safety guard — outer while catches this on next pass

        sender      = SENDER_CONFIG[sender_index]
        service     = services[sender_index]
        sender_name = sender["email"].split("@")[0]
        row         = rows[row_index]

        # Extract recipient email — skip this row if the field is blank
        email_column = CSV_MAPPING.get("email", "Email")
        recipient    = row.get(email_column, "").strip()
        if not recipient:
            print(f"  WARNING: Row {row_index + 1} has no email address — skipping.")
            row_index    += 1
            sender_index  = (sender_index + 1) % num_senders
            continue

        body     = personalise_body(EMAIL_BODY, row, CSV_MAPPING, sender_name)
        progress = f"{total_sent + total_failed + 1}/{min(total_cap, len(rows))}"
        print(f"  Email ({progress})  [{sender['email']}]  ->  {recipient}")

        success = send_email(service, sender["email"], recipient, EMAIL_SUBJECT, body)

        if success:
            send_counts[sender_index] += 1
            total_sent  += 1
        else:
            total_failed += 1

        row_index    += 1
        sender_index  = (sender_index + 1) % num_senders

        # Always wait — success or failure — to maintain timing rhythm
        remaining = min(total_cap, len(rows)) - (total_sent + total_failed)
        if remaining > 0:
            print(f"  Waiting {DELAY_SECONDS}s...")
            time.sleep(DELAY_SECONDS)

    # ── Summary ──────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print(f"  Campaign complete.")
    print(f"  Sent: {total_sent}   Failed: {total_failed}")
    for i, sender in enumerate(SENDER_CONFIG):
        print(f"  {sender['email']}: {send_counts[i]} emails sent")
    print("=" * 60)


# ── Entry Point ───────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  Gmail Round-Robin Campaign Script")
    print("=" * 60)

    validate_config()

    if RUN_MODE == "TEST":
        run_test_mode()
    else:
        run_live_mode()


if __name__ == "__main__":
    main()
