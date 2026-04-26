"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                         GMAIL OUTREACH SCRIPT                               ║
║                    GitHub Actions — Round-Robin Sender                      ║
╚══════════════════════════════════════════════════════════════════════════════╝

All configuration lives in email_config.json.
This file never needs to be edited.

Dependencies (requirements.txt):
    google-api-python-client  google-auth  google-auth-httplib2  requests
"""

# ==============================================================================
#
#   ██████╗ ██████╗ ███╗  ██╗███████╗██╗ ██████╗
#   ████╗ ████║██╔══██╗██╔════╝╚══██╔══╝██╔════╝██╔══██╗
#   ██╔████╔██║███████║███████╗   ██║   █████╗  ██████╔╝
#   ██║╚██╔╝██║██╔══██║╚════██║   ██║   ██╔══╝  ██╔══██╗
#   ██║ ╚═╝ ██║██║  ██║███████║   ██║   ███████╗██║  ██║
#   ╚═╝     ╚═╝╚═╝  ╚═╝╚══════╝   ╚═╝   ╚══════╝╚═╝  ╚═╝
#
#   THIS FILE HAS NO CONFIGURATION.
#   Edit email_config.json for everything — senders, mode, subject, body, delays.
#
# ==============================================================================

# Path to config file — only change this if you rename email_config.json
CONFIG_FILE_PATH = "email_config.json"

# GitHub repo details for writing sent_counts back after each run
# Set GITHUB_PAT as a GitHub Secret. GITHUB_USERNAME and GITHUB_REPO are
# read from environment variables set in the workflow file.
GITHUB_USERNAME = ""   # Set env var: GITHUB_USERNAME
GITHUB_REPO     = ""   # Set env var: GITHUB_REPO

# ==============================================================================
#   END OF CONFIGURATION
# ==============================================================================


import os
import csv
import json
import time
import base64
import logging
import requests
from email.mime.multipart import MIMEMultipart
from email.mime.text      import MIMEText

from google.oauth2.credentials      import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery      import build
from googleapiclient.errors         import HttpError


logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt = "%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.send"]


# ==============================================================================
# ── Load Config ────────────────────────────────────────────────────────────────
# ==============================================================================

def load_config(path: str) -> dict:
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"Config file not found: '{path}'\n"
            f"Make sure email_config.json is committed to the root of your repo."
        )
    with open(path, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    required = {
        "run_mode", "senders", "test_recipients", "email_subject",
        "email_body", "emails_per_account_limit", "delay_seconds",
        "csv_file_path", "csv_mapping", "sent_counts",
    }
    missing = required - set(cfg.keys())
    if missing:
        raise ValueError(f"email_config.json is missing keys: {missing}")

    log.info(f"✅ Config loaded from '{path}'")
    log.info(f"   run_mode                : {cfg['run_mode']}")
    log.info(f"   senders                 : {[s['email'] for s in cfg['senders']]}")
    log.info(f"   emails_per_account_limit: {cfg['emails_per_account_limit']}")
    log.info(f"   delay_seconds           : {cfg['delay_seconds']}")
    return cfg


# ==============================================================================
# ── Write Back Sent Counts ─────────────────────────────────────────────────────
# ==============================================================================

def write_back_sent_counts(cfg: dict, updated_counts: dict) -> None:
    """Commits updated sent_counts to email_config.json on GitHub after each run."""
    pat      = os.environ.get("GITHUB_PAT", "").strip()
    username = os.environ.get("GITHUB_USERNAME", GITHUB_USERNAME).strip()
    repo     = os.environ.get("GITHUB_REPO",     GITHUB_REPO).strip()

    if not all([pat, username, repo]):
        log.warning("⚠️  GITHUB_PAT / GITHUB_USERNAME / GITHUB_REPO not set — skipping write-back.")
        return

    cfg["sent_counts"] = updated_counts
    cfg_clean = {k: v for k, v in cfg.items() if not k.startswith("_")}

    url     = f"https://api.github.com/repos/{username}/{repo}/contents/{CONFIG_FILE_PATH}"
    headers = {
        "Authorization": f"token {pat}",
        "Accept":        "application/vnd.github.v3+json",
    }

    get_resp = requests.get(url, headers=headers, timeout=10)
    if get_resp.status_code != 200:
        log.warning(f"⚠️  Could not fetch file SHA: {get_resp.status_code}")
        return

    sha         = get_resp.json()["sha"]
    new_content = base64.b64encode(
        json.dumps(cfg_clean, indent=2, ensure_ascii=False).encode("utf-8")
    ).decode("utf-8")

    put_resp = requests.put(url, headers=headers, timeout=10, json={
        "message": "chore: update sent_counts after email run [skip ci]",
        "content": new_content,
        "sha":     sha,
    })

    if put_resp.status_code in (200, 201):
        log.info("✅ sent_counts written back to GitHub.")
    else:
        log.warning(f"⚠️  Write-back failed: {put_resp.status_code}")


# ==============================================================================
# ── Authentication ─────────────────────────────────────────────────────────────
# ==============================================================================

def build_gmail_service(sender: dict):
    """Loads token JSON from the GitHub Secret named in sender['secret_name']."""
    secret_name = sender["secret_name"]
    token_json  = os.environ.get(secret_name, "").strip()

    if not token_json:
        raise EnvironmentError(
            f"Secret '{secret_name}' is not set. "
            f"Add it to GitHub → Settings → Secrets and variables → Actions."
        )

    try:
        token_data = json.loads(token_json)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"Secret '{secret_name}' is not valid JSON. Re-run token_generator.py."
        ) from exc

    creds = Credentials.from_authorized_user_info(token_data, GMAIL_SCOPES)
    if creds.expired and creds.refresh_token:
        log.info(f"  🔄 Refreshing token for {sender['email']}...")
        creds.refresh(Request())

    return build("gmail", "v1", credentials=creds)


# ==============================================================================
# ── Email Helpers ──────────────────────────────────────────────────────────────
# ==============================================================================

def fill_template(template: str, placeholders: dict) -> str:
    for key, value in placeholders.items():
        template = template.replace(f"{{{key}}}", str(value))
    return template


def build_mime_message(to: str, subject: str, html_body: str, sender: dict) -> dict:
    msg            = MIMEMultipart("alternative")
    msg["From"]    = sender["email"]
    msg["To"]      = to
    msg["Subject"] = subject
    msg.attach(MIMEText(html_body, "html"))
    return {"raw": base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")}


def send_one_email(service, message_obj: dict, sender: dict, recipient: str) -> bool:
    try:
        result = service.users().messages().send(userId="me", body=message_obj).execute()
        log.info(f"  ✅ Sent  →  {recipient}  (id: {result.get('id')})  via {sender['email']}")
        return True
    except HttpError as exc:
        log.error(f"  ❌ FAILED  →  {recipient}  via {sender['email']}  |  {exc}")
        return False
    except Exception as exc:
        log.error(f"  ❌ UNEXPECTED  →  {recipient}  via {sender['email']}  |  {exc}")
        return False


# ==============================================================================
# ── CSV Loading ────────────────────────────────────────────────────────────────
# ==============================================================================

def load_leads(cfg: dict) -> list[dict]:
    csv_path   = cfg["csv_file_path"]
    csv_mapping = cfg["csv_mapping"]

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV not found: '{csv_path}'.")

    leads = []
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        missing = [c for c in csv_mapping.values() if c not in reader.fieldnames]
        if missing:
            raise ValueError(f"CSV missing columns: {missing}. Available: {reader.fieldnames}")
        for row in reader:
            leads.append({
                placeholder: row[col].strip()
                for placeholder, col in csv_mapping.items()
            })

    log.info(f"📄 Loaded {len(leads)} leads from '{csv_path}'.")
    return leads


# ==============================================================================
# ── TEST MODE ──────────────────────────────────────────────────────────────────
# ==============================================================================

def run_test_mode(cfg: dict) -> None:
    log.info("=" * 64)
    log.info("  🔶  TEST MODE  —  no real recipients will be contacted")
    log.info("=" * 64)

    sender  = cfg["senders"][0]
    service = build_gmail_service(sender)
    dummy   = {key: f"[{key}]" for key in cfg["csv_mapping"]}
    sent    = 0

    for i, recipient in enumerate(cfg["test_recipients"]):
        subject = fill_template(cfg["email_subject"], dummy)
        body    = fill_template(cfg["email_body"],    dummy)
        msg_obj = build_mime_message(recipient, subject, body, sender)

        log.info(f"\n📧 Test email [{i+1}/{len(cfg['test_recipients'])}]  →  {recipient}")
        if send_one_email(service, msg_obj, sender, recipient):
            sent += 1

        if i < len(cfg["test_recipients"]) - 1:
            log.info(f"  ⏳ Waiting {cfg['delay_seconds']}s...")
            time.sleep(cfg["delay_seconds"])

    log.info("\n" + "=" * 64)
    log.info(f"  TEST COMPLETE  —  {sent}/{len(cfg['test_recipients'])} sent")
    log.info("=" * 64)


# ==============================================================================
# ── LIVE MODE ──────────────────────────────────────────────────────────────────
# ==============================================================================

def run_live_mode(cfg: dict) -> None:
    log.info("=" * 64)
    log.info("  🚀  LIVE MODE  —  sending to real recipients")
    log.info("=" * 64)

    leads     = load_leads(cfg)
    senders   = cfg["senders"]
    limit     = cfg["emails_per_account_limit"]
    delay     = cfg["delay_seconds"]

    # Load cumulative sent counts — auto-initialise any new sender to 0
    sent_counts = {
        s["email"]: cfg["sent_counts"].get(s["email"], 0)
        for s in senders
    }

    log.info("  Current sent counts:")
    for email, count in sent_counts.items():
        log.info(f"    {email:40s}  {count} / {limit}")

    # Authenticate all accounts upfront
    services = []
    for sender in senders:
        log.info(f"  Authenticating: {sender['email']} ...")
        try:
            svc = build_gmail_service(sender)
            services.append({"service": svc, "config": sender})
            log.info(f"  ✅ Auth OK: {sender['email']}")
        except Exception as exc:
            log.error(f"  ❌ Auth FAILED for {sender['email']}: {exc}")

    if not services:
        log.critical("No accounts authenticated. Aborting.")
        raise SystemExit(1)

    total_sent   = 0
    total_failed = 0
    sender_idx   = 0
    n_senders    = len(services)

    for lead_num, lead in enumerate(leads, start=1):
        recipient = lead.get("email", "").strip()
        if not recipient:
            log.warning(f"  ⚠️  Lead #{lead_num} has no email — skipping.")
            continue

        # Find the next sender that hasn't hit the limit
        attempts = 0
        while sent_counts[services[sender_idx]["config"]["email"]] >= limit:
            sender_idx = (sender_idx + 1) % n_senders
            attempts  += 1
            if attempts >= n_senders:
                log.info("⛔ All accounts have reached the limit. Stopping.")
                _print_summary(total_sent, total_failed, len(leads), sent_counts, limit)
                write_back_sent_counts(cfg, sent_counts)
                return

        account      = services[sender_idx]
        sender       = account["config"]
        sender_email = sender["email"]

        subject = fill_template(cfg["email_subject"], lead)
        body    = fill_template(cfg["email_body"],    lead)
        msg_obj = build_mime_message(recipient, subject, body, sender)

        log.info(f"\n📧 [{lead_num}/{len(leads)}]  {recipient}  ←  {sender_email}")

        if send_one_email(account["service"], msg_obj, sender, recipient):
            sent_counts[sender_email] += 1
            total_sent += 1
        else:
            total_failed += 1

        sender_idx = (sender_idx + 1) % n_senders

        if lead_num < len(leads):
            log.info(f"  ⏳ Waiting {delay}s...")
            time.sleep(delay)

    _print_summary(total_sent, total_failed, len(leads), sent_counts, limit)
    write_back_sent_counts(cfg, sent_counts)


def _print_summary(sent, failed, total, sent_counts, limit):
    log.info("\n" + "=" * 64)
    log.info("  SEND COMPLETE")
    log.info(f"  ✅ Sent:   {sent}")
    log.info(f"  ❌ Failed: {failed}")
    log.info(f"  📋 Total leads: {total}")
    log.info("  ── Per-account totals ──")
    for email, count in sent_counts.items():
        log.info(f"     {email:40s}  {count} / {limit}")
    log.info("=" * 64)


# ==============================================================================
# ── Entry Point ────────────────────────────────────────────────────────────────
# ==============================================================================

if __name__ == "__main__":
    cfg  = load_config(CONFIG_FILE_PATH)
    mode = cfg["run_mode"].strip().upper()

    if mode == "TEST":
        run_test_mode(cfg)
    elif mode == "LIVE":
        run_live_mode(cfg)
    else:
        raise ValueError(
            f"run_mode in email_config.json must be 'TEST' or 'LIVE'. Got: '{cfg['run_mode']}'"
        )
