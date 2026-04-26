"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                      SLACK CONFIG BOT                                        ║
║     Drop a file in Slack → emails send → results posted back in Slack        ║
╚══════════════════════════════════════════════════════════════════════════════╝

Two things this bot does:
  1. File drop  → reads CSV/Excel from Slack, sends emails, posts results back
  2. Config cmds → read/update email_config.json on GitHub from Slack

Slack commands:
  config show
  config set emails_per_account 12
  config set delay 60
  config set subject Your new subject
  config reset counts

Environment variables (set in Render dashboard):
  SLACK_BOT_TOKEN     xoxb-...
  SLACK_APP_TOKEN     xapp-...
  GH_PAT              fine-grained GitHub PAT (Contents: Read & Write)
  GITHUB_USERNAME     your GitHub username
  GITHUB_REPO         your repo name
  CONFIG_FILE_PATH    email_config.json
  GMAIL_TOKEN_1       token JSON for account 1
  GMAIL_TOKEN_2       token JSON for account 2  (add as many as you have)
  GMAIL_TOKEN_3       ...
  GMAIL_TOKEN_4       ...
  GMAIL_TOKEN_5       ...
"""

import os
import io
import csv
import json
import time
import base64
import logging
import requests
import threading

from email.mime.multipart import MIMEMultipart
from email.mime.text      import MIMEText

from google.oauth2.credentials      import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery      import build
from googleapiclient.errors         import HttpError

from slack_bolt                     import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s  %(levelname)-8s  %(message)s",
)
log = logging.getLogger(__name__)


# ==============================================================================
# ── Env vars ───────────────────────────────────────────────────────────────────
# ==============================================================================

SLACK_BOT_TOKEN  = os.environ["SLACK_BOT_TOKEN"]
SLACK_APP_TOKEN  = os.environ["SLACK_APP_TOKEN"]
GITHUB_PAT       = os.environ["GH_PAT"]
GITHUB_USERNAME  = os.environ.get("GITHUB_USERNAME", "")
GITHUB_REPO      = os.environ.get("GITHUB_REPO", "")
CONFIG_FILE_PATH = os.environ.get("CONFIG_FILE_PATH", "email_config.json")

GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.send"]

app = App(token=SLACK_BOT_TOKEN)

GITHUB_API_URL = (
    f"https://api.github.com/repos/{GITHUB_USERNAME}/{GITHUB_REPO}"
    f"/contents/{CONFIG_FILE_PATH}"
)
GITHUB_HEADERS = {
    "Authorization": f"token {GITHUB_PAT}",
    "Accept":        "application/vnd.github.v3+json",
}


# ==============================================================================
# ── GitHub config helpers ──────────────────────────────────────────────────────
# ==============================================================================

def github_read_config() -> tuple[dict, str]:
    resp = requests.get(GITHUB_API_URL, headers=GITHUB_HEADERS, timeout=10)
    resp.raise_for_status()
    data    = resp.json()
    content = base64.b64decode(data["content"]).decode("utf-8")
    return json.loads(content), data["sha"]


def github_write_config(cfg: dict, sha: str, message: str) -> bool:
    cfg_clean   = {k: v for k, v in cfg.items() if not k.startswith("_")}
    new_content = base64.b64encode(
        json.dumps(cfg_clean, indent=2, ensure_ascii=False).encode("utf-8")
    ).decode("utf-8")
    resp = requests.put(
        GITHUB_API_URL, headers=GITHUB_HEADERS, timeout=10,
        json={"message": message, "content": new_content, "sha": sha},
    )
    return resp.status_code in (200, 201)


# ==============================================================================
# ── Gmail helpers ──────────────────────────────────────────────────────────────
# ==============================================================================

def build_gmail_service(sender: dict):
    token_json = os.environ.get(sender["secret_name"], "").strip()
    if not token_json:
        raise EnvironmentError(f"Secret '{sender['secret_name']}' is not set.")
    creds = Credentials.from_authorized_user_info(json.loads(token_json), GMAIL_SCOPES)
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
    return build("gmail", "v1", credentials=creds)


def fill_template(template: str, data: dict) -> str:
    for key, value in data.items():
        template = template.replace(f"{{{key}}}", str(value))
    return template


def build_mime(to: str, subject: str, body: str, sender: dict) -> dict:
    msg            = MIMEMultipart("alternative")
    msg["From"]    = sender["email"]
    msg["To"]      = to
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "html"))
    return {"raw": base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")}


def send_one(service, msg_obj: dict, sender: dict, recipient: str) -> bool:
    try:
        result = service.users().messages().send(userId="me", body=msg_obj).execute()
        log.info(f"✅ Sent → {recipient} via {sender['email']} (id: {result.get('id')})")
        return True
    except HttpError as exc:
        log.error(f"❌ Failed → {recipient} via {sender['email']} | {exc}")
        return False
    except Exception as exc:
        log.error(f"❌ Unexpected → {recipient} | {exc}")
        return False


# ==============================================================================
# ── File parser (CSV or Excel, from bytes) ─────────────────────────────────────
# ==============================================================================

def parse_file_bytes(file_bytes: bytes, filename: str, csv_mapping: dict) -> list[dict]:
    """
    Reads a CSV or Excel file from raw bytes.
    Returns a list of lead dicts keyed by placeholder name.
    """
    ext = os.path.splitext(filename)[1].lower()

    if ext in (".xlsx", ".xls"):
        import openpyxl
        wb      = openpyxl.load_workbook(io.BytesIO(file_bytes), read_only=True, data_only=True)
        ws      = wb.active
        rows    = list(ws.iter_rows(values_only=True))
        headers = [str(h).strip() if h is not None else "" for h in rows[0]]
        leads   = []
        for row in rows[1:]:
            row_dict = dict(zip(headers, row))
            lead = {
                placeholder: str(row_dict.get(col, "") or "").strip()
                for placeholder, col in csv_mapping.items()
            }
            if lead.get("email", "").strip():
                leads.append(lead)
        wb.close()

    else:
        # Treat as CSV
        text    = file_bytes.decode("utf-8-sig")
        reader  = csv.DictReader(io.StringIO(text))
        leads   = []
        for row in reader:
            lead = {
                placeholder: str(row.get(col, "") or "").strip()
                for placeholder, col in csv_mapping.items()
            }
            if lead.get("email", "").strip():
                leads.append(lead)

    return leads


# ==============================================================================
# ── Core send job (runs in background thread) ──────────────────────────────────
# ==============================================================================

def run_send_job(file_bytes: bytes, filename: str, channel: str, user: str):
    """
    Called in a background thread when a file is dropped in Slack.
    Posts progress and final summary back to the same Slack channel.
    """

    def post(text: str):
        app.client.chat_postMessage(channel=channel, text=text)

    post(f"📥 Got `{filename}` — loading config and authenticating...")

    # ── Load config from GitHub ───────────────────────────────────────────────
    try:
        cfg, sha = github_read_config()
    except Exception as exc:
        post(f"❌ Could not load config from GitHub: `{exc}`")
        return

    senders     = cfg.get("senders", [])
    limit       = cfg.get("emails_per_account_limit", 30)
    delay       = cfg.get("delay_seconds", 120)
    csv_mapping = cfg.get("csv_mapping", {
        "first_name": "First Name",
        "last_name":  "Last Name",
        "company":    "Company Name",
        "title":      "Title",
        "email":      "Email",
    })

    # ── Parse file ────────────────────────────────────────────────────────────
    try:
        leads = parse_file_bytes(file_bytes, filename, csv_mapping)
    except Exception as exc:
        post(f"❌ Could not read file: `{exc}`")
        return

    if not leads:
        post("⚠️ No valid leads found in the file (missing Email column or all rows empty).")
        return

    post(f"✅ Loaded *{len(leads)} leads*. Authenticating sender accounts...")

    # ── Authenticate senders ──────────────────────────────────────────────────
    sent_counts = {s["email"]: cfg.get("sent_counts", {}).get(s["email"], 0) for s in senders}
    services    = []

    for sender in senders:
        try:
            svc = build_gmail_service(sender)
            services.append({"service": svc, "config": sender})
            log.info(f"✅ Auth OK: {sender['email']}")
        except Exception as exc:
            log.error(f"❌ Auth failed for {sender['email']}: {exc}")
            post(f"⚠️ Could not authenticate `{sender['email']}` — skipping.")

    if not services:
        post("❌ No sender accounts could be authenticated. Aborting.")
        return

    auth_list = "\n".join(f"  • `{s['config']['email']}`" for s in services)
    post(f"✅ Authenticated senders:\n{auth_list}\n\n🚀 Starting send...")

    # ── Round-robin send loop ─────────────────────────────────────────────────
    total_sent   = 0
    total_failed = 0
    skipped      = 0
    sender_idx   = 0
    n_senders    = len(services)
    log_lines    = []   # Collect per-email results for final summary

    for lead_num, lead in enumerate(leads, start=1):
        recipient = lead.get("email", "").strip()
        if not recipient:
            skipped += 1
            continue

        # Find next sender under limit
        attempts = 0
        while sent_counts[services[sender_idx]["config"]["email"]] >= limit:
            sender_idx = (sender_idx + 1) % n_senders
            attempts  += 1
            if attempts >= n_senders:
                post("⛔ All sender accounts have reached their limit. Stopping early.")
                _post_summary(post, total_sent, total_failed, skipped, len(leads), sent_counts, limit, log_lines)
                _write_back(cfg, sha, sent_counts)
                return

        account      = services[sender_idx]
        sender       = account["config"]
        sender_email = sender["email"]

        subject = fill_template(cfg.get("email_subject", "Hello {first_name}"), lead)
        body    = fill_template(cfg.get("email_body", "<p>Hi {first_name}</p>"), lead)
        msg_obj = build_mime(recipient, subject, body, sender)

        success = send_one(account["service"], msg_obj, sender, recipient)

        if success:
            sent_counts[sender_email] += 1
            total_sent += 1
            log_lines.append(f"✅ `{recipient}` ← `{sender_email}`")
        else:
            total_failed += 1
            log_lines.append(f"❌ `{recipient}` — send failed")

        sender_idx = (sender_idx + 1) % n_senders

        if lead_num < len(leads):
            log.info(f"Waiting {delay}s before next email...")
            time.sleep(delay)

    _post_summary(post, total_sent, total_failed, skipped, len(leads), sent_counts, limit, log_lines)
    _write_back(cfg, sha, sent_counts)


def _post_summary(post, sent, failed, skipped, total, sent_counts, limit, log_lines):
    detail = "\n".join(log_lines) if log_lines else "_No emails attempted._"
    counts = "\n".join(
        f"  • `{email}` — {count} / {limit} sent"
        for email, count in sent_counts.items()
    )
    post(
        f"{'─' * 40}\n"
        f"*📊 Send Complete*\n"
        f"✅ Sent:    *{sent}*\n"
        f"❌ Failed:  *{failed}*\n"
        f"⏭️ Skipped: *{skipped}*\n"
        f"📋 Total leads: *{total}*\n\n"
        f"*Per-account totals:*\n{counts}\n\n"
        f"*Per-email log:*\n{detail}\n"
        f"{'─' * 40}"
    )


def _write_back(cfg, sha, sent_counts):
    cfg["sent_counts"] = sent_counts
    try:
        ok = github_write_config(cfg, sha, "chore: update sent_counts after Slack send [skip ci]")
        if ok:
            log.info("✅ sent_counts written back to GitHub.")
        else:
            log.warning("⚠️ sent_counts write-back failed.")
    except Exception as exc:
        log.warning(f"⚠️ sent_counts write-back error: {exc}")


# ==============================================================================
# ── Slack: File drop handler ───────────────────────────────────────────────────
# ==============================================================================

@app.event("message")
def handle_message(event, say, client):
    """
    Handles all message events.
    - If it contains a file → kick off email send job in background thread
    - If it starts with 'config' → route to config handler
    - Otherwise → ignore silently
    """
    text    = event.get("text", "") or ""
    files   = event.get("files", [])
    channel = event["channel"]
    user    = event.get("user", "")

    # ── File dropped ──────────────────────────────────────────────────────────
    if files:
        file_info = files[0]
        filename  = file_info.get("name", "leads.csv")
        file_url  = file_info.get("url_private_download")

        ext = os.path.splitext(filename)[1].lower()
        if ext not in (".csv", ".xlsx", ".xls"):
            say(f"⚠️ Unsupported file type `{ext}`. Please upload a `.csv` or `.xlsx` file.")
            return

        # Download file from Slack
        resp = requests.get(
            file_url,
            headers={"Authorization": f"Bearer {SLACK_BOT_TOKEN}"},
            timeout=30,
        )
        if resp.status_code != 200:
            say(f"❌ Could not download file (HTTP {resp.status_code}).")
            return

        file_bytes = resp.content

        # Run in background so Slack doesn't time out
        thread = threading.Thread(
            target=run_send_job,
            args=(file_bytes, filename, channel, user),
            daemon=True,
        )
        thread.start()
        return

    # ── Config command ────────────────────────────────────────────────────────
    if text.strip().lower().startswith("config"):
        handle_config_command(text.strip(), say)
        return

    # ── Everything else → ignore ──────────────────────────────────────────────


# ==============================================================================
# ── Config command handler ─────────────────────────────────────────────────────
# ==============================================================================

SETTABLE_KEYS = {
    "delay":              ("delay_seconds",            int),
    "emails_per_account": ("emails_per_account_limit", int),
    "subject":            ("email_subject",            str),
    "body":               ("email_body",               str),
}


def handle_config_command(text: str, say) -> None:
    parts = text.split(None, 3)

    if len(parts) < 2:
        say("Usage: `config show` | `config set <key> <value>` | `config reset counts`")
        return

    sub = parts[1].lower()

    # config show
    if sub == "show":
        try:
            cfg, _ = github_read_config()
            say(format_config(cfg))
        except Exception as exc:
            say(f"❌ Could not read config: `{exc}`")
        return

    # config reset counts
    if sub == "reset" and len(parts) >= 3 and parts[2].lower() == "counts":
        try:
            cfg, sha = github_read_config()
            cfg["sent_counts"] = {k: 0 for k in cfg.get("sent_counts", {})}
            ok = github_write_config(cfg, sha, "chore: reset sent_counts via Slack")
            say("✅ All sent counts reset to 0." if ok else "❌ GitHub write failed.")
        except Exception as exc:
            say(f"❌ Error: `{exc}`")
        return

    # config set <key> <value>
    if sub == "set":
        if len(parts) < 4:
            say(f"Usage: `config set <key> <value>`\nKeys: {' | '.join(f'`{k}`' for k in SETTABLE_KEYS)}")
            return

        key_alias = parts[2].lower()
        raw_value = parts[3].strip()

        if key_alias not in SETTABLE_KEYS:
            say(f"❌ Unknown key `{key_alias}`. Available: {' | '.join(f'`{k}`' for k in SETTABLE_KEYS)}")
            return

        json_key, cast_fn = SETTABLE_KEYS[key_alias]
        try:
            value = cast_fn(raw_value)
        except (ValueError, TypeError):
            say(f"❌ `{key_alias}` expects a {cast_fn.__name__}. Got: `{raw_value}`")
            return

        try:
            cfg, sha = github_read_config()
            old      = cfg.get(json_key, "N/A")
            cfg[json_key] = value
            ok = github_write_config(cfg, sha, f"config: set {json_key}={value!r} via Slack")
            if ok:
                say(f"✅ *{key_alias}* updated: `{old}` → `{value}` _(committed to GitHub)_")
            else:
                say("❌ GitHub write failed.")
        except Exception as exc:
            say(f"❌ Error: `{exc}`")
        return

    say(
        "❓ Unknown command. Try:\n"
        "  `config show`\n"
        "  `config set emails_per_account 12`\n"
        "  `config set delay 60`\n"
        "  `config set subject Your subject here`\n"
        "  `config reset counts`"
    )


def format_config(cfg: dict) -> str:
    senders = cfg.get("senders", [])
    counts  = cfg.get("sent_counts", {})
    limit   = cfg.get("emails_per_account_limit", "?")
    counts_lines = "\n".join(
        f"  • `{s['email']}` — {counts.get(s['email'], 0)} / {limit} sent"
        for s in senders
    ) or "  (none configured)"
    body_preview = str(cfg.get("email_body", ""))[:120].replace("\n", " ") + "..."
    return (
        f"*📋 Current Config*\n{'─'*40}\n"
        f"*Subject:*            {cfg.get('email_subject', '?')}\n"
        f"*Body preview:*       _{body_preview}_\n"
        f"*Per-account limit:*  `{limit}`\n"
        f"*Delay:*              `{cfg.get('delay_seconds', '?')}s`\n"
        f"*Senders & counts:*\n{counts_lines}\n"
        f"{'─'*40}\n_Drop a CSV/Excel file here to send emails._"
    )


# ==============================================================================
# ── Start ──────────────────────────────────────────────────────────────────────
# ==============================================================================

if __name__ == "__main__":
    log.info("🤖 Slack email bot starting (Socket Mode)...")
    SocketModeHandler(app, SLACK_APP_TOKEN).start()