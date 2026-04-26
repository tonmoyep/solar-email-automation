"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                      SLACK CONFIG BOT                                        ║
║         Read and update email_config.json from Slack                         ║
╚══════════════════════════════════════════════════════════════════════════════╝

Deployed on Railway. Commit this file to your GitHub repo root.

Slack commands:
  config show                          → display full current config
  config set run_mode LIVE             → change run mode
  config set emails_per_account 12     → change per-account limit
  config set delay 60                  → change delay in seconds
  config set subject Your new subject  → change email subject
  config reset counts                  → zero out all sent_counts

Dependencies (already in requirements.txt):
    slack-bolt  requests
"""

# ==============================================================================
#
#   CONFIGURATION  ——  Set these in Railway Variables tab
#
#   SLACK_BOT_TOKEN     xoxb-...
#   SLACK_APP_TOKEN     xapp-...
#   GH_PAT              your fine-grained GitHub PAT (Contents: Read & Write)
#   GITHUB_USERNAME     your GitHub username
#   GITHUB_REPO         your repo name
#   CONFIG_FILE_PATH    email_config.json
#
# ==============================================================================

import os
import json
import base64
import logging
import requests

from slack_bolt                     import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s  %(levelname)-8s  %(message)s",
)
log = logging.getLogger(__name__)

# ── Read env vars ─────────────────────────────────────────────────────────────
SLACK_BOT_TOKEN  = os.environ["SLACK_BOT_TOKEN"]
SLACK_APP_TOKEN  = os.environ["SLACK_APP_TOKEN"]
GITHUB_PAT       = os.environ["GH_PAT"]
GITHUB_USERNAME  = os.environ.get("GITHUB_USERNAME", "")
GITHUB_REPO      = os.environ.get("GITHUB_REPO",     "")
CONFIG_FILE_PATH = os.environ.get("CONFIG_FILE_PATH", "email_config.json")

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
# ── GitHub Helpers ─────────────────────────────────────────────────────────────
# ==============================================================================

def github_read_config() -> tuple[dict, str]:
    """Fetches email_config.json from GitHub. Returns (config_dict, file_sha)."""
    resp = requests.get(GITHUB_API_URL, headers=GITHUB_HEADERS, timeout=10)
    resp.raise_for_status()
    data    = resp.json()
    content = base64.b64decode(data["content"]).decode("utf-8")
    cfg     = json.loads(content)
    sha     = data["sha"]
    return cfg, sha


def github_write_config(cfg: dict, sha: str, commit_message: str) -> bool:
    """Writes updated email_config.json back to GitHub as a commit."""
    cfg_clean   = {k: v for k, v in cfg.items() if not k.startswith("_")}
    new_content = base64.b64encode(
        json.dumps(cfg_clean, indent=2, ensure_ascii=False).encode("utf-8")
    ).decode("utf-8")

    resp = requests.put(
        GITHUB_API_URL,
        headers=GITHUB_HEADERS,
        timeout=10,
        json={
            "message": commit_message,
            "content": new_content,
            "sha":     sha,
        },
    )
    return resp.status_code in (200, 201)


# ==============================================================================
# ── Config Formatter ───────────────────────────────────────────────────────────
# ==============================================================================

def format_config(cfg: dict) -> str:
    counts      = cfg.get("sent_counts", {})
    limit       = cfg.get("emails_per_account_limit", "?")
    senders     = cfg.get("senders", [])

    counts_lines = "\n".join(
        f"    • `{s['email']}` — {counts.get(s['email'], 0)} / {limit} sent"
        for s in senders
    ) or "    (none configured)"

    body_preview = str(cfg.get("email_body", ""))
    body_preview = body_preview[:120].replace("\n", " ")
    if len(cfg.get("email_body", "")) > 120:
        body_preview += "..."

    return (
        f"*📋 Current Email Config*\n"
        f"{'─' * 40}\n"
        f"*Run Mode:*           `{cfg.get('run_mode', '?')}`\n"
        f"*Test Recipients:*    `{', '.join(cfg.get('test_recipients', []))}`\n"
        f"*Subject:*            {cfg.get('email_subject', '?')}\n"
        f"*Body preview:*       _{body_preview}_\n"
        f"*Per-account limit:*  `{limit}` emails\n"
        f"*Delay:*              `{cfg.get('delay_seconds', '?')}` seconds\n"
        f"*Sender Accounts:*\n{counts_lines}\n"
        f"{'─' * 40}\n"
        f"_Edit with:_ `config set <key> <value>`"
    )


# ==============================================================================
# ── Command Router ─────────────────────────────────────────────────────────────
# ==============================================================================

# Slack alias → (json key in email_config.json, type)
SETTABLE_KEYS = {
    "run_mode":           ("run_mode",                 str),
    "delay":              ("delay_seconds",             int),
    "emails_per_account": ("emails_per_account_limit",  int),
    "subject":            ("email_subject",             str),
    "body":               ("email_body",                str),
}


def handle_config_command(text: str, say) -> None:
    parts = text.strip().split(None, 3)

    if len(parts) < 2:
        say(
            "Usage:\n"
            "  `config show`\n"
            "  `config set <key> <value>`\n"
            "  `config reset counts`"
        )
        return

    sub = parts[1].lower()

    # ── config show ───────────────────────────────────────────────────────────
    if sub == "show":
        try:
            cfg, _ = github_read_config()
            say(format_config(cfg))
        except Exception as exc:
            say(f"❌ Could not read config from GitHub: `{exc}`")
        return

    # ── config reset counts ───────────────────────────────────────────────────
    if sub == "reset" and len(parts) >= 3 and parts[2].lower() == "counts":
        try:
            cfg, sha = github_read_config()
            cfg["sent_counts"] = {k: 0 for k in cfg.get("sent_counts", {})}
            ok = github_write_config(cfg, sha, "chore: reset sent_counts via Slack")
            if ok:
                say("✅ All sent counts reset to 0.")
            else:
                say("❌ GitHub write failed. Check GH_PAT has Contents: Write permission.")
        except Exception as exc:
            say(f"❌ Error: `{exc}`")
        return

    # ── config set <key> <value> ──────────────────────────────────────────────
    if sub == "set":
        if len(parts) < 4:
            keys_list = " | ".join(f"`{k}`" for k in SETTABLE_KEYS)
            say(f"Usage: `config set <key> <value>`\nAvailable keys: {keys_list}")
            return

        key_alias = parts[2].lower()
        raw_value = parts[3].strip()

        if key_alias not in SETTABLE_KEYS:
            keys_list = " | ".join(f"`{k}`" for k in SETTABLE_KEYS)
            say(f"❌ Unknown key `{key_alias}`. Available keys: {keys_list}")
            return

        json_key, cast_fn = SETTABLE_KEYS[key_alias]

        try:
            value = cast_fn(raw_value)
        except (ValueError, TypeError):
            say(f"❌ `{key_alias}` expects a {cast_fn.__name__} value. Got: `{raw_value}`")
            return

        if json_key == "run_mode":
            if value.upper() not in ("TEST", "LIVE"):
                say("❌ `run_mode` must be `TEST` or `LIVE`.")
                return
            value = value.upper()

        try:
            cfg, sha = github_read_config()
            old_value = cfg.get(json_key, "N/A")
            cfg[json_key] = value
            ok = github_write_config(
                cfg, sha,
                f"config: set {json_key}={value!r} via Slack"
            )
            if ok:
                say(
                    f"✅ *{key_alias}* updated.\n"
                    f"   `{old_value}` → `{value}`\n"
                    f"   _Committed to GitHub._"
                )
            else:
                say("❌ GitHub write failed. Check GH_PAT has Contents: Write permission.")
        except Exception as exc:
            say(f"❌ Error: `{exc}`")
        return

    # ── Unknown ───────────────────────────────────────────────────────────────
    say(
        "❓ Unknown command. Try:\n"
        "  `config show`\n"
        "  `config set run_mode LIVE`\n"
        "  `config set emails_per_account 12`\n"
        "  `config set delay 60`\n"
        "  `config set subject Your new subject line`\n"
        "  `config reset counts`"
    )


# ==============================================================================
# ── Slack Event Listener ───────────────────────────────────────────────────────
# ==============================================================================

@app.message("config")
def on_config_message(message, say):
    text = message.get("text", "").strip()
    if text.lower().startswith("config"):
        handle_config_command(text, say)


# ==============================================================================
# ── Start ──────────────────────────────────────────────────────────────────────
# ==============================================================================

if __name__ == "__main__":
    log.info("🤖 Slack config bot starting (Socket Mode)...")
    SocketModeHandler(app, SLACK_APP_TOKEN).start()
