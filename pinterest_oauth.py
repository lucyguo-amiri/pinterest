import os
import urllib.parse as urlparse
from flask import Flask, request, redirect
import requests
from dotenv import load_dotenv
import base64
import json
import time

TOKEN_FILE = "pinterest_tokens.json"

load_dotenv()

PINTEREST_CLIENT_ID = os.environ["PINTEREST_CLIENT_ID"]
PINTEREST_CLIENT_SECRET = os.environ["PINTEREST_CLIENT_SECRET"]
PINTEREST_REDIRECT_URI = os.environ["PINTEREST_REDIRECT_URI"]
PINTEREST_SCOPES = os.environ.get("PINTEREST_SCOPES", "ads:read,ads:write")

AUTH_URL = "https://www.pinterest.com/oauth/"
TOKEN_URL = "https://api.pinterest.com/v5/oauth/token"

app = Flask(__name__)


def build_auth_url() -> str:
    params = {
        "client_id": PINTEREST_CLIENT_ID,
        "redirect_uri": PINTEREST_REDIRECT_URI,
        "response_type": "code",
        "scope": PINTEREST_SCOPES,
    }
    return AUTH_URL + "?" + urlparse.urlencode(params)


def _basic_auth_header() -> str:
    """
    Build Pinterest Basic Auth header from client_id:client_secret
    """
    raw = f"{PINTEREST_CLIENT_ID}:{PINTEREST_CLIENT_SECRET}".encode("utf-8")
    b64 = base64.b64encode(raw).decode("utf-8")
    return f"Basic {b64}"


def exchange_code_for_token(code: str) -> dict:
    """
    Exchange the authorization code for the access token + refresh token
    (Pinterest OAuth 2.0)
    """
    data = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": PINTEREST_REDIRECT_URI,
        "client_id": PINTEREST_CLIENT_ID,
        "client_secret": PINTEREST_CLIENT_SECRET,
    }

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
        "Authorization": _basic_auth_header(),
    }

    resp = requests.post(TOKEN_URL, headers=headers, data=data)

    # Always print Pinterest's error JSON when something goes wrong
    print("Token endpoint status:", resp.status_code)
    print("Token endpoint body:", resp.text)

    resp.raise_for_status()
    return resp.json()


def refresh_access_token(refresh_token: str) -> dict:
    """
    Refresh an access token using the refresh_token.
    """
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": PINTEREST_CLIENT_ID,
        "client_secret": PINTEREST_CLIENT_SECRET,
    }

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
        "Authorization": _basic_auth_header(),
    }

    resp = requests.post(TOKEN_URL, headers=headers, data=data)

    print("Refresh endpoint status:", resp.status_code)
    print("Refresh endpoint body:", resp.text)

    resp.raise_for_status()
    return resp.json()


def save_tokens(token_data: dict):
    """Save tokens + timestamp to a local JSON file."""
    token_data = dict(token_data)
    token_data["obtained_at"] = int(time.time())
    with open(TOKEN_FILE, "w") as f:
        json.dump(token_data, f, indent=2)


def load_tokens() -> dict:
    """Load tokens from local JSON file."""
    with open(TOKEN_FILE, "r") as f:
        return json.load(f)


def get_pinterest_token() -> str:
    """
    Return a valid Pinterest access token.

    - Loads tokens from TOKEN_FILE.
    - If near expiry, uses refresh_access_token() to get a new one and saves it.
    - No browser / redirect needed, as long as TOKEN_FILE already exists.
    """
    try:
        tokens = load_tokens()
    except FileNotFoundError:
        raise RuntimeError(
            f"No {TOKEN_FILE} found. You must run the OAuth flow once to create it "
            "(start the Flask app and log in to Pinterest)."
        )

    access_token = tokens.get("access_token")
    refresh_token_val = tokens.get("refresh_token")
    expires_in = tokens.get("expires_in")  # seconds
    obtained_at = tokens.get("obtained_at")

    # If we don't have expiry info, just return what we have
    if not expires_in or not obtained_at:
        return access_token

    now = int(time.time())
    # Refresh 5 minutes before expiry
    if now >= obtained_at + expires_in - 300 and refresh_token_val:
        print("Access token is expired/expiring soon — refreshing...")
        new_tokens = refresh_access_token(refresh_token_val)
        save_tokens(new_tokens)
        access_token = new_tokens.get("access_token")

    return access_token


# Optional: keep these routes for first-time OAuth if you ever need them again.
@app.route("/")
def index():
    """
    Manual OAuth entrypoint (only needed if you want to re-run the login flow).
    Not used in normal 'headless' refresh mode.
    """
    return f"""
    <h1>Pinterest OAuth Login</h1>
    <p>This is only needed if you want to re-authorize the app.</p>
    <a href="{build_auth_url()}">Connect Pinterest</a>
    """


@app.route("/callback")
def callback():
    if "error" in request.args:
        return f"Pinterest error: {request.args['error']}", 400

    code = request.args.get("code")
    if not code:
        return "Missing code", 400

    # Exchange code for an access token
    token_data = exchange_code_for_token(code)

    # Persist token data (access + refresh + obtained_at)
    save_tokens(token_data)

    access_token = token_data.get("access_token")
    refresh_token = token_data.get("refresh_token")
    expires_in = token_data.get("expires_in")

    return f"""
    <h2>SUCCESS — Live Pinterest Token Generated</h2>
    <p><b>Access Token:</b><br><code>{access_token}</code></p>
    <p><b>Refresh Token:</b><br><code>{refresh_token}</code></p>
    <p><b>Expires In:</b> {expires_in} seconds</p>
    <p>Tokens saved to <code>{TOKEN_FILE}</code>.</p>
    <p>You can now call <code>get_pinterest_token()</code> from your other scripts to get a valid access token without any clicks.</p>
    """


if __name__ == "__main__":
    """
    Headless mode:
    - Just ensure you have pinterest_tokens.json already.
    - This will refresh the access token if needed and print it.
    - No redirect, no browser, no clicking.
    """
    try:
        token = get_pinterest_token()
        tokens = load_tokens()

        print("✅ Access token is valid/refreshed.")
        print("Access token:", token)
        print("Refresh token:", tokens.get("refresh_token"))
        print("Expires in (seconds):", tokens.get("expires_in"))
        print("Obtained at (epoch):", tokens.get("obtained_at"))
    except Exception as e:
        print("⚠️ Failed to get Pinterest token:", e)